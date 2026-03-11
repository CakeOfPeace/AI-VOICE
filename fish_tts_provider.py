from __future__ import annotations

import asyncio
import os
import ssl
from dataclasses import dataclass, replace
import time
import re
import inspect
import uuid
from typing import Literal

from livekit.agents import (
    APIConnectionError,
    APIConnectOptions,
    APITimeoutError,
    tts,
)
from livekit.agents.types import DEFAULT_API_CONNECT_OPTIONS


# We import lazily inside runtime paths to avoid import errors at module import time
# if optional deps are not installed yet.


SAMPLE_RATE = 24000
NUM_CHANNELS = 1


@dataclass
class _Options:
    api_key: str
    model: str
    reference_id: str  # voice id / reference speaker
    response_format: Literal["wav", "mp3", "aac", "flac", "opus", "pcm"]
    latency: Literal["normal", "balanced", "low"]
    temperature: float
    top_p: float
    speed: float  # prosody.speed
    volume_db: float  # prosody.volume
    emotion_prefix: str | None  # control tags to prepend to text
    frame_size_ms: int


class FishSpeechTTS(tts.TTS):
    """LiveKit TTS provider for Fish Audio 'S1' model over websocket API.

    Uses `wss://api.fish.audio/v1/tts/live` and streams audio chunks to LiveKit.
    Supports prepending control tags (emotion/style) to text.
    """

    def __init__(
        self,
        *,
        api_key: str,
        reference_id: str,
        model: str = "s1",
        response_format: Literal["wav", "mp3", "aac", "flac", "opus", "pcm"] = "wav",
        latency: Literal["normal", "balanced", "low"] = "normal",
        temperature: float = 0.7,
        top_p: float = 0.7,
        speed: float = 1.1,
        volume_db: float = 0.3,
        emotion_prefix: str | None = None,
        frame_size_ms: int = 200,
    ) -> None:
        super().__init__(
            capabilities=tts.TTSCapabilities(streaming=True),
            sample_rate=SAMPLE_RATE,
            num_channels=NUM_CHANNELS,
        )

        if not api_key:
            raise ValueError("FishSpeechTTS requires an API key")

        # Basic validation
        if response_format not in {"wav", "mp3", "aac", "flac", "opus", "pcm"}:
            raise ValueError("Unsupported response format for FishSpeechTTS")

        if latency not in {"normal", "balanced", "low"}:
            raise ValueError("latency must be one of: normal | balanced | low")

        self._opts = _Options(
            api_key=api_key,
            model=model,
            reference_id=reference_id,
            response_format=response_format,
            latency=latency,
            temperature=temperature,
            top_p=top_p,
            speed=speed,
            volume_db=volume_db,
            emotion_prefix=emotion_prefix,
            frame_size_ms=frame_size_ms,
        )

        self._prewarm_task: asyncio.Task | None = None

        # Optional telemetry callbacks for latency/observability
        # Each callback receives a dict payload with at least {"utterance_id": str, "ts": float}
        # Additional fields:
        # - on_llm_first_token: {"text_so_far": str}
        # - on_llm_text_final: {"text": str}
        # - on_tts_first_audio: {}
        # - on_tts_finished: {"tts_started_ts": float}
        self._on_llm_first_token = None
        self._on_llm_text_final = None
        self._on_tts_first_audio = None
        self._on_tts_finished = None

    def set_telemetry_callbacks(
        self,
        *,
        on_llm_first_token=None,
        on_llm_text_final=None,
        on_tts_first_audio=None,
        on_tts_finished=None,
    ) -> None:
        self._on_llm_first_token = on_llm_first_token
        self._on_llm_text_final = on_llm_text_final
        self._on_tts_first_audio = on_tts_first_audio
        self._on_tts_finished = on_tts_finished

    # --- Tag handling helpers (Fish Audio control tags) ---
    # Reference: https://docs.fish.audio/emotion-control/tts-emotion-and-control-tags-user-guide
    _EMOTION_TAGS = {
        "angry", "sad", "disdainful", "excited", "surprised", "satisfied", "unhappy",
        "anxious", "hysterical", "delighted", "scared", "worried", "indifferent", "upset",
        "impatient", "nervous", "guilty", "scornful", "frustrated", "depressed", "panicked",
        "furious", "empathetic", "embarrassed", "reluctant", "disgusted", "keen", "moved",
        "proud", "relaxed", "grateful", "confident", "interested", "curious", "confused",
        "joyful", "disapproving", "negative", "denying", "astonished", "serious", "sarcastic",
        "conciliative", "comforting", "sincere", "sneering", "hesitating", "yielding", "painful",
        "awkward", "amused"
    }

    _TONE_TAGS = {
        "in a hurry tone", "shouting", "screaming", "whispering", "soft tone"
    }

    _SPECIAL_TAGS = {
        "laughing", "chuckling", "sobbing", "crying loudly", "sighing", "panting", "groaning",
        "crowd laughing", "background laughter", "audience laughing"
    }

    # Tags we never want to apply (and never speak). Lowercase compare.
    _BLOCKLIST_TAGS = {"soft tone"}

    @staticmethod
    def _split_leading_tags(text: str) -> tuple[list[str], str]:
        # Collect a contiguous run of leading tags like "(sincere)(soft tone)Text..."
        tags: list[str] = []
        i = 0
        while i < len(text) and text[i] == "(":
            close = text.find(")", i + 1)
            if close == -1:
                break
            tag = text[i + 1:close].strip()
            tags.append(tag)
            i = close + 1
        return tags, text[i:]

    @classmethod
    def _strip_mid_emotion_tags(cls, text: str) -> str:
        # remove any emotion tags that are NOT at the very beginning
        # Keep tone and special tags anywhere
        import re
        # pattern for any emotion tag
        emotion_pat = r"\\((?:" + "|".join(map(re.escape, sorted(cls._EMOTION_TAGS))) + r")\\)"
        # Replace all emotion tags globally
        return re.sub(emotion_pat, "", text)

    @classmethod
    def _dedupe_and_join_tags(cls, tags: list[str]) -> str:
        seen = set()
        out: list[str] = []
        for t in tags:
            if t not in seen:
                seen.add(t)
                out.append(f"({t})")
        return "".join(out)

    def _sanitize_and_prefix(self, text: str) -> str:
        # Extract leading tags from the text and keep only valid ones
        leading_tags, remainder = self._split_leading_tags(text)
        valid_leading = [
            t for t in leading_tags
            if (t in self._EMOTION_TAGS or t in self._TONE_TAGS or t in self._SPECIAL_TAGS) and (t.lower() not in self._BLOCKLIST_TAGS)
        ]

        # Build the final prefix: profile emotion_prefix first then any valid leading tags from input (deduped)
        prefix_tags: list[str] = []
        if self._opts.emotion_prefix:
            p_tags, _ = self._split_leading_tags(self._opts.emotion_prefix)
            prefix_tags.extend([pt for pt in p_tags if pt.lower() not in self._BLOCKLIST_TAGS])
        prefix_tags.extend(valid_leading)

        # For the remainder of the text, strip any mid-sentence EMOTION tags so they aren't read aloud.
        cleaned_remainder = self._strip_mid_emotion_tags(remainder)
        cleaned_remainder = self._strip_blocklisted_tags(cleaned_remainder)

        # Join
        final_prefix = self._dedupe_and_join_tags(prefix_tags)
        return f"{final_prefix}{cleaned_remainder}"

    @classmethod
    def _strip_blocklisted_tags(cls, text: str) -> str:
        if not text:
            return text
        try:
            # Build a pattern like \((soft tone)\)
            choices = "|".join(map(re.escape, sorted(cls._BLOCKLIST_TAGS)))
            if not choices:
                return text
            pat = r"\((?:" + choices + r")\)"
            return re.sub(pat, "", text, flags=re.IGNORECASE)
        except Exception:
            return text

    def update_options(
        self,
        *,
        speed: float | None = None,
        volume_db: float | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        emotion_prefix: str | None = None,
    ) -> None:
        new_opts = replace(self._opts)
        if speed is not None:
            new_opts.speed = speed
        if volume_db is not None:
            new_opts.volume_db = volume_db
        if temperature is not None:
            new_opts.temperature = temperature
        if top_p is not None:
            new_opts.top_p = top_p
        if emotion_prefix is not None:
            new_opts.emotion_prefix = emotion_prefix
        self._opts = new_opts

    # Convenience controls for runtime adjustments and parity with agent helpers
    def set_volume(self, volume_db: float) -> None:
        self.update_options(volume_db=volume_db)

    def get_volume(self) -> float:
        return float(self._opts.volume_db)

    def set_speed(self, speed: float) -> None:
        self.update_options(speed=speed)

    def get_speed(self) -> float:
        return float(self._opts.speed)

    def set_emotion_prefix(self, emotion_prefix: str | None) -> None:
        self.update_options(emotion_prefix=emotion_prefix)

    def get_emotion_prefix(self) -> str | None:
        return self._opts.emotion_prefix

    def synthesize(
        self, text: str, *, conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS
    ) -> tts.ChunkedStream:
        return _FishChunkedStream(tts=self, input_text=text, conn_options=conn_options)

    def stream(
        self, *, conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS
    ) -> tts.SynthesizeStream:
        return _FishSynthesizeStream(tts=self, conn_options=conn_options)

    async def synthesize_to_file(self, text: str, file_path: str) -> None:
        """Synthesize the given text and write encoded audio bytes to file_path.

        Uses the same websocket API but writes received encoded frames directly,
        preserving the configured response_format (e.g., opus/mp3/wav).
        """
        try:
            import ormsgpack  # type: ignore
            import websockets  # type: ignore
        except Exception as e:
            raise APIConnectionError("FishSpeechTTS requires 'ormsgpack' and 'websockets' packages") from e

        # Prepare prefixed text with emotion/tone tags (same sanitization as stream)
        full_text = self._sanitize_and_prefix(text)

        uri = "wss://api.fish.audio/v1/tts/live"
        headers = {"Authorization": f"Bearer {self._opts.api_key}"}

        import ssl as _ssl
        ssl_ctx = _ssl.create_default_context()

        # Open file for binary write and concatenate chunks
        import asyncio as _asyncio
        async with _asyncio.timeout(DEFAULT_API_CONNECT_OPTIONS.timeout or 10.0):
            connect_kwargs: dict = {"ssl": ssl_ctx}
            try:
                import inspect as _inspect
                sig = _inspect.signature(websockets.connect)  # type: ignore
                if "additional_headers" in sig.parameters:
                    connect_kwargs["additional_headers"] = headers
                elif "extra_headers" in sig.parameters:
                    connect_kwargs["extra_headers"] = headers
                if "open_timeout" in sig.parameters:
                    connect_kwargs["open_timeout"] = DEFAULT_API_CONNECT_OPTIONS.timeout or 10.0
            except Exception:
                connect_kwargs["extra_headers"] = headers

            async with websockets.connect(uri, **connect_kwargs) as websocket:  # type: ignore
                import aiofiles  # type: ignore
                # Start request
                start_message = {
                    "event": "start",
                    "request": {
                        "text": "",
                        "latency": self._opts.latency,
                        "format": self._opts.response_format,
                        "temperature": float(self._opts.temperature),
                        "top_p": float(self._opts.top_p),
                        "reference_id": self._opts.reference_id,
                        "prosody": {"speed": float(self._opts.speed), "volume": float(self._opts.volume_db)},
                        "model": self._opts.model,
                    },
                    "debug": False,
                }
                await websocket.send(ormsgpack.packb(start_message))
                await websocket.send(ormsgpack.packb({"event": "text", "text": full_text}))
                await websocket.send(ormsgpack.packb({"event": "stop"}))

                async with aiofiles.open(file_path, mode="wb") as f:
                    while True:
                        resp_bytes = await websocket.recv()
                        try:
                            data = ormsgpack.unpackb(resp_bytes)
                        except Exception:
                            # Some servers may send raw bytes directly
                            if isinstance(resp_bytes, (bytes, bytearray)):
                                await f.write(bytes(resp_bytes))
                                continue
                            raise

                        event = data.get("event")
                        if event == "audio":
                            audio_chunk = data.get("audio")
                            if audio_chunk:
                                await f.write(audio_chunk)
                        elif event == "finish":
                            break
                        elif event == "error":
                            raise APIConnectionError(str(data))
                        else:
                            # ignore logs/others
                            pass

    def prewarm(self) -> None:
        async def _prewarm() -> None:
            # Attempt a quick auth handshake to warm up TLS/WS in the pool
            try:
                import websockets  # type: ignore
                uri = "wss://api.fish.audio/v1/tts/live"
                headers = {"Authorization": f"Bearer {self._opts.api_key}"}
                ssl_ctx = ssl.create_default_context()
                connect_kwargs: dict = {"ssl": ssl_ctx}
                try:
                    sig = inspect.signature(websockets.connect)  # type: ignore
                    if "additional_headers" in sig.parameters:
                        connect_kwargs["additional_headers"] = headers
                    elif "extra_headers" in sig.parameters:
                        connect_kwargs["extra_headers"] = headers
                except Exception:
                    # Best effort fallback for unknown version
                    connect_kwargs["extra_headers"] = headers

                async with websockets.connect(uri, **connect_kwargs) as ws:  # type: ignore
                    await ws.close()
            except Exception:
                pass

        self._prewarm_task = asyncio.create_task(_prewarm())

    async def aclose(self) -> None:
        if self._prewarm_task:
            try:
                await asyncio.wait_for(self._prewarm_task, timeout=0.1)
            except Exception:
                pass


class _FishChunkedStream(tts.ChunkedStream):
    def __init__(self, *, tts: FishSpeechTTS, input_text: str, conn_options: APIConnectOptions) -> None:
        super().__init__(tts=tts, input_text=input_text, conn_options=conn_options)
        self._tts: FishSpeechTTS = tts

    async def _run(self, output_emitter: tts.AudioEmitter) -> None:
        try:
            import ormsgpack  # type: ignore
            import websockets  # type: ignore
        except Exception as e:
            raise APIConnectionError("FishSpeechTTS requires 'ormsgpack' and 'websockets' packages") from e

        uri = "wss://api.fish.audio/v1/tts/live"
        headers = {"Authorization": f"Bearer {self._tts._opts.api_key}"}

        # Initialize emitter before we start pushing bytes
        output_emitter.initialize(
            request_id=uuid.uuid4().hex,
            sample_rate=SAMPLE_RATE,
            num_channels=NUM_CHANNELS,
            mime_type=f"audio/{self._tts._opts.response_format}",
            frame_size_ms=self._tts._opts.frame_size_ms,
            stream=False,  # we push a single segment per synthesize()
        )

        # Prepare text: enforce emotion tags only at the beginning and prepend profile prefix
        full_text = self._tts._sanitize_and_prefix(self.input_text)

        # Establish connection and stream audio
        try:
            ssl_ctx = ssl.create_default_context()
            timeout_s = self._conn_options.timeout or 10.0
            async with asyncio.timeout(timeout_s):
                connect_kwargs: dict = {"ssl": ssl_ctx}
                try:
                    sig = inspect.signature(websockets.connect)  # type: ignore
                    if "additional_headers" in sig.parameters:
                        connect_kwargs["additional_headers"] = headers
                    elif "extra_headers" in sig.parameters:
                        connect_kwargs["extra_headers"] = headers
                    if "open_timeout" in sig.parameters:
                        connect_kwargs["open_timeout"] = timeout_s
                except Exception:
                    # Fallbacks
                    connect_kwargs["extra_headers"] = headers

                async with websockets.connect(uri, **connect_kwargs) as websocket:  # type: ignore
                    # Start message using Fish Audio's live TTS schema
                    start_message = {
                        "event": "start",
                        "request": {
                            # Leave text empty here; we send it in a separate event
                            "text": "",
                            "latency": self._tts._opts.latency,
                            "format": self._tts._opts.response_format,
                            "temperature": float(self._tts._opts.temperature),
                            "top_p": float(self._tts._opts.top_p),
                            "reference_id": self._tts._opts.reference_id,
                            "prosody": {
                                "speed": float(self._tts._opts.speed),
                                "volume": float(self._tts._opts.volume_db),
                            },
                            # Some deployments expect the model field
                            "model": self._tts._opts.model,
                        },
                        "debug": False,
                    }

                    await websocket.send(ormsgpack.packb(start_message))

                    # Send text (can include emotion/control tags inline)
                    await websocket.send(ormsgpack.packb({"event": "text", "text": full_text}))

                    # Signal stop to begin audio flushing
                    await websocket.send(ormsgpack.packb({"event": "stop"}))

                    # Receive audio frames and forward to LiveKit
                    while True:
                        try:
                            resp_bytes = await asyncio.wait_for(websocket.recv(), timeout=timeout_s)
                        except asyncio.TimeoutError as e:
                            raise APITimeoutError() from e

                        try:
                            data = ormsgpack.unpackb(resp_bytes)
                        except Exception:
                            # Some servers may send raw audio frames directly
                            # If so, push bytes through and continue
                            if isinstance(resp_bytes, (bytes, bytearray)):
                                output_emitter.push(bytes(resp_bytes))
                                continue
                            raise

                        event = data.get("event")
                        if event == "audio":
                            audio_chunk = data.get("audio")
                            if audio_chunk:
                                # Server typically sends encoded frames matching the requested format
                                output_emitter.push(audio_chunk)
                        elif event == "finish":
                            break
                        elif event == "error":
                            raise APIConnectionError(str(data))
                        else:
                            # ignore logs and other events
                            pass

            # Flush the final frame to emit is_final
            output_emitter.flush()

        except APITimeoutError:
            raise
        except Exception as e:
            # Wrap any other exception as APIConnectionError for LiveKit to handle/retry
            raise APIConnectionError() from e


class _FishSynthesizeStream(tts.SynthesizeStream):
    def __init__(self, *, tts: FishSpeechTTS, conn_options: APIConnectOptions) -> None:
        super().__init__(tts=tts, conn_options=conn_options)
        self._tts: FishSpeechTTS = tts

    async def _run(self, output_emitter: tts.AudioEmitter) -> None:
        try:
            import ormsgpack  # type: ignore
            import websockets  # type: ignore
        except Exception as e:
            raise APIConnectionError("FishSpeechTTS requires 'ormsgpack' and 'websockets' packages") from e

        uri = "wss://api.fish.audio/v1/tts/live"
        headers = {"Authorization": f"Bearer {self._tts._opts.api_key}"}

        # Initialize for streaming segments
        output_emitter.initialize(
            request_id=uuid.uuid4().hex,
            sample_rate=SAMPLE_RATE,
            num_channels=NUM_CHANNELS,
            mime_type=f"audio/{self._tts._opts.response_format}",
            frame_size_ms=self._tts._opts.frame_size_ms,
            stream=True,
        )

        ssl_ctx = ssl.create_default_context()
        timeout_s = self._conn_options.timeout or 10.0

        async with asyncio.timeout(timeout_s):
            # Prepare connect kwargs supporting different websockets versions
            import inspect as _inspect

            connect_kwargs: dict = {"ssl": ssl_ctx}
            try:
                sig = _inspect.signature(websockets.connect)  # type: ignore
                if "additional_headers" in sig.parameters:
                    connect_kwargs["additional_headers"] = headers
                elif "extra_headers" in sig.parameters:
                    connect_kwargs["extra_headers"] = headers
                if "open_timeout" in sig.parameters:
                    connect_kwargs["open_timeout"] = timeout_s
            except Exception:
                connect_kwargs["extra_headers"] = headers

            # Keepalive pings to prevent idle disconnects during long turns
            connect_kwargs.setdefault("ping_interval", 20)
            connect_kwargs.setdefault("ping_timeout", 20)

            async with websockets.connect(uri, **connect_kwargs) as websocket:  # type: ignore
                # Send start request
                start_message = {
                    "event": "start",
                    "request": {
                        "text": "",
                        "latency": self._tts._opts.latency,
                        "format": self._tts._opts.response_format,
                        "temperature": float(self._tts._opts.temperature),
                        "top_p": float(self._tts._opts.top_p),
                        "reference_id": self._tts._opts.reference_id,
                        "prosody": {
                            "speed": float(self._tts._opts.speed),
                            "volume": float(self._tts._opts.volume_db),
                        },
                        "model": self._tts._opts.model,
                    },
                    "debug": False,
                }
                await websocket.send(ormsgpack.packb(start_message))

                # Start a segment
                segment_id = uuid.uuid4().hex[:8]
                output_emitter.start_segment(segment_id=segment_id)

                finished_event = asyncio.Event()
                sent_stop = asyncio.Event()
                first_token_sent = False
                collected_text: list[str] = []
                llm_first_token_ts: float | None = None

                async def writer_task() -> None:
                    nonlocal first_token_sent
                    nonlocal llm_first_token_ts
                    # Buffer tokens to avoid emitting incomplete tag sequences like "(soft tone"
                    pending: list[str] = []
                    # Queue of tag strings to prepend to the next plain-text chunk
                    tag_queue: list[str] = []
                    def _ends_with_unclosed_tag(text: str) -> bool:
                        # Detect if there is an opening parenthesis with no closing parenthesis
                        # at the end of the accumulated buffer (common with streamed tokens).
                        # Example: "(soft tone" → True, "(soft tone)" → False
                        return re.search(r"\([^)]*$", text) is not None
                    def _extract_trailing_complete_tags(text: str) -> tuple[str, list[str]]:
                        # Repeatedly peel off complete tags at the end of the string, collecting valid ones
                        tags: list[str] = []
                        cur = text
                        while True:
                            m = re.search(r"\(([^()]+)\)\s*$", cur)
                            if not m:
                                break
                            candidate = m.group(1).strip()
                            if candidate in self._EMOTION_TAGS or candidate in self._TONE_TAGS or candidate in self._SPECIAL_TAGS:
                                tags.append(candidate)
                                cur = cur[:m.start()].rstrip()
                                continue
                            # Unknown tag-like content at end → stop stripping; leave as-is
                            break
                        # Preserve original order (left-to-right)
                        tags.reverse()
                        return cur, tags
                    try:
                        # read tokens/flushes from input channel
                        async for item in self._input_ch:
                            if isinstance(item, self._FlushSentinel):
                                # mark end of segment
                                # Before stopping, flush any pending buffered text
                                if pending:
                                    buf = "".join(pending)
                                    pending.clear()
                                    if not first_token_sent:
                                        buf = self._tts._sanitize_and_prefix(buf)
                                        first_token_sent = True
                                        llm_first_token_ts = llm_first_token_ts or time.perf_counter()
                                        try:
                                            if self._tts._on_llm_first_token is not None:
                                                self._tts._on_llm_first_token({
                                                    "utterance_id": segment_id,
                                                    "ts": llm_first_token_ts,
                                                    "text_so_far": buf[:80],
                                                })
                                        except Exception:
                                            pass
                                    else:
                                        # Remove mid-sentence EMOTION tags; allow tone/special tags
                                        buf = self._tts._strip_mid_emotion_tags(buf)
                                    await websocket.send(ormsgpack.packb({"event": "text", "text": buf}))
                                    collected_text.append(buf)

                                await websocket.send(ormsgpack.packb({"event": "stop"}))
                                sent_stop.set()
                                # Notify final LLM text available
                                try:
                                    if self._tts._on_llm_text_final is not None:
                                        self._tts._on_llm_text_final({
                                            "utterance_id": segment_id,
                                            "ts": time.perf_counter(),
                                            "text": "".join(collected_text),
                                        })
                                except Exception:
                                    pass
                                # wait for finish in reader
                                return
                            else:
                                token = item
                                if not token:
                                    continue
                                pending.append(token)
                                combined = "".join(pending)
                                if _ends_with_unclosed_tag(combined):
                                    # Wait for the rest of the tag in future tokens
                                    continue

                                # We have a safe chunk without trailing open tag:
                                # 1) peel and queue any valid trailing tags
                                trimmed, trailing_tags = _extract_trailing_complete_tags(combined)
                                tag_queue.extend(trailing_tags)
                                if not trimmed.strip():
                                    # Only tags so far; wait for actual text before emitting
                                    pending.clear()
                                    continue
                                # 2) emit the chunk, prefixing any queued tags
                                pending.clear()
                                prefix = self._dedupe_and_join_tags(tag_queue) if tag_queue else ""
                                tag_queue.clear()
                                out_text = f"{prefix}{trimmed}"
                                if not first_token_sent:
                                    out_text = self._tts._sanitize_and_prefix(out_text)
                                    first_token_sent = True
                                    llm_first_token_ts = time.perf_counter()
                                    # Emit first token telemetry early
                                    try:
                                        if self._tts._on_llm_first_token is not None:
                                            self._tts._on_llm_first_token({
                                                "utterance_id": segment_id,
                                                "ts": llm_first_token_ts,
                                                "text_so_far": out_text[:80],
                                            })
                                    except Exception:
                                        pass
                                else:
                                    # For subsequent chunks, strip mid-sentence EMOTION tags only
                                    out_text = self._tts._strip_mid_emotion_tags(out_text)
                                await websocket.send(ormsgpack.packb({"event": "text", "text": out_text}))
                                collected_text.append(out_text)
                    finally:
                        # if input ends without explicit flush, send stop
                        if not sent_stop.is_set():
                            try:
                                await websocket.send(ormsgpack.packb({"event": "stop"}))
                            except Exception:
                                pass

                async def reader_task() -> None:
                    tts_first_audio_ts: float | None = None
                    try:
                        while True:
                            try:
                                resp_bytes = await websocket.recv()
                            except Exception as ws_exc:
                                # Treat normal closure as expected end of segment
                                import websockets as _ws  # type: ignore
                                if isinstance(ws_exc, _ws.exceptions.ConnectionClosedOK):
                                    return
                                raise
                            try:
                                data = ormsgpack.unpackb(resp_bytes)
                            except Exception:
                                if isinstance(resp_bytes, (bytes, bytearray)):
                                    output_emitter.push(bytes(resp_bytes))
                                    continue
                                raise

                            event = data.get("event")
                            if event == "audio":
                                audio_chunk = data.get("audio")
                                if audio_chunk:
                                    output_emitter.push(audio_chunk)
                                    if tts_first_audio_ts is None:
                                        tts_first_audio_ts = time.perf_counter()
                                        try:
                                            if self._tts._on_tts_first_audio is not None:
                                                self._tts._on_tts_first_audio({
                                                    "utterance_id": segment_id,
                                                    "ts": tts_first_audio_ts,
                                                })
                                        except Exception:
                                            pass
                            elif event == "finish":
                                finished_event.set()
                                # close out the LiveKit segment
                                output_emitter.end_segment()
                                # telemetry: finished
                                try:
                                    if self._tts._on_tts_finished is not None:
                                        self._tts._on_tts_finished({
                                            "utterance_id": segment_id,
                                            "ts": time.perf_counter(),
                                            "tts_started_ts": tts_first_audio_ts,
                                        })
                                except Exception:
                                    pass
                                return
                            else:
                                # ignore logs
                                pass
                    finally:
                        finished_event.set()

                wt = asyncio.create_task(writer_task())
                rt = asyncio.create_task(reader_task())

                # Await both and suppress unhandled task exceptions
                done, _ = await asyncio.wait({wt, rt}, return_when=asyncio.ALL_COMPLETED)
                for t in done:
                    try:
                        _ = t.result()
                    except Exception:
                        # Errors are already handled by APIConnectionError wrapping
                        pass

        # End input to finalize emitter
        output_emitter.end_input()


