import asyncio
import logging
import os
import re
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

import aiohttp
from livekit import api as lk_api

logger = logging.getLogger(__name__)


def _resolve_config_api_base() -> Optional[str]:
    base = os.getenv("CONFIG_API_BASE") or os.getenv("API_BASE") or os.getenv("RECORDINGS_API_BASE")
    if not base:
        return None
    return base.rstrip("/")


async def post_internal(path: str, payload: Dict[str, Any]) -> None:
    base = _resolve_config_api_base()
    if not base:
        return
    url = f"{base}{path}"
    headers = {"Content-Type": "application/json"}
    internal_key = os.getenv("INTERNAL_API_KEY", "")
    if internal_key:
        headers["X-Internal-Key"] = internal_key
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    logger.warning("Internal POST %s failed: %s %s", path, resp.status, text[:200])
    except Exception as exc:
        logger.warning("Internal POST %s exception: %s", path, exc)


def _http_livekit_url() -> Optional[str]:
    raw = os.getenv("LIVEKIT_URL", "")
    if not raw:
        return None
    if raw.startswith("wss://"):
        return "https://" + raw[len("wss://") :]
    if raw.startswith("ws://"):
        return "http://" + raw[len("ws://") :]
    return raw


def _sanitize_fragment(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]", "_", value or "")
    return cleaned or "room"


class CallRecorder:
    """Start LiveKit RoomComposite recordings and register metadata with the config API."""

    def __init__(self) -> None:
        self.enabled = os.getenv("LIVEKIT_RECORD_CALLS", "0") == "1"
        self.audio_only = os.getenv("LIVEKIT_RECORDING_AUDIO_ONLY", "1") == "1"
        self.layout = os.getenv("LIVEKIT_RECORDING_LAYOUT", "grid")
        self.prefix = os.getenv("LIVEKIT_RECORDING_OUTPUT_PREFIX", "livekit")
        self.s3_bucket = os.getenv("LIVEKIT_RECORDING_S3_BUCKET", "").strip()
        self.s3_region = os.getenv("LIVEKIT_RECORDING_S3_REGION", "").strip()
        self.s3_endpoint = os.getenv("LIVEKIT_RECORDING_S3_ENDPOINT", "").strip()
        self.s3_access_key = os.getenv("LIVEKIT_RECORDING_S3_ACCESS_KEY", "").strip()
        self.s3_secret_key = os.getenv("LIVEKIT_RECORDING_S3_SECRET_KEY", "").strip()
        self.s3_force_path = os.getenv("LIVEKIT_RECORDING_S3_FORCE_PATH_STYLE", "0") == "1"
        self._lock = asyncio.Lock()
        self._active: Optional[Dict[str, Any]] = None

        if self.enabled:
            missing = [name for name, value in (
                ("LIVEKIT_RECORDING_S3_BUCKET", self.s3_bucket),
                ("LIVEKIT_RECORDING_S3_ACCESS_KEY", self.s3_access_key),
                ("LIVEKIT_RECORDING_S3_SECRET_KEY", self.s3_secret_key),
            ) if not value]
            if missing:
                logger.warning("CallRecorder disabled; missing env variables: %s", ", ".join(missing))
                self.enabled = False

    async def start(
        self,
        *,
        room_name: Optional[str],
        call_id: Optional[str],
        agent_id: Optional[str],
        agent_name: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
        if not room_name:
            logger.warning("CallRecorder start skipped: room name missing")
            return None
        async with self._lock:
            if self._active:
                return self._active
            request = self._build_request(room_name)
            if request is None:
                return None
            lk_url = _http_livekit_url()
            lk_key = os.getenv("LIVEKIT_API_KEY")
            lk_secret = os.getenv("LIVEKIT_API_SECRET")
            if not lk_url or not lk_key or not lk_secret:
                logger.warning("CallRecorder start skipped: LiveKit credentials missing")
                return None
            logger.info("Starting LiveKit RoomComposite recording for room=%s", room_name)
            client: Optional[lk_api.LiveKitAPI] = None
            egress_info = None
            try:
                client = lk_api.LiveKitAPI(url=lk_url, api_key=lk_key, api_secret=lk_secret)
                egress_info = await client.egress.start_room_composite_egress(request)
            except Exception as exc:
                logger.error("CallRecorder start failed: %s", exc)
                return None
            finally:
                if client is not None:
                    try:
                        await client.aclose()
                    except Exception:
                        pass
            egress_id = getattr(egress_info, "egress_id", None)
            filepath = request.file_outputs[0].filepath if request.file_outputs else ""
            record = {
                "egress_id": egress_id,
                "filepath": filepath,
                "bucket": self.s3_bucket,
                "status": "starting",
                "audio_only": self.audio_only,
                "layout": self.layout,
                "started_at": int(time.time()),
                "call_id": call_id,
                "room_name": room_name,
            }
            self._active = record
        await self._notify_backend(
            call_id=call_id,
            room_name=room_name,
            egress_id=egress_id,
            filepath=filepath,
            status="starting",
            agent_id=agent_id,
            agent_name=agent_name,
        )
        return record

    async def stop(
        self,
        *,
        call_id: Optional[str],
        room_name: Optional[str],
        status: str = "stopping",
    ) -> None:
        if not self.enabled:
            return
        async with self._lock:
            active = self._active
            self._active = None
        if not active:
            return
        egress_id = active.get("egress_id")
        if not egress_id:
            return
        lk_url = _http_livekit_url()
        lk_key = os.getenv("LIVEKIT_API_KEY")
        lk_secret = os.getenv("LIVEKIT_API_SECRET")
        if lk_url and lk_key and lk_secret:
            client = None
            try:
                client = lk_api.LiveKitAPI(url=lk_url, api_key=lk_key, api_secret=lk_secret)
                await client.egress.stop_egress(lk_api.StopEgressRequest(egress_id=egress_id))
                status = "stopped"
            except Exception as exc:
                logger.warning("CallRecorder stop failed: %s", exc)
            finally:
                if client is not None:
                    try:
                        await client.aclose()
                    except Exception:
                        pass
        await self._notify_backend(
            call_id=call_id or active.get("call_id"),
            room_name=room_name or active.get("room_name"),
            egress_id=egress_id,
            filepath=active.get("filepath", ""),
            status=status,
            agent_id=None,
            agent_name=None,
        )

    def _build_request(self, room_name: str) -> Optional[lk_api.RoomCompositeEgressRequest]:
        try:
            req = lk_api.RoomCompositeEgressRequest()
            req.room_name = room_name
            if self.layout:
                req.layout = self.layout
            req.audio_only = self.audio_only
            req.video_only = False
            output = req.file_outputs.add()
            output.file_type = lk_api.EncodedFileType.OGG if self.audio_only else lk_api.EncodedFileType.MP4
            filepath = self._build_filepath(room_name)
            output.filepath = filepath
            output.disable_manifest = False
            s3 = output.s3
            s3.bucket = self.s3_bucket
            if self.s3_region:
                s3.region = self.s3_region
            if self.s3_endpoint:
                s3.endpoint = self.s3_endpoint
            s3.access_key = self.s3_access_key
            s3.secret = self.s3_secret_key
            s3.force_path_style = self.s3_force_path
            req.file.CopyFrom(output)
            return req
        except Exception as exc:
            logger.error("Failed to build recording request: %s", exc)
            return None

    def _build_filepath(self, room_name: str) -> str:
        today = datetime.utcnow()
        folder = today.strftime("%Y/%m/%d")
        room_fragment = _sanitize_fragment(room_name)
        ext = "ogg" if self.audio_only else "mp4"
        uniq = uuid.uuid4().hex[:8]
        prefix = self.prefix.strip("/ ")
        return f"{prefix}/{folder}/{room_fragment}-{int(time.time())}-{uniq}.{ext}"

    async def _notify_backend(
        self,
        *,
        call_id: Optional[str],
        room_name: str,
        egress_id: Optional[str],
        filepath: str,
        status: str,
        agent_id: Optional[str],
        agent_name: Optional[str],
    ) -> None:
        payload = {
            "call_id": call_id,
            "room_name": room_name,
            "egress_id": egress_id,
            "filepath": filepath,
            "status": status,
            "storage": {
                "kind": "s3",
                "bucket": self.s3_bucket,
                "region": self.s3_region,
                "endpoint": self.s3_endpoint,
                "force_path_style": self.s3_force_path,
            },
            "options": {
                "audio_only": self.audio_only,
                "layout": self.layout,
            },
            "agent": {
                "id": agent_id,
                "name": agent_name,
            },
            "started_at": int(time.time()),
        }
        # Fire and forget to avoid blocking the agent startup/setup
        asyncio.create_task(post_internal("/internal/livekit/recordings", payload))


class TranscriptUploader:
    """Upload stored transcripts to the config API for later review."""

    async def upload(
        self,
        *,
        call_id: Optional[str],
        room_name: Optional[str],
        entries: Optional[list],
        started_at: Optional[datetime],
        ended_at: Optional[datetime],
        agent_meta: Optional[Dict[str, Any]],
    ) -> None:
        if not call_id or not entries:
            return
        payload = {
            "call_id": call_id,
            "room_name": room_name,
            "entries": entries,
            "agent": agent_meta or {},
            "started_at": started_at.isoformat() if isinstance(started_at, datetime) else None,
            "ended_at": ended_at.isoformat() if isinstance(ended_at, datetime) else None,
        }
        await post_internal("/internal/livekit/transcript", payload)

