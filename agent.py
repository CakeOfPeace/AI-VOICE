
"""
LiveKit Hospital Receptionist Agent with Fish Audio emotion/tone tags
"""

import asyncio
import json
import logging
import os
import sys
import signal
import tempfile
from datetime import datetime
import uuid
from typing import Optional, Any, Dict, Set
from pathlib import Path
from dataclasses import asdict, is_dataclass
import re
import subprocess
from livekit.plugins.openai import realtime
import time # Added for time.time()
import traceback # Added for traceback.print_exc()

from dotenv import load_dotenv
from storage import get_storage_backend

# LiveKit imports
from livekit import agents, rtc, api
from livekit.agents import Agent, AgentSession, RoomInputOptions, RoomOutputOptions, BackgroundAudioPlayer, AudioConfig, BuiltinAudioClip
from livekit.agents import metrics, MetricsCollectedEvent, RunContext, function_tool, get_job_context
from livekit.agents import llm as lk_llm
from livekit.plugins import openai, silero
try:
    from livekit.plugins import bey as bey_plugin
except ImportError:
    bey_plugin = None
from livekit.agents.worker import RunningJobInfo, JobAcceptArguments
# Note: In livekit-agents 1.3+, Worker is now agents.AgentServer (use agents.WorkerOptions for opts)
from livekit.agents.types import APIConnectOptions as LKAPIConnectOptions
from livekit.agents.voice.agent_session import SessionConnectOptions as LKSessionConnectOptions
from fish_tts_provider import FishSpeechTTS
from call_artifacts import CallRecorder, TranscriptUploader, post_internal
from transcription_manager import TranscriptionManager, TranscriptSegment
from tools import build_agent_tools, create_http_callback_tool
from workflow_engine import load_workflow_tools
from memory import get_memory_manager

# Multi-tenant credential loading
try:
    from providers.loader import load_pipeline_credentials, get_legacy_credentials
    from providers.factory import create_stt, create_llm, create_tts, create_realtime_model
    PROVIDERS_AVAILABLE = True
except ImportError:
    PROVIDERS_AVAILABLE = False
    logger.warning("Providers module not available, using legacy credential handling")

# Setup logging (trim noisy third-party logs)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
logger = logging.getLogger(__name__)
# Surface LiveKit agent lifecycle events while keeping third-party noise down
logging.getLogger("livekit").setLevel(logging.INFO)
logging.getLogger("livekit.agents").setLevel(logging.INFO)
logging.getLogger("livekit.agents.worker").setLevel(logging.DEBUG)
# Quiet noisy third-party libraries
try:
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("websockets.client").setLevel(logging.WARNING)
except Exception:
    pass
logging.getLogger("asyncio").setLevel(logging.ERROR)

# Load environment variables
load_dotenv()

_AGENTS_CACHE: dict | None = None


# -------------------- Multi-number Supervisor --------------------

def _read_numbers_config() -> dict:
    try:
        import requests
        api_base = os.getenv("CONFIG_API_BASE", os.getenv("API_BASE", "http://localhost:5057"))
        r = requests.get(f"{api_base}/numbers", timeout=3)
        if r.ok:
            return r.json() or {"routes": {}}
    except Exception:
        pass
    try:
        agents_dir = os.getenv("AGENTS_DIR", str((Path(__file__).parent / "agents").resolve()))
        p = Path(agents_dir) / "numbers.json"
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"routes": {}}


def _load_agents_store() -> dict:
    """Load agents list from configured storage backend (Postgres or JSON)."""
    global _AGENTS_CACHE
    if _AGENTS_CACHE is not None:
        return _AGENTS_CACHE

    agents_dir = Path(os.getenv("AGENTS_DIR", str((Path(__file__).parent / "agents").resolve())))
    data: dict | None = None

    backend_name = os.getenv("STORAGE_BACKEND", "json").strip().lower()
    if backend_name == "postgres":
        try:
            backend = get_storage_backend(agents_dir)
            data = backend.load_agents()
        except Exception as exc:
            logger.warning(f"Failed to load agents from Postgres backend: {exc}")

    if not data or not data.get("agents"):
        agents_file = agents_dir / "agents.json"
        if agents_file.exists():
            try:
                data = json.loads(agents_file.read_text(encoding="utf-8"))
            except Exception as exc:
                logger.warning(f"Failed to load agents.json: {exc}")
                data = {"agents": []}
        else:
            data = {"agents": []}

    _AGENTS_CACHE = data or {"agents": []}
    return _AGENTS_CACHE


def _extract_numbers_to_run(config: dict) -> set[str]:
    routes = (config or {}).get("routes", {})
    # Only run workers for numbers that are mapped to an agent_id (non-empty)
    numbers = {num for num, meta in routes.items() if isinstance(meta, dict) and (meta.get("agent_id") or "" ) != ""}
    return numbers


def _spawn_worker_for_number(number: str, extra_env: dict | None = None) -> subprocess.Popen:
    env = os.environ.copy()
    # Ensure the worker registers as this number without relying on .env files
    env["AGENT_NAME"] = number
    env.setdefault("DIALED_NUMBER", number)
    env.setdefault("AGENT_PHONE_NUMBER", number)
    # Signal child mode via environment instead of CLI arg (avoids Click/Typer argv parsing issues)
    env["RUN_AS_WORKER"] = "1"
    if extra_env:
        env.update(extra_env)
    # Reinvoke this module to start a dedicated worker process
    return subprocess.Popen([sys.executable, __file__], env=env)


def run_supervisor_forever(poll_interval_seconds: float = 2.0) -> None:
    logger.info("Starting multi-number supervisor")
    children: dict[str, subprocess.Popen] = {}
    try:
        while True:
            cfg = _read_numbers_config()
            desired = _extract_numbers_to_run(cfg)

            # Start new workers for newly added numbers
            for number in desired - set(children.keys()):
                try:
                    children[number] = _spawn_worker_for_number(number)
                    logger.info(f"Spawned worker for number {number}")
                except Exception as e:
                    logger.error(f"Failed to spawn worker for {number}: {e}")

            # Stop workers for numbers removed from config
            for number in list(children.keys() - desired):
                proc = children.pop(number, None)
                if proc is not None:
                    try:
                        proc.terminate()
                        logger.info(f"Terminated worker for number {number}")
                    except Exception:
                        pass

            # Reap exited children and restart if still desired
            for number, proc in list(children.items()):
                ret = proc.poll()
                if ret is not None:
                    logger.warning(f"Worker for {number} exited with code {ret}; restarting")
                    try:
                        children[number] = _spawn_worker_for_number(number)
                    except Exception as e:
                        logger.error(f"Failed to respawn worker for {number}: {e}")

            time.sleep(poll_interval_seconds)
    except KeyboardInterrupt:
        logger.info("Supervisor interrupted; shutting down workers")
    finally:
        for number, proc in list(children.items()):
            try:
                proc.terminate()
            except Exception:
                pass

def make_json_serializable(obj: Any) -> Any:
    """Convert complex objects to JSON serializable format"""
    if obj is None:
        return None
    
    # Handle basic types
    if isinstance(obj, (str, int, float, bool)):
        return obj
    
    # Handle datetime objects
    if isinstance(obj, datetime):
        return obj.isoformat()
    
    # Handle dataclasses
    if is_dataclass(obj) and not isinstance(obj, type):
        try:
            return asdict(obj)
        except Exception:
            # If asdict fails, try to convert manually
            return f"<dataclass: {obj.__class__.__name__}>"
    
    # Handle Pydantic models
    if not isinstance(obj, type) and hasattr(obj, 'model_dump'):
        try:
            return obj.model_dump()
        except Exception:
            # If model_dump fails, try dict()
            if hasattr(obj, 'dict'):
                try:
                    return obj.dict()
                except Exception:
                    return f"<pydantic: {obj.__class__.__name__}>"
    
    if not isinstance(obj, type) and hasattr(obj, 'dict'):
        try:
            return obj.dict()
        except Exception:
            return f"<object: {obj.__class__.__name__}>"
    
    # Handle lists and tuples
    if isinstance(obj, (list, tuple)):
        try:
            return [make_json_serializable(item) for item in obj]
        except Exception:
            return f"<{type(obj).__name__}: length={len(obj)}>"
    
    # Handle dictionaries
    if isinstance(obj, dict):
        try:
            result = {}
            for key, value in obj.items():
                # Ensure key is string
                str_key = str(key) if not isinstance(key, str) else key
                try:
                    result[str_key] = make_json_serializable(value)
                except Exception:
                    result[str_key] = f"<serialization_error: {type(value).__name__}>"
            return result
        except Exception:
            return f"<dict: {len(obj)} items>"
    
    # Handle specific LiveKit objects
    if not isinstance(obj, type) and hasattr(obj, '__dict__'):
        try:
            result = {}
            for key, value in obj.__dict__.items():
                if not key.startswith('_'):  # Skip private attributes
                    try:
                        result[key] = make_json_serializable(value)
                    except Exception as e:
                        result[key] = f"<error: {type(value).__name__}>"
            return result
        except Exception:
            return f"<object: {obj.__class__.__name__}>"
    
    # Handle objects with specific properties we care about
    if not isinstance(obj, type) and hasattr(obj, '__class__'):
        try:
            class_name = obj.__class__.__name__
            if 'Usage' in class_name:
                # Handle Usage objects
                result = {'type': class_name}
                for attr in ['tokens', 'characters', 'duration', 'audio_duration', 'count']:
                    if hasattr(obj, attr):
                        try:
                            result[attr] = getattr(obj, attr)
                        except Exception:
                            result[attr] = f"<error_getting_{attr}>"
                return result
        except Exception:
            pass
    
    # Last resort: convert to string representation
    try:
        str_repr = str(obj)
        # Limit string length to prevent huge serializations
        if len(str_repr) > 1000:
            return f"<{type(obj).__name__}: {str_repr[:100]}...>"
        return str_repr
    except Exception:
        return f"<{type(obj).__name__}: serialization_failed>"


# -------------------- Agent Tools --------------------
# Tools are now managed via the tools/ module
# See tools/__init__.py for EndCallTool, CallControlToolset, etc.


class VoiceAssistant(Agent):
    """Advanced hospital receptionist agent for Ein al Khaleej Hospital"""
    
    def __init__(self, custom_instructions: Optional[str] = None,
                 follow_up_messages: Optional[list[str]] = None,
                 no_reply_timeout: Optional[float] = None,
                 greeting_text: Optional[str] = None,
                 greeting_allow_interruptions: Optional[bool] = None,
                 localized_greetings: Optional[Dict[str, str]] = None,
                 localized_followups: Optional[Dict[str, list[str]]] = None,
                 language_policy: Optional[Dict[str, Any]] = None,
                 realtime_mode: bool = False):
        super().__init__(
            instructions=(custom_instructions or ""),
            # Tools are built externally via build_agent_tools() and attached to session
        )
        self._agent_session = None
        self._room = None
        # Volume post-processing removed; control volume at TTS generation via FishSpeechTTS
        
        # Track if we're in realtime mode (for greeting handling)
        self._realtime_mode = realtime_mode
        
        # Greeting configuration: text provided dynamically via config
        self._greeting_text = greeting_text
        # In avatar mode with realtime, allow_interruptions must be True (server-side turn detection)
        # Otherwise, enforce non-interruptible greeting
        self._avatar_mode = os.getenv("AVATAR_ENABLED") == "1"
        self._greeting_allow_interruptions = True if self._avatar_mode else False

        # No-reply follow-up system
        self._last_agent_speech_time = None
        self._no_reply_timer_task = None
        self._no_reply_timeout = float(no_reply_timeout) if no_reply_timeout is not None else 5.0
        self._follow_up_messages = follow_up_messages or []
        self._follow_up_count = 0
        # Dynamic language state and follow-ups (loaded dynamically)
        self._current_language = "auto"  # auto | ar | en | es | de | fr | ko | ja | zh
        self._lang_history = []  # keep last few detected languages for stability
        self._localized_greetings = localized_greetings or {}
        self._localized_followups = localized_followups or {}
        # Preserve base instructions so we can layer runtime language directives without polluting persona
        self._base_instructions = self.instructions

        # Track if we already greeted to avoid repeated greetings after language switches
        self._has_greeted = False

        # Allowed languages policy
        # language_policy: { "allowed": ["en","ar",...], "default": "en" }
        allowed = set()
        default_lang = "en"
        try:
            if isinstance(language_policy, dict):
                if isinstance(language_policy.get("allowed"), (list, tuple)):
                    allowed = {str(x).lower() for x in language_policy.get("allowed")}
                if isinstance(language_policy.get("default"), str) and language_policy.get("default"):
                    default_lang = str(language_policy.get("default")).lower()
        except Exception:
            pass
        # If no allowed list configured, treat as all supported
        self._allowed_languages = allowed or {"ar", "en", "es", "de", "fr", "ko", "ja", "zh"}
        self._default_language = default_lang if default_lang in {"ar", "en", "es", "de", "fr", "ko", "ja", "zh"} else "en"
        # Handover support
        self._handover_callback = None
        self._disabled = False
        
        # Memory support
        self._memory_enabled = False
        self._memory_manager = None
        self._agent_id_for_memory = None
        self._caller_id_for_memory = None

        # Lightweight timing & last-turn tracking for logging
        self._stt_start_ts = None  # perf_counter at user speech start
        self._stt_commit_ts = None # perf_counter when STT commit text received
        self._last_user_text = ""
        self._last_assistant_text = ""
        self._commit_seq = 0
        self._stt_commit_seq = -1
        self._last_llm_logged_seq = -1
        self._last_tts_logged_seq = -1
        self._transcript_uploader = None
        self._call_recorder = None
        self._call_metadata: dict[str, Any] = {}
        
        # Transcript tracking
        self._transcript_entries = []
        self._call_start_time = None
        self._call_id = None
        self._call_room_name = None
        self._transcription_manager: TranscriptionManager | None = None
        self._suppress_transcript_events = False
        self._processed_chat_ids: Set[str] = set()
        self._realtime_transcript_mode = False
        self._fish_tts = None
        self._fish_tts_response_format = "wav"
        self._tts_provider: str | None = None
        self._openai_voice: str | None = None
        self._openai_tts_response_format = "wav"
        self._openai_tts_model = os.getenv("GREETING_OPENAI_TTS_MODEL", "gpt-4o-mini-tts")

    # -------------------- Agent Tools --------------------
    # Tools are now managed via the tools/ module
    # EndCallTool, CallControlToolset, etc. are built via build_agent_tools()
    # and attached to the session externally

    async def _wait_for_sip_caller_ready(self, delay_seconds: float = 5.0) -> None:
        """
        Wait before greeting SIP callers to ensure they have answered.
        
        For SIP calls, the participant joins the room before the human actually answers
        the phone. A simple delay ensures the caller has time to answer and hear the
        full greeting.
        
        Args:
            delay_seconds: Seconds to wait before greeting (default 7.0)
        """
        logger.info(f"[greeting] SIP call detected, waiting {delay_seconds}s for caller to answer...")
        await asyncio.sleep(delay_seconds)
        logger.info("[greeting] Delay complete, proceeding with greeting")

    async def on_connect(self, room: rtc.Room):
        """Called when the agent connects to a room (when a call comes in)"""
        logger.info(f"Agent connected to room: {room.name}")
        self._room = room
        
        # Initialize transcript tracking
        self._call_start_time = datetime.now()
        self._call_id = room.name
        self._transcript_entries = []
        logger.info(f"Transcript tracking initialized for room: {room.name}")
        
        # Load memory context for the caller if memory is enabled
        if self._memory_enabled and self._memory_manager and self._agent_id_for_memory:
            try:
                callers = [p for p in room.remote_participants.values()]
                if callers:
                    caller = callers[0]
                    self._caller_id_for_memory = caller.identity or room.name
                    memory_context = self._memory_manager.get_memory_context(
                        self._agent_id_for_memory, self._caller_id_for_memory
                    )
                    if memory_context:
                        self._base_instructions = self._base_instructions + "\n\n" + memory_context
                        self.instructions = self._base_instructions
                        logger.info(f"[memory] Injected memory context for caller {self._caller_id_for_memory}")
            except Exception as mem_err:
                logger.warning(f"[memory] Failed to load memory context: {mem_err}")
        
        # If any participants are already in the room (SIP or web clients), greet now
        try:
            callers = [p for p in room.remote_participants.values()]
        except Exception:
            callers = []
        if callers:
            logger.info(f"Found {len(callers)} caller(s) already in room")
            
            # Check if any caller is a SIP participant - wait for them to answer
            # Only apply delay for INBOUND calls (outbound SIP joins after person answers)
            has_sip_caller = any(
                p.kind == rtc.ParticipantKind.PARTICIPANT_KIND_SIP 
                for p in callers
            )
            is_outbound = room.name.startswith("outbound-") if room and room.name else False
            if has_sip_caller and not is_outbound:
                await self._wait_for_sip_caller_ready(delay_seconds=5.0)
            
            await self._greet_caller()

    async def on_participant_connected(self, participant: rtc.Participant):
        """Called when a new participant joins the room"""
        logger.info(f"Participant connected: {participant.identity} (kind: {participant.kind})")
        
        # Greet any real caller (SIP trunks or web clients). Guard inside _greet_caller avoids duplicates.
        if participant.kind in (rtc.ParticipantKind.PARTICIPANT_KIND_SIP, rtc.ParticipantKind.PARTICIPANT_KIND_STANDARD):
            logger.info(f"Caller joined: {participant.identity}")
            
            # For SIP callers, wait for them to answer before greeting
            # Only apply delay for INBOUND calls (outbound SIP joins after person answers)
            is_outbound = self._room.name.startswith("outbound-") if self._room and self._room.name else False
            if participant.kind == rtc.ParticipantKind.PARTICIPANT_KIND_SIP and not is_outbound:
                await self._wait_for_sip_caller_ready(delay_seconds=5.0)
            
            await self._greet_caller()

    async def on_participant_disconnected(self, participant: rtc.Participant):
        """Called when a participant leaves the room"""
        logger.info(f"Participant disconnected: {participant.identity} (kind: {participant.kind})")
        
        # Check if the leaver was a human (SIP or Web Client)
        is_human = participant.kind in (rtc.ParticipantKind.PARTICIPANT_KIND_SIP, rtc.ParticipantKind.PARTICIPANT_KIND_STANDARD)
        
        if is_human:
            logger.info(f"User/Caller left: {participant.identity}")
            
            # Count remaining humans in the room
            human_count = 0
            if self._room:
                for p in self._room.remote_participants.values():
                    if p.kind in (rtc.ParticipantKind.PARTICIPANT_KIND_SIP, rtc.ParticipantKind.PARTICIPANT_KIND_STANDARD):
                        human_count += 1
            
            if human_count == 0:
                logger.info("No humans left in room. Saving transcript and stopping recording before disconnect...")
                
                # ✅ CRITICAL: Save transcript BEFORE disconnecting (SIGTERM may come immediately after disconnect)
                try:
                    await self._save_transcript_from_session()
                except Exception as e:
                    logger.error(f"Error saving transcript before disconnect: {e}")
                
                # Save memories from this conversation via Mem0
                if self._memory_enabled and self._memory_manager and self._caller_id_for_memory:
                    try:
                        messages = self._build_messages_for_memory()
                        if messages:
                            self._memory_manager.save_memories(
                                self._agent_id_for_memory, self._caller_id_for_memory, messages
                            )
                            logger.info(f"[memory] Saved {len(messages)} messages to memory for caller {self._caller_id_for_memory}")
                    except Exception as mem_err:
                        logger.warning(f"[memory] Failed to save memories: {mem_err}")
                
                # ✅ Stop recording before disconnect
                try:
                    if self._call_recorder:
                        logger.info("Stopping call recording...")
                        await self._call_recorder.stop(
                            call_id=self._call_id,
                            room_name=self._room.name if self._room else None,
                        )
                        logger.info("Call recording stopped.")
                except Exception as e:
                    logger.error(f"Error stopping recording before disconnect: {e}")
                
                logger.info("Cleanup complete. Agent disconnecting to finish call.")
                if self._room:
                    await self._room.disconnect()
            else:
                logger.info(f"{human_count} humans remaining; session active.")

    def _build_messages_for_memory(self) -> list:
        """Convert transcript entries to the message format Mem0 expects."""
        messages = []
        try:
            entries = getattr(self, '_transcript_entries', []) or []
            for entry in entries:
                role = entry.get("role", "")
                text = entry.get("text", "")
                if role in ("user", "assistant") and text.strip():
                    messages.append({"role": role, "content": text})
        except Exception:
            pass
        return messages

    async def _greet_caller(self):
        """Send initial greeting to the caller"""
        if not self._agent_session:
            logger.warning("Session not available for greeting")
            return

        # Avoid double-greeting on multiple join callbacks
        if self._has_greeted:
            return

        allow_interruptions = bool(self._greeting_allow_interruptions)
        greeting_text = self._greeting_text

        # In avatar mode, use generate_reply() for cleaner audio routing (per official bey example)
        if self._avatar_mode and greeting_text:
            logger.info("Avatar mode: using generate_reply() for greeting")
            self._agent_session.generate_reply(instructions=f"Greet the user with: {greeting_text}")
            self._has_greeted = True
            self._start_no_reply_timer()
            return

        # In realtime mode with separate TTS, use generate_reply() to avoid VAD interruption
        # The greeting goes through the normal flow: Realtime LLM (text) -> Fish TTS -> Audio
        if self._realtime_mode and greeting_text:
            logger.info("[greeting] Realtime mode: using generate_reply() to avoid VAD interruption")
            self._agent_session.generate_reply(instructions=f"Say exactly this greeting to the user (do not add anything else): {greeting_text}")
            self._has_greeted = True
            self._add_transcript_entry("assistant", f"[Greeting] {greeting_text}")
            self._start_no_reply_timer()
            return

        if greeting_text:
            if await self._play_dynamic_greeting_audio(greeting_text):
                self._has_greeted = True
                self._start_no_reply_timer()
                try:
                    await self._apply_language_preference(self._choose_language(self._current_language))
                except Exception:
                    pass
                return
            try:
                logger.info("Dynamic greeting playback unavailable; falling back to assets")
            except Exception:
                pass

        # Prefer pre-generated greeting audio only when TTS greeting was unavailable
        try:
            from pathlib import Path as _P
            greeting_audio_rel = os.getenv("ACTIVE_GREETING_AUDIO")
            if greeting_audio_rel:
                greeting_abs = _P(__file__).parent / greeting_audio_rel
                if os.path.exists(greeting_abs):
                    logger.info(f"Playing pre-generated greeting audio: {greeting_audio_rel}")
                    from livekit.agents.utils.audio import audio_frames_from_file
                    audio_iter = audio_frames_from_file(str(greeting_abs), sample_rate=24000, num_channels=1)
                    speech_handle = self._agent_session.say(
                        self._greeting_text or "[Greeting]",
                        audio=audio_iter, 
                        allow_interruptions=self._greeting_allow_interruptions,
                        add_to_chat_ctx=True,
                    )
                    try:
                        await speech_handle
                        logger.info("[greeting] Pre-generated greeting playback completed")
                    except Exception as wait_err:
                        logger.warning(f"[greeting] Error waiting for pre-generated greeting: {wait_err}")
                    self._has_greeted = True
                    if self._greeting_text:
                        self._add_transcript_entry("assistant", f"[Greeting Audio] {self._greeting_text}")
                    self._start_no_reply_timer()
                    try:
                        await self._apply_language_preference(self._choose_language(self._current_language))
                    except Exception:
                        pass
                    return
            # If not explicitly configured, attempt to locate a greeting file in greetings/ by active agent id
            active_id = os.getenv("ACTIVE_AGENT_ID")
            if active_id:
                greeting_dir = _P(__file__).parent / os.getenv("ACTIVE_GREETING_DIR", "greetings")
                for ext in ("opus", "mp3", "wav", "aac", "flac", "ogg", "m4a"):
                    candidate = greeting_dir / f"{active_id}.{ext}"
                    if os.path.exists(candidate):
                        logger.info(f"Playing greeting audio from greetings/: {candidate.name}")
                        from livekit.agents.utils.audio import audio_frames_from_file
                        audio_iter = audio_frames_from_file(str(candidate), sample_rate=24000, num_channels=1)
                        speech_handle = self._agent_session.say(
                            self._greeting_text or "[Greeting]",
                            audio=audio_iter, 
                            allow_interruptions=self._greeting_allow_interruptions,
                            add_to_chat_ctx=True,
                        )
                        try:
                            await speech_handle
                            logger.info("[greeting] Greeting from greetings/ playback completed")
                        except Exception as wait_err:
                            logger.warning(f"[greeting] Error waiting for greetings/ audio: {wait_err}")
                        self._has_greeted = True
                        if self._greeting_text:
                            self._add_transcript_entry("assistant", f"[Greeting Audio] {self._greeting_text}")
                        self._start_no_reply_timer()
                        try:
                            await self._apply_language_preference(self._choose_language(self._current_language))
                        except Exception:
                            pass
                        return
        except Exception as exc:
            logger.warning(f"Failed streaming greeting audio: {exc}")

        if not self._has_greeted:
            if not greeting_text:
                logger.info("No greeting text configured; skipping automated greeting.")
            else:
                logger.warning("Failed to deliver configured greeting; proceeding without intro.")

    def _start_no_reply_timer(self):
        """Start the no-reply timer to send follow-up if no user response"""
        if getattr(self, "_disabled", False):
            return
        # Cancel any existing timer
        self._cancel_no_reply_timer()
        
        # Record when agent last spoke
        self._last_agent_speech_time = time.time()
        
        # Start new timer
        self._no_reply_timer_task = asyncio.create_task(self._no_reply_timer())
        logger.debug(f"Started no-reply timer for {self._no_reply_timeout} seconds")

    def _cancel_no_reply_timer(self):
        """Cancel the current no-reply timer"""
        if self._no_reply_timer_task and not self._no_reply_timer_task.done():
            self._no_reply_timer_task.cancel()
            logger.debug("Cancelled no-reply timer - user activity detected")

    async def _no_reply_timer(self):
        """Timer that triggers follow-up message if no user response"""
        try:
            await asyncio.sleep(self._no_reply_timeout)
            
            # Check if we should send follow-up
            if self._agent_session and self._last_agent_speech_time:
                elapsed = time.time() - self._last_agent_speech_time
                if elapsed >= self._no_reply_timeout:
                    await self._send_follow_up_message()
                    
        except asyncio.CancelledError:
            # Timer was cancelled due to user activity - this is normal
            pass
        except Exception as e:
            logger.error(f"Error in no-reply timer: {e}")

    async def _send_follow_up_message(self):
        """Send a follow-up message when no user response is detected"""
        if getattr(self, "_disabled", False) or not self._agent_session:
            return
            
        # If no follow-up messages configured, do nothing
        if not self._follow_up_messages:
            return
        # Cycle through follow-up messages
        message = self._follow_up_messages[self._follow_up_count % len(self._follow_up_messages)]
        self._follow_up_count += 1
        
        logger.info(f"Sending no-reply follow-up message: {message[:50]}...")
        
        try:
            await self._say_with_timer(message, allow_interruptions=True)
            
            # Start timer again for next potential follow-up
            self._start_no_reply_timer()
            
        except Exception as e:
            logger.error(f"Error sending follow-up message: {e}")

    def _on_user_speech_detected(self):
        """Called when user speech is detected to cancel no-reply timer"""
        self._cancel_no_reply_timer()
        # Reset follow-up count when user becomes active
        self._follow_up_count = 0

    async def _say_with_timer(self, text: str, allow_interruptions: bool = True):
        """Enhanced say method that manages no-reply timer"""
        if not self._agent_session:
            logger.warning("No agent session available for speech")
            return None
            
        try:
            # Logging setup for LLM + TTS timing per utterance
            self._last_assistant_text = text or ""
            
            # ✅ Add to transcript UNIVERSALLY (works for all agents)
            self._add_transcript_entry("assistant", text)
            
            llm_started_ts = time.perf_counter()
            stt_commit_ts = getattr(self, "_stt_commit_ts", None)
            commit_seq = getattr(self, "_stt_commit_seq", -1)
            llm_seq = commit_seq

            tts_first_audio_ts_holder = {"ts": None}
            def _on_llm_first_token(payload: dict):
                try:
                    ts = float(payload.get("ts", time.perf_counter()))
                    # LLM latency from STT commit if available
                    if stt_commit_ts is not None:
                        logger.info(f"LLM first token ({(ts - stt_commit_ts)*1000.0:.0f} ms since STT): {self._last_assistant_text[:80]}")
                    else:
                        logger.info("LLM first token")
                except Exception:
                    pass

            def _on_llm_text_final(payload: dict):
                try:
                    ts = float(payload.get("ts", time.perf_counter()))
                    final_text = str(payload.get("text", self._last_assistant_text))
                    
                    # Note: Transcript entry already added above universally
                    
                    # total LLM generation time
                    total_ms = (ts - llm_started_ts) * 1000.0
                    if stt_commit_ts is not None:
                        since_stt_ms = (ts - stt_commit_ts) * 1000.0
                        logger.info(f"LLM reply (total {total_ms:.0f} ms; {since_stt_ms:.0f} ms since STT): {final_text}")
                    else:
                        logger.info(f"LLM reply ({total_ms:.0f} ms): {final_text}")
                except Exception:
                    pass

            def _on_tts_first_audio(payload: dict):
                try:
                    tts_ts = float(payload.get("ts", time.perf_counter()))
                    tts_first_audio_ts_holder["ts"] = tts_ts
                    # relative to LLM final or start if final unknown
                    ref_ts = llm_started_ts
                    logger.info(f"TTS first audio at {(tts_ts - ref_ts)*1000.0:.0f} ms since LLM start")
                except Exception:
                    pass

            def _on_tts_finished(payload: dict):
                try:
                    end_ts = float(payload.get("ts", time.perf_counter()))
                    start_ts = payload.get("tts_started_ts") or tts_first_audio_ts_holder["ts"] or llm_started_ts
                    logger.info(f"TTS finished in {(end_ts - float(start_ts))*1000.0:.0f} ms")
                except Exception:
                    pass

            # Attach telemetry callbacks to Fish TTS if available
            try:
                if hasattr(self, "_fish_tts") and self._fish_tts is not None and hasattr(self._fish_tts, "set_telemetry_callbacks"):
                    self._fish_tts.set_telemetry_callbacks(
                        on_llm_first_token=_on_llm_first_token,
                        on_llm_text_final=_on_llm_text_final,
                        on_tts_first_audio=_on_tts_first_audio,
                        on_tts_finished=_on_tts_finished,
                    )
            except Exception:
                pass
            # Say the text
            result = await self._agent_session.say(text, allow_interruptions=allow_interruptions)
            
            # Start no-reply timer after agent finishes speaking
            self._start_no_reply_timer()
            
            return result
            
        except Exception as e:
            logger.error(f"Error in enhanced say method: {e}")
            return None

    def set_session(self, session: AgentSession):
        """Set the agent session reference"""
        self._agent_session = session

    def set_transcript_uploader(self, uploader):
        self._transcript_uploader = uploader
    
    def set_call_recorder(self, recorder):
        """Set the call recorder reference for cleanup on disconnect"""
        self._call_recorder = recorder

    def set_call_metadata(self, metadata: dict | None):
        self._call_metadata = metadata or {}

    def set_runtime_voice(self, provider: Optional[str], voice: Optional[str] = None, response_format: Optional[str] = None):
        provider_norm = (provider or "").strip().lower() or None
        self._tts_provider = provider_norm
        if provider_norm == "openai":
            self._openai_voice = voice or os.getenv("OPENAI_REALTIME_VOICE", "verse")
            if response_format:
                fmt = response_format.strip().lower()
                self._openai_tts_response_format = fmt or "wav"
        elif provider_norm == "fish":
            if response_format:
                fmt = response_format.strip().lower()
                self._fish_tts_response_format = fmt or self._fish_tts_response_format
        else:
            self._openai_voice = None

    
    def set_fish_tts(self, fish_tts):
        """Set the Fish Audio TTS reference"""
        self._fish_tts = fish_tts
        try:
            self._fish_tts_response_format = getattr(getattr(fish_tts, "_opts", None), "response_format", "wav") or "wav"
        except Exception:
            self._fish_tts_response_format = "wav"

    def set_transcription_manager(self, manager: TranscriptionManager | None):
        self._transcription_manager = manager

    def _emit_transcription_segment(self, role: str, text: str):
        manager = getattr(self, "_transcription_manager", None)
        if not manager or not text or not text.strip():
            return
        if getattr(self, "_suppress_transcript_events", False):
            return
        call_key = self._call_id or self._call_room_name
        if not call_key:
            logger.debug("Skipping realtime transcript emit; call_id unresolved yet.")
            return
        segment = TranscriptSegment(
            role=role,
            text=text.strip(),
            timestamp_ms=int(time.time() * 1000),
        )
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            loop.create_task(manager.emit(segment))
        else:
            logger.warning("No running loop for transcript emit; skipping realtime push")

    def _extract_text_from_message(self, message: Any) -> str:
        """Return concatenated text content from a ChatMessage-like object."""
        if message is None:
            return ""
        try:
            text = getattr(message, "text_content", None)
            if text and text.strip():
                return text.strip()
        except Exception:
            pass
        try:
            content = getattr(message, "content", [])
            if not isinstance(content, list):
                return ""
            parts = [str(part) for part in content if isinstance(part, str) and part.strip()]
            return "\n".join(parts).strip()
        except Exception:
            return ""
        return ""
    
    async def _save_transcript(self):
        """Save the conversation transcript to a JSON file"""
        try:
            if not self._transcript_entries:
                logger.info("No transcript entries to save")
                return
            
            # Prepare transcript data
            ended_at = datetime.now()
            transcript_data = {
                "room": self._call_id,
                "call_id": self._call_id,
                "started_at": self._call_start_time.isoformat() if self._call_start_time else None,
                "ended_at": ended_at.isoformat(),
                "entries": self._transcript_entries
            }
            
            # Create transcripts directory if it doesn't exist
            transcripts_dir = Path(__file__).parent / "transcripts"
            transcripts_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = transcripts_dir / f"{self._call_id}_{timestamp}.json"
            
            # Save to file
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(transcript_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Transcript saved: {filename} ({len(self._transcript_entries)} entries)")

            if getattr(self, "_transcript_uploader", None):
                try:
                    await self._transcript_uploader.upload(
                        call_id=self._call_id,
                        room_name=self._room.name if self._room else None,
                        entries=self._transcript_entries,
                        started_at=self._call_start_time,
                        ended_at=ended_at,
                        agent_meta=self._call_metadata,
                    )
                    logger.info("Transcript uploaded to config API")
                except Exception as exc:
                    logger.error(f"Failed to upload transcript: {exc}")
            
        except Exception as e:
            logger.error(f"Failed to save transcript: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _save_transcript_from_session(self):
        """Extract transcript from AgentSession.history and save it.
        
        This should be called BEFORE session.aclose() or room.disconnect() 
        because those operations may destroy the session history.
        """
        try:
            logger.info("Extracting transcript from session history...")
            self._suppress_transcript_events = True
            
            # Preserve greeting entry if one was added (it won't be in LLM history)
            greeting_entry = None
            for entry in self._transcript_entries:
                if entry.get("text", "").startswith("[Greeting"):
                    greeting_entry = entry
                    break
            
            # Clear existing entries to avoid duplicates - history is authoritative
            self._transcript_entries = []
            
            # Re-add greeting entry first if it existed
            if greeting_entry:
                self._transcript_entries.append(greeting_entry)
                logger.info("Preserved greeting entry in transcript")
            
            # Extract from session history
            session = self._agent_session
            if session and hasattr(session, 'history') and session.history:
                history_items = session.history.items if hasattr(session.history, 'items') else []
                logger.info(f"Extracting {len(history_items)} items from session history")
                
                for item in history_items:
                    role = getattr(item, 'role', 'unknown')
                    item_type = getattr(item, 'type', None)
                    
                    # Skip function_call type items entirely
                    if item_type == 'function_call':
                        continue
                    
                    # Extract text content from the item
                    content = ""
                    if hasattr(item, 'content'):
                        item_content = item.content
                        if isinstance(item_content, str):
                            content = item_content
                        elif isinstance(item_content, list):
                            # Multi-part content - extract text parts only
                            text_parts = []
                            for part in item_content:
                                # Skip function_call parts
                                if getattr(part, 'type', None) == 'function_call':
                                    continue
                                # Get text from part
                                if hasattr(part, 'text') and part.text:
                                    text_parts.append(part.text)
                                elif isinstance(part, str):
                                    text_parts.append(part)
                            content = ' '.join(text_parts)
                        elif item_content is not None:
                            content = str(item_content)
                    
                    # Skip empty content
                    content = content.strip() if content else ""
                    if not content:
                        continue
                    
                    # Map roles to transcript format
                    if role == 'user':
                        transcript_role = 'user'
                    elif role in ('assistant', 'agent'):
                        transcript_role = 'assistant'
                    else:
                        transcript_role = str(role)
                    
                    self._add_transcript_entry(transcript_role, content)
                
                logger.info(f"Extracted {len(self._transcript_entries)} total transcript entries")
            else:
                logger.info("No session history available")
            
        except Exception as e:
            logger.error(f"Error extracting session history: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self._suppress_transcript_events = False
        
        # Save the transcript
        await self._save_transcript()
    
    def _add_transcript_entry(self, role: str, text: str):
        """Add an entry to the transcript"""
        try:
            if not text or not text.strip():
                return
            
            entry = {
                "role": role,  # "user" or "assistant"
                "text": text.strip(),
                "timestamp": datetime.now().isoformat()
            }
            self._transcript_entries.append(entry)
            logger.debug(f"Transcript entry added: {role} - {text[:50]}...")
            self._emit_transcription_segment(role, text)
            
        except Exception as e:
            logger.error(f"Failed to add transcript entry: {e}")
    
    # Legacy direct Fish Audio playback removed; FishSpeechTTS is already configured as the session's TTS provider.

    # Removed @function_tool decorator to prevent automatic call ending
    async def end_call_manual(self, ctx: RunContext):
        """Manual call ending function - NOT available as a tool to prevent accidental triggering."""
        logger.info("Manual call ending initiated")
        
        try:
            # Let the agent finish speaking before ending
            current_speech = ctx.session.current_speech
            if current_speech:
                await current_speech.wait_for_playout()
            
            # Save transcript immediately
            await self._save_transcript()
            
            logger.info("Call ending completed successfully")
            
        except Exception as e:
            logger.error(f"Error during call ending: {e}")
            # Continue with cleanup even if there's an error
            try:
                await self._save_transcript()
            except Exception as save_error:
                logger.error(f"Error saving transcript during call end: {save_error}")
        
        # Note: This function is now manual only - no automatic session closing

    # Removed SSML pronunciation helpers; FishSpeechTTS handles pronunciation adequately for our use case.

    # Post-processing volume removed; prefer FishSpeechTTS volume (generation-time)
    
    def set_fish_audio_volume(self, volume_db: float):
        """Set Fish Audio TTS volume at generation level
        
        Args:
            volume_db: Volume adjustment in dB (-20.0 to +20.0)
        """
        if hasattr(self, '_fish_tts') and self._fish_tts is not None:
            self._fish_tts.volume_db = max(-20.0, min(20.0, volume_db))
            logger.info(f"Fish Audio TTS volume set to {volume_db}dB")
        else:
            logger.warning("Fish Audio TTS not available for volume control")
    
    def set_fish_audio_speed(self, speed: float):
        """Set Fish Audio TTS speech speed
        
        Args:
            speed: Speech speed multiplier (0.5 to 2.0)
        """
        if hasattr(self, '_fish_tts') and self._fish_tts is not None:
            self._fish_tts.speed = max(0.5, min(2.0, speed))
            logger.info(f"Fish Audio TTS speed set to {speed}x")
        else:
            logger.warning("Fish Audio TTS not available for speed control")
    
    def get_fish_audio_volume(self) -> Optional[float]:
        if hasattr(self, '_fish_tts') and self._fish_tts is not None:
            return self._fish_tts.volume_db
        return None

    # Removed: get_speech_volume (no post-processing volume stage)

    # Removed: say_with_volume (post-processing)

    async def say_with_fish_volume(self, text: str, volume_db: Optional[float] = None, 
                                  speed: Optional[float] = None,
                                  allow_interruptions: bool = True):
        """Say text with Fish Audio volume and speed control
        
        Args:
            text: The text to speak
            volume_db: Volume adjustment in dB (-20.0 to +20.0), uses current if None
            speed: Speech speed multiplier (0.5 to 2.0), uses current if None
            allow_interruptions: Whether to allow user interruptions
        """
        if not self._agent_session:
            logger.warning("No agent session available for speech")
            return
        
        # Store original settings
        original_volume = None
        original_speed = None
        
        # Temporarily adjust Fish Audio settings if specified
        if hasattr(self, '_fish_tts'):
            if volume_db is not None:
                original_volume = self._fish_tts.get_volume()
                self._fish_tts.set_volume(volume_db)
            
            if speed is not None:
                original_speed = self._fish_tts.get_speed()
                self._fish_tts.set_speed(speed)
        
        try:
            result = await self._agent_session.say(
                text,
                allow_interruptions=allow_interruptions
            )
            return result
        finally:
            # Restore original settings if they were temporarily changed
            if hasattr(self, '_fish_tts'):
                if original_volume is not None:
                    self._fish_tts.set_volume(original_volume)
                if original_speed is not None:
                    self._fish_tts.set_speed(original_speed)



    def _on_metrics_collected(self, ev: MetricsCollectedEvent):
        """Metrics disabled in transcript-only mode."""
        return

    async def on_user_speech_committed(self, user_msg: str):
        """Called when user speech is committed (transcribed)"""
        if getattr(self, "_disabled", False):
            logger.debug("User speech committed ignored (assistant disabled after handover)")
            return
        logger.debug(f"User speech detected: {user_msg[:50]}...")
        # Cancel no-reply timer when user speaks
        self._on_user_speech_detected()
        # Timing: mark STT commit and log transcript with duration
        try:
            now_ts = time.perf_counter()
            stt_ms = None
            if getattr(self, "_stt_start_ts", None) is not None:
                stt_ms = (now_ts - self._stt_start_ts) * 1000.0
            self._stt_commit_ts = now_ts
            # Increment commit sequence for correlation
            self._commit_seq += 1
            self._stt_commit_seq = self._commit_seq
            self._last_user_text = user_msg or ""
            
            # Add to transcript
            self._add_transcript_entry("user", user_msg)
            
            if stt_ms is not None:
                logger.info(f"STT transcript (Caller): ({stt_ms:.0f} ms): {user_msg}")
            else:
                logger.info(f"STT transcript: {user_msg}")
        except Exception:
            pass
        # Detect and switch language dynamically
        try:
            detected = self._detect_language(user_msg)
            if detected:
                # store short history and apply majority vote to avoid flapping back to default
                self._lang_history.append(detected)
                self._lang_history = self._lang_history[-3:]
                counts = {c: self._lang_history.count(c) for c in set(self._lang_history)}
                stable = max(counts, key=counts.get)
                # Choose final language respecting allowed list
                final_lang = self._choose_language(stable)
                if final_lang != self._current_language:
                    logger.info(f"Switching language preference: {self._current_language} -> {final_lang}")
                    await self._apply_language_preference(final_lang)
                # Handover if language unsupported
                if final_lang not in self._allowed_languages and getattr(self, "_handover_callback", None) and not getattr(self, "_disabled", False):
                    pretty = {"ar":"Arabic","en":"English","es":"Spanish","de":"German","fr":"French","ko":"Korean","ja":"Japanese","zh":"Chinese"}.get(final_lang, final_lang)
                    try:
                        await self._agent_session.say(f"One moment, I will hand you over to a colleague who speaks {pretty}.")
                    except Exception:
                        pass
                    self._disabled = True
                    try:
                        asyncio.create_task(self._handover_callback(final_lang))
                    except Exception as e:
                        logger.error(f"Handover callback failed: {e}")
        except Exception as e:
            logger.error(f"Language detection error: {e}")

    async def on_user_speech_started(self):
        """Called when user starts speaking"""
        logger.debug("User speech started")
        # Cancel no-reply timer as soon as user starts speaking
        self._on_user_speech_detected()
        # Mark STT timing start
        try:
            self._stt_start_ts = time.perf_counter()
        except Exception:
            pass

    async def _play_dynamic_greeting_audio(self, text: str) -> bool:
        """Synthesize the greeting text via selected TTS and play it as an audio clip."""
        provider = (self._tts_provider or "fish").lower()
        logger.info(f"[greeting] Synthesizing greeting via {provider}...")
        
        # Small delay to allow SIP media bridge to settle before starting synthesis/playback
        await asyncio.sleep(0.5)
        
        audio_path = await self._synthesize_greeting_audio(text)
        if not audio_path:
            logger.warning(f"[greeting] Synthesis failed for {provider}")
            return False
        try:
            file_size = audio_path.stat().st_size
            logger.info(f"[greeting] Audio synthesized: {audio_path} ({file_size} bytes)")
            if file_size < 100:
                logger.warning("[greeting] Audio file is suspiciously small")
                
            from livekit.agents.utils.audio import audio_frames_from_file
            # Use 24kHz which is standard for TTS output (OpenAI TTS uses 24kHz)
            audio_iter = audio_frames_from_file(str(audio_path), sample_rate=24000, num_channels=1)
            
            logger.info(f"[greeting] Playing greeting audio to session (interruption={self._greeting_allow_interruptions})...")
            
            # CRITICAL: Await the speech handle to ensure greeting completes before STT starts
            # Pass the text so session context knows what was said
            speech_handle = self._agent_session.say(
                text or "[Greeting]",  # Pass actual text for context
                audio=audio_iter, 
                allow_interruptions=self._greeting_allow_interruptions,
                add_to_chat_ctx=True,  # Add to conversation context
            )
            
            # Wait for the greeting to finish playing
            try:
                await speech_handle
                logger.info(f"[greeting] Greeting playback completed (interrupted={getattr(speech_handle, 'interrupted', False)})")
            except Exception as wait_err:
                logger.warning(f"[greeting] Error waiting for greeting playback: {wait_err}")
            
            if text:
                self._add_transcript_entry("assistant", f"[Greeting Audio] {text}")
            return True
        except Exception as exc:
            logger.warning(f"Failed to play synthesized greeting audio: {exc}")
            return False
        finally:
            try:
                audio_path.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass

    async def _synthesize_greeting_audio(self, text: str) -> Path | None:
        provider = (self._tts_provider or "").lower()
        if provider == "openai":
            path = await self._synthesize_openai_tts(text)
            if path:
                return path
        fish = getattr(self, "_fish_tts", None)
        if fish:
            try:
                tmp_dir = Path(tempfile.gettempdir()) / "livekit_greeting_cache"
                tmp_dir.mkdir(parents=True, exist_ok=True)
                call_slug = re.sub(r"[^a-zA-Z0-9_-]", "", self._call_id or "call")
                ext = (self._fish_tts_response_format or "wav").strip(".")
                filename = f"{call_slug or 'call'}_{int(time.time())}.{ext}"
                out_path = tmp_dir / filename
                await fish.synthesize_to_file(text, str(out_path))
                return out_path
            except Exception as exc:
                logger.warning(f"Failed to synthesize greeting audio via Fish TTS: {exc}")
                try:
                    out_path.unlink(missing_ok=True)  # type: ignore[arg-type]
                except Exception:
                    pass
        return None

    async def _synthesize_openai_tts(self, text: str) -> Path | None:
        try:
            from openai import AsyncClient
        except Exception as exc:
            logger.warning(f"OpenAI SDK not available for greeting TTS: {exc}")
            return None
        voice = self._openai_voice or os.getenv("OPENAI_REALTIME_VOICE", "verse")
        if not voice:
            logger.warning("OpenAI voice not configured; cannot synthesize greeting audio")
            return None
        
        # Use MP3 for better decoder support/compatibility via FFmpeg
        response_format = "mp3"
        model = self._openai_tts_model or "gpt-4o-mini-tts"
        ext = "mp3"
        client: AsyncClient | None = None
        out_path: Path | None = None
        try:
            tmp_dir = Path(tempfile.gettempdir()) / "livekit_greeting_cache"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            call_slug = re.sub(r"[^a-zA-Z0-9_-]", "", self._call_id or "call")
            filename = f"{call_slug or 'call'}_{int(time.time())}.{ext}"
            out_path = tmp_dir / filename
            
            # Explicitly pass base_url and api_key from env if available
            client_kwargs = {}
            if os.getenv("OPENAI_API_KEY"):
                client_kwargs["api_key"] = os.getenv("OPENAI_API_KEY")
            if os.getenv("OPENAI_BASE_URL"):
                client_kwargs["base_url"] = os.getenv("OPENAI_BASE_URL")
                
            client = AsyncClient(**client_kwargs)
            stream = client.audio.speech.with_streaming_response.create(
                model=model,
                voice=voice,
                input=text,
                response_format=response_format,
            )
            async with stream as response:
                with open(out_path, "wb") as f:
                    async for chunk in response.iter_bytes():
                        f.write(chunk)
            if client:
                try:
                    await client.close()
                except Exception:
                    pass
            return out_path
        except Exception as exc:
            logger.warning(f"Failed to synthesize greeting audio via OpenAI TTS: {exc}")
            if out_path:
                try:
                    out_path.unlink(missing_ok=True)
                except Exception:
                    pass
            if client:
                try:
                    await client.close()
                except Exception:
                    pass
            return None

    def _detect_language(self, text: str) -> str:
        """Lightweight multi-language heuristic for ar, en, es, de, fr, ko, ja, zh.
        Defaults to current language when ambiguous.
        """
        # Arabic script
        if re.search(r"[\u0600-\u06FF]", text):
            return "ar"
        # CJK: Chinese/Japanese/Korean detection via script ranges
        if re.search(r"[\u4E00-\u9FFF]", text):  # CJK Unified Ideographs → zh/ja/ko
            # Try to distinguish Japanese (hiragana/katakana)
            if re.search(r"[\u3040-\u309F\u30A0-\u30FF]", text):
                return "ja"
            # Try to distinguish Korean Hangul
            if re.search(r"[\uAC00-\uD7AF]", text):
                return "ko"
            return "zh"
        # Hangul
        if re.search(r"[\uAC00-\uD7AF]", text):
            return "ko"
        # Hiragana/Katakana without Kanji
        if re.search(r"[\u3040-\u309F\u30A0-\u30FF]", text):
            return "ja"
        # Latin: attempt to pick among en/es/de/fr using common function words/characters
        lower = text.lower()
        # Quick flags
        if any(w in lower for w in [" el ", " la ", " de ", " que ", " y ", "para ", "¿", "¡"]):
            return "es"
        if any(w in lower for w in [" der ", " die ", " das ", " und ", " nicht ", " ist "]):
            return "de"
        if any(w in lower for w in [" le ", " la ", " et ", " est ", " pas ", " vous ", " à ", " ç", " é", " è", " ê"]):
            return "fr"
        if re.search(r"[A-Za-z]", text):
            return "en"
        return self._current_language

    async def _apply_language_preference(self, lang: str):
        if lang not in ("ar", "en", "es", "de", "fr", "ko", "ja", "zh"):
            return
        # Enforce allowed list
        if lang not in self._allowed_languages:
            lang = self._choose_language(lang)
        self._current_language = lang
        # Apply localized greeting and follow-ups if provided by configuration
        if self._localized_greetings:
            self._greeting_text = self._localized_greetings.get(lang, self._greeting_text)
        if self._localized_followups:
            self._follow_up_messages = self._localized_followups.get(lang, self._follow_up_messages)
        # Update STT to prefer the detected language, but KEEP detect_language enabled to avoid getting stuck
        try:
            if self._agent_session and self._agent_session.stt:
                # Hint the language while still allowing detection to switch on future turns
                self._agent_session.stt.update_options(language=lang, detect_language=True)
            # Optional: adjust TTS emotion_prefix to avoid language-tagging in text
            if hasattr(self, '_fish_tts') and self._fish_tts is not None:
                # Do not set a language at TTS-level; Fish TTS uses reference voice and text
                pass
        except Exception as e:
            logger.error(f"Failed to apply language preference to providers ({lang}): {e}")
        
        # Programmatically steer LLM output language without modifying the long persona text permanently
        try:
            lang_directives = {
                "en": "Always reply entirely in English. Do not use Arabic unless quoting the user.",
                "ar": "الرجاء الرد باللغة العربية فقط. لا تستخدم الإنجليزية إلا عند اقتباس كلام المستخدم.",
                "es": "Responde siempre completamente en español.",
                "de": "Antworte stets vollständig auf Deutsch.",
                "fr": "Réponds toujours entièrement en français.",
                "ko": "항상 한국어로만 답변하세요.",
                "ja": "常に日本語で回答してください。",
                "zh": "请始终使用中文进行回答。",
            }
            directive = lang_directives.get(lang, "")
            # After initial greeting, instruct the LLM to avoid re-greeting on language switches
            if self._has_greeted:
                directive = (directive + "\n" if directive else "") + (
                    "Respond directly to the user's request; do not preface with greetings (e.g., 'hello', 'مرحبا', 'hola', 'bonjour', 'hallo', 'こんにちは', '안녕하세요', '你好')."
                )
            if directive:
                # Build updated instructions from the immutable base plus a concise runtime directive
                updated_instructions = f"{self._base_instructions}\n\n[Runtime Language Directive]\n{directive}"
                await self.update_instructions(updated_instructions)
        except Exception as e:
            logger.error(f"Failed to update instructions for language '{lang}': {e}")

    def _choose_language(self, detected_lang: str) -> str:
        """Return a language code that is allowed, preferring detected_lang, otherwise default or first allowed."""
        # If everything is allowed, keep detected
        if not self._allowed_languages:
            return detected_lang
        # If detected allowed → use it
        if detected_lang in self._allowed_languages:
            return detected_lang
        # Otherwise use configured default if allowed
        if self._default_language in self._allowed_languages:
            return self._default_language
        # Fallback to a deterministic pick (sorted for stability)
        try:
            return sorted(self._allowed_languages)[0]
        except Exception:
            return "en"

    async def on_disconnect(self):
        """Called when the agent disconnects from the room"""
        logger.info("Agent disconnected from room")
        
        # Cancel any active timers
        self._cancel_no_reply_timer()
        
        # Save transcript then return
        logger.info("Disconnect logged")

    async def aclose(self):
        """Cleanup when agent is closing"""
        logger.info("Hospital Receptionist Agent is closing...")
        
        # Cancel any active no-reply timers
        self._cancel_no_reply_timer()
        
        # Minimal cleanup to avoid WebRTC issues
        try:
            logger.info("Agent cleanup completed; background audio handled by framework")
        except Exception as e:
            logger.error(f"Error during aclose: {e}")
        
        # No additional cleanup to avoid WebRTC panics

    async def cleanup_session(self):
        """Clean up session resources"""
        try:
            logger.info("Session cleanup completed")
        except Exception as e:
            logger.error(f"Error during session cleanup: {e}")
            traceback.print_exc()

async def _send_questionnaire_callback(
    callback_url: str = None,
    call_id: str = None,
    candidate_name: str = None,
    phone_number: str = None,
    questionnaire: list = None,
    transcript_entries: list = None,
    call_start_time: datetime = None,
    interview_id: str = None,
    room_name: str = None,
    include_recording: bool = True,
) -> None:
    """
    Save questionnaire results to DB and optionally send to callback URL.
    
    ALWAYS saves results to DB for later fetching via GET /api/callbacks.
    Only POSTs to callback_url if one is provided.
    
    Extracts Q&A pairs by analyzing the transcript against the questionnaire.
    
    Args:
        callback_url: Optional URL to POST results to (can be None)
        call_id: The call/room identifier
        interview_id: External system's identifier for matching (used for fetching)
        room_name: The LiveKit room name (used for recording URL construction)
        include_recording: Whether to include recording URL in callback (default True)
    """
    import aiohttp
    
    try:
        # Calculate call duration
        call_duration_seconds = 0
        if call_start_time:
            call_duration_seconds = int((datetime.now() - call_start_time).total_seconds())
        
        # Extract Q&A pairs from transcript using GPT-4o-mini for intelligent extraction
        # This handles complex scenarios: follow-up questions, foreign language answers,
        # clarifications, and agent interpretations
        questionnaire_results = []
        if questionnaire and transcript_entries:
            questions = questionnaire if isinstance(questionnaire, list) else [questionnaire]
            
            # Build readable transcript for LLM
            transcript_lines = []
            for entry in transcript_entries:
                role = entry.get("role", "unknown").upper()
                text = entry.get("text", "")
                if text and not text.startswith("[Greeting Audio]"):  # Skip audio markers
                    transcript_lines.append(f"{role}: {text}")
            
            transcript_text = "\n".join(transcript_lines)
            
            # Build numbered questions list
            questions_text = "\n".join([f"{i+1}. {q}" for i, q in enumerate(questions)])
            
            # Use GPT-4o-mini to extract Q&A pairs intelligently
            try:
                import openai
                
                extraction_prompt = f"""You are analyzing a phone interview transcript to extract answers to specific questions.

QUESTIONS TO FIND ANSWERS FOR:
{questions_text}

INTERVIEW TRANSCRIPT:
{transcript_text}

INSTRUCTIONS:
1. For each question, find the candidate's FINAL answer from the transcript
2. If the agent asked follow-up questions for clarification, use the FINAL clarified answer
3. If the candidate answered in a foreign language but the agent translated/confirmed it, use the English translation
4. If the agent summarized or confirmed the answer, use that summary
5. If a question was not asked or not answered, respond with "Not answered"
6. Keep answers concise but complete - include the essential information

Respond in this EXACT JSON format (no markdown, just raw JSON):
{{
  "answers": [
    {{"question_index": 1, "answer": "the answer here"}},
    {{"question_index": 2, "answer": "the answer here"}},
    ...
  ]
}}"""

                client = openai.OpenAI()
                response = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You extract Q&A pairs from interview transcripts. Respond only with valid JSON."},
                        {"role": "user", "content": extraction_prompt}
                    ],
                    temperature=0.1,  # Low temperature for consistent extraction
                    max_tokens=1000,
                )
                
                response_text = response.choices[0].message.content.strip()
                
                # Parse JSON response
                # Handle potential markdown code blocks
                if response_text.startswith("```"):
                    response_text = response_text.split("```")[1]
                    if response_text.startswith("json"):
                        response_text = response_text[4:]
                    response_text = response_text.strip()
                
                extracted = json.loads(response_text)
                
                # Build results from LLM response
                answers_map = {a["question_index"]: a["answer"] for a in extracted.get("answers", [])}
                
                for q_idx, question in enumerate(questions):
                    q_text = str(question).strip()
                    answer = answers_map.get(q_idx + 1, "Not answered")
                    
                    questionnaire_results.append({
                        "question_index": q_idx + 1,
                        "question": q_text,
                        "answer": answer,
                    })
                
                logger.info(f"LLM Q&A extraction complete: {len([r for r in questionnaire_results if r['answer'] != 'Not answered'])}/{len(questions)} questions answered")
                
            except Exception as llm_error:
                logger.error(f"LLM Q&A extraction failed: {llm_error}, falling back to basic extraction")
                
                # Fallback: basic sequential extraction if LLM fails
                transcript_list = [{"role": e.get("role", "unknown"), "text": e.get("text", "")} for e in transcript_entries]
                search_idx = 0
                
                for q_idx, question in enumerate(questions):
                    q_text = str(question).strip()
                    answer = "Not answered"
                    
                    # Simple forward search for any user response after question keywords appear
                    q_words = set(q_text.lower().split())
                    for i in range(search_idx, len(transcript_list)):
                        entry = transcript_list[i]
                        if entry["role"] == "assistant":
                            a_words = set(entry["text"].lower().split())
                            if len(q_words & a_words) >= 2:  # At least 2 words overlap
                                for j in range(i + 1, len(transcript_list)):
                                    if transcript_list[j]["role"] == "user":
                                        answer = transcript_list[j]["text"]
                                        search_idx = j + 1
                                        break
                                if answer != "Not answered":
                                    break
                    
                    questionnaire_results.append({
                        "question_index": q_idx + 1,
                        "question": q_text,
                        "answer": answer,
                    })
                
                logger.info(f"Fallback Q&A extraction: {len([r for r in questionnaire_results if r['answer'] != 'Not answered'])}/{len(questions)} matched")
        
        # Build callback payload
        payload = {
            "call_id": call_id,
            "interview_id": interview_id,  # External system's ID for matching
            "candidate_name": candidate_name,
            "phone_number": phone_number,
            "questionnaire_results": questionnaire_results,
            "transcript": [
                {"role": e.get("role"), "text": e.get("text"), "timestamp": e.get("timestamp")}
                for e in (transcript_entries or [])
            ],
            "call_duration_seconds": call_duration_seconds,
            "call_status": "completed",
            "timestamp": datetime.now().isoformat(),
        }
        
        # Add recording information if enabled
        if include_recording:
            api_base = os.getenv("CONFIG_API_BASE", os.getenv("API_BASE", "http://localhost:5057"))
            # Use room_name or call_id for the recording lookup
            recording_call_id = room_name or call_id
            
            # Provide URLs for fetching recordings
            # recordings_url: Lists all recordings for this call
            # recording_file_url: Direct URL pattern for downloading (use first recording)
            payload["recording"] = {
                "status": "processing",  # Recording may still be processing when callback is sent
                "recordings_list_url": f"{api_base}/api/livekit/calls/{recording_call_id}/recordings",
                "recording_file_url_pattern": f"{api_base}/api/livekit/calls/{recording_call_id}/recordings/{{rec_id}}/file",
                "note": "Recording may take a few seconds to become available after call ends. Poll the recordings_list_url to check status.",
            }
            
            # Also store callback_url in the call record for later recording-ready callback
            # Only register if callback_url is provided (otherwise there's nowhere to send it)
            if callback_url:
                try:
                    import requests
                    internal_key = os.getenv("INTERNAL_API_KEY", "")
                    headers = {"Content-Type": "application/json"}
                    if internal_key:
                        headers["X-Internal-Key"] = internal_key
                    
                    # Store callback info for recording-ready notification
                    callback_data = {
                        "call_id": recording_call_id,
                        "callback_url": callback_url,
                        "interview_id": interview_id,
                        "candidate_name": candidate_name,
                        "phone_number": phone_number,
                    }
                    requests.post(
                        f"{api_base}/internal/callback/register",
                        json=callback_data,
                        headers=headers,
                        timeout=5
                    )
                    logger.info(f"Registered callback URL for recording-ready notification: {recording_call_id}")
                except Exception as reg_err:
                    logger.warning(f"Failed to register callback for recording notification: {reg_err}")
        
        logger.info(f"Questionnaire results ready: {len(questionnaire_results)} Q&A pairs, interview_id={interview_id}, callback_url={'yes' if callback_url else 'none'}")
        
        # Save callback result to database (ALWAYS, even without callback_url)
        result_id = None
        try:
            import requests
            api_base = os.getenv("CONFIG_API_BASE", os.getenv("API_BASE", "http://localhost:5057"))
            internal_key = os.getenv("INTERNAL_API_KEY", "")
            headers = {"Content-Type": "application/json"}
            if internal_key:
                headers["X-Internal-Key"] = internal_key
            
            save_resp = requests.post(
                f"{api_base}/internal/callback/save",
                json={
                    "call_id": call_id,
                    "interview_id": interview_id,
                    "callback_type": "questionnaire",
                    "callback_url": callback_url,
                    "payload": payload,
                },
                headers=headers,
                timeout=5
            )
            if save_resp.status_code == 200:
                result_id = save_resp.json().get("id")
                logger.info(f"Saved questionnaire callback payload to DB: result_id={result_id}")
            else:
                logger.warning(f"Failed to save callback payload: {save_resp.status_code}")
        except Exception as save_err:
            logger.warning(f"Failed to save callback payload to DB: {save_err}")
        
        # POST to callback URL (only if provided)
        http_status = None
        http_response = None
        success = False
        
        if callback_url:
            logger.info(f"Sending callback to {callback_url} with {len(questionnaire_results)} Q&A pairs")
            try:
                async with aiohttp.ClientSession() as http_session:
                    async with http_session.post(
                        callback_url,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as response:
                        http_status = response.status
                        if response.status >= 200 and response.status < 300:
                            logger.info(f"Callback successful: {response.status}")
                            success = True
                        else:
                            http_response = await response.text()
                            logger.warning(f"Callback returned {response.status}: {http_response[:200]}")
                
                # Update callback result with response info
                if result_id:
                    try:
                        requests.post(
                            f"{api_base}/internal/callback/update",
                            json={
                                "id": result_id,
                                "http_status": http_status,
                                "http_response": http_response[:500] if http_response else None,
                                "success": success,
                            },
                            headers=headers,
                            timeout=5
                        )
                    except Exception as upd_err:
                        logger.warning(f"Failed to update callback result: {upd_err}")
            except Exception as post_err:
                logger.warning(f"Failed to POST callback to {callback_url}: {post_err}")
                # Update result with error info
                if result_id:
                    try:
                        requests.post(
                            f"{api_base}/internal/callback/update",
                            json={
                                "id": result_id,
                                "http_status": None,
                                "http_response": str(post_err)[:500],
                                "success": False,
                            },
                            headers=headers,
                            timeout=5
                        )
                    except Exception:
                        pass
        else:
            logger.info(f"No callback_url provided - results saved to DB only (interview_id={interview_id}, can fetch via GET /api/callbacks)")
                    
    except Exception as e:
        logger.error(f"Failed to send questionnaire callback: {e}")
        import traceback
        logger.error(traceback.format_exc())


async def entrypoint(ctx: agents.JobContext):
    """Main entrypoint for the agent"""
    
    # Startup notice will be refined once we parse metadata / number
    logger.info("=" * 80)
    logger.info("[DEBUG] ENTRYPOINT FUNCTION CALLED - Agent is starting!")
    logger.info(f"[DEBUG] Context type: {type(ctx)}")
    logger.info(f"[DEBUG] Room: {getattr(ctx, 'room', 'N/A')}")
    logger.info("=" * 80)
    logger.info("Starting LiveKit Agent worker...")
    
    # Select agent profile by dialed number or explicit env; do NOT use legacy active.json anymore
    agents_dir = Path(os.getenv("AGENTS_DIR", Path(__file__).parent / "agents"))
    agents_dir.mkdir(parents=True, exist_ok=True)
    agents_file = agents_dir / "agents.json"
    active_profile = None

    def _looks_like_agent(obj: Any) -> bool:
        return isinstance(obj, dict) and any(k in obj for k in ("instructions", "llm", "tts", "stt", "name", "id"))

    # If the worker was started by a dispatch rule whose agent name equals the dialed number,
    # LiveKit typically exposes it as ctx.job.metadata["agent_name"]. Be robust to different key names.
    def _normalize_number(raw: Any) -> Optional[str]:
        if raw is None:
            return None
        s = str(raw).strip()
        if not s:
            return None
        # remove spaces, dashes, parentheses
        import re
        s_clean = re.sub(r"[\s\-()]+", "", s)
        # keep both "+" and non-plus variants downstream; here, prefer leading "+" if present or if looks like E.164
        if s_clean.startswith("+"):
            return s_clean
        # convert leading 00 to +
        if s_clean.startswith("00"):
            return "+" + s_clean[2:]
        return s_clean

    dialed_number = None
    raw_metadata = {}
    try:
        raw_metadata = getattr(ctx, "job", None) and getattr(ctx.job, "metadata", {}) or {}
    except Exception:
        raw_metadata = {}
    # Normalize metadata to a dict
    meta_dict: dict[str, Any] = {}
    try:
        if isinstance(raw_metadata, dict):
            meta_dict = raw_metadata
        elif isinstance(raw_metadata, str):
            try:
                parsed = json.loads(raw_metadata)
                if isinstance(parsed, dict):
                    meta_dict = parsed
            except Exception:
                meta_dict = {}
        else:
            meta_dict = {}
            
        # Also check Room metadata and merge it (Room metadata often used for Outbound SIP context)
        try:
            if ctx.room and ctx.room.metadata:
                room_meta_str = ctx.room.metadata
                if isinstance(room_meta_str, str) and room_meta_str.strip():
                    try:
                        parsed_room = json.loads(room_meta_str)
                        if isinstance(parsed_room, dict):
                            # Merge room metadata, but prefer Job metadata for conflicts
                            parsed_room.update(meta_dict)
                            meta_dict = parsed_room
                    except Exception:
                        pass
        except Exception:
            pass
            
    except Exception:
        meta_dict = {}

    # Log a compact view of metadata for debugging number extraction
    try:
        keys_of_interest = [
            "agent_name", "expected_agent_name", "to", "destination", "phone_number",
            "dialed_number", "sip_to", "number"
        ]
        compact_meta = {k: str(meta_dict.get(k)) for k in keys_of_interest if k in meta_dict}
        logger.info(f"Job metadata (subset): {compact_meta}")
    except Exception:
        pass

    # Try common keys in order of likelihood
    candidate_keys = [
        "agent_name", "expected_agent_name", "dialed_number", "phone_number", "to", "destination", "sip_to", "number"
    ]
    for key in candidate_keys:
        if key in (meta_dict or {}):
            dialed_number = _normalize_number(meta_dict.get(key))
            if dialed_number:
                break

    # Optional: override by environment
    env_override = os.getenv("DIALED_NUMBER") or os.getenv("AGENT_NAME") or os.getenv("AGENT_PHONE_NUMBER")
    dialed_number = _normalize_number(env_override) or dialed_number
    logger.info(f"Dialed number (raw extracted): {dialed_number}")

    # Choose agent id: prefer explicit env from playground; otherwise map by dialed number
    selected_agent_id = os.getenv("AGENT_PROFILE_ID")
    if selected_agent_id:
        logger.info("Using AGENT_PROFILE_ID from environment; skipping number routing lookup")
    else:
        try:
            if dialed_number:
                import requests
                api_base = os.getenv("CONFIG_API_BASE", os.getenv("API_BASE", "http://localhost:5057"))
                r = requests.get(f"{api_base}/numbers")
                if r.ok:
                    routes = (r.json() or {}).get("routes", {})
                    # Try exact, no-plus, and plus-added variants
                    candidates = [
                        str(dialed_number),
                        str(dialed_number).lstrip("+"),
                    ]
                    if not str(dialed_number).startswith("+"):
                        candidates.append("+" + str(dialed_number))
                    route = None
                    for cand in candidates:
                        route = routes.get(cand)
                        if route:
                            break
                    if route and route.get("agent_id"):
                        selected_agent_id = route["agent_id"]
                    else:
                        logger.warning(f"No agent mapping found for dialed number {dialed_number}")
                else:
                    logger.warning(f"Fetching /numbers failed status={r.status_code}")
        except Exception as e:
            logger.warning(f"Number→agent mapping fetch failed: {e}")

    # Local file fallback: read agents/numbers.json directly to map the dialed number
    if not selected_agent_id and dialed_number:
        try:
            numbers_path = Path(os.getenv("AGENTS_DIR", Path(__file__).parent / "agents")) / "numbers.json"
            if numbers_path.exists():
                numbers_data = json.loads(numbers_path.read_text(encoding="utf-8"))
                routes = (numbers_data or {}).get("routes", {})
                candidates = [str(dialed_number), str(dialed_number).lstrip("+")]
                if not str(dialed_number).startswith("+"):
                    candidates.append("+" + str(dialed_number))
                route = None
                for cand in candidates:
                    route = routes.get(cand)
                    if route:
                        break
                if route and route.get("agent_id"):
                    selected_agent_id = route["agent_id"]
                    logger.info(f"Mapped via local numbers.json: {dialed_number} → agent_id={selected_agent_id}")
        except Exception as e:
            logger.warning(f"Local numbers.json mapping failed: {e}")

    # Single-route fallback: if exactly one number has an agent assigned, use it
    if not selected_agent_id:
        try:
            numbers_path = Path(os.getenv("AGENTS_DIR", Path(__file__).parent / "agents")) / "numbers.json"
            if numbers_path.exists():
                numbers_data = json.loads(numbers_path.read_text(encoding="utf-8"))
                routes = (numbers_data or {}).get("routes", {})
                assigned = [v.get("agent_id") for v in routes.values() if v and v.get("agent_id")]
                assigned = [a for a in assigned if a]
                if len(assigned) == 1:
                    selected_agent_id = assigned[0]
                    logger.warning("No dialed number match; falling back to the only assigned route agent_id=%s", selected_agent_id)
        except Exception as e:
            logger.warning(f"Single-route fallback failed: {e}")

    # selected_agent_id may come from env or the lookups above

    # Load agents from configured storage backend
    agents_data = _load_agents_store()
    if selected_agent_id:
        active_profile = next((a for a in agents_data.get("agents", []) if a.get("id") == selected_agent_id), None)
        if not active_profile:
            raise ValueError(f"Agent id {selected_agent_id} not found in agents.json")
        logger.info(f"Selected agent id={active_profile.get('id')} name={active_profile.get('name')}")
    else:
        # If exactly one agent exists, use it; otherwise force explicit selection
        candidates = agents_data.get("agents", [])
        if len(candidates) == 1 and _looks_like_agent(candidates[0]):
            active_profile = candidates[0]
            logger.info(f"Single agent found; defaulting to id={active_profile.get('id')} name={active_profile.get('name')}")
        else:
            raise ValueError("No agent selected. Map dialed number to an agent in /numbers or set AGENT_PROFILE_ID.")

    # Load dynamic HTTP callback tools from playground session cache
    dynamic_tools = []
    try:
        room_name = ctx.room.name if ctx.room else None
        if room_name:
            import requests
            api_base = os.getenv("CONFIG_API_BASE", "http://localhost:5057")
            internal_key = os.getenv("INTERNAL_API_KEY", "")
            headers = {"X-Internal-Key": internal_key} if internal_key else {}
            
            r = requests.get(
                f"{api_base}/internal/playground/{room_name}/tools",
                headers=headers,
                timeout=3
            )
            if r.ok:
                tools_data = r.json().get("tools", [])
                logger.info(f"Loaded {len(tools_data)} dynamic tools for room {room_name}")
                
                for tool_schema in tools_data:
                    try:
                        tool_fn = create_http_callback_tool(tool_schema)
                        dynamic_tools.append(tool_fn)
                        logger.info(f"Registered HTTP callback tool: {tool_schema.get('name')}")
                    except Exception as e:
                        logger.error(f"Failed to create tool {tool_schema.get('name')}: {e}")
            else:
                logger.debug(f"No dynamic tools endpoint or tools not found for room {room_name}")
    except Exception as e:
        logger.warning(f"Failed to load dynamic tools: {e}")

    # ==========================================================================
    # MULTI-TENANT CREDENTIAL LOADING
    # Load credentials based on agent owner's configured API keys
    # ==========================================================================
    global_defaults = {}
    if PROVIDERS_AVAILABLE and active_profile:
        try:
            # Load credentials using multi-tenant resolution
            # get_legacy_credentials now fetches both user creds and global defaults via internal API
            legacy_creds = get_legacy_credentials(active_profile)
            
            # Inject resolved credentials into environment for legacy code paths
            for key, value in legacy_creds.items():
                if value:
                    os.environ[key] = value
                    logger.info(f"[credentials] Set {key} from multi-tenant resolver")
            
            # Also get the global defaults for other parts of the script
            # We fetch them once here to avoid multiple API calls
            from providers.loader import fetch_user_credentials
            user_data = fetch_user_credentials(active_profile.get("owner_id", ""))
            global_defaults = user_data.get("global_defaults") or {}
            
            owner_id = active_profile.get("owner_id", "")
            owner_role = user_data.get("user_role", "user")
            logger.info(f"[credentials] Agent owner: id={owner_id[:8] if owner_id else 'none'}... role={owner_role}")
            
        except Exception as e:
            logger.warning(f"Multi-tenant credential loading failed, using legacy: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    # Get configuration defaults from environment (profile overrides below)
    default_voice = os.getenv("DEFAULT_VOICE", "925d2b07cf9e44a49a03197b8f796091")
    fish_api_key = os.getenv("FISH_AUDIO_API_KEY", "")
    fish_model = os.getenv("FISH_AUDIO_MODEL", "s1")
    fish_temperature = float(os.getenv("FISH_AUDIO_TEMPERATURE", "0.7"))
    fish_top_p = float(os.getenv("FISH_AUDIO_TOP_P", "0.7"))
    fish_volume_db = float(os.getenv("FISH_AUDIO_VOLUME_DB", "0.0"))  # dB adjustment
    fish_speed = float(os.getenv("FISH_AUDIO_SPEED", "1.0"))          # Speed multiplier
    
    # Recording feature removed to prevent WebRTC panics and execution stopping
    
    # Determine realtime config early; merge defaults.realtime when available from config API
    realtime_cfg = (active_profile or {}).get("realtime", {}) if active_profile else {}
    if global_defaults:
        rt_def = global_defaults.get("realtime") or {}
        if isinstance(rt_def, dict):
            # Only fill missing values from defaults
            merged = dict(rt_def)
            merged.update({k: v for k, v in (realtime_cfg or {}).items() if v is not None})
            realtime_cfg = merged

    realtime_enabled = bool((realtime_cfg or {}).get("enabled", False))
    provider_rt = str(realtime_cfg.get("tts_provider", "openai")).lower()
    voice_rt = realtime_cfg.get("voice") or os.getenv("OPENAI_REALTIME_VOICE", "verse")
    realtime_voice_format = str(realtime_cfg.get("response_format") or os.getenv("OPENAI_REALTIME_GREETING_FORMAT", "wav"))

    # Determine which TTS provider will be used
    if realtime_enabled:
        current_tts_provider = str(realtime_cfg.get("tts_provider", "openai")).lower()
    else:
        current_tts_provider = str((active_profile or {}).get("tts", {}).get("provider", "fish")).lower()

    # Validate Fish Audio configuration only when Fish TTS is actually selected
    will_use_fish_tts = (current_tts_provider == "fish")
    if will_use_fish_tts and not ((active_profile and active_profile.get("tts", {}).get("api_key")) or fish_api_key):
        logger.error("Fish Audio API key is required for Fish TTS (profile.tts.api_key or FISH_AUDIO_API_KEY env)")
        raise ValueError("Fish Audio API key is required for Fish TTS")

    if will_use_fish_tts:
        logger.info(f"Fish Audio Voice ID: {default_voice}")
        logger.info(f"Fish Audio Model: {fish_model}")
        logger.info(f"Fish Audio Temperature: {fish_temperature}")
        logger.info(f"Fish Audio Top-P: {fish_top_p}")
        logger.info(f"Fish Audio Volume: {fish_volume_db}dB")
        logger.info(f"Fish Audio Speed: {fish_speed}x")
    else:
        logger.info(f"Using TTS provider: {current_tts_provider}")
    
    # ==========================================================================
    # MULTI-TENANT PROVIDER INITIALIZATION
    # Use factories to create providers based on agent configuration
    # ==========================================================================
    stt = None
    llm = None
    tts = None
    realtime_model = None
    
    if PROVIDERS_AVAILABLE and active_profile:
        try:
            # 1. Resolve credentials first
            # resolved is {stt: {...}, llm: {...}, tts: {...}, realtime: {...}}
            resolved, is_admin = load_pipeline_credentials(active_profile)
            
            # 2. Inject legacy environment variables for backward compatibility
            legacy_creds = get_legacy_credentials(active_profile)
            for key, value in legacy_creds.items():
                if value:
                    os.environ[key] = value
            
            # 3. Create providers based on mode
            if realtime_enabled:
                # Realtime Pipeline
                use_separate_tts = bool(realtime_cfg.get("use_separate_tts", False))
                
                # Create realtime model - if using separate TTS (Fish Audio), set modalities=["text"]
                realtime_model = create_realtime_model(
                    resolved.get("realtime", {}),
                    use_separate_tts=use_separate_tts
                )
                
                # Create Fish Audio TTS if requested
                if use_separate_tts:
                    tts = create_tts(resolved.get("tts", {}))
                    logger.info(f"[realtime] Using Fish Audio TTS: {resolved.get('tts', {}).get('provider', 'fish')}")
            else:
                # Normal Pipeline
                stt = create_stt(resolved.get("stt", {}))
                llm = create_llm(resolved.get("llm", {}))
                tts = create_tts(resolved.get("tts", {}))
                
            logger.info(f"[providers] Initialized multi-tenant pipeline (realtime={realtime_enabled})")
            
        except Exception as e:
            logger.error(f"Multi-tenant provider initialization failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise RuntimeError(f"Failed to initialize providers for agent. Check API keys and configuration. Error: {e}")
    
    # Verify all required providers were initialized
    if realtime_enabled:
        if not realtime_model:
            raise RuntimeError("Realtime mode enabled but realtime_model failed to initialize. Check OPENAI_API_KEY or GOOGLE_API_KEY.")
    else:
        missing = []
        if not stt:
            missing.append("STT")
        if not llm:
            missing.append("LLM")
        if not tts:
            missing.append("TTS")
        if missing:
            raise RuntimeError(f"Failed to initialize required providers: {', '.join(missing)}. Check API keys and agent configuration.")

    # Create agent session using the initialized providers
    conn_options = LKSessionConnectOptions(
        stt_conn_options=LKAPIConnectOptions(timeout=20.0, max_retry=2),
        llm_conn_options=LKAPIConnectOptions(timeout=45.0, max_retry=2),
        tts_conn_options=LKAPIConnectOptions(timeout=30.0, max_retry=2),
        max_unrecoverable_errors=12,
    )

    if realtime_enabled and realtime_model:
        logger.info(f"Realtime mode enabled — using {realtime_cfg.get('provider', 'openai')} model")
        
        # Build session with either built-in voice or Fish Audio TTS
        session = AgentSession(
            llm=realtime_model,
            tts=tts if realtime_cfg.get("use_separate_tts") else None,
            min_endpointing_delay=0.1,
            user_away_timeout=180.0,
            conn_options=conn_options,
        )
    else:
        # Normal Pipeline Mode
        logger.info("Standard mode: using separate STT + LLM + TTS")

        stt_provider = str((active_profile or {}).get("stt", {}).get("provider", "openai")).lower()
        non_streaming_stt_providers = {"groq"}
        vad = None
        if stt_provider in non_streaming_stt_providers:
            logger.info(f"STT provider '{stt_provider}' is non-streaming — loading Silero VAD for utterance detection")
            vad = silero.VAD.load()

        session = AgentSession(
            vad=vad,
            stt=stt,
            llm=llm,
            tts=tts,
            min_endpointing_delay=0.1,
            user_away_timeout=180.0,
            conn_options=conn_options,
        )



    #session = AgentSession(
    #    llm=openai.realtime.RealtimeModel(
    #        voice="onyx",
    #        speed=1.1,
    #        turn_detection=TurnDetection(
    #            type="semantic_vad",
    #            eagerness="auto",
    #            create_response=True,
    #            interrupt_response=True,
    #        )
    #    ),
    #)
    
    # Beyond Presence Avatar initialization (if enabled)
    avatar_session = None
    if os.getenv("AVATAR_ENABLED") == "1":
        if bey_plugin is None:
            logger.warning("Avatar requested but livekit-plugins-bey is not installed")
        else:
            avatar_id = os.getenv("AVATAR_ID") or os.getenv("BEY_AVATAR_ID")  # Optional, defaults to BEY stock avatar
            logger.info(f"Avatar mode enabled, avatar_id={avatar_id or 'default'}")
            try:
                avatar_session = bey_plugin.AvatarSession(avatar_id=avatar_id)
            except Exception as e:
                logger.error(f"Failed to create avatar session: {e}")
                avatar_session = None
    
    # Create assistant instance and pass session manager
    # Build assistant with optional instruction override and behavior settings
    custom_instructions = None
    follow_ups = None
    no_reply_timeout = None
    if active_profile:
        custom_instructions = active_profile.get("instructions")
        follow_ups = active_profile.get("behavior", {}).get("follow_up_messages")
        no_reply_timeout = active_profile.get("behavior", {}).get("no_reply_timeout")
        greeting_text = active_profile.get("greeting", {}).get("text")
        greeting_allow = active_profile.get("greeting", {}).get("allow_interruptions")
        localized_greetings = active_profile.get("greeting", {}).get("localized")
        localized_followups = active_profile.get("behavior", {}).get("localized_followups")
        language_policy = None  # removed; global auto-detect
        # Pass greeting audio path via environment for assistant to pick up
        try:
            assets = active_profile.get("assets", {}) if isinstance(active_profile.get("assets"), dict) else {}
            if assets.get("greeting_audio"):
                os.environ["ACTIVE_GREETING_AUDIO"] = str(assets.get("greeting_audio"))
            # Always expose active agent id for greeting file discovery
            if active_profile.get("id"):
                os.environ["ACTIVE_AGENT_ID"] = str(active_profile.get("id"))
            os.environ.setdefault("ACTIVE_GREETING_DIR", "greetings")
        except Exception:
            pass

    # Determine runtime TTS settings for greeting
    runtime_tts_provider = None
    runtime_tts_voice = None
    runtime_tts_response_format = "wav"
    
    if realtime_enabled:
        runtime_tts_provider = provider_rt
        runtime_tts_voice = voice_rt
        runtime_tts_response_format = realtime_voice_format
    else:
        # Standard mode
        tts_cfg = (active_profile or {}).get("tts", {})
        runtime_tts_provider = tts_cfg.get("provider", "fish")
        runtime_tts_voice = tts_cfg.get("voice") or tts_cfg.get("reference_id")
        runtime_tts_response_format = tts_cfg.get("response_format", "wav")

    # Set environment variables for logging and children
    os.environ["AGENT_RUNTIME_TTS"] = str(runtime_tts_provider or "")
    os.environ["AGENT_RUNTIME_LLM"] = str((active_profile or {}).get("llm", {}).get("provider", "openai"))
    os.environ["AGENT_RUNTIME_STT"] = str((active_profile or {}).get("stt", {}).get("provider", "openai"))

    assistant = VoiceAssistant(custom_instructions=custom_instructions,
                               follow_up_messages=follow_ups,
                               no_reply_timeout=no_reply_timeout,
                               greeting_text=greeting_text if active_profile else None,
                               greeting_allow_interruptions=greeting_allow if active_profile else None,
                               localized_greetings=localized_greetings if active_profile else None,
                               localized_followups=localized_followups if active_profile else None,
                               language_policy=language_policy if active_profile else None,
                               realtime_mode=realtime_enabled)
    
    # Update assistant with specific model settings from profile
    if not realtime_enabled:
        llm_cfg = (active_profile or {}).get("llm", {})
        if llm_cfg.get("model"):
            assistant._openai_tts_model = llm_cfg.get("model") # Fallback for greeting if needed
        
        tts_cfg = (active_profile or {}).get("tts", {})
        if tts_cfg.get("model"):
            assistant._openai_tts_model = tts_cfg.get("model")
            
    assistant.set_runtime_voice(runtime_tts_provider, runtime_tts_voice, runtime_tts_response_format)
    
    # Initialize agent memory if configured
    agent_id = (active_profile or {}).get("id")
    if agent_id:
        try:
            from storage import get_storage_backend
            from pathlib import Path as _MemPath
            _mem_storage = get_storage_backend(_MemPath(os.getenv("AGENTS_DIR", _MemPath(__file__).parent / "agents")))
            mem_cfg = _mem_storage.load_agent_memory_config(agent_id)
            if mem_cfg.get("enabled"):
                memory_mgr = get_memory_manager(mem_cfg.get("config"))
                if memory_mgr.available:
                    assistant._memory_enabled = True
                    assistant._memory_manager = memory_mgr
                    assistant._agent_id_for_memory = agent_id
                    logger.info(f"[memory] Memory enabled for agent {agent_id}")
                else:
                    logger.info(f"[memory] Mem0 not available, skipping memory for agent {agent_id}")
        except Exception as mem_init_err:
            logger.warning(f"[memory] Failed to initialize memory: {mem_init_err}")
    
    # Ensure fish_tts is defined for Fish Audio specific hooks
    fish_tts = None
    if tts and isinstance(tts, FishSpeechTTS):
        fish_tts = tts
    call_recorder = CallRecorder()
    transcript_uploader = TranscriptUploader()
    assistant.set_transcript_uploader(transcript_uploader)
    assistant.set_call_recorder(call_recorder)
    assistant.set_call_metadata({
        "agent_id": (active_profile or {}).get("id") if active_profile else None,
        "agent_name": (active_profile or {}).get("name") if active_profile else None,
    })
    
    # Helper to add xAI built-in tools directly to realtime session
    # These tools (XSearch, WebSearch) are not hashable and can't go through VoiceAssistant.update_tools()
    async def add_xai_builtin_tools_to_session(sess):
        if realtime_enabled and realtime_provider == "xai":
            try:
                from tools import get_xai_builtin_tools
                xai_tools_cfg = (active_profile or {}).get("tools", {}).get("provider_tools", {})
                xai_builtin = get_xai_builtin_tools(xai_tools_cfg)
                if xai_builtin and hasattr(sess, '_activity') and sess._activity:
                    rt_sess = getattr(sess._activity, '_rt_session', None)
                    if rt_sess is not None:
                        # Get current tools and add xAI builtin tools
                        current_tools = []
                        if hasattr(rt_sess, '_tools') and rt_sess._tools:
                            if hasattr(rt_sess._tools, 'flatten'):
                                current_tools = list(rt_sess._tools.flatten())
                            else:
                                current_tools = list(rt_sess._tools)
                        await rt_sess.update_tools(current_tools + xai_builtin)
                        logger.info(f"Added {len(xai_builtin)} xAI built-in tools to realtime session")
                    else:
                        logger.debug("xAI builtin tools: realtime session not available yet")
            except Exception as e:
                logger.warning(f"Failed to add xAI built-in tools: {e}")
    
    # Build and attach agent tools
    # Tools from agent config (EndCallTool, CallControlToolset, provider tools)
    # Plus dynamic HTTP callback tools from playground session
    # Plus workflow-generated tools (integrations + handoffs)
    try:
        # Determine realtime provider for provider-specific tools (xAI, Google)
        realtime_provider = None
        if realtime_enabled:
            realtime_provider = str(realtime_cfg.get("provider", "openai")).lower()
        
        # Build tools from agent profile
        agent_tools = build_agent_tools(active_profile or {}, realtime_provider)
        
        # Build workflow tools (integrations + handoffs) from the visual workflow builder
        workflow_tools = []
        agent_id = (active_profile or {}).get("id")
        if agent_id:
            try:
                workflow_tools = load_workflow_tools(agent_id, active_profile or {})
                if workflow_tools:
                    logger.info(f"Loaded {len(workflow_tools)} tool(s) from workflow engine")
            except Exception as wf_err:
                logger.warning(f"Failed to load workflow tools: {wf_err}")
        
        # Combine all tool sources
        all_tools = agent_tools + dynamic_tools + workflow_tools
        
        if all_tools:
            await assistant.update_tools((assistant.tools or []) + all_tools)
            logger.info(f"Attached {len(all_tools)} tools to assistant ({len(agent_tools)} config, {len(dynamic_tools)} dynamic, {len(workflow_tools)} workflow)")
    except Exception as e:
        logger.warning(f"Failed to attach tools to assistant: {e}")
        import traceback
        logger.debug(traceback.format_exc())

    # ✅ Initialize transcript tracking immediately (AgentSession doesn't call on_connect)
    assistant._transcript_entries = []
    assistant._call_start_time = datetime.now()
    initial_room_name = getattr(ctx.room, "name", None) if getattr(ctx, "room", None) else None
    if initial_room_name and initial_room_name.strip():
        assistant._call_id = initial_room_name.strip()
        assistant._call_room_name = assistant._call_id
    else:
        assistant._call_id = f"call_{int(time.time())}"
        assistant._call_room_name = None
    logger.info(f"Transcript tracking initialized for call: {assistant._call_id}")
    transcript_manager = TranscriptionManager(assistant._call_id or "call")
    assistant.set_transcription_manager(transcript_manager)
    try:
        is_realtime_llm = isinstance(session.llm, lk_llm.RealtimeModel)
    except Exception:
        is_realtime_llm = False
    assistant._realtime_transcript_mode = bool(is_realtime_llm)

    # Note: Using HISTORY-ONLY transcript approach. Real-time hooks disabled to avoid duplicate entries.
    # The authoritative transcript is extracted from session.history at shutdown.
    if assistant._realtime_transcript_mode:
        logger.info("Realtime mode detected - transcript will be extracted from session history at shutdown")

    async def _push_transcript_segment(segment: TranscriptSegment) -> None:
        try:
            call_identifier = assistant._call_id or assistant._call_room_name
            if not call_identifier:
                return
            payload = {
                "call_id": call_identifier,
                "room_name": assistant._call_room_name or assistant._call_id,
                "segment": {
                    "role": segment.role,
                    "text": segment.text,
                    "timestamp_ms": segment.timestamp_ms,
                    "confidence": segment.confidence,
                },
                "agent": assistant._call_metadata or {},
            }
            await post_internal("/internal/livekit/transcript/events", payload)
        except Exception as exc:
            logger.warning("Failed to push transcript segment: %s", exc)

    async def _push_transcript_segment_nonblocking(segment: TranscriptSegment) -> None:
        """Wrapper to push segments without blocking the caller's task."""
        asyncio.create_task(_push_transcript_segment(segment))

    transcript_manager.register_listener(_push_transcript_segment_nonblocking)
    # Inject Fish Audio emotion/control tags helper only when Fish TTS is actually used
    try:
        if will_use_fish_tts and isinstance(fish_tts, FishSpeechTTS):
            # Concise user-guide aligned directive (only for Fish)
            fish_tags_guide = (
                "When speaking, you may prepend emotion/control tags to text for Fish Audio, e.g. "
                "(happy)(smile)(soft tone) Hello, how can I help you today? "
                "Use tags sparingly: one to three concise tags that fit the context. "
                "Valid examples include (happy), (sad), (angry), (calm), (excited), (soft tone), (whisper), (slow), (fast). "
                "Do not overuse tags or repeat them. Only include tags when they clearly improve tone."
            )
            # Append to instructions without polluting persona permanently
            try:
                base = assistant._base_instructions or ""
                emo_block = "\n\n[Fish Audio Emotion/Control Tags]\n" + fish_tags_guide
                await assistant.update_instructions(base + emo_block)
            except Exception:
                pass
    except Exception:
        pass
    # Support "realtime" unified mode: use OpenAI realtime model for LLM+TTS
    # Set session reference in the assistant
    assistant.set_session(session)
    assistant.set_fish_tts(fish_tts)  # Set Fish TTS reference for custom usage

    # Inject dynamic questionnaire/context from API
    try:
        # meta_dict is already parsed from ctx.job.metadata in entrypoint
        if meta_dict:
            # Check for questionnaire list or text
            q_data = meta_dict.get("questionnaire")
            candidate_name = meta_dict.get("candidate_name")
            
            additions = []
            
            if candidate_name:
                additions.append(f"The candidate's name is {candidate_name}.")
                
            if q_data:
                if isinstance(q_data, list):
                    q_text = "\n".join(q_data)
                else:
                    q_text = str(q_data)
                
                additions.append("\n[Mandatory Questionnaire]")
                additions.append("You must ask the following questions clearly, one by one, and wait for the answer:")
                additions.append(q_text)
            
            if additions:
                base = assistant._base_instructions or ""
                # Append to the end of existing instructions
                new_instructions = base + "\n\n" + "\n".join(additions)
                await assistant.update_instructions(new_instructions)
                logger.info("Injected dynamic questionnaire into instructions")
    except Exception as e:
        logger.warning(f"Failed to inject dynamic context: {e}")

    # Attach global telemetry callbacks for LLM/TTS timing logs
    try:
        last_llm_first_token_ts_holder = {"ts": None}
        def _tele_llm_first_token(payload: dict):
            try:
                ts = float(payload.get("ts", time.perf_counter()))
                last_llm_first_token_ts_holder["ts"] = ts
                base = getattr(assistant, "_stt_commit_ts", None)
                if base is not None:
                    logger.info(f"LLM first token ({(ts - base)*1000.0:.0f} ms since STT)")
                else:
                    logger.info("LLM first token")
            except Exception:
                pass
        def _tele_llm_text_final(payload: dict):
            try:
                ts = float(payload.get("ts", time.perf_counter()))
                text_final = str(payload.get("text", ""))
                base = getattr(assistant, "_stt_commit_ts", None)
                if base is not None:
                    logger.info(f"LLM reply (Agent): ({(ts - base)*1000.0:.0f} ms since STT): {text_final}")
                else:
                    logger.info(f"LLM reply: {text_final}")
            except Exception:
                pass
        def _tele_tts_first_audio(payload: dict):
            try:
                ts = float(payload.get("ts", time.perf_counter()))
                first_llm = last_llm_first_token_ts_holder["ts"]
                if first_llm is not None:
                    logger.info(f"TTS first audio ({(ts - float(first_llm))*1000.0:.0f} ms since LLM start)")
                else:
                    logger.info("TTS first audio")
            except Exception:
                pass
        def _tele_tts_finished(payload: dict):
            try:
                end_ts = float(payload.get("ts", time.perf_counter()))
                start_ts = payload.get("tts_started_ts")
                if start_ts is not None:
                    logger.info(f"TTS finished in {(end_ts - float(start_ts))*1000.0:.0f} ms")
                else:
                    logger.info("TTS finished")
            except Exception:
                pass
        if hasattr(fish_tts, "set_telemetry_callbacks"):
            fish_tts.set_telemetry_callbacks(
                on_llm_first_token=_tele_llm_first_token,
                on_llm_text_final=_tele_llm_text_final,
                on_tts_first_audio=_tele_tts_first_audio,
                on_tts_finished=_tele_tts_finished,
            )
    except Exception:
        pass
    
    # Connect event handlers for comprehensive session tracking
    session.on("metrics_collected", assistant._on_metrics_collected)
    
    # STT/Speech diagnostics - log when user speech is detected and transcribed
    def _on_user_started_speaking(ev):
        logger.info("[STT] User started speaking")
    
    def _on_user_stopped_speaking(ev):
        logger.info("[STT] User stopped speaking")
    
    def _on_user_speech_committed(ev):
        try:
            text = getattr(ev, 'text', '') or getattr(ev, 'transcript', '') or str(ev)
            logger.info(f"[STT] User speech committed: {text[:100]}...")
            # Also call assistant method to add to transcript
            if hasattr(assistant, 'on_user_speech_committed'):
                asyncio.create_task(assistant.on_user_speech_committed(text))
        except Exception as e:
            logger.warning(f"[STT] Error processing user speech: {e}")
    
    def _on_agent_started_speaking(ev):
        logger.info("[TTS] Agent started speaking")
    
    def _on_agent_stopped_speaking(ev):
        logger.info("[TTS] Agent stopped speaking")
    
    # Register speech event handlers
    try:
        session.on("user_started_speaking", _on_user_started_speaking)
        session.on("user_stopped_speaking", _on_user_stopped_speaking)
        session.on("user_speech_committed", _on_user_speech_committed)
        session.on("agent_started_speaking", _on_agent_started_speaking)
        session.on("agent_stopped_speaking", _on_agent_stopped_speaking)
        logger.info("[events] Registered speech event handlers")
    except Exception as e:
        logger.warning(f"[events] Failed to register some speech handlers: {e}")
    
    # Extra diagnostics: log close reasons and provider errors (concise)
    def _on_session_close(ev):
        logger.warning(
            "Session closed",
            extra={
                "reason": getattr(ev, "reason", None),
                "error": getattr(ev, "error", None),
            },
        )
    session.on("close", _on_session_close)
    
    def _on_tts_error(ev):
        try:
            logger.error("TTS error", extra={"recoverable": getattr(ev, "recoverable", None)})
        except Exception:
            logger.error("TTS error event received")
    try:
        fish_tts.on("error", _on_tts_error)
    except Exception:
        pass

    # Optional: LLM error logging if supported
    try:
        llm_openai.on("error", lambda ev: logger.error("LLM error", extra={"recoverable": getattr(ev, "recoverable", None)}))
    except Exception:
        pass
    
    # Set up shutdown callbacks with minimal cleanup
    async def safe_shutdown():
        """Safely shutdown the assistant with minimal cleanup"""
        try:
            logger.info("Starting safe shutdown...")
            
            # ✅ Extract conversation from AgentSession.history (works for OpenAI Realtime!)
            # Use HISTORY-ONLY approach: clear real-time entries and rebuild from authoritative history
            try:
                assistant._suppress_transcript_events = True
                
                # Preserve greeting entry if one was added (it won't be in LLM history)
                greeting_entry = None
                for entry in assistant._transcript_entries:
                    if entry.get("text", "").startswith("[Greeting Audio]"):
                        greeting_entry = entry
                        break
                
                # Clear existing entries to avoid duplicates - history is authoritative
                assistant._transcript_entries = []
                
                # Re-add greeting entry first if it existed
                if greeting_entry:
                    assistant._transcript_entries.append(greeting_entry)
                    logger.info("Preserved greeting entry in transcript")
                
                if hasattr(session, 'history') and session.history:
                    # ChatContext has .items property to access messages
                    history_items = session.history.items if hasattr(session.history, 'items') else []
                    logger.info(f"Extracting {len(history_items)} items from session history")
                    
                    for item in history_items:
                        # Convert session history items to transcript entries
                        role = getattr(item, 'role', 'unknown')
                        item_type = getattr(item, 'type', None)
                        
                        # Skip function_call type items entirely
                        if item_type == 'function_call':
                            logger.debug(f"Skipping function_call item")
                            continue
                        
                        # Extract text content from the item
                        content = ""
                        if hasattr(item, 'content'):
                            item_content = item.content
                            if isinstance(item_content, str):
                                content = item_content
                            elif isinstance(item_content, list):
                                # Multi-part content - extract text parts only
                                text_parts = []
                                for part in item_content:
                                    # Skip function_call parts
                                    if getattr(part, 'type', None) == 'function_call':
                                        continue
                                    # Get text from part
                                    if hasattr(part, 'text') and part.text:
                                        text_parts.append(part.text)
                                    elif isinstance(part, str):
                                        text_parts.append(part)
                                content = ' '.join(text_parts)
                            elif item_content is not None:
                                content = str(item_content)
                        
                        # Skip empty content
                        content = content.strip() if content else ""
                        if not content:
                            continue
                        
                        # Map roles to transcript format
                        if role == 'user':
                            transcript_role = 'user'
                        elif role in ('assistant', 'agent'):
                            transcript_role = 'assistant'
                        else:
                            transcript_role = str(role)
                        
                        assistant._add_transcript_entry(transcript_role, content)
                    
                    logger.info(f"Converted {len(assistant._transcript_entries)} history items to transcript (history-only mode)")
                else:
                    logger.info("No session history available")
            except Exception as e:
                logger.error(f"Error extracting session history: {e}")
                import traceback
                logger.error(traceback.format_exc())
            finally:
                assistant._suppress_transcript_events = False
            
            # Save transcript (whether from history or from _say_with_timer captures)
            await assistant._save_transcript()
            if transcript_manager:
                try:
                    await transcript_manager.close()
                except Exception as exc:
                    logger.warning("Failed closing transcript manager: %s", exc)
            
            # ✅ Send questionnaire results to callback URL if configured
            try:
                outbound_ctx = getattr(assistant, '_outbound_context', None) or {}
                callback_already_sent = getattr(assistant, '_callback_sent', False)
                logger.info(f"[callback] outbound_ctx keys: {list(outbound_ctx.keys()) if outbound_ctx else 'None'}, already_sent={callback_already_sent}")
                callback_url = outbound_ctx.get("callback_url")
                interview_id = outbound_ctx.get("interview_id")
                questionnaire = outbound_ctx.get("questionnaire")
                logger.info(f"[callback] callback_url={bool(callback_url)}, interview_id={interview_id}")
                # Save results to DB if we have interview_id or questionnaire (even without callback_url)
                has_data_to_save = interview_id or questionnaire
                if has_data_to_save and not callback_already_sent:
                    logger.info(f"Preparing to save/send questionnaire results (callback_url={'yes' if callback_url else 'none'})")
                    await _send_questionnaire_callback(
                        callback_url=callback_url if callback_url else None,
                        call_id=assistant._call_id,
                        candidate_name=outbound_ctx.get("candidate_name"),
                        phone_number=outbound_ctx.get("phone_number"),
                        questionnaire=questionnaire,
                        transcript_entries=assistant._transcript_entries,
                        call_start_time=assistant._call_start_time,
                        interview_id=interview_id,
                        room_name=assistant._call_room_name or assistant._call_id,
                    )
                    assistant._callback_sent = True
                    logger.info("[callback] Results saved/sent successfully, marked as done")
                elif callback_already_sent:
                    logger.info("[callback] Skipping - already processed from another cleanup path")
                elif not has_data_to_save:
                    logger.info("[callback] Skipping - no interview_id or questionnaire in context")
            except Exception as e:
                logger.error(f"Failed to send callback: {e}")
            
            # Minimal cleanup; let framework handle the rest
            logger.info("Safe shutdown completed")
        except Exception as e:
            logger.error(f"Error during safe shutdown: {e}")
    
    ctx.add_shutdown_callback(safe_shutdown)
    
    # Start the session with error handling
    try:
        # Optional noise suppression (best-effort)
        try:
            from livekit.agents import noise_cancellation as _nc  # type: ignore
            _noise_cancel = _nc.BVC()
        except Exception:
            _noise_cancel = None

        # Default: start via worker-run context. Only use the direct manual join when explicitly forced.
        force_direct = os.getenv("PLAYGROUND_FORCE_DIRECT") == "1"
        manual_room = os.getenv("PLAYGROUND_ROOM_NAME") if force_direct else None
        
        # Initialize actual_room reference for event handlers (set after session.start)
        actual_room = None
        
        if not manual_room:
            # Connect the worker participant to the room before starting the agent session (per docs)
            try:
                await ctx.connect()
                logger.info("Connected to room successfully")
            except Exception as e:
                logger.error(f"Error connecting to room: {e}")
                raise

            # Inject Room Metadata context BEFORE session.start() so initial instructions include it
            # This is critical for OpenAI Realtime - instructions must be set before the session begins
            # Also extract callback_url for posting results when call ends
            try:
                if ctx.room and ctx.room.metadata:
                    room_meta = json.loads(ctx.room.metadata)
                    q_data = room_meta.get("questionnaire") or room_meta.get("context", {}).get("questionnaire")
                    c_name = room_meta.get("candidate_name") or room_meta.get("context", {}).get("candidate_name")
                    callback_url = room_meta.get("callback_url") or room_meta.get("context", {}).get("callback_url")
                    # Get external system's identifier for matching the callback
                    interview_id = room_meta.get("interview_id") or room_meta.get("context", {}).get("interview_id")
                    
                    # Store context on assistant for callback at end of call
                    assistant._outbound_context = {
                        "callback_url": callback_url,
                        "candidate_name": c_name,
                        "questionnaire": q_data,
                        "phone_number": room_meta.get("to_number") or room_meta.get("phone_number"),
                        "interview_id": interview_id,  # External system's ID for matching
                    }
                    assistant._callback_sent = False  # Flag to prevent duplicate callbacks
                    logger.info(f"[callback] Stored _outbound_context with callback_url={bool(callback_url)}, interview_id={interview_id}")
                    
                    if q_data or c_name:
                        additions = []
                        if c_name:
                            additions.append(f"The candidate's name is {c_name}.")
                        if q_data:
                            if isinstance(q_data, list):
                                q_text = "\n".join(q_data)
                            else:
                                q_text = str(q_data)
                            additions.append("\n[Mandatory Questionnaire]")
                            additions.append("You must ask the following questions clearly, one by one, and wait for the answer:")
                            additions.append(q_text)
                        
                        if additions:
                            logger.info(f"Injecting context BEFORE session start: candidate={c_name}, q_len={len(q_data) if q_data else 0}, callback={bool(callback_url)}")
                            new_block = "\n\n" + "\n".join(additions)
                            # Update BOTH _base_instructions AND _instructions (SDK internal) BEFORE session starts
                            # The LiveKit Agent SDK reads from _instructions for the actual session
                            current_instr = assistant._base_instructions or assistant._instructions or ""
                            assistant._base_instructions = current_instr + new_block
                            assistant._instructions = assistant._base_instructions  # SDK internal attribute
                            logger.info(f"Updated assistant._instructions with dynamic context (total len={len(assistant._instructions)})")
            except Exception as e:
                logger.warning(f"Failed to inject room metadata context: {e}")

            actual_room = ctx.room
            
            # Start session FIRST, then avatar (per Beyond Presence official example)
            # https://github.com/bey-dev/bey-examples/blob/main/livekit-agent/main.py
            await session.start(
                room=ctx.room,
                agent=assistant,
                room_input_options=RoomInputOptions(
                    audio_enabled=True,
                    text_enabled=True,
                    video_enabled=False,
                    audio_sample_rate=16000,
                    audio_num_channels=1,
                    pre_connect_audio=True,
                    pre_connect_audio_timeout=10.0,
                    close_on_disconnect=False,
                    noise_cancellation=_noise_cancel,
                ),
                room_output_options=RoomOutputOptions(
                    audio_enabled=True,
                    audio_sample_rate=24000,  # TTS output sample rate
                    audio_num_channels=1,
                    transcription_enabled=True,
                ),
            )
            logger.info("Session started successfully")
            
            # Add xAI built-in tools to realtime session (bypasses VoiceAssistant's set() issue)
            await add_xai_builtin_tools_to_session(session)
            
            # Start avatar AFTER session (per Beyond Presence official example)
            if avatar_session is not None:
                try:
                    await avatar_session.start(session, room=ctx.room)
                    logger.info("Avatar session started - audio output configured for avatar")
                    # CRITICAL: Disable wait_remote_track to prevent deadlock
                    # The BEY plugin sets wait_remote_track=KIND_VIDEO which blocks audio
                    # until avatar publishes video, but avatar needs audio to generate video!
                    if hasattr(session.output, 'audio') and hasattr(session.output.audio, '_wait_remote_track'):
                        session.output.audio._wait_remote_track = None
                        logger.info("Disabled wait_remote_track to allow immediate audio streaming to avatar")
                except Exception as e:
                    logger.error(f"Failed to start avatar session: {e}")
        else:
            # Manual playground mode - per Beyond Presence official example:
            # 1. Connect to room with AUDIO_ONLY (critical for avatar mode!)
            # 2. Start session FIRST (simpler call for avatar mode)
            # 3. Start avatar AFTER
            # https://github.com/bey-dev/bey-examples/blob/main/livekit-agent/main.py
            
            if avatar_session is not None:
                # Avatar mode: use AUDIO_ONLY and simple session.start() per BEY official example
                from livekit.agents import AutoSubscribe
                await ctx.connect(auto_subscribe=AutoSubscribe.AUDIO_ONLY)
                logger.info("Manual playground (avatar mode): connected with AUDIO_ONLY")
                
                # Simple session start for avatar mode (no RoomInputOptions per official example)
                await session.start(agent=assistant, room=ctx.room)
                logger.info("Session started (avatar mode - simple)")
                
                # Add xAI built-in tools to realtime session
                await add_xai_builtin_tools_to_session(session)
                
                # Start avatar AFTER session
                try:
                    await avatar_session.start(session, room=ctx.room)
                    logger.info("Avatar session started (manual mode) - audio output configured for avatar")
                    # CRITICAL: Disable wait_remote_track to prevent deadlock
                    # The BEY plugin sets wait_remote_track=KIND_VIDEO which blocks audio
                    # until avatar publishes video, but avatar needs audio to generate video!
                    if hasattr(session.output, 'audio') and hasattr(session.output.audio, '_wait_remote_track'):
                        session.output.audio._wait_remote_track = None
                        logger.info("Disabled wait_remote_track to allow immediate audio streaming to avatar")
                except Exception as e:
                    logger.error(f"Failed to start avatar session (manual mode): {e}")
            else:
                # Non-avatar mode: use full RoomInputOptions
                await ctx.connect()
                logger.info("Manual playground: connected to room via JobContext")
                
                await session.start(
                    room=ctx.room,
                    agent=assistant,
                    room_input_options=RoomInputOptions(
                        audio_enabled=True,
                        text_enabled=True,
                        video_enabled=False,
                        audio_sample_rate=16000,
                        audio_num_channels=1,
                        pre_connect_audio=True,
                        pre_connect_audio_timeout=10.0,
                        close_on_disconnect=False,
                        noise_cancellation=_noise_cancel,
                    ),
                    room_output_options=RoomOutputOptions(
                        audio_enabled=True,
                        audio_sample_rate=24000,  # TTS output sample rate
                        audio_num_channels=1,
                        transcription_enabled=True,
                    ),
                )
                logger.info("Session started (manual mode)")
                
                # Add xAI built-in tools to realtime session
                await add_xai_builtin_tools_to_session(session)
            
            actual_room = ctx.room
            logger.info("Manual playground join completed")
    except Exception as e:
        logger.error(f"Error starting session: {e}")
        raise
    
    if actual_room:
        assistant._call_room_name = actual_room.name
        if actual_room.name and (not assistant._call_id or assistant._call_id.startswith("call_")):
            assistant._call_id = actual_room.name
        if transcript_manager:
            transcript_manager.call_id = assistant._call_id or assistant._call_room_name or transcript_manager.call_id
        # Manually invoke assistant lifecycle hooks because manual room connections
        # bypass the standard worker pipeline that normally triggers them.
        try:
            if getattr(assistant, "_room", None) is None:
                await assistant.on_connect(actual_room)
        except Exception as exc:
            logger.warning(f"Assistant on_connect failed: {exc}")
    recorder_room_name = actual_room.name if actual_room else (ctx.room.name if ctx.room else None)
    try:
        await call_recorder.start(
            room_name=recorder_room_name,
            call_id=assistant._call_id,
            agent_id=(active_profile or {}).get("id") if active_profile else None,
            agent_name=(active_profile or {}).get("name") if active_profile else None,
        )
    except Exception as e:
        logger.error(f"CallRecorder start error: {e}")
    
    # Initial greeting is handled by event hooks; no hardcoded greeting here

    # Start background audio to keep a low-level media stream flowing.
    # This helps prevent RTP inactivity timeouts on certain SIP/PBX paths.
    # Background audio disabled

    # Background audio cleanup removed

    # Safety cleanup: if no remote NON-AGENT participants remain, exit cleanly
    # actual_room was set above in session.start() (either ctx.room or manual room)
    if actual_room is None:
        logger.error("CRITICAL: actual_room is None - event handlers will not work!")
        actual_room = ctx.room  # Fallback to ctx.room
    logger.info(f"Setting up exit handler for room: {actual_room.name if actual_room else 'UNKNOWN'}")
    
    async def _maybe_exit_if_empty():
        try:
            await asyncio.sleep(5.0)  # grace window for SDK to update participant list
            # Filter out AGENT participants - only count real users (SIP, CLIENT)
            non_agent_participants = [
                p for p in actual_room.remote_participants.values() 
                if p.kind != rtc.ParticipantKind.PARTICIPANT_KIND_AGENT
            ]
            logger.info(f"Participant check: total_remote={len(actual_room.remote_participants)}, non_agent={len(non_agent_participants)}")
            
            if not non_agent_participants:
                logger.info("No real users remain; cleaning up and exiting")
                # Stop any active recorder and close session to flush transcripts
                try:
                    await call_recorder.stop(
                        call_id=assistant._call_id,
                        room_name=actual_room.name if actual_room else None,
                    )
                except Exception as e:
                    logger.error(f"CallRecorder stop error: {e}")
                try:
                    await session.aclose()
                except Exception as e:
                    logger.error(f"Session close error: {e}")
                # Save session data before exit
                try:
                    await assistant.cleanup_session()
                except Exception:
                    pass
                if transcript_manager:
                    try:
                        await transcript_manager.close()
                    except Exception as exc:
                        logger.warning("Failed closing transcript manager: %s", exc)
                
                # Send questionnaire results to callback URL if configured (_maybe_exit_if_empty path)
                try:
                    outbound_ctx = getattr(assistant, '_outbound_context', None) or {}
                    callback_already_sent = getattr(assistant, '_callback_sent', False)
                    logger.info(f"[callback-exit-empty] outbound_ctx keys: {list(outbound_ctx.keys()) if outbound_ctx else 'None'}, already_sent={callback_already_sent}")
                    callback_url = outbound_ctx.get("callback_url")
                    interview_id = outbound_ctx.get("interview_id")
                    questionnaire = outbound_ctx.get("questionnaire")
                    logger.info(f"[callback-exit-empty] callback_url={bool(callback_url)}, interview_id={interview_id}")
                    has_data_to_save = interview_id or questionnaire
                    if has_data_to_save and not callback_already_sent:
                        logger.info(f"Preparing to save/send questionnaire results (exit-empty cleanup)")
                        await _send_questionnaire_callback(
                            callback_url=callback_url if callback_url else None,
                            call_id=assistant._call_id,
                            candidate_name=outbound_ctx.get("candidate_name"),
                            phone_number=outbound_ctx.get("phone_number"),
                            questionnaire=questionnaire,
                            transcript_entries=assistant._transcript_entries,
                            call_start_time=assistant._call_start_time,
                            interview_id=interview_id,
                            room_name=assistant._call_room_name or assistant._call_id,
                        )
                        assistant._callback_sent = True
                        logger.info("[callback-exit-empty] Results saved/sent successfully")
                    elif callback_already_sent:
                        logger.info("[callback-exit-empty] Skipping - already processed")
                except Exception as e:
                    logger.error(f"Failed to send callback during exit-empty cleanup: {e}")
                
                # Delete the room using LiveKit API (same as cleanup_rooms.py)
                try:
                    logger.info("Deleting room to free LiveKit resources...")
                    from livekit import api
                    
                    # Get credentials from environment
                    lk_url = os.getenv("LIVEKIT_URL", "")
                    lk_key = os.getenv("LIVEKIT_API_KEY", "")
                    lk_secret = os.getenv("LIVEKIT_API_SECRET", "")
                    room_name = actual_room.name
                    
                    if lk_url and lk_key and lk_secret and room_name:
                        lkapi = api.LiveKitAPI(lk_url, lk_key, lk_secret)
                        try:
                            await lkapi.room.delete_room(api.DeleteRoomRequest(room=room_name))
                            logger.info(f"Room {room_name} deleted successfully via API")
                        except Exception as e:
                            logger.error(f"Error deleting room via API: {e}")
                        finally:
                            await lkapi.aclose()
                    else:
                        logger.warning("Cannot delete room: missing credentials or room name")
                except Exception as e:
                    logger.error(f"Error in room deletion: {e}")
                
                # Disconnect and exit
                try:
                    await actual_room.disconnect()
                    logger.info("Disconnected from room")
                except Exception as e:
                    logger.error(f"Error disconnecting: {e}")
                
                # Force exit
                os._exit(0)
        except Exception as e:
            logger.error(f"Error in empty-room cleanup: {e}")

    @actual_room.on("participant_connected")
    def _on_any_participant_connected(p: rtc.Participant):
        logger.info(f"Participant connected: {p.identity} kind={p.kind}")
        try:
            asyncio.create_task(assistant.on_participant_connected(p))
        except Exception as exc:
            logger.warning(f"assistant.on_participant_connected failed: {exc}")

    @actual_room.on("participant_disconnected")
    def _on_any_participant_disconnected(p: rtc.Participant):
        logger.info(f"Participant disconnected: {p.identity} kind={p.kind}")
        try:
            asyncio.create_task(assistant.on_participant_disconnected(p))
        except Exception as exc:
            logger.warning(f"assistant.on_participant_disconnected failed: {exc}")
        asyncio.create_task(_maybe_exit_if_empty())

    # Room disconnected/deleted handler - critical for cleanup when LiveKit closes the room
    @actual_room.on("disconnected")
    def _on_room_disconnected(reason=None):
        logger.info(f"Room disconnected event received. Reason: {reason}")
        # Immediately trigger cleanup - the room is gone
        asyncio.create_task(_force_exit_on_room_close(reason))
    
    async def _force_exit_on_room_close(reason):
        """Force cleanup and exit when the room itself is disconnected/deleted"""
        try:
            logger.info(f"Room closed (reason={reason}). Performing emergency cleanup...")
            
            # Stop recorder
            try:
                await call_recorder.stop(
                    call_id=assistant._call_id,
                    room_name=actual_room.name if actual_room else None,
                )
            except Exception as e:
                logger.error(f"CallRecorder stop error on room close: {e}")
            
            # ✅ IMPORTANT: Extract transcript BEFORE closing session (session.aclose() destroys history!)
            try:
                assistant._suppress_transcript_events = True
                
                # Preserve greeting entry if one was added
                greeting_entry = None
                for entry in assistant._transcript_entries:
                    if entry.get("text", "").startswith("[Greeting Audio]"):
                        greeting_entry = entry
                        break
                
                # Clear and rebuild from authoritative history
                assistant._transcript_entries = []
                if greeting_entry:
                    assistant._transcript_entries.append(greeting_entry)
                
                if hasattr(session, 'history') and session.history:
                    history_items = session.history.items if hasattr(session.history, 'items') else []
                    logger.info(f"[emergency] Extracting {len(history_items)} items from session history")
                    
                    for item in history_items:
                        role = getattr(item, 'role', 'unknown')
                        item_type = getattr(item, 'type', None)
                        
                        if item_type == 'function_call':
                            continue
                        
                        content = ""
                        if hasattr(item, 'content'):
                            item_content = item.content
                            if isinstance(item_content, str):
                                content = item_content
                            elif isinstance(item_content, list):
                                text_parts = []
                                for part in item_content:
                                    if getattr(part, 'type', None) == 'function_call':
                                        continue
                                    if hasattr(part, 'text') and part.text:
                                        text_parts.append(part.text)
                                    elif isinstance(part, str):
                                        text_parts.append(part)
                                content = ' '.join(text_parts)
                            elif item_content is not None:
                                content = str(item_content)
                        
                        content = content.strip() if content else ""
                        if not content:
                            continue
                        
                        if role == 'user':
                            transcript_role = 'user'
                        elif role in ('assistant', 'agent'):
                            transcript_role = 'assistant'
                        else:
                            transcript_role = str(role)
                        
                        assistant._add_transcript_entry(transcript_role, content)
                    
                    logger.info(f"[emergency] Extracted {len(assistant._transcript_entries)} history items")
                else:
                    logger.info("[emergency] No session history available")
            except Exception as e:
                logger.error(f"[emergency] Error extracting history: {e}")
            finally:
                assistant._suppress_transcript_events = False
            
            # Save the transcript (before closing session to ensure data is persisted)
            try:
                await assistant._save_transcript()
            except Exception as e:
                logger.error(f"[emergency] Failed to save transcript: {e}")
            
            # NOW close session (after transcript is extracted and saved)
            try:
                await session.aclose()
            except Exception as e:
                logger.error(f"Session close error on room close: {e}")
            
            # Save session data
            try:
                await assistant.cleanup_session()
            except Exception:
                pass
            
            # Close transcript manager
            if transcript_manager:
                try:
                    await transcript_manager.close()
                except Exception as exc:
                    logger.warning("Failed closing transcript manager on room close: %s", exc)
            
            # Send questionnaire results to callback URL if configured
            try:
                outbound_ctx = getattr(assistant, '_outbound_context', None) or {}
                callback_already_sent = getattr(assistant, '_callback_sent', False)
                logger.info(f"[callback-emergency] outbound_ctx keys: {list(outbound_ctx.keys()) if outbound_ctx else 'None'}, already_sent={callback_already_sent}")
                callback_url = outbound_ctx.get("callback_url")
                interview_id = outbound_ctx.get("interview_id")
                questionnaire = outbound_ctx.get("questionnaire")
                logger.info(f"[callback-emergency] callback_url={bool(callback_url)}, interview_id={interview_id}")
                has_data_to_save = interview_id or questionnaire
                if has_data_to_save and not callback_already_sent:
                    logger.info(f"Preparing to save/send questionnaire results (emergency cleanup)")
                    await _send_questionnaire_callback(
                        callback_url=callback_url if callback_url else None,
                        call_id=assistant._call_id,
                        candidate_name=outbound_ctx.get("candidate_name"),
                        phone_number=outbound_ctx.get("phone_number"),
                        questionnaire=questionnaire,
                        transcript_entries=assistant._transcript_entries,
                        call_start_time=assistant._call_start_time,
                        interview_id=interview_id,
                        room_name=assistant._call_room_name or assistant._call_id,
                    )
                    assistant._callback_sent = True
                    logger.info("[callback-emergency] Results saved/sent successfully")
                elif callback_already_sent:
                    logger.info("[callback-emergency] Skipping - already processed")
            except Exception as e:
                logger.error(f"Failed to send callback during emergency cleanup: {e}")
            
            logger.info("Emergency cleanup complete. Exiting process.")
            os._exit(0)
        except Exception as e:
            logger.error(f"Error in force exit on room close: {e}")
            os._exit(1)

    # Additional media visibility logs
    try:
        @ctx.room.on("track_published")
        def _on_track_published(pub):
            try:
                part = getattr(pub, 'participant', None)
                pid = getattr(part, 'identity', '') if part else ''
                source = getattr(pub, 'source', '')
                kind = getattr(pub, 'kind', '') if hasattr(pub, 'kind') else ''
                codec = getattr(pub, 'codec', '') if hasattr(pub, 'codec') else ''
                logger.info(f"Media: track_published by={pid} source={source} kind={kind} codec={codec}")
            except Exception:
                pass

        @ctx.room.on("track_unpublished")
        def _on_track_unpublished(pub):
            try:
                part = getattr(pub, 'participant', None)
                pid = getattr(part, 'identity', '') if part else ''
                source = getattr(pub, 'source', '')
                logger.info(f"Media: track_unpublished by={pid} source={source}")
            except Exception:
                pass
    except Exception:
        pass

    logger.info("Agent ready")
    try:
        logger.info(f"Runtime: LLM={os.getenv('AGENT_RUNTIME_LLM','')} TTS={os.getenv('AGENT_RUNTIME_TTS','')} STT={os.getenv('AGENT_RUNTIME_STT','')}")
    except Exception:
        pass

    # Heartbeat + network/codec stats logs to keep visibility during long calls
    # Also includes safety checks for empty rooms and max call duration
    async def _heartbeat():
        # Safety thresholds
        EMPTY_ROOM_TIMEOUT_SECONDS = 60  # Exit after 60 seconds with no real users
        # Max call duration from env var (in minutes), default 30 minutes
        MAX_CALL_DURATION_MINUTES = int(os.getenv("MAX_CALL_DURATION_MINUTES", "30"))
        MAX_CALL_DURATION_SECONDS = MAX_CALL_DURATION_MINUTES * 60
        HEARTBEAT_INTERVAL = 10  # seconds
        
        empty_room_start_time = None
        call_start_time = time.time()
        exit_triggered = False
        
        try:
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                
                # Skip if exit already triggered
                if exit_triggered:
                    continue
                
                try:
                    # Check room connection state first
                    conn_state = getattr(actual_room, 'connection_state', None)
                    if conn_state and hasattr(rtc, 'ConnectionState'):
                        # If room is disconnected, trigger exit
                        if conn_state == rtc.ConnectionState.CONN_DISCONNECTED:
                            logger.warning("HB: Room connection state is DISCONNECTED - triggering exit")
                            exit_triggered = True
                            asyncio.create_task(_force_exit_on_room_close("connection_state_disconnected"))
                            continue
                    
                    # Only count real users (exclude agents)
                    all_remote = list(actual_room.remote_participants.values())
                    rp = [p for p in all_remote if p.kind != rtc.ParticipantKind.PARTICIPANT_KIND_AGENT]
                    kinds = [getattr(p, 'kind', None) for p in rp]
                    
                    # Empty room timeout check
                    if len(rp) == 0:
                        if empty_room_start_time is None:
                            empty_room_start_time = time.time()
                            logger.info(f"HB: No real users detected - starting empty room timer")
                        else:
                            empty_duration = time.time() - empty_room_start_time
                            if empty_duration >= EMPTY_ROOM_TIMEOUT_SECONDS:
                                logger.warning(f"HB: Empty room timeout ({empty_duration:.0f}s >= {EMPTY_ROOM_TIMEOUT_SECONDS}s) - triggering cleanup")
                                exit_triggered = True
                                asyncio.create_task(_maybe_exit_if_empty())
                                continue
                            else:
                                logger.info(f"HB: Empty room for {empty_duration:.0f}s (timeout at {EMPTY_ROOM_TIMEOUT_SECONDS}s)")
                    else:
                        # Users present - reset empty timer
                        if empty_room_start_time is not None:
                            logger.info(f"HB: Users returned - resetting empty room timer")
                            empty_room_start_time = None
                    
                    # Max call duration failsafe
                    call_duration = time.time() - call_start_time
                    if call_duration >= MAX_CALL_DURATION_SECONDS:
                        logger.warning(f"HB: Max call duration exceeded ({call_duration:.0f}s >= {MAX_CALL_DURATION_SECONDS}s) - forcing exit")
                        exit_triggered = True
                        asyncio.create_task(_force_exit_on_room_close("max_duration_exceeded"))
                        continue
                    
                    # Try surface inbound/outbound audio stats if available
                    stats = {}
                    try:
                        # Some SDKs expose get_stats on room or pc; best-effort
                        if hasattr(actual_room, 'get_stats'):
                            s = await actual_room.get_stats()
                            stats = make_json_serializable(s)
                    except Exception:
                        pass
                    if stats:
                        # summarize RTP audio
                        def _sum(key):
                            try:
                                return sum(float(x.get(key, 0)) for x in (stats.get('audio') or []))
                            except Exception:
                                return None
                        jitter = _sum('jitter')
                        packets_lost = _sum('packetsLost')
                        logger.info(f"HB: real_users={len(rp)} (total_remote={len(all_remote)}) kinds={kinds} dur={call_duration:.0f}s jitter={jitter} lost={packets_lost}")
                    else:
                        logger.info(f"HB: real_users={len(rp)} (total_remote={len(all_remote)}) kinds={kinds} dur={call_duration:.0f}s")
                except Exception as e:
                    logger.info(f"HB: alive (error: {e})")
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    try:
        asyncio.create_task(_heartbeat())
    except Exception:
        pass

def download_models():
    """Download required models for the agent.
    Simplified: rely on runtime lazy loading by Silero; no external downloads here.
    """
    try:
        silero.VAD.load()
        return True
    except Exception as e:
        logger.error(f"Model warmup failed: {e}")
        return False

def main():
    """Main function to run the agent"""
    
    # Set up signal handling in the main thread
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully")
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Set up better error handling for the main process
    try:
        # Check if user wants to download models
        if len(sys.argv) > 1 and sys.argv[1] == "download-files":
            return download_models()
        
        # Check required environment variables
        required_vars = ["LIVEKIT_API_KEY", "LIVEKIT_API_SECRET", "LIVEKIT_URL"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        
        if missing_vars:
            logger.error(f"Missing required environment variables: {missing_vars}")
            logger.error("Please create a .env file with your LiveKit credentials")
            return
        
        # Allow overriding the LiveKit Agents worker name and bound agent profile via env
        # - AGENT_NAME:          the worker name referenced by LiveKit dispatch rule (should be the phone number)
        # - AGENT_PROFILE_ID:    id from agents.json to force a specific UI agent profile
        # If AGENT_NAME is not set, try to derive it from the numbers routing config so it
        # equals the configured phone number. If multiple numbers exist, prefer WORKER_PHONE_NUMBER/AGENT_NUMBER,
        # otherwise pick the first one.
        agent_name = os.getenv("AGENT_NAME")
        logger.info(f"OVER HERE THIS IS THE Agent name: {agent_name}")
        if not agent_name:
            # Prefer UI selection from numbers.json via API; fall back to local file; avoid hard-coded defaults
            try:
                import requests, json
                from pathlib import Path as _P
                api_base = os.getenv("CONFIG_API_BASE", os.getenv("API_BASE", "http://localhost:5057"))
                r = requests.get(f"{api_base}/numbers", timeout=3)
                payload = (r.json() or {}) if r.ok else {}
                routes = payload.get("routes", {})
                selected_number = payload.get("selected_number")
                if isinstance(selected_number, str) and selected_number:
                    agent_name = selected_number
                if not agent_name:
                    prefer = os.getenv("WORKER_PHONE_NUMBER") or os.getenv("AGENT_NUMBER")
                    if prefer and prefer in routes:
                        agent_name = prefer
                    elif len(routes) == 1:
                        agent_name = list(routes.keys())[0]
            except Exception:
                # If API unavailable (e.g., unauthorized), read the same numbers.json directly
                try:
                    agents_dir = os.getenv("AGENTS_DIR", str((_P(__file__).parent / "agents").resolve()))
                    numbers_path = _P(agents_dir) / "numbers.json"
                    data = json.loads(numbers_path.read_text(encoding="utf-8")) if numbers_path.exists() else {}
                    routes = data.get("routes", {})
                    selected_number = data.get("selected_number")
                    if isinstance(selected_number, str) and selected_number:
                        agent_name = selected_number
                    if not agent_name:
                        prefer = os.getenv("WORKER_PHONE_NUMBER") or os.getenv("AGENT_NUMBER")
                        if prefer and prefer in routes:
                            agent_name = prefer
                        elif len(routes) == 1:
                            agent_name = list(routes.keys())[0]
                except Exception:
                    agent_name = agent_name or None
        # Ensure a non-empty agent_name to avoid RegisterWorkerRequest NoneType errors
        if not agent_name:
            # sanitize to pure phone number when possible
            raw = os.getenv("DIALED_NUMBER") or os.getenv("AGENT_PHONE_NUMBER") or os.getenv("PLAYGROUND_ROOM_NAME") or ""
            try:
                import re as _re
                s = str(raw).strip()
                if s.startswith("+"):
                    s = "+" + _re.sub(r"[^0-9]", "", s[1:])
                else:
                    s = _re.sub(r"[^0-9]", "", s)
                agent_name = s if s else f"agent-{uuid.uuid4().hex[:6]}"
            except Exception:
                agent_name = f"agent-{uuid.uuid4().hex[:6]}"
            logger.warning(f"No agent_name configured; using '{agent_name}'")
        # Propagate the worker name as environment so entrypoint can read it when metadata is empty
        try:
            os.environ["AGENT_NAME"] = str(agent_name)
            os.environ.setdefault("DIALED_NUMBER", str(agent_name))
            os.environ.setdefault("AGENT_PHONE_NUMBER", str(agent_name))
            logger.info(f"Worker agent_name set to {agent_name}")
        except Exception:
            pass

        if os.getenv("AGENT_PROFILE_ID"):
            # Propagate to entrypoint via env so it selects that profile instead of global active
            os.environ["AGENT_ID_FORCE"] = os.getenv("AGENT_PROFILE_ID") or ""
        # Check if this is a manual spawn for a specific room (web/SIP/Twilio)
        manual_room = os.getenv("PLAYGROUND_ROOM_NAME")
        force_direct = os.getenv("PLAYGROUND_FORCE_DIRECT") == "1"
        
        if manual_room and force_direct:
            # DIRECT CONNECTION - Bypass Worker completely to guarantee single agent
            logger.info(f"Manual spawn: direct connection (no Worker) for room={manual_room}")
            
            participant_identity = f"agent-{manual_room[-6:]}"
            logger.info(f"[DEBUG] Room: {manual_room}, Identity: {participant_identity}")
            os.environ["PLAYGROUND_AGENT_IDENTITY"] = participant_identity
            
            # Run directly without Worker
            async def run_direct():
                # The entrypoint handles PLAYGROUND_ROOM_NAME and creates its own room connection
                # We just need to pass a minimal JobContext
                from livekit.agents import JobContext, JobProcess, JobExecutorType
                from livekit.agents.worker import RunningJobInfo, JobAcceptArguments
                from livekit.agents.job import _JobContextVar  # For registering job context
                from livekit.protocol import agent as agent_proto, models
                from livekit import rtc
                
                logger.info(f"[DEBUG] Creating minimal JobContext for direct connection...")
                
                # Create minimal JobContext (won't be used much due to manual_room mode)
                proc = JobProcess(
                    executor_type=JobExecutorType.THREAD,
                    user_arguments=None,
                    http_proxy=None,
                )
                
                room_info = models.Room(sid=f"RM_{manual_room}", name=manual_room)
                job = agent_proto.Job(
                    id=f"job-{manual_room}",
                    room=room_info,
                    type=agent_proto.JobType.JT_ROOM,
                    participant=None,
                )
                
                # Generate a proper token for the job context (needed by session.start() in SDK 1.3+)
                from livekit import api
                lk_key = os.getenv("LIVEKIT_API_KEY", "")
                lk_secret = os.getenv("LIVEKIT_API_SECRET", "")
                _tok = api.AccessToken(lk_key, lk_secret)
                _tok.with_identity(participant_identity)
                _tok.with_name(participant_identity)
                _tok.with_grants(api.VideoGrants(room=manual_room, room_join=True, agent=True))
                ctx_token = _tok.to_jwt()
                
                info = RunningJobInfo(
                    worker_id="direct",
                    accept_arguments=JobAcceptArguments(
                        identity=participant_identity,
                        name="",
                        metadata=""
                    ),
                    job=job,
                    url=os.getenv("LIVEKIT_URL", "ws://localhost:7880"),
                    token=ctx_token,  # Valid token for SDK 1.3+ job context connection
                    fake_job=True,  # Required in livekit-agents 1.3+
                )
                
                # Dummy room (won't be used since entrypoint creates its own)
                dummy_room = rtc.Room()
                
                ctx = JobContext(
                    proc=proc,
                    info=info,
                    room=dummy_room,
                    on_connect=lambda: None,
                    on_shutdown=lambda reason: logger.info(f"Shutdown: {reason}"),
                    inference_executor=None,
                )
                
                # Register job context so plugins like bey can find it via get_job_context()
                _job_ctx_token = _JobContextVar.set(ctx)
                logger.info(f"[DEBUG] Registered job context for avatar/plugin support")
                
                # Initialize HTTP context for plugins (required for Beyond Presence avatar, etc.)
                from livekit.agents.utils.http_context import _new_session_ctx, _close_http_ctx
                _new_session_ctx()
                logger.info(f"[DEBUG] Initialized HTTP session context for plugins")
                
                try:
                    logger.info(f"[DEBUG] Calling entrypoint...")
                    await entrypoint(ctx)
                    logger.info(f"[DEBUG] Entrypoint returned, keeping event loop alive...")
                    
                    # Keep the event loop alive indefinitely
                    # The agent's os._exit() will terminate when users leave
                    await asyncio.Event().wait()
                    
                except Exception as e:
                    logger.error(f"[DEBUG] Direct connection error: {e}")
                    import traceback
                    traceback.print_exc()
                finally:
                    # Clean up HTTP session context
                    try:
                        await _close_http_ctx()
                    except Exception:
                        pass
                    # Clean up job context registration
                    try:
                        _JobContextVar.reset(_job_ctx_token)
                    except Exception:
                        pass
            
            try:
                # Run the direct connection
                asyncio.run(run_direct())
            except KeyboardInterrupt:
                logger.info("Interrupted")
            except Exception as e:
                logger.error(f"Direct connection failed: {e}")
                import traceback
                traceback.print_exc()
            
            return  # Exit - don't continue to Worker code below
        
        # ELSE: Named workers use start mode and wait for dispatch (normal supervisor mode)
            try:
                sys.argv = [sys.argv[0], "start"]
            except Exception:
                pass
        
        async def _log_job_request(job_req: agents.JobRequest) -> None:
            """Log every job request before accepting it so we can see dispatch flow."""
            try:
                job = getattr(job_req, "job", None)
                room = getattr(job, "room", None)
                metadata = getattr(job, "metadata", None)
                logger.info(
                    "[DEBUG] Job request received: job_id=%s room=%s agent=%s metadata=%s",
                    getattr(job_req, "id", None),
                    getattr(room, "name", None),
                    getattr(job_req, "agent_name", None),
                    metadata,
                )
            except Exception:
                logger.exception("[DEBUG] Failed to log job request metadata")
            await job_req.accept()
            logger.info("[DEBUG] Job request accepted: job_id=%s", getattr(job_req, "id", None))
        
        # Create worker options with LiveKit credentials
        opts = agents.WorkerOptions(
            entrypoint_fnc=entrypoint,
            request_fnc=_log_job_request,
            agent_name=agent_name,
            ws_url=os.getenv("LIVEKIT_URL", "ws://localhost:7880"),
            api_key=os.getenv("LIVEKIT_API_KEY", ""),
            api_secret=os.getenv("LIVEKIT_API_SECRET", ""),
            initialize_process_timeout=60.0,
            port=0,  # assign an ephemeral HTTP port per worker to avoid 8081 collisions
        )
        
        try:
            logger.info(f"[DEBUG] Starting normal Worker in supervisor mode")
            logger.info(f"[DEBUG] WorkerOptions: agent_name={agent_name}")
            logger.info(
                "[DEBUG] Worker LiveKit target: url=%s key=%s...",
                opts.ws_url,
                (opts.api_key or "")[:6],
            )
            
            # Create event loop and worker
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # In livekit-agents 1.3+, use from_server_options to create AgentServer from WorkerOptions
            worker = agents.AgentServer.from_server_options(opts)

            def _on_worker_started(*_: object) -> None:
                logger.info("[DEBUG] Worker emitted worker_started event")

            def _on_worker_registered(worker_id: str, server_info: object) -> None:
                try:
                    region = getattr(server_info, "region", None)
                except Exception:
                    region = None
                logger.info(
                    "[DEBUG] Worker registered: id=%s region=%s", worker_id, region
                )

            try:
                worker.on("worker_started", _on_worker_started)
                worker.on("worker_registered", _on_worker_registered)
            except Exception:
                logger.warning("Unable to attach worker event listeners", exc_info=True)
                
            # Run the worker
            async def _worker_run():
                try:
                    await worker.run(devmode=True)
                except Exception as e:
                    logger.exception("Worker failed")
                    raise
            
            try:
                main_task = loop.create_task(_worker_run(), name="agent_runner")
                loop.run_until_complete(main_task)
            except KeyboardInterrupt:
                pass
            finally:
                try:
                    loop.run_until_complete(worker.aclose())
                finally:
                    loop.close()
            
                logger.info(f"[DEBUG] Worker completed")
        except SystemExit as e:
            code = getattr(e, "code", None)
            if code not in (0, None):
                logger.error(f"Worker app exited unexpectedly: code={code}")
                return
        
    except KeyboardInterrupt:
        logger.info("Agent interrupted by user")
        return
    except Exception as e:
        logger.error(f"Error in main: {e}")
        traceback.print_exc()
        return
    finally:
        logger.info("Agent main function completed")

if __name__ == "__main__":
    # Sanitize argv to avoid accidental passthrough to any Click/Typer CLIs in dependencies
    if len(sys.argv) > 1 and sys.argv[1].startswith("-"):
        sys.argv = [sys.argv[0]]

    # If invoked as a worker (spawned by supervisor), run main() directly.
    if os.getenv("RUN_AS_WORKER") == "1":
        main()
    else:
        # If AGENT_NAME is not set, and multiple numbers are assigned, run supervisor.
        # Otherwise, default to single-worker behavior.
        try:
            cfg = _read_numbers_config()
            numbers = _extract_numbers_to_run(cfg)
        except Exception:
            numbers = set()
        if os.getenv("AGENT_NAME"):
            main()
        elif len(numbers) >= 1:
            run_supervisor_forever()
        else:
            # No configured numbers; run main to surface the usual warnings/logs
            main()