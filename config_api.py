import os
import json
import uuid
import uuid as _uuid
import time
import re
import copy
import secrets
import threading
import asyncio
import requests
import subprocess
import sys
import contextlib
import signal
from collections import deque
from datetime import datetime, timedelta, timezone
from functools import wraps, lru_cache
from pathlib import Path
from typing import Any, Dict

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv(override=True)
from flask import Flask, request, jsonify, send_from_directory, g, Response, has_request_context
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from livekit import api as lk_api
from livekit.protocol import sip as sip_proto
from livekit.protocol import room as room_proto
from storage import get_storage_backend
from storage.base import StorageBackend
from analytics import CallAnalyticsService
from encryption import encrypt_value, decrypt_value, mask_api_key, is_encrypted
from providers import list_providers, get_provider_info, get_models_for_provider, get_voices_for_provider, get_voices_for_realtime_provider
from providers.validators import validate_credential

try:
    # LiveKit webhook verification (optional, enabled if env vars present)
    from livekit.api import WebhookReceiver as LKWebhookReceiver
except Exception:
    LKWebhookReceiver = None
try:
    from livekit.api import twirp_client as lk_twirp_client  # type: ignore
except Exception:
    lk_twirp_client = None
try:
    from flask_cors import CORS
except Exception:
    CORS = None  # Will handle gracefully if package missing
try:
    from twilio.twiml.voice_response import VoiceResponse
    from twilio.request_validator import RequestValidator as TwilioRequestValidator
    from twilio.rest import Client as TwilioClient
except Exception:
    VoiceResponse = None
    TwilioRequestValidator = None
    TwilioClient = None

# Flask app must be defined before using @app.* decorators
app = Flask(__name__)

# ------------------ ENVIRONMENT INITIALIZATION ------------------
def _init_env_from_defaults():
    """Load and decrypt sensitive keys from defaults into os.environ for internal services (e.g. analytics)."""
    try:
        # Load defaults
        defaults = _load_defaults()
        env_section = defaults.get("env", {}) or {}
        
        # Keys we want to ensure are in environment (plaintext or decrypted)
        keys_to_load = [
            "OPENAI_API_KEY",
            "FISH_AUDIO_API_KEY",
            "ANTHROPIC_API_KEY",
            "DEEPGRAM_API_KEY",
            "ELEVENLABS_API_KEY",
            "CARTESIA_API_KEY",
            "GOOGLE_API_KEY",
            "GROQ_API_KEY",
            "ASSEMBLYAI_API_KEY",
            "AZURE_SPEECH_KEY"
        ]
        
        for key in keys_to_load:
            # Check if already set in environment (shell/PM2/env takes precedence)
            if os.getenv(key):
                continue
                
            # Check plaintext in defaults.env
            val = env_section.get(key)
            if val:
                os.environ[key] = val
                continue
                
            # Check encrypted in defaults.env
            enc_key = f"{key}_encrypted"
            enc_val = env_section.get(enc_key)
            if enc_val:
                try:
                    dec = decrypt_value(enc_val)
                    if dec:
                        os.environ[key] = dec
                        # We don't log the key itself, just that we loaded it
                except Exception:
                    pass
    except Exception:
        # Silently fail if storage not ready yet
        pass

# Call init_env after app definition but before storage is used
# We need to be careful with circularity, so we'll call it later or use a different approach
# Actually, _load_defaults needs _storage_backend, which needs _agents_dir.
# All these are defined.

# Load environment from .env if present (allows LIVEKIT_* without exporting globally)
try:
    load_dotenv()
except Exception:
    pass

# Initialize environment from defaults (for analytics etc)
# This will be called after all storage-related functions are defined
# but before background threads start.

@lru_cache(maxsize=1)
def _get_agent_lookup() -> dict:
    """Return map of agent_id -> agent_obj."""
    try:
        data = _load_all()
        return {a.get("id"): a for a in data.get("agents", [])}
    except Exception:
        return {}

# Suppress noisy endpoint logging
import logging
class EndpointFilter(logging.Filter):
    def filter(self, record):
        # Suppress GET /numbers logs
        if hasattr(record, 'getMessage'):
            msg = record.getMessage()
            if 'GET /numbers' in msg or 'GET /api/numbers' in msg:
                return False
        return True

# Apply filter to werkzeug logger
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.addFilter(EndpointFilter())

# Enable permissive CORS for local development (React at :3000)
if CORS is not None:
    CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=False)
else:
    # Fallback minimal CORS headers if extension missing
    @app.after_request
    def _cors_headers(resp):
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        return resp

# Simple reverse-proxy friendly prefix for production deployments
API_PREFIX = os.getenv("API_PREFIX", "/api")

# Feature flag: control whether webhook should auto-spawn agent workers.
# When using LiveKit Cloud dispatch rules for named workers, set WEBHOOK_AUTOSPAWN=0
# to avoid duplicate workers joining the same room.
ENABLE_WEBHOOK_AUTOSPAWN = (os.getenv("WEBHOOK_AUTOSPAWN", "1") == "1")
AUTOSPAWN_DIRECT = (os.getenv("AUTOSPAWN_DIRECT", "0") == "1")
ENABLE_CALL_ANALYTICS = (os.getenv("ENABLE_CALL_ANALYTICS", "1") == "1")
ENABLE_ANALYTICS_TICKER = (os.getenv("ENABLE_ANALYTICS_TICKER", "0") == "1")

# ------------------ EMBED SESSION STORE (EPHEMERAL) ------------------
# sid -> { agent_id, room, title, theme, created_at, exp }
_EMBED_SESSIONS: dict[str, dict] = {}
_EMBED_TTL_SECONDS_DEFAULT = int(os.getenv("EMBED_TTL_SECONDS", "900"))  # 15 minutes

# ------------------ PLAYGROUND TOOLS CACHE (EPHEMERAL) ------------------
# room -> [tool_schemas]
_PLAYGROUND_TOOLS: dict[str, list[dict]] = {}

_CONFIG_CACHE_TTL = max(1, int(os.getenv("CONFIG_CACHE_TTL", "5")))
_USERS_CACHE: dict | None = None
_USERS_CACHE_TS = 0.0
_SESSIONS_CACHE: dict | None = None
_SESSIONS_CACHE_TS = 0.0
_KV_CACHE_LOCK = threading.Lock()

def _embed_create_session(agent_id: str, room: str, title: str | None = None, theme: str | None = None, ttl_seconds: int | None = None) -> dict:
    sid = _uuid.uuid4().hex
    now = int(time.time())
    exp = now + int(ttl_seconds or _EMBED_TTL_SECONDS_DEFAULT)
    rec = {
        "sid": sid,
        "agent_id": agent_id,
        "room": room,
        "title": (title or ""),
        "theme": (theme or ""),
        "created_at": now,
        "exp": exp,
        "uses": 0,
    }
    _EMBED_SESSIONS[sid] = rec
    return rec

def _embed_get_session(sid: str) -> dict | None:
    try:
        rec = _EMBED_SESSIONS.get(str(sid))
        if not rec:
            return None
        if int(time.time()) > int(rec.get("exp", 0)):
            try:
                del _EMBED_SESSIONS[str(sid)]
            except Exception:
                pass
            return None
        return rec
    except Exception:
        return None

def _start_embed_gc_thread():
    def _gc():
        while True:
            try:
                now = int(time.time())
                for k, v in list(_EMBED_SESSIONS.items()):
                    try:
                        if now > int(v.get("exp", 0)):
                            _EMBED_SESSIONS.pop(k, None)
                    except Exception:
                        _EMBED_SESSIONS.pop(k, None)
            except Exception:
                pass
            time.sleep(30)
    try:
        t = threading.Thread(target=_gc, daemon=True)
        t.start()
    except Exception:
        pass
_start_embed_gc_thread()

def _playground_set_tools(room: str, tools: list[dict]) -> None:
    """Store tools for a playground session keyed by room name."""
    _PLAYGROUND_TOOLS[str(room)] = tools or []

def _playground_get_tools(room: str) -> list[dict]:
    """Retrieve tools for a room."""
    return _PLAYGROUND_TOOLS.get(str(room), [])

def _playground_clear_tools(room: str) -> None:
    """Clear tools for a room after they're loaded."""
    _PLAYGROUND_TOOLS.pop(str(room), None)

def _cached_kv(cache_name: str, loader):
    global _USERS_CACHE, _USERS_CACHE_TS, _SESSIONS_CACHE, _SESSIONS_CACHE_TS
    now = time.time()
    with _KV_CACHE_LOCK:
        if cache_name == "users":
            if _USERS_CACHE is not None and (now - _USERS_CACHE_TS) < _CONFIG_CACHE_TTL:
                return copy.deepcopy(_USERS_CACHE)
            data = loader()
            _USERS_CACHE = copy.deepcopy(data)
            _USERS_CACHE_TS = now
            return copy.deepcopy(data)
        if cache_name == "sessions":
            if _SESSIONS_CACHE is not None and (now - _SESSIONS_CACHE_TS) < _CONFIG_CACHE_TTL:
                return copy.deepcopy(_SESSIONS_CACHE)
            data = loader()
            _SESSIONS_CACHE = copy.deepcopy(data)
            _SESSIONS_CACHE_TS = now
            return copy.deepcopy(data)
    return loader()

def _invalidate_cache(cache_name: str) -> None:
    global _USERS_CACHE_TS, _SESSIONS_CACHE_TS
    with _KV_CACHE_LOCK:
        if cache_name == "users":
            _USERS_CACHE_TS = 0
        elif cache_name == "sessions":
            _SESSIONS_CACHE_TS = 0

def _agents_dir() -> Path:
    return Path(os.getenv("AGENTS_DIR", Path(__file__).parent / "agents"))

_STORAGE_BACKEND: StorageBackend | None = None
_ANALYTICS_SERVICE: CallAnalyticsService | None = None
_ANALYTICS_WATCHER_STARTED = False
_CALL_IDLE_FINALIZE_SECONDS = max(5, int(os.getenv("CALL_FINALIZE_IDLE_SECONDS", "60")))
_WEBHOOK_DEDUP_MAX = max(256, int(os.getenv("WEBHOOK_DEDUP_MAX", "2048")))
_WEBHOOK_DEDUP_TTL_SECONDS = max(1, int(os.getenv("WEBHOOK_DEDUP_TTL_SECONDS", "20")))
_WEBHOOK_DEDUP_CACHE: deque[tuple[str, float]] = deque()
_WEBHOOK_DEDUP_IDS: set[str] = set()
_WEBHOOK_DEDUP_LOCK = threading.Lock()

def _storage_backend() -> StorageBackend:
    global _STORAGE_BACKEND
    if _STORAGE_BACKEND is None:
        _STORAGE_BACKEND = get_storage_backend(_agents_dir())
    return _STORAGE_BACKEND

def _analytics_service() -> CallAnalyticsService | None:
    global _ANALYTICS_SERVICE
    if not ENABLE_CALL_ANALYTICS:
        return None
    if _ANALYTICS_SERVICE is None:
        try:
            _ANALYTICS_SERVICE = CallAnalyticsService()
        except Exception:
            _ANALYTICS_SERVICE = None
    return _ANALYTICS_SERVICE


def _start_analytics_backfill_loop() -> None:
    global _ANALYTICS_WATCHER_STARTED
    if _ANALYTICS_WATCHER_STARTED or not ENABLE_CALL_ANALYTICS or not ENABLE_ANALYTICS_TICKER:
        return
    _ANALYTICS_WATCHER_STARTED = True

    interval = max(5, int(os.getenv("CALL_ANALYTICS_POLL_SECONDS", "20")))

    def _loop() -> None:
        while True:
            try:
                svc = _analytics_service()
                if svc:
                    processed_metrics = svc.process_missing_calls()
                    processed_analysis = svc.process_missing_analysis()
                    print(
                        f"[analytics] watcher tick metrics={processed_metrics} analysis={processed_analysis}",
                        file=sys.stderr,
                        flush=True,
                    )
            except Exception as exc:
                print(f"[analytics] watcher error: {exc}", file=sys.stderr, flush=True)
            time.sleep(interval)

    try:
        threading.Thread(target=_loop, daemon=True).start()
    except Exception as exc:
        print(f"[analytics] failed to start watcher: {exc}", file=sys.stderr, flush=True)


def _trigger_analytics(call_id: str, retry_for_transcript: bool = False) -> None:
    """Trigger immediate background analysis for a finished call."""
    if not ENABLE_CALL_ANALYTICS:
        return
    svc = _analytics_service()
    if not svc or not call_id:
        return

    def _run():
        try:
            import asyncio
            asyncio.run(svc.process_call(call_id, retry_for_transcript=retry_for_transcript))
        except Exception as exc:
            print(f"[analytics] trigger failed for {call_id}: {exc}", file=sys.stderr, flush=True)

    threading.Thread(target=_run, daemon=True).start()


# Initialize environment from defaults (for analytics etc)
_init_env_from_defaults()

if ENABLE_ANALYTICS_TICKER:
    _start_analytics_backfill_loop()

def _call_has_active_participants(call: dict) -> bool:
    try:
        participants = call.get("participants") or {}
        if not participants:
            return False
        now = int(time.time())
        for pdata in participants.values():
            state = str((pdata or {}).get("state") or "").upper()
            last_seen = int((pdata or {}).get("last_seen_ts") or call.get("last_received_at") or 0)
            joined = int((pdata or {}).get("joined_at_ts") or 0)
            idle = last_seen and (now - last_seen) > _CALL_IDLE_FINALIZE_SECONDS
            if not state or state not in {"DISCONNECTED", "LEFT", "INACTIVE", "ENDED", "ROOM_DELETED"}:
                if idle and joined and (now - joined) > _CALL_IDLE_FINALIZE_SECONDS:
                    continue
                return True
        return False
    except Exception:
        return False


def _call_has_active_recordings(call: dict) -> bool:
    try:
        now = int(time.time())
        for rec in (call.get("recordings") or []):
            status = str((rec or {}).get("status") or "").lower()
            updated = int((rec or {}).get("updated_at") or 0)
            idle = updated and (now - updated) > _CALL_IDLE_FINALIZE_SECONDS
            if not status or status not in {"stopped", "ended", "completed", "failed"}:
                if idle:
                    continue
                return True
        return False
    except Exception:
        return False


def _webhook_is_duplicate(event_id: str | None) -> bool:
    """Return True if we've processed this webhook id recently."""
    if not event_id:
        return False
    now = time.time()
    with _WEBHOOK_DEDUP_LOCK:
        while _WEBHOOK_DEDUP_CACHE and (now - _WEBHOOK_DEDUP_CACHE[0][1]) > _WEBHOOK_DEDUP_TTL_SECONDS:
            old_id, _ = _WEBHOOK_DEDUP_CACHE.popleft()
            _WEBHOOK_DEDUP_IDS.discard(old_id)
        if event_id in _WEBHOOK_DEDUP_IDS:
            return True
        _WEBHOOK_DEDUP_CACHE.append((event_id, now))
        _WEBHOOK_DEDUP_IDS.add(event_id)
        while len(_WEBHOOK_DEDUP_CACHE) > _WEBHOOK_DEDUP_MAX:
            old_id, _ = _WEBHOOK_DEDUP_CACHE.popleft()
            _WEBHOOK_DEDUP_IDS.discard(old_id)
        return False


def _ensure_room_finished(call_id: str, call: dict, reason: str = "room_finished_auto") -> bool:
    if call.get("last_event") == "room_finished":
        return False
    call["last_event"] = "room_finished"
    call["ended_at"] = call.get("ended_at") or int(time.time())
    try:
        for pdata in (call.get("participants") or {}).values():
            if isinstance(pdata, dict):
                pdata["state"] = "DISCONNECTED"
                now = int(time.time())
                pdata.setdefault("left_at_ts", now)
                pdata.setdefault("last_seen_ts", now)
    except Exception:
        pass
    try:
        for rec in (call.get("recordings") or []):
            if isinstance(rec, dict) and not rec.get("status"):
                rec["status"] = "stopped"
    except Exception:
        pass
    call.setdefault("timeline", []).append({"t": int(time.time()), "event": reason})
    return True


def _convert_ws_to_http(url: str) -> str:
    if not url:
        return ""
    if url.startswith("wss://"):
        return "https://" + url[len("wss://"):]
    if url.startswith("ws://"):
        return "http://" + url[len("ws://"):]
    return url


def _get_livekit_api_creds() -> tuple[str, str, str] | tuple[()]:
    try:
        defaults = _load_defaults() or {}
    except Exception:
        defaults = {}
    lk_def = (defaults or {}).get("livekit", {}) or {}
    lk_url = str(lk_def.get("url") or os.getenv("LIVEKIT_URL", "") or "").strip()
    lk_key = str(lk_def.get("api_key") or os.getenv("LIVEKIT_API_KEY", "") or "").strip()
    lk_secret = str(lk_def.get("api_secret") or os.getenv("LIVEKIT_API_SECRET", "") or "").strip()
    http_url = _convert_ws_to_http(lk_url)
    if not (http_url and lk_key and lk_secret):
        return ()
    return http_url, lk_key, lk_secret


def _clear_dispatch_rule_agents_async() -> None:
    creds = _get_livekit_api_creds()
    if not creds:
        return
    http_url, lk_key, lk_secret = creds

    async def _runner() -> None:
        client = lk_api.LiveKitAPI(url=http_url, api_key=lk_key, api_secret=lk_secret)
        try:
            try:
                list_req = lk_api.ListSIPDispatchRuleRequest()
            except Exception:
                list_req = sip_proto.ListSIPDispatchRuleRequest()
            try:
                resp = await client.sip.list_sip_dispatch_rule(list_req)
            except Exception:
                resp = None
            if not resp:
                return
            for info in getattr(resp, "items", []):
                room_config = getattr(info, "room_config", None)
                agents = list(getattr(room_config, "agents", []) or [])
                if not agents:
                    continue
                scrubbed = sip_proto.SIPDispatchRuleInfo()
                scrubbed.CopyFrom(info)
                try:
                    scrubbed.room_config.ClearField("agents")
                except Exception:
                    try:
                        del scrubbed.room_config.agents[:]
                    except Exception:
                        scrubbed.room_config.agents[:] = []
                agent_names = []
                try:
                    agent_names = [getattr(agent, "agent_name", "") for agent in agents]
                except Exception:
                    agent_names = []
                try:
                    await client.sip.update_sip_dispatch_rule(
                        info.sip_dispatch_rule_id, scrubbed
                    )
                    if agent_names:
                        print(
                            f"[dispatch] Removed LiveKit auto agents for rule {info.name or info.sip_dispatch_rule_id}: {agent_names}",
                            file=sys.stderr,
                            flush=True,
                        )
                except Exception:
                    continue
        finally:
            await client.aclose()

    def _sync_runner() -> None:
        try:
            asyncio.run(_runner())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(_runner())
            finally:
                loop.close()
        except Exception:
            pass

    try:
        t = threading.Thread(target=_sync_runner, daemon=True)
        t.start()
    except Exception:
        pass


async def _create_dispatch_async(
    http_url: str,
    lk_key: str,
    lk_secret: str,
    agent_name: str,
    room_name: str,
    metadata: str | None = None,
    max_attempts: int = 5,
    retry_delay: float = 0.6,
) -> None:
    """
    Create an AgentDispatch entry, retrying until the worker registers.
    LiveKit docs: https://docs.livekit.io/agents/server/agent-dispatch/#explicit
    """
    client = lk_api.LiveKitAPI(url=http_url, api_key=lk_key, api_secret=lk_secret)
    last_error: Exception | None = None
    try:
        try:
            req = lk_api.CreateAgentDispatchRequest(agent_name=agent_name, room=room_name, metadata=metadata or "")
        except Exception:
            from livekit.protocol.agent_dispatch import CreateAgentDispatchRequest as _Req  # type: ignore
            req = _Req(agent_name=agent_name, room=room_name, metadata=metadata or "")
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            try:
                await client.agent_dispatch.create_dispatch(req)
                print(
                    f"[dispatch] Created agent dispatch room={room_name} agent={agent_name} attempt={attempt}",
                    file=sys.stderr,
                    flush=True,
                )
                return
            except Exception as exc:
                last_error = exc
                code = None
                if lk_twirp_client and isinstance(exc, getattr(lk_twirp_client, "TwirpError", tuple())):
                    code = getattr(exc, "code", None)
                if attempt >= max_attempts:
                    break
                print(
                    f"[dispatch] Failed creating dispatch (attempt {attempt}) room={room_name} agent={agent_name} code={getattr(exc, 'code', None)} err={exc}",
                    file=sys.stderr,
                    flush=True,
                )
                await asyncio.sleep(retry_delay)
        if last_error:
            raise last_error
    finally:
        await client.aclose()


def _create_agent_dispatch(room_name: str | None, agent_name: str | None, metadata: dict | None = None) -> None:
    if not room_name or not agent_name:
        return
    try:
        d = _load_defaults() or {}
    except Exception:
        d = {}
    lk_def = (d or {}).get("livekit", {}) or {}
    lk_url = str(lk_def.get("url") or os.getenv("LIVEKIT_URL", "") or "").strip()
    lk_key = str(lk_def.get("api_key") or os.getenv("LIVEKIT_API_KEY", "") or "").strip()
    lk_secret = str(lk_def.get("api_secret") or os.getenv("LIVEKIT_API_SECRET", "") or "").strip()
    http_url = _convert_ws_to_http(lk_url)
    if not (http_url and lk_key and lk_secret):
        return
    async def _runner():
        meta_str = ""
        if metadata:
            try:
                meta_str = json.dumps(metadata, ensure_ascii=False)
            except Exception:
                pass
        await _create_dispatch_async(http_url, lk_key, lk_secret, agent_name, room_name, meta_str or None)
    try:
        asyncio.run(_runner())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_runner())
        finally:
            loop.close()
    except Exception:
        pass


def _auto_finalize_call_if_idle(call_id: str, call: dict) -> bool:
    # Grace period: Don't finalize newly created calls immediately (allow 15s for joins)
    created_at = call.get("created_at") or 0
    if time.time() - created_at < 15:
        return False

    if _call_has_active_participants(call):
        return False
    if _call_has_active_recordings(call):
        return False
    changed = _ensure_room_finished(call_id, call)
    if changed:
        print(f"[webhook] synthesized room_finished for {call_id}", file=sys.stderr, flush=True)
    return changed


def _ensure_storage() -> None:
    ad = _agents_dir()
    ad.mkdir(parents=True, exist_ok=True)
    use_files = os.getenv("STORAGE_BACKEND", "json").strip().lower() != "postgres"
    if use_files:
        agents_file = ad / "agents.json"
        if not agents_file.exists():
            agents_file.write_text(json.dumps({"agents": [], "active_agent_id": None}, indent=2), encoding="utf-8")
        active_file = ad / "active.json"
        if not active_file.exists():
            active_file.write_text(json.dumps({}, indent=2), encoding="utf-8")
        users_file = ad / "users.json"
        if not users_file.exists():
            users_file.write_text(json.dumps({"users": []}, indent=2), encoding="utf-8")
        sessions_file = ad / "sessions.json"
        if not sessions_file.exists():
            sessions_file.write_text(json.dumps({"sessions": {}}, indent=2), encoding="utf-8")
    uploads_dir = Path(__file__).parent
    (uploads_dir / "transcripts").mkdir(parents=True, exist_ok=True)
    (uploads_dir / "greetings").mkdir(parents=True, exist_ok=True)
    if use_files:
        numbers_file = ad / "numbers.json"
        if not numbers_file.exists():
            numbers_file.write_text(json.dumps({"routes": {}}, indent=2), encoding="utf-8")
        twilio_file = ad / "twilio_accounts.json"
        if not twilio_file.exists():
            twilio_file.write_text(json.dumps({"accounts": [], "numbers": {}}, indent=2), encoding="utf-8")

def _can_access_agent(user: dict, agent: dict) -> bool:
    try:
        if not isinstance(user, dict) or not isinstance(agent, dict):
            return False
        if user.get("role") in ["admin", "subscriber", "convix"]:
            return True
        return agent.get("owner_id") == user.get("id")
    except Exception:
        return False

def _get_agent(agent_id: str) -> dict | None:
    data = _load_all()
    for a in data.get("agents", []):
        if a.get("id") == agent_id:
            return a
    return None

def _synthesize_greeting_async(agent_obj: dict, agent_id: str, *, background: bool = True) -> None:
    """Generate greeting audio for agent_obj in a background thread.

    Writes an audio file to greetings/{agent_id}.(mp3|opus) and updates
    agents.json and active.json with assets.greeting_audio when complete.
    """
    try:
        greeting_text = ((agent_obj or {}).get("greeting", {}) or {}).get("text") or ""
        if not greeting_text:
            return

        # Determine whether to use OpenAI realtime voice or Fish TTS
        realtime_cfg = (agent_obj or {}).get("realtime", {}) if isinstance((agent_obj or {}).get("realtime"), dict) else {}
        use_openai_voice = bool(realtime_cfg.get("enabled")) and str(realtime_cfg.get("tts_provider", "openai")).lower() == "openai"

        # Paths helper
        def _out_paths(fmt: str) -> tuple[str, Path]:
            rel = f"greetings/{agent_id}.{fmt}"
            return rel, Path(__file__).parent / rel

        def _persist_assets(primary_rel: str, preview_rel: str | None = None) -> None:
            # Update agents.json
            try:
                data = _load_all()
                changed = False
                for i, a in enumerate(data.get("agents", [])):
                    if a.get("id") == agent_id:
                        a.setdefault("assets", {})["greeting_audio"] = primary_rel
                        if preview_rel:
                            a["assets"]["greeting_audio_preview"] = preview_rel
                        data["agents"][i] = a
                        changed = True
                        break
                if changed:
                    _save_all(data)
            except Exception:
                pass
            # Update active agent entries via storage backend
            try:
                amap = _storage_backend().load_active_agent()
                for uid, prof in list(amap.items()):
                    if isinstance(prof, dict) and (prof.get("id") == agent_id or (agent_id == "active")):
                        prof.setdefault("assets", {})["greeting_audio"] = primary_rel
                        if preview_rel:
                            prof["assets"]["greeting_audio_preview"] = preview_rel
                        amap[uid] = prof
                _storage_backend().save_active_agent(amap)
            except Exception:
                pass

        def _worker_openai() -> None:
            try:
                # Resolve OpenAI API key/base_url and voice
                d = _load_defaults()
                openai_key = os.getenv("OPENAI_API_KEY") or ((d.get("openai", {}) or {}).get("api_key") if isinstance(d, dict) else None) or ""
                base_url = ((d.get("openai", {}) or {}).get("base_url") if isinstance(d, dict) else None) or None
                voice = realtime_cfg.get("voice") or ((d.get("realtime", {}) or {}).get("voice") if isinstance(d, dict) else None) or "verse"
                if not openai_key:
                    # Without a key, skip to Fish TTS fallback
                    _worker_fish()
                    return
                from openai import OpenAI  # type: ignore
                client = OpenAI(api_key=openai_key, base_url=base_url) if base_url else OpenAI(api_key=openai_key)

                # 1) Generate WAV for call playout (widely supported by decoder)
                wav_rel, wav_abs = _out_paths("wav")
                try:
                    with client.audio.speech.with_streaming_response.create(
                        model="gpt-4o-mini-tts",
                        voice=str(voice or "verse"),
                        input=greeting_text,
                        format="wav",
                    ) as resp:
                        resp.stream_to_file(str(wav_abs))
                except Exception:
                    audio_resp = client.audio.speech.create(
                        model="gpt-4o-mini-tts",
                        voice=str(voice or "verse"),
                        input=greeting_text,
                        format="wav",
                    )
                    try:
                        with open(wav_abs, "wb") as f:
                            f.write(audio_resp.content if hasattr(audio_resp, "content") else audio_resp)  # type: ignore
                    except Exception:
                        pass

                # 2) Generate MP3 for UI preview
                mp3_rel, mp3_abs = _out_paths("mp3")
                try:
                    with client.audio.speech.with_streaming_response.create(
                        model="gpt-4o-mini-tts",
                        voice=str(voice or "verse"),
                        input=greeting_text,
                        format="mp3",
                    ) as resp:
                        resp.stream_to_file(str(mp3_abs))
                except Exception:
                    audio_resp2 = client.audio.speech.create(
                        model="gpt-4o-mini-tts",
                        voice=str(voice or "verse"),
                        input=greeting_text,
                        format="mp3",
                    )
                    try:
                        with open(mp3_abs, "wb") as f:
                            f.write(audio_resp2.content if hasattr(audio_resp2, "content") else audio_resp2)  # type: ignore
                    except Exception:
                        pass

                _persist_assets(wav_rel, mp3_rel)
            except Exception:
                # If OpenAI path fails for any reason, try Fish as a fallback
                try:
                    _worker_fish()
                except Exception:
                    pass

        def _worker_fish() -> None:
            try:
                tts_cfg = (agent_obj or {}).get("tts", {}) if isinstance((agent_obj or {}).get("tts"), dict) else {}
                voice_id = tts_cfg.get("reference_id") or os.getenv("DEFAULT_VOICE", "")
                api_key = tts_cfg.get("api_key") or os.getenv("FISH_AUDIO_API_KEY", "")
                if not (voice_id and api_key):
                    return
                from fish_tts_provider import FishSpeechTTS
                tts = FishSpeechTTS(
                    api_key=api_key,
                    reference_id=voice_id,
                    model=tts_cfg.get("model", os.getenv("FISH_AUDIO_MODEL", "s1")),
                    response_format="opus",
                    latency=tts_cfg.get("latency", os.getenv("FISH_AUDIO_LATENCY", "low")),
                    temperature=float(tts_cfg.get("temperature", os.getenv("FISH_AUDIO_TEMPERATURE", "0.7"))),
                    top_p=float(tts_cfg.get("top_p", os.getenv("FISH_AUDIO_TOP_P", "0.7"))),
                    speed=float(tts_cfg.get("speed", os.getenv("FISH_AUDIO_SPEED", "1.0"))),
                    volume_db=float(tts_cfg.get("volume_db", os.getenv("FISH_AUDIO_VOLUME_DB", "0.0"))),
                    frame_size_ms=int(tts_cfg.get("frame_size_ms", 40)),
                    emotion_prefix=(tts_cfg.get("emotion_prefix") or None),
                )
                # OPUS for calls
                opus_rel, opus_abs = _out_paths("opus")
                asyncio.run(tts.synthesize_to_file(greeting_text, str(opus_abs)))
                # MP3 for preview
                try:
                    tts_mp3 = FishSpeechTTS(
                        api_key=api_key,
                        reference_id=voice_id,
                        model=tts_cfg.get("model", os.getenv("FISH_AUDIO_MODEL", "s1")),
                        response_format="mp3",
                        latency=tts_cfg.get("latency", os.getenv("FISH_AUDIO_LATENCY", "low")),
                        temperature=float(tts_cfg.get("temperature", os.getenv("FISH_AUDIO_TEMPERATURE", "0.7"))),
                        top_p=float(tts_cfg.get("top_p", os.getenv("FISH_AUDIO_TOP_P", "0.7"))),
                        speed=float(tts_cfg.get("speed", os.getenv("FISH_AUDIO_SPEED", "1.0"))),
                        volume_db=float(tts_cfg.get("volume_db", os.getenv("FISH_AUDIO_VOLUME_DB", "0.0"))),
                        frame_size_ms=int(tts_cfg.get("frame_size_ms", 40)),
                        emotion_prefix=(tts_cfg.get("emotion_prefix") or None),
                    )
                    mp3_rel, mp3_abs = _out_paths("mp3")
                    asyncio.run(tts_mp3.synthesize_to_file(greeting_text, str(mp3_abs)))
                except Exception:
                    mp3_rel = None
                _persist_assets(opus_rel, mp3_rel)
            except Exception:
                # best-effort
                pass

        def _run(worker):
            if background:
                threading.Thread(target=worker, daemon=True).start()
            else:
                worker()

        # Dispatch to selected provider
        if use_openai_voice:
            _run(_worker_openai)
        else:
            _run(_worker_fish)
    except Exception:
        pass

def _load_all():
    _ensure_storage()
    return _storage_backend().load_agents()

def _numbers_path() -> Path:
    return _agents_dir() / "numbers.json"

def _load_numbers() -> dict:
    _ensure_storage()
    return _storage_backend().load_numbers()

def _save_numbers(data: dict) -> None:
    _storage_backend().save_numbers(data or {"routes": {}})

def _save_all(data):
    _storage_backend().save_agents(data)

# ------------------ TWILIO MULTI-TENANT STORAGE ------------------
def _twilio_store_path() -> Path:
    return _agents_dir() / "twilio_accounts.json"

def _load_twilio_store() -> dict:
    _ensure_storage()
    return _storage_backend().load_twilio_store()

def _save_twilio_store(data: dict) -> None:
    _storage_backend().save_twilio_store(data or {"accounts": [], "numbers": {}})

def _digits_only(raw: str) -> str:
    return re.sub(r"[^0-9]", "", str(raw or ""))

def _normalize_e164(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return s
    d = _digits_only(s)
    if s.startswith("+"):
        return "+" + d
    # Assume US default if 10 digits; otherwise just return digits
    if len(d) == 10:
        return "+1" + d
    if d:
        return "+" + d if not s.startswith("+") else s
    return s


def _first_nonempty(src: dict, keys: list[str]) -> str | None:
    """Return the first non-empty string value for any key in `keys`."""
    for key in keys:
        if key in src:
            val = src.get(key)
            if val is None:
                continue
            sval = str(val).strip()
            if sval:
                return sval
    return None


def _extract_call_metadata(payload: dict, participant: dict) -> tuple[dict, str | None, str | None, list[str]]:
    """Aggregate call metadata (SIP/Web/Twilio) and derive dialed/caller numbers with candidates."""
    meta: dict[str, Any] = {}

    def _merge_flat(source: dict | None, prefix: str = "") -> None:
        if not isinstance(source, dict):
            return
        for key, value in source.items():
            full_key = f"{prefix}{key}" if prefix else str(key)
            if isinstance(value, dict):
                _merge_flat(value, f"{full_key}.")
                continue
            if value is None:
                continue
            meta.setdefault(full_key, value)

    # Core LiveKit call metadata
    _merge_flat(payload.get("call"))
    # SIP specific metadata (store both prefixed and plain keys)
    sip_call = (payload.get("sip") or {}).get("call") or {}
    _merge_flat(sip_call)
    for key, value in (sip_call or {}).items():
        if value is None:
            continue
        meta.setdefault(f"sip.{key}", value)
    # Twilio bridge metadata if provided
    _merge_flat((payload.get("twilio") or {}).get("call"))
    _merge_flat(payload.get("twilio"))
    # Ingress/egress info can contain origin/destination
    _merge_flat(payload.get("ingress"))
    _merge_flat(payload.get("egress"))
    # Participant attributes (keep both prefixed and stripped variants)
    attrs = (participant or {}).get("attributes") or {}
    for key, value in attrs.items():
        if value is None:
            continue
        meta.setdefault(str(key), value)
        if key.startswith("sip.") and len(key) > 4:
            meta.setdefault(key[4:], value)

    dialed = _first_nonempty(
        meta,
        [
            "dialedNumber",
            "trunkPhoneNumber",
            "calledNumber",
            "calleeNumber",
            "to",
            "destination",
            "sip.calledNumber",
            "sip.calleeNumber",
            "sip.trunkPhoneNumber",
            "call.to",
            "twilio.To",
        ],
    )
    caller = _first_nonempty(
        meta,
        [
            "callerNumber",
            "from",
            "phoneNumber",
            "caller",
            "source",
            "sip.phoneNumber",
            "call.from",
            "twilio.From",
        ],
    )

    number_candidates: list[str] = []
    dialed_tokens = ("dialed", "callee", "called", "to", "destination", "target")
    caller_tokens = ("caller", "from", "phone", "source", "origin", "ani")

    for key, value in list(meta.items()):
        try:
            text = str(value)
        except Exception:
            continue
        matches = re.findall(r"[+]?\d{6,}", text)
        if not matches:
            continue
        for match in matches:
            if match not in number_candidates:
                number_candidates.append(match)
            key_lower = str(key).lower()
            if not dialed and any(tok in key_lower for tok in dialed_tokens):
                dialed = match
            elif not caller and any(tok in key_lower for tok in caller_tokens):
                caller = match

    # Fallback: extract from participant identity (e.g., sip_+971523368049)
    identity = str((participant or {}).get("identity") or "")
    if identity:
        match = re.search(r"([+]?\d{6,})", identity)
        if match:
            num = match.group(1)
            if identity.lower().startswith(("sip_", "tel_", "twilio_", "caller_")):
                if not caller:
                    caller = num
            elif not dialed:
                dialed = num
            if num not in number_candidates:
                number_candidates.append(num)

    # Fallback from room name (e.g., call-_+9715xxx)
    room_name = (payload.get("room") or {}).get("name")
    if isinstance(room_name, str):
        match = re.search(r"([+]?\d{6,})", room_name)
        if match:
            cand = match.group(1)
            if not dialed:
                dialed = cand
            if cand not in number_candidates:
                number_candidates.append(cand)

    return meta, dialed, caller, number_candidates


def _candidate_route_keys(number: str) -> list[tuple[str, str]]:
    """Generate potential lookup keys for a dialed number."""
    out: list[tuple[str, str]] = []
    if not number:
        return out
    e164 = _normalize_e164(number)
    digits = _digits_only(number)
    digits_no_leading_zero = digits.lstrip("0") if digits else ""
    if e164:
        out.append(("direct_e164", e164))
    if digits:
        out.append(("digits", digits))
    if digits_no_leading_zero and digits_no_leading_zero != digits:
        out.append(("digits_no_leading_zero", digits_no_leading_zero))
    # Heuristic: UAE local format 0XXXXXXXXX → +971XXXXXXXXX
    if digits and digits.startswith("0") and len(digits) == 10:
        ua_digits = "971" + digits[1:]
        out.append(("uae_local_to_e164_digits", ua_digits))
        out.append(("uae_local_to_e164", f"+{ua_digits}"))
    return out


def _collect_call_stats(call: dict, payload: dict) -> None:
    """Aggregate available stats sections from a LiveKit webhook payload into the call record."""
    try:
        stats = call.setdefault("stats", {})

        def _merge(slot: str, data: dict | None) -> None:
            if isinstance(data, dict) and data:
                slot_map = stats.setdefault(slot, {})
                slot_map.update(data)

        _merge("root", payload.get("stats"))
        _merge("call", (payload.get("call") or {}).get("stats"))
        _merge("participant", (payload.get("participant") or {}).get("stats"))
        _merge("ingress", (payload.get("ingress") or {}).get("stats"))
        _merge("egress", (payload.get("egress") or {}).get("stats"))
        _merge("session", (payload.get("session") or {}).get("stats"))
    except Exception:
        pass

def _twilio_get_accounts_for_user(user_id: str) -> list[dict]:
    store = _load_twilio_store()
    out = []
    for acc in store.get("accounts", []):
        try:
            if acc.get("user_id") == user_id:
                out.append(acc)
        except Exception:
            continue
    return out

def _twilio_get_account_by_id(account_id: str) -> dict | None:
    store = _load_twilio_store()
    for acc in store.get("accounts", []):
        if acc.get("id") == account_id:
            return acc
    return None

def _twilio_auth_token_for_number(number: str) -> str | None:
    """Resolve the Twilio auth token for the owner of a given phone number."""
    try:
        num_key = _normalize_e164(number)
        
        # 1. Check multi-tenant storage (users/subscribers)
        store = _load_twilio_store()
        entry = (store.get("numbers", {}) or {}).get(num_key)
        if not entry:
            entry = (store.get("numbers", {}) or {}).get(_digits_only(num_key))
        if entry:
            acc_id = entry.get("account_id")
            if acc_id:
                acc = _twilio_get_account_by_id(acc_id)
                if acc:
                    return acc.get("auth_token")
                    
        # 2. Check global admin routes (numbers.json) -> fallback to env vars
        # If the number is in the global admin routing table but not in the multi-tenant store,
        # it implies it uses the system-level Twilio account.
        admin_routes = (_load_numbers() or {}).get("routes", {})
        if num_key in admin_routes or _digits_only(num_key) in admin_routes:
            return os.getenv("TWILIO_AUTH_TOKEN")
            
    except Exception:
        pass
    
    # 3. Last resort fallback to env (e.g. for unmapped numbers hitting the webhook)
    # Only if number is NOT mapped to a user.
    admin_routes = (_load_numbers() or {}).get("routes", {})
    is_admin_route = num_key in admin_routes or _digits_only(num_key) in admin_routes
    
    if not entry and not is_admin_route:
        # Number is unknown/unmapped. Use system env for validation?
        # If we allow unmapped inbound calls (e.g. new numbers), system env is the only choice.
        return os.getenv("TWILIO_AUTH_TOKEN")
        
    if is_admin_route:
        return os.getenv("TWILIO_AUTH_TOKEN")
        
    return None

def _load_routes_union() -> dict:
    """Merge global numbers.json routes with per-user Twilio numbers mapping.
    Per-user mappings take precedence on conflict.
    Returns { number: { agent_id, trunk, user_id?, account_id? } }
    """
    routes = {}
    try:
        gcur = (_load_numbers() or {}).get("routes", {})
        for k, v in (gcur or {}).items():
            routes[str(k)] = {"agent_id": (v or {}).get("agent_id"), "trunk": (v or {}).get("trunk")}
    except Exception:
        pass
    try:
        store = _load_twilio_store() or {}
        for k, v in (store.get("numbers") or {}).items():
            try:
                routes[str(k)] = {
                    "agent_id": (v or {}).get("agent_id"),
                    "trunk": (v or {}).get("trunk"),
                    "user_id": (v or {}).get("user_id"),
                    "account_id": (v or {}).get("account_id"),
                }
            except Exception:
                continue
    except Exception:
        pass
    return routes

def _looks_like_agent_profile(obj: dict) -> bool:
    return isinstance(obj, dict) and (
        ("name" in obj and "instructions" in obj)
        or ("llm" in obj and "tts" in obj)
        or ("id" in obj and ("greeting" in obj or "behavior" in obj))
    )

def _set_active_global(agent: dict | None) -> None:
    """Store a single global active agent profile."""
    _ensure_storage()
    _storage_backend().save_active_agent(agent or {})

def _load_active_global() -> dict:
    """Load the single global active agent. If file contains a legacy per-user map,
    return the first agent-shaped value.
    """
    _ensure_storage()
    raw = _storage_backend().load_active_agent()
    if _looks_like_agent_profile(raw):
        return raw
    if isinstance(raw, dict) and raw:
        # Legacy map: { user_id: { ...agent } } → pick first agent-like value
        for v in raw.values():
            if isinstance(v, dict):
                return v
    return {}

def _set_active_for_user(user_id: str, agent: dict | None):
    # Backward-compatible wrapper: now we use a single global active agent
    _set_active_global(agent)

def _load_active_for_user(user_id: str):
    # Backward-compatible wrapper: load the single global active agent
    return _load_active_global()

def _users_path() -> Path:
    return _agents_dir() / "users.json"

def _sessions_path() -> Path:
    return _agents_dir() / "sessions.json"

# ------------------ DEFAULTS (GLOBAL) ------------------
def _defaults_path() -> Path:
    return _agents_dir() / "default.json"

def _load_defaults() -> dict:
    _ensure_storage()
    return _storage_backend().load_defaults()

def _save_defaults(data: dict) -> None:
    _storage_backend().save_defaults(data or {})

# ------------------ VOICES STORAGE ------------------
def _voices_path() -> Path:
    return _agents_dir() / "voices.json"

def _ensure_voices_file() -> None:
    _ensure_storage()
    p = _voices_path()
    if not p.exists():
        p.write_text(json.dumps({"voices": []}, indent=2), encoding="utf-8")

def _load_voices() -> dict:
    _ensure_storage()
    return _storage_backend().load_voices()

def _save_voices(data: dict) -> None:
    _storage_backend().save_voices(data or {"voices": []})

# ------------------ LIVEKIT WEBHOOK STORAGE ------------------
def _livekit_store_path() -> Path:
    return _agents_dir() / "livekit_store.json"

def _load_livekit_store() -> dict:
    _ensure_storage()
    return _storage_backend().load_livekit_store()

def _save_livekit_store(data: dict) -> None:
    _storage_backend().save_livekit_store(data or {"calls": {}})

def _with_livekit_store(mutator):
    """Atomically load, mutate, and persist the LiveKit webhook store in one step.

    IMPORTANT: This must be atomic because webhooks arrive concurrently.
    Use the storage backend's mutate_livekit_store() which includes locking.
    """
    try:
        backend = _storage_backend()
        if backend and hasattr(backend, "mutate_livekit_store"):
            return backend.mutate_livekit_store(mutator)
    except Exception:
        # Fall back to non-atomic path only if backend mutation is unavailable.
        pass
    store = _load_livekit_store()
    try:
        mutator(store)
    finally:
        try:
            _save_livekit_store(store)
        except Exception:
            pass
    return store

def _load_users() -> dict:
    _ensure_storage()
    return _storage_backend().load_users()

def _load_users_cached() -> dict:
    _ensure_storage()
    return _cached_kv("users", lambda: _storage_backend().load_users())

def _save_users(data: dict) -> None:
    payload = data or {"users": []}
    _storage_backend().save_users(payload)
    _invalidate_cache("users")

def _get_user_by_id(user_id: str) -> dict | None:
    try:
        for u in _load_users().get("users", []):
            if u.get("id") == user_id:
                return u
    except Exception:
        return None
    return None

def _load_sessions() -> dict:
    _ensure_storage()
    return _storage_backend().load_sessions()

def _load_sessions_cached() -> dict:
    _ensure_storage()
    return _cached_kv("sessions", lambda: _storage_backend().load_sessions())

def _save_sessions(data: dict) -> None:
    payload = data or {"sessions": {}, "resets": {}}
    _storage_backend().save_sessions(payload)
    _invalidate_cache("sessions")

def _create_reset_token(user_id: str, ttl_seconds: int = 900) -> str:
    sess = _load_sessions()
    token = uuid.uuid4().hex
    resets = sess.setdefault("resets", {})
    resets[token] = {"user_id": user_id, "exp": int(time.time()) + ttl_seconds}
    _save_sessions(sess)
    return token

def _consume_reset_token(token: str) -> str | None:
    sess = _load_sessions()
    entry = sess.get("resets", {}).get(token)
    if not entry:
        return None
    if int(time.time()) > int(entry.get("exp", 0)):
        # expired
        del sess["resets"][token]
        _save_sessions(sess)
        return None
    user_id = entry.get("user_id")
    del sess["resets"][token]
    _save_sessions(sess)
    return user_id

def _load_usage() -> dict:
    _ensure_storage()
    return _storage_backend().load_usage()

def _save_usage(data: dict) -> None:
    _storage_backend().save_usage(data or {"usage": {}})

def _load_subs() -> dict:
    _ensure_storage()
    return _storage_backend().load_subscriptions()

def _save_subs(data: dict) -> None:
    _storage_backend().save_subscriptions(data or {"subs": []})

def _issue_token(user_id: str) -> str:
    sessions = _load_sessions()
    token = uuid.uuid4().hex
    sessions.setdefault("sessions", {})[token] = {"user_id": user_id, "issued_at": int(time.time())}
    _save_sessions(sessions)
    return token

def _get_user_by_token(token: str) -> dict | None:
    if not token:
        return None
    sessions = _load_sessions_cached().get("sessions", {})
    entry = sessions.get(token)
    if not entry:
        return None
    users = _load_users_cached().get("users", [])
    for u in users:
        if u.get("id") == entry.get("user_id"):
            return u
    return None

def require_auth(admin: bool = False, subscriber: bool = False):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            auth = request.headers.get("Authorization", "")
            token = auth.split(" ")[-1] if auth.startswith("Bearer ") else auth
            user = _get_user_by_token(token)
            if not user:
                return jsonify({"error": "unauthorized"}), 401
            role = user.get("role")
            if admin and role not in ["admin", "subscriber", "convix"]:
                return jsonify({"error": "forbidden"}), 403
            if subscriber and role not in ["admin", "subscriber", "convix"]:
                return jsonify({"error": "forbidden"}), 403
            g.current_user = user
            return fn(*args, **kwargs)
        return wrapper
    return decorator

def require_internal():
    """Require an internal shared secret for backend-only endpoints.

    Checks header X-Internal-Key against env INTERNAL_API_KEY. Returns 403
    when missing/mismatched. Returns 500 if not configured to avoid
    accidentally exposing the endpoint without protection.
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            expected = os.getenv("INTERNAL_API_KEY", "")
            if not expected:
                return jsonify({"error": "internal_key_not_configured"}), 500
            provided = request.headers.get("X-Internal-Key") or request.headers.get("x-internal-key")
            if str(provided or "") != str(expected):
                return jsonify({"error": "forbidden"}), 403
            return fn(*args, **kwargs)
        return wrapper
    return decorator

def _internal_key_matches() -> bool:
    try:
        expected = os.getenv("INTERNAL_API_KEY", "")
        if not expected:
            return False
        provided = request.headers.get("X-Internal-Key") or request.headers.get("x-internal-key")
        return str(provided or "") == str(expected)
    except Exception:
        return False

"""Legacy plan/usage/subscription logic fully removed."""

# ------------------ EMBED API ------------------
@app.post("/embed/create")
@require_auth()
def embed_create():
    """Create an embeddable session and return an iframe URL.
    Payload: { agent_id: string, room?: string, title?: string, theme?: string, ttl_seconds?: number }
    Response: { sid, room, iframe_url, expires_at }
    """
    payload = request.json or {}
    agent_id = (payload.get("agent_id") or "").strip()
    if not agent_id:
        return jsonify({"error": "agent_id_required"}), 400
    room = (payload.get("room") or f"embed-{_uuid.uuid4().hex[:8]}").strip()
    title = (payload.get("title") or "").strip()
    theme = (payload.get("theme") or "").strip()
    ttl_seconds = payload.get("ttl_seconds")
    rec = _embed_create_session(agent_id, room, title, theme, ttl_seconds)
    # Build iframe URL on this host
    try:
        base = (os.getenv("PUBLIC_BASE_URL") or request.host_url).rstrip("/")
    except Exception:
        base = (request.host_url if hasattr(request, "host_url") else "").rstrip("/")
    iframe_url = f"{base}/embed/widget?sid={rec['sid']}"
    return jsonify({
        "sid": rec["sid"],
        "room": room,
        "iframe_url": iframe_url,
        "expires_at": int(rec["exp"]),
    })

@app.get("/embed/token/<sid>")
def embed_token(sid: str):
    """Issue a LiveKit token for the embed widget and spawn the agent in background if needed.

    This endpoint is intentionally unauthenticated; possession of the time-limited sid is required.
    Optionally restrict by EMBED_ALLOWED_ORIGINS (comma-separated exact origins).
    """
    # Optional Origin allowlist
    try:
        allow = [x.strip() for x in str(os.getenv("EMBED_ALLOWED_ORIGINS", "")).split(",") if x.strip()]
        if allow:
            origin = request.headers.get("Origin") or ""
            if origin and origin not in allow:
                return jsonify({"error": "forbidden_origin"}), 403
    except Exception:
        pass

    rec = _embed_get_session(sid)
    if not rec:
        return jsonify({"error": "invalid_or_expired"}), 404

    # Resolve LiveKit config from agent-specific config or defaults
    try:
        data = _load_all()
        agent_conf = next((a for a in (data.get("agents") or []) if a.get("id") == rec.get("agent_id")), {})
    except Exception:
        agent_conf = {}
    d = _load_defaults()
    lk_cfg = (agent_conf or {}).get("livekit", {}) if isinstance(agent_conf, dict) else {}
    lk_def = (d or {}).get("livekit", {})
    lk_url = str(lk_cfg.get("url") or lk_def.get("url") or os.getenv("LIVEKIT_URL", ""))
    lk_key = str(lk_cfg.get("api_key") or lk_def.get("api_key") or os.getenv("LIVEKIT_API_KEY", ""))
    lk_secret = str(lk_cfg.get("api_secret") or lk_def.get("api_secret") or os.getenv("LIVEKIT_API_SECRET", ""))
    if not (lk_url and lk_key and lk_secret):
        return jsonify({"error": "missing_livekit_env"}), 500

    # Mint browser token
    identity = f"user-{_uuid.uuid4().hex[:6]}"
    try:
        token = lk_api.AccessToken(lk_key, lk_secret)
        token.with_identity(identity)
        token.with_name(identity)
        grants = lk_api.VideoGrants(
            room=rec["room"],
            room_join=True,
            can_publish=True,
            can_subscribe=True,
        )
        token.with_grants(grants)
        jwt = token.to_jwt()
    except Exception as e:
        return jsonify({"error": f"token_error: {e}"}), 500

    # Spawn agent once per session (best-effort)
    try:
        if not rec.get("spawned"):
            rec["spawned"] = True
            def _spawn() -> None:
                try:
                    env = os.environ.copy()
                    env.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", os.getenv("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python"))
                    env.setdefault("PYTHONUNBUFFERED", "1")
                    env["RUN_AS_WORKER"] = "1"
                    env["PLAYGROUND_ROOM_NAME"] = str(rec["room"]) 
                    env.setdefault("PLAYGROUND_IDENTITY", f"agent-{_uuid.uuid4().hex[:6]}")
                    env["AGENT_PROFILE_ID"] = str(rec["agent_id"]) 
                    env["LIVEKIT_URL"] = lk_url
                    env["LIVEKIT_API_KEY"] = lk_key
                    env["LIVEKIT_API_SECRET"] = lk_secret
                    try:
                        openai_def = (d or {}).get("openai", {})
                        if openai_def.get("api_key"):
                            env.setdefault("OPENAI_API_KEY", str(openai_def.get("api_key")))
                        if openai_def.get("base_url"):
                            env.setdefault("OPENAI_BASE_URL", str(openai_def.get("base_url")))
                    except Exception:
                        pass
                    env.setdefault("AGENT_NAME", f"embed-{rec['agent_id']}")
                    try:
                        env.setdefault("CONFIG_API_BASE", os.getenv("CONFIG_API_BASE", f"http://localhost:{os.getenv('CONFIG_API_PORT','5057')}"))
                    except Exception:
                        pass
                    agent_path = str((Path(__file__).parent / "agent.py").resolve())
                    try:
                        logs_dir = Path(__file__).parent / "logs"
                        logs_dir.mkdir(parents=True, exist_ok=True)
                        log_fp = open(str(logs_dir / f"embed_{rec['room']}.log"), "wb")
                    except Exception:
                        log_fp = None
                    if log_fp is not None:
                        proc = subprocess.Popen([sys.executable, agent_path], env=env, stdout=log_fp, stderr=subprocess.STDOUT)
                    else:
                        proc = subprocess.Popen([sys.executable, agent_path], env=env)
                    _bind_worker(rec["room"], proc, log_fp)
                except Exception:
                    pass
            try:
                threading.Thread(target=_spawn, daemon=True).start()
            except Exception:
                _spawn()
    except Exception:
        pass

    # Accounting
    try:
        rec["uses"] = int(rec.get("uses", 0)) + 1
    except Exception:
        pass

    return jsonify({
        "server_url": lk_url,
        "room": rec["room"],
        "token": jwt,
        "identity": identity,
        "title": rec.get("title") or "",
        "theme": rec.get("theme") or "",
    })

@app.get("/embed/widget")
def serve_embed_widget():
    """Serve the embeddable widget HTML. Requires ?sid=... in query."""
    base_dir = Path(__file__).parent / "embed"
    base_dir.mkdir(parents=True, exist_ok=True)
    try:
        return send_from_directory(str(base_dir), "widget.html")
    except Exception:
        # If file missing, return a minimal placeholder to avoid 404s
        html = (
            "<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<title>Agent Widget</title></head><body>Widget file not found.</body></html>"
        )
        return Response(html, mimetype="text/html")

@app.get("/embed/agents")
def serve_embed_agents():
    """Serve a minimal admin page to list and delete agents."""
    base_dir = Path(__file__).parent / "embed"
    base_dir.mkdir(parents=True, exist_ok=True)
    try:
        return send_from_directory(str(base_dir), "agents.html")
    except Exception:
        html = (
            "<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<title>Agents Admin</title></head><body>Page not found.</body></html>"
        )
        return Response(html, mimetype="text/html")

try:
    _duplicate_routes_with_prefix(app, API_PREFIX)
except Exception:
    # Function defined later; will be called again after definition.
    pass

def _deep_merge(dest: dict, src: dict) -> dict:
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dest.get(k), dict):
            dest[k] = _deep_merge(dest.get(k, {}), v)
        else:
            dest[k] = v
    return dest

@app.get("/agents")
@require_auth()
def list_agents():
    user = g.current_user
    is_admin = user.get("role") in ["admin", "subscriber", "convix"]
    has_internal = _internal_key_matches()
    data = _load_all()
    # Return all agents to all authenticated users, but include non-sensitive owner info for display
    users = _load_users().get("users", [])
    uid_to_owner = {u.get("id"): {"id": u.get("id"), "username": u.get("username"), "email": u.get("email"), "role": u.get("role")} for u in users}
    enriched = []
    changed_any = False
    for a in data.get("agents", []):
        try:
            owner = uid_to_owner.get(a.get("owner_id"))
            # Hide non-admin-owned agents from non-admin users by default (UI).
            # If internal key is provided, show own agents plus admin-owned.
            if not is_admin:
                # Show admin-owned agents and the current user's own agents
                if not ((owner and owner.get("role") in ["admin", "subscriber", "convix"]) or a.get("owner_id") == user.get("id")):
                    continue
            # Persistently remove deprecated fields from storage
            try:
                removed = False
                if "background_audio" in a:
                    del a["background_audio"]
                    removed = True
                if "language_policy" in a:
                    del a["language_policy"]
                    removed = True
                if removed:
                    changed_any = True
            except Exception:
                pass

            proj = {**a}
            # Mask MCP headers in list responses (never return tokens)
            try:
                mcp_cfg = (proj.get("mcp") or {}) if isinstance(proj, dict) else {}
                servers = (mcp_cfg.get("servers") or []) if isinstance(mcp_cfg, dict) else []
                masked = []
                for s in servers:
                    if not isinstance(s, dict):
                        continue
                    s2 = {k: v for k, v in s.items() if k != "headers"}
                    if s.get("headers"):
                        s2["has_headers"] = True
                    masked.append(s2)
                if masked:
                    proj.setdefault("mcp", {})["servers"] = masked
            except Exception:
                pass
            if owner:
                proj["owner"] = owner
            # Frontend hint: who can delete this agent under current policy
            proj["can_delete"] = bool(is_admin or a.get("owner_id") == user.get("id"))
            enriched.append(proj)
        except Exception:
            enriched.append(a)
    # Apply storage cleanup if we removed deprecated fields
    try:
        if changed_any:
            _save_all(data)
    except Exception:
        pass

    # include single global active id for backwards compatibility
    active = _load_active_global()
    return jsonify({"agents": enriched, "active_agent_id": active.get("id") if isinstance(active, dict) else None})


@app.post("/agents")
@require_auth()
def create_agent():
    user = g.current_user
    if user.get("role") not in ["admin", "subscriber", "convix"] and not _internal_key_matches():
        return jsonify({"error": "forbidden"}), 403
    payload = request.json or {}
    data = _load_all()
    agent_id = payload.get("id") or uuid.uuid4().hex
    payload["id"] = agent_id
    payload["owner_id"] = user.get("id")
    data["agents"].append(payload)
    _save_all(data)
    # Generate greeting audio asynchronously best-effort using thread
    _synthesize_greeting_async(payload, agent_id)
    return jsonify(payload), 201


@app.post("/internal/agents")
@require_auth()
@require_internal()
def internal_create_agent():
    """Backend-only: create an agent owned by the authenticated user.

    Protected by X-Internal-Key to prevent UI-triggered creation for non-admins.
    """
    user = g.current_user
    payload = request.json or {}
    data = _load_all()
    agent_id = payload.get("id") or uuid.uuid4().hex
    payload["id"] = agent_id
    payload["owner_id"] = user.get("id")
    data.setdefault("agents", []).append(payload)
    _save_all(data)
    _synthesize_greeting_async(payload, agent_id)
    return jsonify(payload), 201


@app.put("/agents/<agent_id>")
@require_auth()
def update_agent(agent_id):
    user = g.current_user
    is_admin = user.get("role") in ["admin", "subscriber", "convix"]
    if not is_admin and not _internal_key_matches():
        return jsonify({"error": "forbidden"}), 403
    payload = request.json or {}
    data = _load_all()
    agents = data.get("agents", [])
    for i, a in enumerate(agents):
        if a.get("id") == agent_id:
            # Non-admin internal callers may only edit their own agents
            if not is_admin and a.get("owner_id") != user.get("id"):
                return jsonify({"error": "forbidden"}), 403
            updated = {**a, **payload, "id": agent_id, "owner_id": a.get("owner_id")}
            # Preserve MCP headers when UI sends masked servers without headers
            try:
                if isinstance(payload.get("mcp"), dict) and isinstance((payload.get("mcp") or {}).get("servers"), list):
                    existing_servers = ((a.get("mcp") or {}).get("servers") or []) if isinstance(a.get("mcp"), dict) else []
                    new_servers = ((updated.get("mcp") or {}).get("servers") or []) if isinstance(updated.get("mcp"), dict) else []
                    def _match(prev, cur):
                        try:
                            pu = str((prev or {}).get("url") or "").strip()
                            cu = str((cur or {}).get("url") or "").strip()
                            if pu and cu and pu == cu:
                                return True
                            pn = str((prev or {}).get("name") or "").strip()
                            cn = str((cur or {}).get("name") or "").strip()
                            return bool(pn and cn and pn == cn)
                        except Exception:
                            return False
                    for idx, s in enumerate(new_servers):
                        if not isinstance(s, dict) or ("headers" in s and isinstance(s.get("headers"), dict)):
                            continue
                        # Only attempt copy when headers are missing
                        for prev in existing_servers:
                            if _match(prev, s) and isinstance(prev, dict) and isinstance(prev.get("headers"), dict):
                                s = {k: v for k, v in s.items() if k != "has_headers"}
                                s["headers"] = prev.get("headers")
                                new_servers[idx] = s
                                break
                    updated.setdefault("mcp", {})["servers"] = new_servers
            except Exception:
                pass
            agents[i] = updated
            _save_all(data)
            # If greeting text or voice/provider changed, regenerate greeting audio and delete the old one
            try:
                changed_greeting = ((a.get("greeting", {}) or {}).get("text") != (updated.get("greeting", {}) or {}).get("text"))
                changed_voice = ((a.get("tts", {}) or {}).get("reference_id") != (updated.get("tts", {}) or {}).get("reference_id"))
                old_rt = (a.get("realtime", {}) or {})
                new_rt = (updated.get("realtime", {}) or {})
                changed_provider = str(old_rt.get("tts_provider", "openai")).lower() != str(new_rt.get("tts_provider", "openai")).lower() or bool(old_rt.get("enabled")) != bool(new_rt.get("enabled")) or (old_rt.get("voice") != new_rt.get("voice"))
                missing_asset = True
                try:
                    cur_asset = (updated.get("assets", {}) or {}).get("greeting_audio")
                    if cur_asset:
                        missing_asset = not (Path(__file__).parent / cur_asset).exists()
                except Exception:
                    missing_asset = True
                if changed_greeting or changed_voice or changed_provider or missing_asset:
                    tts_cfg = updated.get("tts", {}) if isinstance(updated.get("tts"), dict) else {}
                    if True:
                        # Delete old greeting file(s) if present
                        try:
                            # remove primary and preview for this agent id
                            base = agent_id
                            gdir = Path(__file__).parent / "greetings"
                            for p in gdir.glob(f"{base}.*"):
                                try:
                                    p.unlink(missing_ok=True)
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        # Kick off async synthesis and optimistically set path
                        _synthesize_greeting_async(updated, agent_id)
                        # Choose optimistic extension based on provider
                        rt = (updated.get("realtime", {}) or {}) if isinstance(updated.get("realtime"), dict) else {}
                        if bool(rt.get("enabled")) and str(rt.get("tts_provider", "openai")).lower() == "openai":
                            ext = "wav"
                        else:
                            ext = (tts_cfg.get("response_format") or os.getenv("FISH_AUDIO_RESPONSE_FORMAT", "opus")).lower()
                            if ext not in {"wav","mp3","aac","flac","opus","pcm"}:
                                ext = "opus"
                        updated.setdefault("assets", {})["greeting_audio"] = f"greetings/{agent_id}.{ext}"
                        data["agents"][i] = updated
                        _save_all(data)
            except Exception:
                pass
            return jsonify(updated)
    return jsonify({"error": "not_found"}), 404


@app.get("/livekit/room_token/<room>")
@require_auth()
def issue_room_token(room: str):
    """Issue a viewer token for the specified LiveKit room.
    The token is short-lived and intended for debugging/monitoring.
    """
    try:
        # Disallow reviving ended rooms unless explicitly requested
        try:
            revive = str(request.args.get("revive") or "0").strip() in {"1", "true", "yes"}
            store = _load_livekit_store()
            ended = False
            # Find call by room name
            for v in (store.get("calls") or {}).values():
                try:
                    rname = v.get("room_name") or (v.get("room") or {}).get("name")
                    if str(rname) == str(room):
                        if v.get("ended_at"):
                            ended = True
                        break
                except Exception:
                    continue
            if ended and not revive:
                return (
                    jsonify(
                        {"error": "room_ended", "hint": "Pass ?revive=1 to force a token for an ended room."}
                    ),
                    410,
                )
        except Exception:
            pass
        # Prefer configured LiveKit settings
        data = _load_all()
        d = _load_defaults()
        lk_cfg = (data.get("defaults", {}) or {}).get("livekit", {})
        lk_def = d.get("livekit", {})
        lk_url = str(lk_cfg.get("url") or lk_def.get("url") or os.getenv("LIVEKIT_URL", ""))
        lk_key = str(lk_cfg.get("api_key") or lk_def.get("api_key") or os.getenv("LIVEKIT_API_KEY", ""))
        lk_secret = str(
            lk_cfg.get("api_secret") or lk_def.get("api_secret") or os.getenv("LIVEKIT_API_SECRET", "")
        )
        if not (lk_url and lk_key and lk_secret):
            return (
                jsonify(
                    {
                        "error": "missing_livekit_env",
                        "hint": "Set LIVEKIT_URL/KEY/SECRET in server env or in defaults.livekit.",
                    }
                ),
                500,
            )

        identity = f"viewer-{_uuid.uuid4().hex[:6]}"
        token = lk_api.AccessToken(lk_key, lk_secret)
        token.with_identity(identity)
        token.with_name(identity)
        grants = lk_api.VideoGrants(
            room=room,
            room_join=True,
            can_publish=False,
            can_subscribe=True,
        )
        token.with_grants(grants)
        jwt = token.to_jwt()
        return jsonify({"server_url": lk_url, "room": room, "token": jwt, "identity": identity})
    except Exception as e:
        return jsonify({"error": f"token_error: {e}"}), 500


@app.delete("/agents/<agent_id>")
@require_auth()
def delete_agent(agent_id):
    data = _load_all()
    user = g.current_user
    is_admin = user.get("role") in ["admin", "subscriber", "convix"]
    agents = data.get("agents", [])
    target = None
    for a in agents:
        if a.get("id") == agent_id:
            target = a
            break
    if not target:
        return jsonify({"error": "not_found"}), 404
    if (not is_admin) and (target.get("owner_id") != user.get("id")):
        return jsonify({"error": "forbidden"}), 403
    before = len(agents)
    data["agents"] = [a for a in agents if a.get("id") != agent_id]
    _save_all(data)
    return ("", 204) if len(data["agents"]) < before else (jsonify({"error": "not_found"}), 404)


# ==================== INTEGRATIONS API ====================

@app.get("/integrations/catalog")
@require_auth()
def list_integration_catalog():
    """Return metadata for all available integrations."""
    try:
        from integrations import get_all_integrations
        return jsonify(get_all_integrations())
    except Exception as exc:
        print(f"[integrations] Failed to load catalog: {exc}", file=sys.stderr, flush=True)
        return jsonify([])


@app.get("/integrations/<agent_id>")
@require_auth()
def get_agent_integrations(agent_id):
    """Get enabled integrations for a specific agent."""
    storage = _storage_backend()
    data = storage.load_agent_integrations(agent_id)
    return jsonify(data)


@app.put("/integrations/<agent_id>")
@require_auth()
def save_agent_integrations(agent_id):
    """Save enabled integrations for an agent."""
    body = request.get_json(force=True, silent=True) or []
    if not isinstance(body, list):
        return jsonify({"error": "Body must be a JSON array of integration configs."}), 400
    storage = _storage_backend()
    storage.save_agent_integrations(agent_id, body)
    return jsonify({"ok": True})


@app.post("/integrations/<agent_id>/test")
@require_auth()
def test_agent_integration(agent_id):
    """Test credentials for a specific integration."""
    body = request.get_json(force=True, silent=True) or {}
    iid = body.get("integration_id", "")
    creds = body.get("credentials", {})
    try:
        from integrations import get_integration
        integration = get_integration(iid)
        if not integration:
            return jsonify({"ok": False, "message": f"Unknown integration: {iid}"}), 404
        import asyncio
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(integration.test_credentials(creds))
        loop.close()
        return jsonify(result)
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc)}), 500


# ==================== WORKFLOWS API ====================

@app.get("/workflows/<agent_id>")
@require_auth()
def get_agent_workflow(agent_id):
    """Get workflow definition for an agent."""
    storage = _storage_backend()
    data = storage.load_agent_workflow(agent_id)
    return jsonify(data)


@app.put("/workflows/<agent_id>")
@require_auth()
def save_agent_workflow(agent_id):
    """Save workflow definition for an agent."""
    body = request.get_json(force=True, silent=True) or {}
    storage = _storage_backend()
    storage.save_agent_workflow(agent_id, body)
    return jsonify({"ok": True})


# ==================== MEMORY CONFIG API ====================

@app.get("/memory/<agent_id>")
@require_auth()
def get_agent_memory_config(agent_id):
    """Get memory configuration for an agent."""
    storage = _storage_backend()
    data = storage.load_agent_memory_config(agent_id)
    return jsonify(data)


@app.put("/memory/<agent_id>")
@require_auth()
def save_agent_memory_config(agent_id):
    """Save memory configuration for an agent."""
    body = request.get_json(force=True, silent=True) or {}
    storage = _storage_backend()
    storage.save_agent_memory_config(agent_id, body)
    return jsonify({"ok": True})


@app.put("/active/<agent_id>")
@require_auth()
def set_active(agent_id):
    data = _load_all()
    user = g.current_user
    for a in data.get("agents", []):
        if a.get("id") == agent_id:
            if user.get("role") not in ["admin", "subscriber", "convix"] and a.get("owner_id") != user.get("id"):
                # Allow non-admins to activate agents owned by any admin (read-only sharing)
                owner = _get_user_by_id(a.get("owner_id"))
                if not owner or owner.get("role") not in ["admin", "subscriber", "convix"]:
                    return jsonify({"error": "forbidden"}), 403
            _set_active_for_user(user.get("id"), a)
            return jsonify({"ok": True, "active_agent_id": agent_id})
    return jsonify({"error": "not_found"}), 404


@app.get("/active")
@require_auth()
def get_active():
    user = g.current_user
    data = _load_active_for_user(user.get("id"))
    return jsonify(data or {})


@app.put("/active")
@require_auth()
def update_active_config():
    payload = request.json or {}
    user = g.current_user
    current = _load_active_for_user(user.get("id"))
    updated = _deep_merge(current if isinstance(current, dict) else {}, payload if isinstance(payload, dict) else {})
    # Preserve MCP headers when UI sends masked servers without headers for active agent
    try:
        if isinstance((payload or {}).get("mcp"), dict) and isinstance(((payload or {}).get("mcp") or {}).get("servers"), list):
            existing_servers = ((current or {}).get("mcp", {}) or {}).get("servers") or []
            new_servers = ((updated or {}).get("mcp", {}) or {}).get("servers") or []
            def _match(prev, cur):
                try:
                    pu = str((prev or {}).get("url") or "").strip(); cu = str((cur or {}).get("url") or "").strip()
                    if pu and cu and pu == cu: return True
                    pn = str((prev or {}).get("name") or "").strip(); cn = str((cur or {}).get("name") or "").strip()
                    return bool(pn and cn and pn == cn)
                except Exception:
                    return False
            for idx, s in enumerate(new_servers):
                if not isinstance(s, dict) or ("headers" in s and isinstance(s.get("headers"), dict)):
                    continue
                for prev in existing_servers:
                    if _match(prev, s) and isinstance(prev, dict) and isinstance(prev.get("headers"), dict):
                        s = {k: v for k, v in s.items() if k != "has_headers"}
                        s["headers"] = prev.get("headers")
                        new_servers[idx] = s
                        break
            updated.setdefault("mcp", {})["servers"] = new_servers
    except Exception:
        pass
    # If greeting text or voice changed, regenerate active greeting audio too
    try:
        old_g = ((current or {}).get("greeting", {}) or {}).get("text") if isinstance(current, dict) else None
        new_g = ((updated or {}).get("greeting", {}) or {}).get("text") if isinstance(updated, dict) else None
        old_v = ((current or {}).get("tts", {}) or {}).get("reference_id") if isinstance(current, dict) else None
        new_v = ((updated or {}).get("tts", {}) or {}).get("reference_id") if isinstance(updated, dict) else None
        old_rt = (current.get("realtime", {}) if isinstance(current, dict) else {}) or {}
        new_rt = (updated.get("realtime", {}) if isinstance(updated, dict) else {}) or {}
        changed_provider = str(old_rt.get("tts_provider", "openai")).lower() != str(new_rt.get("tts_provider", "openai")).lower() or bool(old_rt.get("enabled")) != bool(new_rt.get("enabled")) or (old_rt.get("voice") != new_rt.get("voice"))
        missing_asset = True
        try:
            cur_asset = ((updated or {}).get("assets", {}) or {}).get("greeting_audio") if isinstance(updated, dict) else None
            if cur_asset:
                missing_asset = not (Path(__file__).parent / cur_asset).exists()
        except Exception:
            missing_asset = True
        if old_g != new_g or old_v != new_v or changed_provider or missing_asset:
            # delete old file(s) if present
            try:
                base = (updated.get("id") or "active")
                gdir = Path(__file__).parent / "greetings"
                for p in gdir.glob(f"{base}.*"):
                    try:
                        p.unlink(missing_ok=True)
                    except Exception:
                        pass
            except Exception:
                pass
            active_id = updated.get("id") or "active"
            _synthesize_greeting_async(updated, active_id)
            # Optimistic extension by provider
            tts_cfg = updated.get("tts", {}) if isinstance(updated.get("tts"), dict) else {}
            if bool(new_rt.get("enabled")) and str(new_rt.get("tts_provider", "openai")).lower() == "openai":
                ext = "wav"
            else:
                ext = (tts_cfg.get("response_format") or os.getenv("FISH_AUDIO_RESPONSE_FORMAT", "opus")).lower()
                if ext not in {"wav","mp3","aac","flac","opus","pcm"}:
                    ext = "opus"
            updated.setdefault("assets", {})["greeting_audio"] = f"greetings/{active_id}.{ext}"
    except Exception:
        pass
    _set_active_for_user(user.get("id"), updated)
    return jsonify(updated)


# ------------------ AUTH & USERS ------------------

def _is_first_user() -> bool:
    return len(_load_users().get("users", [])) == 0


@app.post("/auth/register")
def register():
    payload = request.json or {}
    username = (payload.get("username") or "").strip()
    email = (payload.get("email") or "").strip().lower()
    password = payload.get("password") or ""
    if not username or not email or not password:
        return jsonify({"error": "missing_fields"}), 400
    users = _load_users()
    for u in users.get("users", []):
        if u.get("email") == email:
            return jsonify({"error": "email_exists"}), 409
    user = {
        "id": uuid.uuid4().hex,
        "username": username,
        "email": email,
        "password_hash": generate_password_hash(password),
        "role": "admin" if _is_first_user() else "user",
        "created_at": int(time.time()),
    }
    users.setdefault("users", []).append(user)
    _save_users(users)
    token = _issue_token(user["id"])
    public = {k: v for k, v in user.items() if k != "password_hash"}
    return jsonify({"token": token, "user": public})


@app.post("/auth/login")
def login():
    payload = request.json or {}
    email = (payload.get("email") or "").strip().lower()
    password = payload.get("password") or ""
    users = _load_users().get("users", [])
    for u in users:
        if u.get("email") == email and check_password_hash(u.get("password_hash"), password):
            token = _issue_token(u.get("id"))
            public = {k: v for k, v in u.items() if k != "password_hash"}
            return jsonify({"token": token, "user": public})
    return jsonify({"error": "invalid_credentials"}), 401


@app.get("/auth/me")
def me():
    auth = request.headers.get("Authorization", "")
    token = auth.split(" ")[-1] if auth.startswith("Bearer ") else auth
    user = _get_user_by_token(token)
    if not user:
        return jsonify({}), 200
    public = {k: v for k, v in user.items() if k != "password_hash"}
    return jsonify({"user": public})


@app.put("/auth/me")
@require_auth()
def update_me():
    payload = request.json or {}
    users = _load_users()
    updated = None
    for i, u in enumerate(users.get("users", [])):
        if u.get("id") == g.current_user.get("id"):
            if payload.get("username"):
                u["username"] = str(payload["username"]).strip()
            if payload.get("password"):
                u["password_hash"] = generate_password_hash(str(payload["password"]))
            users["users"][i] = u
            updated = {k: v for k, v in u.items() if k != "password_hash"}
            break
    if updated is None:
        return jsonify({"error": "not_found"}), 404
    _save_users(users)
    return jsonify({"user": updated})


@app.get("/defaults")
@require_auth(admin=True)
def get_defaults():
    return jsonify(_load_defaults())


@app.put("/defaults")
@require_auth(admin=True)
def update_defaults():
    payload = request.json or {}
    cur = _load_defaults()
    
    # Merge shallowly per section
    sections = ("llm", "stt", "tts", "realtime", "livekit", "openai", "env")
    for section in sections:
        if isinstance(payload.get(section), dict):
            sec_data = payload.get(section)
            
            # Special handling for 'env' section to encrypt sensitive keys
            if section == "env":
                # All API keys that should be encrypted at rest
                keys_to_encrypt = [
                    "OPENAI_API_KEY", 
                    "GOOGLE_API_KEY",
                    "GROQ_API_KEY",
                    "FISH_AUDIO_API_KEY", 
                    "XAI_API_KEY",
                ]
                for k in keys_to_encrypt:
                    if k in sec_data:
                        val = sec_data.pop(k)
                        if val and not is_encrypted(val):
                            sec_data[f"{k}_encrypted"] = encrypt_value(val)
                        elif val == "": # Allow clearing
                            sec_data.pop(f"{k}_encrypted", None)
                        elif val: # Already encrypted or special value
                            sec_data[f"{k}_encrypted"] = val
            
            # Special handling for 'openai' section
            if section == "openai" and "api_key" in sec_data:
                val = sec_data.pop("api_key")
                if val and not is_encrypted(val):
                    sec_data["api_key_encrypted"] = encrypt_value(val)
                elif val == "":
                    sec_data.pop("api_key_encrypted", None)
                elif val:
                    sec_data["api_key_encrypted"] = val

            cur[section] = {**(cur.get(section) or {}), **sec_data}
            
    _save_defaults(cur)
    return jsonify(cur)

"""Removed .env overlay endpoints by request. Runtime should source from process env or defaults sections only."""


@app.post("/auth/forgot")
def forgot_password():
    payload = request.json or {}
    email = (payload.get("email") or "").strip().lower()
    if not email:
        return jsonify({"error": "missing_email"}), 400
    users = _load_users().get("users", [])
    for u in users:
        if u.get("email") == email:
            token = _create_reset_token(u.get("id"))
            # In production: send email here. For now, return token in response.
            return jsonify({"ok": True, "reset_token": token})
    return jsonify({"ok": True})  # do not reveal if email exists


@app.post("/auth/reset")
def reset_password():
    payload = request.json or {}
    token = payload.get("token")
    new_password = payload.get("password")
    if not token or not new_password:
        return jsonify({"error": "missing"}), 400
    user_id = _consume_reset_token(token)
    if not user_id:
        return jsonify({"error": "invalid_or_expired"}), 400
    users = _load_users()
    for i, u in enumerate(users.get("users", [])):
        if u.get("id") == user_id:
            u["password_hash"] = generate_password_hash(str(new_password))
            users["users"][i] = u
            _save_users(users)
            return jsonify({"ok": True})
    return jsonify({"error": "not_found"}), 404


@app.post("/users")
@require_auth(admin=True)
def admin_create_user():
    payload = request.json or {}
    username = (payload.get("username") or "").strip()
    email = (payload.get("email") or "").strip().lower()
    password = payload.get("password") or ""
    role = payload.get("role") or "user"

    if not username or not email or not password:
        return jsonify({"error": "missing_fields"}), 400
    if role not in ["admin", "user", "subscriber"]:
        return jsonify({"error": "invalid_role"}), 400

    users = _load_users()
    for u in users.get("users", []):
        if u.get("email") == email:
            return jsonify({"error": "email_exists"}), 409

    user = {
        "id": uuid.uuid4().hex,
        "username": username,
        "email": email,
        "password_hash": generate_password_hash(password),
        "role": role,
        "created_at": int(time.time()),
    }
    users.setdefault("users", []).append(user)
    _save_users(users)

    public = {k: v for k, v in user.items() if k != "password_hash"}
    return jsonify({"user": public})


@app.get("/users")
@require_auth(admin=True)
def list_users():
    users = _load_users().get("users", [])
    # Auto-assign any agents missing owner_id to the first admin user to maintain consistency
    data = _load_all()
    agents = data.get("agents", [])
    missing = [a for a in agents if not a.get("owner_id")]
    if missing:
        admin_user = next((u for u in users if u.get("role") == "admin"), None)
        if admin_user:
            for a in agents:
                if not a.get("owner_id"):
                    a["owner_id"] = admin_user.get("id")
            data["agents"] = agents
            _save_all(data)
    rows = []
    for u in users:
        public = u.copy()
        u_agents = [a for a in agents if a.get("owner_id") == u.get("id")]
        public["agents"] = u_agents
        rows.append(public)
    return jsonify({"users": rows})


@app.put("/users/<user_id>/password")
@require_auth(admin=True)
def admin_reset_password(user_id):
    payload = request.json or {}
    new_password = payload.get("password")
    if not new_password:
        return jsonify({"error": "missing_password"}), 400

    users_data = _load_users()
    users_list = users_data.get("users", [])
    
    found = False
    for u in users_list:
        if u.get("id") == user_id:
            u["password_hash"] = generate_password_hash(new_password)
            found = True
            break
            
    if not found:
        return jsonify({"error": "user_not_found"}), 404
        
    _save_users(users_data)
    return jsonify({"ok": True})


@app.put("/users/<user_id>/role")
@require_auth(admin=True)
def admin_update_user_role(user_id):
    payload = request.json or {}
    new_role = payload.get("role")
    if not new_role or new_role not in ["admin", "user", "subscriber"]:
        return jsonify({"error": "invalid_role"}), 400

    users_data = _load_users()
    users_list = users_data.get("users", [])
    
    found = False
    for u in users_list:
        if u.get("id") == user_id:
            u["role"] = new_role
            found = True
            break
            
    if not found:
        return jsonify({"error": "user_not_found"}), 404
        
    _save_users(users_data)
    return jsonify({"ok": True})


# ------------------ USER CREDENTIALS MANAGEMENT ------------------

def _load_user_credentials(user_id: str) -> dict:
    """Load and decrypt credentials for a specific user."""
    backend = _storage_backend()
    encrypted_creds = backend.get_all_credentials_for_user(user_id)
    
    decrypted = {}
    for provider, cred_data in encrypted_creds.items():
        decrypted[provider] = {}
        for key, value in (cred_data or {}).items():
            if key.endswith("_encrypted") and value:
                try:
                    original_key = key.replace("_encrypted", "")
                    decrypted[provider][original_key] = decrypt_value(value)
                except Exception:
                    # Keep encrypted if decryption fails
                    decrypted[provider][key] = value
            else:
                decrypted[provider][key] = value
    return decrypted


def _save_user_credential(user_id: str, provider: str, cred_data: dict) -> None:
    """Encrypt and save credentials for a specific user/provider."""
    backend = _storage_backend()
    
    encrypted = {}
    for key, value in (cred_data or {}).items():
        if key == "api_key" and value and not is_encrypted(value):
            encrypted["api_key_encrypted"] = encrypt_value(value)
        elif key == "api_key_encrypted":
            encrypted[key] = value
        else:
            encrypted[key] = value
    
    backend.set_user_credential(user_id, provider.lower(), encrypted)


def _mask_user_credentials(creds: dict) -> dict:
    """Mask API keys in credentials for safe display."""
    masked = {}
    for provider, cred_data in creds.items():
        masked[provider] = {}
        for key, value in (cred_data or {}).items():
            if key in ("api_key", "api_key_encrypted") and value:
                masked[provider]["api_key_masked"] = mask_api_key(value)
                masked[provider]["has_api_key"] = True
            elif key.endswith("_encrypted"):
                original_key = key.replace("_encrypted", "")
                masked[provider][f"{original_key}_masked"] = "••••••••"
                masked[provider][f"has_{original_key}"] = True
            else:
                masked[provider][key] = value
    return masked


@app.get("/user/credentials")
@require_auth()
def get_user_credentials():
    """Get current user's configured provider credentials (masked)."""
    user_id = g.current_user.get("id")
    creds = _load_user_credentials(user_id)
    masked = _mask_user_credentials(creds)
    return jsonify({"credentials": masked})


@app.get("/user/credentials/<provider>")
@require_auth()
def get_user_credential_for_provider(provider: str):
    """Get current user's credential for a specific provider (masked)."""
    user_id = g.current_user.get("id")
    backend = _storage_backend()
    cred = backend.get_user_credential(user_id, provider.lower())
    if not cred:
        return jsonify({"credential": None})
    
    # Decrypt and mask
    decrypted = {}
    for key, value in cred.items():
        if key.endswith("_encrypted") and value:
            try:
                original_key = key.replace("_encrypted", "")
                decrypted[original_key] = decrypt_value(value)
            except Exception:
                decrypted[key] = value
        else:
            decrypted[key] = value
    
    masked = {}
    for key, value in decrypted.items():
        if key == "api_key" and value:
            masked["api_key_masked"] = mask_api_key(value)
            masked["has_api_key"] = True
        else:
            masked[key] = value
    
    return jsonify({"credential": masked, "provider": provider})


@app.put("/user/credentials/<provider>")
@require_auth()
def set_user_credential_for_provider(provider: str):
    """Set current user's credential for a specific provider."""
    user_id = g.current_user.get("id")
    payload = request.json or {}
    
    provider = provider.lower()
    
    # Validate provider is known
    provider_info = get_provider_info("stt", provider) or get_provider_info("llm", provider) or get_provider_info("tts", provider)
    if not provider_info:
        return jsonify({"error": f"Unknown provider: {provider}"}), 400
    
    # Extract credential data
    cred_data = {}
    if payload.get("api_key"):
        cred_data["api_key"] = str(payload["api_key"]).strip()
    if payload.get("base_url"):
        cred_data["base_url"] = str(payload["base_url"]).strip()
    if payload.get("region"):
        cred_data["region"] = str(payload["region"]).strip()
    if payload.get("default_voice_id"):
        cred_data["default_voice_id"] = str(payload["default_voice_id"]).strip()
    
    if not cred_data.get("api_key"):
        return jsonify({"error": "api_key is required"}), 400
    
    _save_user_credential(user_id, provider, cred_data)
    return jsonify({"ok": True, "provider": provider})


@app.delete("/user/credentials/<provider>")
@require_auth()
def delete_user_credential_for_provider(provider: str):
    """Delete current user's credential for a specific provider."""
    user_id = g.current_user.get("id")
    backend = _storage_backend()
    deleted = backend.delete_user_credential(user_id, provider.lower())
    return jsonify({"ok": deleted, "provider": provider})


@app.post("/validate/<provider>")
@require_auth()
def validate_provider_credential(provider: str):
    """Validate an API key for a specific provider."""
    payload = request.json or {}
    api_key = (payload.get("api_key") or "").strip()
    
    if not api_key:
        return jsonify({"valid": False, "message": "api_key is required"}), 400
    
    provider = provider.lower()
    
    # Get extra params for validation
    kwargs = {}
    if payload.get("base_url"):
        kwargs["base_url"] = payload["base_url"]
    if payload.get("region"):
        kwargs["region"] = payload["region"]
    
    # Run async validation
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        is_valid, message = loop.run_until_complete(validate_credential(provider, api_key, **kwargs))
        loop.close()
    except Exception as e:
        return jsonify({"valid": False, "message": f"Validation error: {str(e)}"}), 500
    
    return jsonify({"valid": is_valid, "message": message, "provider": provider})


@app.get("/internal/credentials/<user_id>")
@require_internal()
def internal_get_user_credentials(user_id: str):
    """Internal endpoint for agent workers to fetch decrypted credentials and global defaults."""
    user_creds = _load_user_credentials(user_id)
    global_defaults = _load_defaults()
    
    # Also fetch user role to determine if they can use global defaults
    users = _load_users().get("users", [])
    user_role = "user"
    for u in users:
        if u.get("id") == user_id:
            user_role = u.get("role", "user")
            break
            
    return jsonify({
        "credentials": user_creds, 
        "global_defaults": global_defaults,
        "user_id": user_id,
        "user_role": user_role
    })


# ------------------ PROVIDER REGISTRY ENDPOINTS ------------------

@app.get("/providers")
@require_auth()
def list_all_providers():
    """List all available providers for STT, LLM, and TTS."""
    return jsonify({
        "stt": list_providers("stt"),
        "llm": list_providers("llm"),
        "tts": list_providers("tts"),
        "realtime": list_providers("realtime"),
    })


@app.get("/providers/<component>")
@require_auth()
def list_providers_for_component(component: str):
    """List providers for a specific component (stt, llm, tts, realtime)."""
    component = component.lower()
    if component not in ("stt", "llm", "tts", "realtime"):
        return jsonify({"error": "Invalid component. Use: stt, llm, tts, realtime"}), 400
    return jsonify({"providers": list_providers(component)})


@app.get("/providers/<component>/<provider_id>/models")
@require_auth()
def list_models_for_provider(component: str, provider_id: str):
    """List available models for a provider."""
    models = get_models_for_provider(component.lower(), provider_id.lower())
    return jsonify({"models": models, "provider": provider_id, "component": component})


@app.get("/providers/tts/<provider_id>/voices")
@require_auth()
def list_voices_for_tts_provider(provider_id: str):
    """List available voices for a TTS provider."""
    voices = get_voices_for_provider(provider_id.lower())
    return jsonify({"voices": voices, "provider": provider_id})


@app.get("/providers/realtime/<provider_id>/voices")
@require_auth()
def list_voices_for_realtime_provider(provider_id: str):
    """List available voices for a Realtime provider."""
    voices = get_voices_for_realtime_provider(provider_id.lower())
    return jsonify({"voices": voices, "provider": provider_id})


# ------------------ ADMIN SEED DEMO USER & MIGRATIONS ------------------

def _seed_demo_user(email: str | None = None, password: str | None = None) -> dict | None:
    users = _load_users()
    for u in users.get("users", []):
        if u.get("username").lower() == "zainlee_demo" or u.get("username").lower() == "zainlee demo":
            return None  # already exists
    pwd = password or secrets.token_hex(4)
    demo = {
        "id": uuid.uuid4().hex,
        "username": "Zainlee_Demo",
        "email": (email or "zainlee_demo@local"),
        "password_hash": generate_password_hash(pwd),
        "role": "user",
        "created_at": int(time.time()),
    }
    users.setdefault("users", []).append(demo)
    _save_users(users)
    return {"user": {k: v for k, v in demo.items() if k != "password_hash"}, "password": pwd}


def _migrate_agents_assign(owner_id: str) -> int:
    data = _load_all()
    changed = 0
    for a in data.get("agents", []):
        if not a.get("owner_id"):
            a["owner_id"] = owner_id
            changed += 1
    _save_all(data)
    return changed


@app.post("/admin/seed_demo")
@require_auth(admin=True)
def admin_seed_demo():
    payload = request.json or {}
    created = _seed_demo_user(payload.get("email"), payload.get("password"))
    if created is None:
        # Already exists; find id
        for u in _load_users().get("users", []):
            if u.get("username").lower() in {"zainlee_demo", "zainlee demo"}:
                demo_id = u.get("id")
                migrated = _migrate_agents_assign(demo_id)
                return jsonify({"status": "exists", "migrated_agents": migrated})
        return jsonify({"status": "exists", "migrated_agents": 0})
    migrated = _migrate_agents_assign(created["user"]["id"])
    return jsonify({"status": "created", **created, "migrated_agents": migrated})


# Simple listing endpoint for uploaded files (optional)
@app.get("/uploads/<path:filename>")
def get_uploaded_file(filename):
    base_dir = Path(__file__).parent
    # allow serving only within the two upload directories
    if filename.startswith("background_ambience/"):
        return send_from_directory(str(base_dir / "background_ambience"), filename.split("/", 1)[1])
    if filename.startswith("greetings/"):
        return send_from_directory(str(base_dir / "greetings"), filename.split("/", 1)[1])
    return jsonify({"error": "not_allowed"}), 403


@app.get("/transcripts")
def list_transcripts():
    base_dir = Path(__file__).parent / "transcripts"
    base_dir.mkdir(parents=True, exist_ok=True)
    files = []
    for p in sorted(base_dir.glob("call_*.json"), reverse=True):
        files.append({
            "filename": p.name,
            "path": f"transcripts/{p.name}",
            "modified": p.stat().st_mtime,
        })
    return jsonify({"files": files})


@app.get("/transcripts/<path:filename>")
def get_transcript(filename):
    base_dir = Path(__file__).parent / "transcripts"
    safe = secure_filename(filename)
    target = base_dir / safe
    if not target.exists():
        return jsonify({"error": "not_found"}), 404
    try:
        with open(target, "r", encoding="utf-8") as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.get("/transcripts/by_room/<room_name>")
def get_transcript_by_room(room_name: str):
    """Return the latest transcript whose 'room' equals room_name."""
    base_dir = Path(__file__).parent / "transcripts"
    base_dir.mkdir(parents=True, exist_ok=True)
    try:
        # Iterate newest first
        for p in sorted(base_dir.glob("call_*.json"), reverse=True):
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
                if str(data.get("room") or "") == str(room_name):
                    return jsonify(data)
            except Exception:
                continue
        return jsonify({}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/transcripts/save")
def save_transcript():
    """Save real-time transcript from web UI."""
    try:
        payload = request.json or {}
        room = payload.get("room")
        agent_id = payload.get("agent_id")
        entries = payload.get("entries", [])
        
        if not room or not entries:
            return jsonify({"error": "missing_data"}), 400
        
        base_dir = Path(__file__).parent / "transcripts"
        base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create transcript file
        timestamp = int(time.time() * 1000)
        filename = f"call_{room}_{timestamp}.json"
        filepath = base_dir / filename
        
        transcript_data = {
            "room": room,
            "agent_id": agent_id,
            "timestamp": timestamp,
            "entries": entries,
            "created_at": time.time()
        }
        
        filepath.write_text(json.dumps(transcript_data, indent=2), encoding="utf-8")
        
        return jsonify({"ok": True, "filename": filename})
    except Exception as e:
        print(f"Error saving transcript: {e}", file=sys.stderr)
        return jsonify({"error": str(e)}), 500


def _parse_dt_arg(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        # treat numeric epochs as seconds unless clearly ms
        if value.isdigit():
            ivalue = int(value)
            if ivalue > 1_000_000_000_000:
                return datetime.fromtimestamp(ivalue / 1000, tz=timezone.utc)
            return datetime.fromtimestamp(ivalue, tz=timezone.utc)
    except Exception:
        pass
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _parse_time_bounds() -> tuple[datetime | None, datetime | None]:
    start = _parse_dt_arg(request.args.get("start") or request.args.get("since"))
    end = _parse_dt_arg(request.args.get("end") or request.args.get("until"))
    range_key = (request.args.get("range") or "").strip().lower()
    if range_key in {"today", "day"}:
        now = datetime.now(timezone.utc)
        start = start or datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        end = end or now
    elif range_key in {"week", "7d", "last_week"}:
        now = datetime.now(timezone.utc)
        start = start or (now - timedelta(days=7))
        end = end or now
    elif range_key in {"month", "30d", "last_month"}:
        now = datetime.now(timezone.utc)
        start = start or (now - timedelta(days=30))
        end = end or now
    return start, end


def _agent_name_lookup() -> dict[str, str]:
    try:
        return {
            str(agent.get("id")): agent.get("agent_name") or agent.get("name") or agent.get("id")
            for agent in (_load_all().get("agents") or [])
        }
    except Exception:
        return {}


@app.get("/analytics/agents")
@require_auth()
def analytics_agents():
    svc = _analytics_service()
    if not svc:
        return jsonify({"agents": []})
    try:
        agent_lookup = {}
        for agent in (_load_all().get("agents") or []):
            agent_lookup[str(agent.get("id"))] = agent.get("agent_name") or agent.get("name") or agent.get("id")
    except Exception:
        agent_lookup = {}
    try:
        start, end = _parse_time_bounds()
        metrics = svc.aggregate_agent_metrics(agent_lookup, start=start, end=end)
        return jsonify({"agents": metrics})
    except Exception:
        return jsonify({"agents": []})


@app.get("/analytics/summary")
def analytics_summary():
    svc = _analytics_service()
    if svc:
        try:
            start, end = _parse_time_bounds()
            data = svc.aggregate_summary(start=start, end=end)
            data["active_agents"] = svc.count_active_agents(start=start, end=end)
            return jsonify(data)
        except Exception:
            pass
    # Fallback to transcript files if analytics disabled
    base_dir = Path(__file__).parent / "transcripts"
    base_dir.mkdir(parents=True, exist_ok=True)
    total_calls = 0
    total_duration = 0.0
    for p in base_dir.glob("call_*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            start = data.get("started_at")
            end = data.get("ended_at")
            if start and end:
                d = (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()
                total_duration += max(0.0, d)
            total_calls += 1
        except Exception:
            continue
    avg = (total_duration / total_calls) if total_calls > 0 else 0.0
    return jsonify({
        "total_calls": total_calls,
        "average_duration_seconds": avg,
        "total_duration_seconds": int(total_duration),
    })


@app.get("/analytics/calls_over_time")
def analytics_calls_over_time():
    svc = _analytics_service()
    if svc:
        try:
            return jsonify({"points": svc.calls_over_time()})
        except Exception:
            pass
    base_dir = Path(__file__).parent / "transcripts"
    base_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for p in base_dir.glob("call_*.json"):
        try:
            name = p.stem
            date_part = name.split("_")[1]
            date_fmt = f"{date_part[0:4]}-{date_part[4:6]}-{date_part[6:8]}"
            counts[date_fmt] = counts.get(date_fmt, 0) + 1
        except Exception:
            continue
    points = sorted(({"date": k, "count": v} for k, v in counts.items()), key=lambda x: x["date"])
    return jsonify({"points": points})


@app.get("/analytics/top_agents")
@require_auth()
def analytics_top_agents():
    svc = _analytics_service()
    if not svc:
        return jsonify({"agents": []})
    try:
        limit = max(1, min(20, int(request.args.get("limit", "3"))))
    except Exception:
        limit = 3
    start, end = _parse_time_bounds()
    agents = svc.top_agents(limit=limit, start=start, end=end, agent_names=_agent_name_lookup())
    return jsonify({"agents": agents})


@app.get("/analysis/latest")
@require_auth()
def analysis_latest():
    svc = _analytics_service()
    if not svc:
        return jsonify({"reports": []})
    try:
        limit = max(1, min(20, int(request.args.get("limit", "5"))))
    except Exception:
        limit = 5
    start, end = _parse_time_bounds()
    rows = svc.latest_analysis(limit=limit, start=start, end=end, agent_names=_agent_name_lookup())
    return jsonify({"reports": rows})


@app.get("/analysis/reports")
@require_auth()
def analysis_reports():
    svc = _analytics_service()
    if not svc:
        return jsonify({"reports": [], "pagination": {"total": 0, "page": 1, "per_page": 20, "has_more": False}})
    start, end = _parse_time_bounds()
    agent_id = request.args.get("agent_id") or None
    try:
        page = max(1, int(request.args.get("page", "1")))
    except Exception:
        page = 1
    try:
        per_page = max(1, min(100, int(request.args.get("per_page", "20"))))
    except Exception:
        per_page = 20
    agent_names = _agent_name_lookup()
    data = svc.fetch_reports(agent_id, start, end, page, per_page, agent_names, require_analysis=True)
    return jsonify(data)


@app.get("/analysis/reports/<call_id>")
@require_auth()
def analysis_report_detail(call_id: str):
    svc = _analytics_service()
    if not svc:
        return jsonify({"error": "analytics_disabled"}), 503
    agent_names = _agent_name_lookup()
    report = svc.fetch_report(call_id, agent_names)
    if not report:
        return jsonify({"error": "not_found"}), 404
    return jsonify(report)


@app.get("/analysis/daily_summary")
@require_auth()
def analysis_daily_summary():
    svc = _analytics_service()
    if not svc:
        return jsonify({"days": []})
    start, end = _parse_time_bounds()
    agent_id = request.args.get("agent_id") or None
    data = svc.daily_summary(agent_id, start, end)
    return jsonify({"days": data})


# ------------------ LIVEKIT WEBHOOK ENDPOINT ------------------

def _lk_get_or_create_call(store: dict, sip_call_id: str) -> Dict[str, Any]:
    calls = store.setdefault("calls", {})
    call = calls.get(sip_call_id) or {
        "id": sip_call_id,
        "events": [],
        "room": {},
        "timeline": [],
        "created_at": int(time.time()),
    }
    calls[sip_call_id] = call
    return call


@app.post("/webhooks/livekit")
def livekit_webhook_receiver():
    """Minimal receiver that stores LiveKit Cloud webhook events.
    It does not verify signatures in this demo setup. For production, verify
    the Authorization header per LiveKit docs.
    """
    raw = request.get_data(cache=False, as_text=True) or "{}"
    auth_header = request.headers.get("Authorization")
    # Validate signature if configured
    receiver = None
    if LKWebhookReceiver and os.getenv("LIVEKIT_WEBHOOK_KEY") and os.getenv("LIVEKIT_WEBHOOK_SECRET"):
        try:
            receiver = LKWebhookReceiver(os.getenv("LIVEKIT_WEBHOOK_KEY"), os.getenv("LIVEKIT_WEBHOOK_SECRET"))
            receiver.receive(raw, auth_header)  # raises on invalid
        except Exception:
            return jsonify({"error": "invalid_signature"}), 401
    try:
        payload = json.loads(raw)
    except Exception:
        payload = {}
    event = str(payload.get("event") or payload.get("type") or "").lower()
    event_id = str(
        payload.get("id")
        or payload.get("event_id")
        or payload.get("eventId")
        or payload.get("eventID")
        or payload.get("request_id")
        or ""
    ).strip()
    # NOTE: LiveKit may reuse the same top-level event id across different event *types*
    # (e.g., egress_started -> egress_updated -> egress_ended). Dedupe should therefore
    # include the event type to avoid dropping real transitions.
    dedupe_key = f"{event}:{event_id}" if (event and event_id) else (event_id or "")
    if dedupe_key and _webhook_is_duplicate(dedupe_key):
        print(f"[webhook] duplicate event ignored id={event_id} type={event}", file=sys.stderr, flush=True)
        return jsonify({"ok": True, "duplicate": True})

    mutation_context = {}
    call = {}
    participant_obj = {}
    sip_meta = {}
    dialed_number = None
    caller_number = None

    def _mutate_livekit_store_block(store):
        # Identify call/session keys. Many LiveKit events include sip_call_id, while others only include room
        # Handle both snake_case and camelCase field names from LiveKit
        egress_info = payload.get("egressInfo") or payload.get("egress") or {}
        ingress_info = payload.get("ingressInfo") or payload.get("ingress") or {}
        room_name = (
            (payload.get("room") or {}).get("name")
            or egress_info.get("roomName") or egress_info.get("room_name")
            or ingress_info.get("roomName") or ingress_info.get("room_name")
        )

        # DEBUG: Log all incoming webhook events
        print(f"[webhook] event={event} room={room_name}", file=sys.stderr, flush=True)
        sip_call_id = (
            payload.get("sip", {}).get("call", {}).get("id")
            or payload.get("sip_call", {}).get("id")
            or payload.get("sip_call_id")
        )
        # Maintain explicit room<->call mappings to unify events for the same session
        room_to_call = store.setdefault("room_to_call", {})
        call_to_room = store.setdefault("call_to_room", {})
        calls = store.setdefault("calls", {})

        # Determine canonical call id
        canonical_call_id = None
        if sip_call_id:
            canonical_call_id = str(sip_call_id)
        elif room_name and room_to_call.get(str(room_name)):
            canonical_call_id = str(room_to_call.get(str(room_name)))
        else:
            # Fallback for non-SIP events before SIP metadata arrives
            canonical_call_id = str(room_name or payload.get("id") or "unknown")

        # If SIP metadata arrives after early room-only events, merge any legacy room-keyed record
        try:
            if sip_call_id and room_name:
                legacy_key = str(room_name)
                target_key = str(sip_call_id)
                if legacy_key != target_key and legacy_key in calls:
                    legacy = calls.get(legacy_key) or {}
                    target = calls.get(target_key) or {
                        "id": target_key,
                        "events": [],
                        "room": {},
                        "timeline": [],
                        "created_at": int(time.time()),
                    }
                    # Merge arrays and maps conservatively
                    try:
                        target.setdefault("events", []).extend(legacy.get("events", []))
                    except Exception:
                        pass
                    try:
                        target.setdefault("timeline", []).extend(legacy.get("timeline", []))
                    except Exception:
                        pass
                    try:
                        if isinstance(legacy.get("room"), dict):
                            target["room"] = {**target.get("room", {}), **legacy.get("room", {})}
                    except Exception:
                        pass
                    try:
                        lp = legacy.get("participants") or {}
                        if isinstance(lp, dict):
                            tp = target.setdefault("participants", {})
                            for k, v in lp.items():
                                tp[str(k)] = v
                    except Exception:
                        pass
                    try:
                        # Preserve earliest creation time
                        target["created_at"] = min(int(target.get("created_at", int(time.time()))), int(legacy.get("created_at", int(time.time()))))
                    except Exception:
                        pass
                    calls[target_key] = target
                    # Drop legacy room-keyed record
                    try:
                        del calls[legacy_key]
                    except Exception:
                        pass
                    # Update canonical mappings
                    room_to_call[str(room_name)] = target_key
                    call_to_room[target_key] = str(room_name)
                    canonical_call_id = target_key
                else:
                    # Establish/refresh mapping for future room-only events
                    room_to_call[str(room_name)] = str(sip_call_id)
                    call_to_room[str(sip_call_id)] = str(room_name)
        except Exception:
            pass

        call = _lk_get_or_create_call(store, str(canonical_call_id))
        mutation_context["call"] = call

        # Force-parse SIP numbers from room name if present (overrides metadata)
        # Format: {Dialed}-_{Caller}_{Suffix} (e.g. +9715...-_+9715..._...)
        if room_name:
            _sip_match = re.search(r"^(\+?\d+)-_(\+?\d+)(?:_|$)", str(room_name))
            if _sip_match:
                _p_dialed, _p_caller = _sip_match.groups()
                mutation_context["dialed_number"] = _p_dialed
                mutation_context["caller_number"] = _p_caller
                
                # Persist to call object for UI/Analytics
                call.setdefault("numbers", {})
                call["numbers"]["dialed"] = _p_dialed
                call["numbers"]["caller"] = _p_caller
                call["numbers"]["dialed_raw"] = _p_dialed
                call["numbers"]["caller_raw"] = _p_caller
                
                # Resolve Agent Name from routing
                try:
                    _routes = _load_routes_union()
                    _route = _routes.get(_p_dialed)
                    if _route:
                        _ag_id = _route.get("agent_id")
                        if _ag_id:
                            _ag_prof = _get_agent(_ag_id)
                            if _ag_prof:
                                _ag_name = _ag_prof.get("name")
                                call["agent_name"] = _ag_name
                                call["agent_id"] = _ag_id
                                call.setdefault("routing", {})
                                call["routing"]["agent_name"] = _ag_name
                                call["routing"]["agent_id"] = _ag_id
                                call["routing"]["dialed_number_raw"] = _p_dialed
                                call["routing"]["caller_number"] = _p_caller
                except Exception:
                    pass

        _handle_egress_webhook_event(store, event, payload)
        # Classify event type for safer updating of ended calls
        participant_obj = payload.get("participant") or {}
        mutation_context["participant_obj"] = participant_obj
        part_kind = str((participant_obj.get("kind") or "")).upper()
        identity_val = str(participant_obj.get("identity") or "")
        sip_meta, dialed_number, caller_number, number_candidates = _extract_call_metadata(payload, participant_obj)
        mutation_context["sip_meta"] = sip_meta or {}
        mutation_context["dialed_number"] = dialed_number
        mutation_context["caller_number"] = caller_number
        is_sip_related = bool(sip_call_id) or part_kind == "SIP" or event.startswith("sip_") or bool(payload.get("sip"))
        # Don't bump last_* on non-SIP viewer/debug events for rooms already marked ended
        if not (call.get("ended_at") and not is_sip_related):
            call["last_event"] = event
            call["last_received_at"] = int(time.time())
        # Basic overlays
        if isinstance(payload.get("room"), dict):
            call["room"] = {**call.get("room", {}), **payload.get("room", {})}
        if isinstance(payload.get("participant"), dict):
            parts = call.setdefault("participants", {})
            pid = payload["participant"].get("identity") or payload["participant"].get("sid") or "unknown"
            existing = parts.get(str(pid), {})
            pdata = {**existing, **payload["participant"]}
            ts = None
            try:
                ts = int(payload.get("created_at") or 0)
            except Exception:
                ts = int(time.time())
            if event == "participant_joined" and ts:
                pdata["joined_at_ts"] = ts
                pdata.setdefault("first_joined_at_ts", ts)
                pdata["last_seen_ts"] = ts
            elif event == "participant_left" and ts:
                pdata["left_at_ts"] = ts
                pdata["last_seen_ts"] = ts
            else:
                if ts:
                    pdata["last_seen_ts"] = ts
            parts[str(pid)] = pdata
        # Aggregate numerical stats and SIP metadata for richer analytics
        _collect_call_stats(call, payload)
        try:
            if sip_meta:
                call.setdefault("sip_metadata", {}).update({k: v for k, v in sip_meta.items() if v is not None})
        except Exception:
            pass
        try:
            nums = call.setdefault("numbers", {})
            if dialed_number:
                nums["dialed_raw"] = dialed_number
                nums["dialed_e164"] = _normalize_e164(dialed_number)
                nums["dialed_digits"] = _digits_only(dialed_number)
            if caller_number:
                nums["caller_raw"] = caller_number
                nums["caller_digits"] = _digits_only(caller_number)
            if number_candidates:
                existing = nums.get("candidates") or []
                seen = set(str(x) for x in existing)
                for cand in number_candidates:
                    sc = str(cand)
                    if sc not in seen:
                        existing.append(sc)
                        seen.add(sc)
                if existing:
                    nums["candidates"] = existing
        except Exception:
            pass
        try:
            routing_ref = call.setdefault("routing", {})
            # Prefer agent name from explicit runtime metadata, otherwise fall back to known participants
            if not routing_ref.get("agent_name"):
                agent_runtime = payload.get("agent_runtime") or {}
                candidate = (agent_runtime.get("agent_name") or "").strip()
                if candidate:
                    routing_ref["agent_name"] = candidate
                else:
                    for pobj in (call.get("participants") or {}).values():
                        ident = str((pobj or {}).get("identity") or "")
                        name = str((pobj or {}).get("name") or "")
                        ident_lower = ident.lower()
                        if ident_lower.startswith("agent-") or ident_lower.startswith("agent_"):
                            routing_ref["agent_name"] = name or ident
                            break
            if not routing_ref.get("agent_id"):
                agent_runtime = payload.get("agent_runtime") or {}
                runtime_agent_id = (agent_runtime.get("agent_id") or "").strip()
                if runtime_agent_id:
                    routing_ref["agent_id"] = runtime_agent_id
            # Align agent_name with canonical lookup if we know the ID
            try:
                agent_id_for_lookup = routing_ref.get("agent_id")
                if agent_id_for_lookup:
                    lookup_name = _agent_name_lookup().get(str(agent_id_for_lookup))
                    if lookup_name:
                        routing_ref["agent_name"] = lookup_name
            except Exception:
                pass
            call_type_hint = (
                ((payload.get("ingress") or {}).get("type"))
                or ((payload.get("call") or {}).get("type"))
                or ("sip" if is_sip_related else None)
            )
            if not call_type_hint:
                rn = str(room_name or call.get("room_name") or "")
                if rn.startswith("play-"):
                    call_type_hint = "web"
                elif rn.startswith("twilio-"):
                    call_type_hint = "twilio"
            if call_type_hint and not routing_ref.get("call_type"):
                routing_ref["call_type"] = str(call_type_hint)
        except Exception:
            pass
        # Append event
        call.setdefault("events", []).append(payload)
        # Simple timeline entry
        call.setdefault("timeline", []).append({"t": int(time.time()), "event": event})
        # Persist a few common fields for list rendering
        if payload.get("room") and isinstance(payload["room"], dict):
            call["room_name"] = payload["room"].get("name") or call.get("room_name")
        # Keep event timestamps if provided
        if isinstance(payload.get("created_at"), (int, float)):
            call.setdefault("event_times", []).append({"event": event, "ts": int(payload.get("created_at"))})

        _auto_finalize_call_if_idle(str(canonical_call_id), call)

        # Attach agent runtime metadata once per call if available via env (set by agent worker)
        try:
            if not call.get("agent_runtime"):
                call["agent_runtime"] = {
                    "llm": os.getenv("AGENT_RUNTIME_LLM", ""),
                    "tts": os.getenv("AGENT_RUNTIME_TTS", ""),
                    "stt": os.getenv("AGENT_RUNTIME_STT", ""),
                }
        except Exception:
            pass

        end_like = {"room_finished", "room_ended", "room_deleted", "ingress_ended", "sip_call_ended", "call_ended"}

        # Stop worker promptly on room/call end events
        try:
            room_name_for_close = (payload.get("room") or {}).get("name") or call.get("room", {}).get("name") or call.get("room_name")
            if room_name_for_close and event in end_like:
                _cleanup_worker_for_room(room_name_for_close, reason=f"event_{event}")
                # Clear room<->call mappings to avoid attributing future events to an ended session
                try:
                    # Prefer current sip_call_id; otherwise resolve via mapping
                    cid = None
                    if sip_call_id:
                        cid = str(sip_call_id)
                    else:
                        cid = (store.get("room_to_call", {}) or {}).get(str(room_name_for_close))
                    try:
                        if cid:
                            (store.get("call_to_room", {}) or {}).pop(str(cid), None)
                    except Exception:
                        pass
                    try:
                        (store.get("room_to_call", {}) or {}).pop(str(room_name_for_close), None)
                    except Exception:
                        pass
                    # Mark call as ended for summary views
                    try:
                        if cid and isinstance(store.get("calls", {}).get(str(cid)), dict):
                            store["calls"][str(cid)]["ended_at"] = int(time.time())
                    except Exception:
                        pass
                except Exception:
                    pass
            elif room_name_for_close and event in {"participant_left", "participant_disconnected"}:
                if not _call_has_active_participants(call):
                    _cleanup_worker_for_room(room_name_for_close, reason="no_active_participants")
        except Exception:
            pass

    store = _with_livekit_store(_mutate_livekit_store_block)
    call = mutation_context.get("call") or {}
    participant_obj = mutation_context.get("participant_obj") or {}
    sip_meta = mutation_context.get("sip_meta") or {}
    dialed_number = mutation_context.get("dialed_number")
    caller_number = mutation_context.get("caller_number")

    # Trigger analytics immediately on call completion
    if event in {"room_finished", "call_ended", "sip_call_ended"}:
        _trigger_cid = (mutation_context.get("call") or {}).get("id")
        if _trigger_cid:
            _trigger_analytics(str(_trigger_cid), retry_for_transcript=True)

    # Auto-spawn/dispatch agent on first SIP participant/ringing
    try:
        room_name = (payload.get("room") or {}).get("name") or call.get("room", {}).get("name") or call.get("room_name")
        part = participant_obj
        call_status = _first_nonempty(sip_meta, ["callStatus", "status"]) or (
            (payload.get("sip", {}).get("call", {}) or {}).get("status")
        )
        should_spawn = False
        # Spawn on participant_joined for SIP participants only (avoid duplicates)
        if event == "participant_joined":
            participant_kind = part.get("kind") or ""
            if participant_kind == "sip" or str(participant_kind).upper() == "SIP":
                # SKIP AUTO-SPAWN FOR OUTBOUND ROOMS
                # Outbound calls (room name starts with "outbound-") already have their agent
                # pre-spawned and dispatched by the outbound call flow in _trigger_outbound_call.
                # The webhook should NOT spawn a second agent.
                if room_name and room_name.startswith("outbound-"):
                    print(f"[webhook] SIP participant joined OUTBOUND room {room_name} - skipping auto-spawn (agent already dispatched)", file=sys.stderr, flush=True)
                else:
                    should_spawn = True
                    print(f"[webhook] SIP participant joined, will spawn agent", file=sys.stderr, flush=True)
        # 1) Local worker autospawn path (disabled when WEBHOOK_AUTOSPAWN=0)
        if ENABLE_WEBHOOK_AUTOSPAWN and should_spawn and room_name:
            # De-dupe: avoid spawning twice for the same room (AGGRESSIVE CHECK)
            with _SPAWN_LOCK:
                # Check if already spawned OR currently spawning
                if room_name in _PLAYGROUND_PROCS or room_name in _SPAWN_IN_PROGRESS:
                    print(f"[webhook] Skipping duplicate spawn for room {room_name} (already in PROCS or IN_PROGRESS)", file=sys.stderr, flush=True)
                    return jsonify({"ok": True})
                
                # EXTRA SAFETY: For outbound rooms, check if ANY worker exists for this room
                # This catches race conditions where pre-spawn completed but sets weren't updated yet
                if room_name.startswith("outbound-"):
                    # Also check via agent name mapping
                    with _AGENT_WORKER_LOCK:
                        for agent_nm, mapped_room in _AGENT_NAME_TO_ROOM.items():
                            if mapped_room == room_name:
                                print(f"[webhook] Skipping spawn for outbound room {room_name} (worker {agent_nm} already bound)", file=sys.stderr, flush=True)
                                return jsonify({"ok": True})
                
                # Mark as spawning BEFORE releasing lock
                _SPAWN_IN_PROGRESS.add(room_name)
                print(f"[webhook] Spawning worker for room {room_name}", file=sys.stderr, flush=True)
            # Map number -> agent id (user-scoped routes take precedence)
            routes = _load_routes_union()
            agent_id = None
            match_method = None
            match_route_key = None
            # Log a concise view of SIP metadata used for routing
            try:
                dbg_called = {
                    "dialedNumber": dialed_number,
                    "callerNumber": caller_number,
                    "phoneNumber": sip_meta.get("phoneNumber"),
                    "trunkPhoneNumber": sip_meta.get("trunkPhoneNumber"),
                    "to": sip_meta.get("to"),
                    "calleeNumber": sip_meta.get("calleeNumber"),
                    "calledNumber": sip_meta.get("calledNumber"),
                }
                print(f"[webhook] Routing: extracted_sip={json.dumps(dbg_called, ensure_ascii=False)}", file=sys.stderr, flush=True)
            except Exception:
                pass
            if dialed_number:
                # Special handling for outbound SIP calls (e.g. sip:outbound-AGENTID@domain)
                if "outbound-" in dialed_number:
                    try:
                        m = re.search(r"outbound-([a-zA-Z0-9_-]+)", dialed_number)
                        if m:
                            agent_id = m.group(1)
                            match_method = "outbound_prefix"
                            match_route_key = dialed_number
                    except Exception:
                        pass
            
            # Also check SIP headers for X-Agent-ID
            if not agent_id:
                try:
                    # Inspect headers from sip payload (LiveKit usually provides these in 'sip.headers' or similar)
                    # The payload structure for SIP headers varies by event version, but often it's in sip.headers dict
                    # We don't have direct access to headers in 'sip_meta' usually unless we extracted them
                    # Let's try to find them in the raw payload if possible
                    # For now, check if we can extract from 'sip' object if available
                    sip_headers = (payload.get("sip") or {}).get("headers") or {}
                    # Look for X-Agent-ID (case insensitive)
                    for k, v in sip_headers.items():
                        if str(k).lower() == "x-agent-id":
                            candidate_id = str(v).strip()
                            if candidate_id:
                                agent_id = candidate_id
                                match_method = "header_x_agent_id"
                                match_route_key = "header"
                                break
                except Exception:
                    pass

            if dialed_number and not agent_id:
                candidates = _candidate_route_keys(dialed_number)
                try:
                    print(f"[webhook] Routing candidates: dialed_raw={dialed_number} candidates={candidates}", file=sys.stderr, flush=True)
                except Exception:
                    pass
                for method, candidate in candidates:
                    entry = (routes or {}).get(candidate)
                    if entry and entry.get("agent_id"):
                        agent_id = entry.get("agent_id")
                        match_method = method
                        match_route_key = candidate
                        break
                # Fallback: try suffix match ignoring country code and leading trunk zeros (e.g., 052xxxx → +97152xxxx)
                if not agent_id:
                    try:
                        called_tail = _digits_only(dialed_number).lstrip("0")
                        if called_tail:
                            for rk, rv in (routes or {}).items():
                                try:
                                    kdigits = _digits_only(rk)
                                    if not kdigits:
                                        continue
                                    if kdigits.endswith(called_tail) or called_tail.endswith(kdigits):
                                        agent_id = (rv or {}).get("agent_id")
                                        if agent_id:
                                            match_method = "suffix"
                                            match_route_key = rk
                                            try:
                                                print(f"[webhook] Resolved by suffix match: called={dialed_number} route_key={rk} -> agent_id={agent_id}", file=sys.stderr, flush=True)
                                            except Exception:
                                                pass
                                            break
                                except Exception:
                                    continue
                    except Exception:
                        pass
            # Abort if we still failed to resolve an agent
            if not agent_id:
                # Final fallback: Check if room name starts with "outbound-" which indicates an outbound SIP call
                # where the agent ID is embedded in the room name by LiveKit SIP convention or our TwiML
                # (e.g. outbound-<agent_id>-<random>)
                if room_name and "outbound-" in room_name:
                     try:
                         # Extract agent ID from room name: outbound-AGENTID-RANDOM or just outbound-AGENTID
                         parts = room_name.split("-")
                         if len(parts) >= 2:
                             # parts[0] is "outbound"
                             # parts[1] is likely agent_id
                             # But careful about agent ids with dashes.
                             # Let's assume agent_id is everything between "outbound-" and the last dash (if random suffix)
                             # OR just try to match known agents.
                             
                             # Try strict extraction first: "outbound-{agent_id}"
                             # Since we generate SIP URI as "sip:outbound-{agent_id}@{domain}", LiveKit usually uses
                             # the user part as the room name or prefix.
                             
                             # Let's just take the whole string after "outbound-" and see if it starts with a known agent ID
                             suffix = room_name[len("outbound-"):]
                             
                             # Check against all known agent IDs
                             try:
                                 all_agents = _get_agent_lookup() # Implement this helper or just load all
                                 for aid in all_agents.keys():
                                     if suffix.startswith(aid):
                                         agent_id = aid
                                         match_method = "room_name_outbound"
                                         match_route_key = room_name
                                         break
                             except:
                                 # If lookup fails, try heuristic: take parts[1]
                                 candidate = parts[1]
                                 if candidate:
                                     agent_id = candidate
                                     match_method = "room_name_heuristic"
                                     match_route_key = room_name
                     except Exception:
                         pass

            if not agent_id:
                with _SPAWN_LOCK:
                    _SPAWN_IN_PROGRESS.discard(room_name)
                try:
                    print(
                        "[webhook] Routing error: no agent match",
                        json.dumps(
                            {
                                "dialed": dialed_number,
                                "caller": caller_number,
                                "candidates": _candidate_route_keys(dialed_number) if dialed_number else [],
                            },
                            ensure_ascii=False,
                        ),
                        file=sys.stderr,
                        flush=True,
                    )
                except Exception:
                    pass
                return jsonify({"ok": True})
            try:
                routing_info = call.setdefault("routing", {})
                if dialed_number:
                    routing_info["dialed_number_raw"] = dialed_number
                    routing_info["dialed_number_e164"] = _normalize_e164(dialed_number)
                routing_info["caller_number"] = caller_number
                routing_info["match_method"] = match_method
                routing_info["route_key"] = match_route_key
                routing_info["agent_id"] = agent_id
                routing_info.setdefault("call_type", "sip")
            except Exception:
                pass
            try:
                print(
                    f"[webhook] Routing resolved: dialed={dialed_number} caller={caller_number} method={match_method} route_key={match_route_key} agent_id={agent_id}",
                    file=sys.stderr,
                    flush=True,
                )
            except Exception:
                pass
            if agent_id:
                # Spawn worker bound to this room
                try:
                    env = os.environ.copy()
                    env.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", os.getenv("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python"))
                    env.setdefault("PYTHONUNBUFFERED", "1")
                    # Force CPU-only execution to avoid noisy GPU/VAAPI init warnings on servers without drivers
                    env["RUN_AS_WORKER"] = "1"
                    if AUTOSPAWN_DIRECT:
                        # Join the SIP room by direct token connect (legacy playground mode)
                        env["PLAYGROUND_ROOM_NAME"] = str(room_name)
                        env["PLAYGROUND_FORCE_DIRECT"] = "1"
                        try:
                            env.setdefault("PLAYGROUND_IDENTITY", f"agent-{_uuid.uuid4().hex[:6]}")
                        except Exception:
                            pass
                    else:
                        # Use standard worker dispatch path (faster join, aligns with LiveKit docs)
                        env.pop("PLAYGROUND_ROOM_NAME", None)
                        env.pop("PLAYGROUND_FORCE_DIRECT", None)
                    env["AGENT_PROFILE_ID"] = str(agent_id)
                    # Log which agent profile we're spawning for this room
                    try:
                        _agent_obj = _get_agent(agent_id)
                        _agent_name = (_agent_obj or {}).get("name") or str(agent_id)
                    except Exception:
                        _agent_name = str(agent_id)
                    try:
                        call.setdefault("routing", {})["agent_name"] = _agent_name
                    except Exception:
                        pass
                    try:
                        print(f"[webhook] Spawning worker for room {room_name} agent_name={_agent_name} agent_id={agent_id}", file=sys.stderr, flush=True)
                    except Exception:
                        pass
                    # Ensure worker registers with AGENT_NAME equal to the ASSIGNED agent number (route key), not the caller
                    try:
                        def _digits_only(raw: str) -> str:
                            return re.sub(r"[^0-9]", "", str(raw or ""))

                        def _normalize_number(raw: str) -> str:
                            return _digits_only(raw)

                        def _format_e164(raw: str) -> str:
                            digits = _digits_only(raw)
                            return f"+{digits}" if digits else ""

                        assigned_num_key = match_route_key
                        dialed_digits = _digits_only(dialed_number) if dialed_number else None
                        # 1) Prefer route that matched this agent AND matches called number (ignoring formatting)
                        for rk, rv in (routes or {}).items():
                            try:
                                if str((rv or {}).get("agent_id")) == str(agent_id):
                                    if dialed_digits and _digits_only(rk) == dialed_digits:
                                        assigned_num_key = rk
                                        break
                            except Exception:
                                continue
                        # 2) Otherwise, pick any route number assigned to this agent
                        if not assigned_num_key:
                            for rk, rv in (routes or {}).items():
                                try:
                                    if str((rv or {}).get("agent_id")) == str(agent_id):
                                        assigned_num_key = rk
                                        break
                                except Exception:
                                    continue
                        # 3) Otherwise, try the globally selected number if it maps to this agent
                        if not assigned_num_key:
                            try:
                                sel = (_load_numbers() or {}).get("selected_number")
                                if sel and str(((routes or {}).get(sel) or {}).get("agent_id")) == str(agent_id):
                                    assigned_num_key = sel
                            except Exception:
                                pass
                        # 4) Normalize for AGENT_NAME; fallback to room-derived digits or random id
                        display_num = _format_e164(assigned_num_key) if assigned_num_key else None
                        if assigned_num_key:
                            num = _normalize_number(assigned_num_key)
                        else:
                            # Attempt to extract a number from room name as last resort
                            m = re.search(r"([+]?\d{6,})", str(room_name))
                            num = _normalize_number(m.group(1)) if m else f"agent-{_uuid.uuid4().hex[:6]}"
                        env["AGENT_NAME"] = num
                        if display_num:
                            env.setdefault("AGENT_NAME_E164", display_num)
                        env.setdefault("DIALED_NUMBER", dialed_number or display_num or num)
                        env.setdefault("AGENT_PHONE_NUMBER", display_num or num)
                    except Exception:
                        env["AGENT_NAME"] = f"agent-{_uuid.uuid4().hex[:6]}"
                    agent_worker_name = env.get("AGENT_NAME")
                    _ensure_single_worker_for_agent(agent_worker_name, reason="pre_spawn", target_room=room_name)
                    # Silence GPU/VAAPI and multimedia noise
                    env.setdefault("LIBVA_MESSAGING_LEVEL", "0")
                    env.setdefault("FFMPEG_LOG_LEVEL", "error")
                    env.setdefault("GLOG_minloglevel", "2")
                    env.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
                    env.setdefault("CUDA_VISIBLE_DEVICES", "")
                    # Provide config API base for number routing and defaults
                    try:
                        env.setdefault("CONFIG_API_BASE", os.getenv("CONFIG_API_BASE", f"http://localhost:{os.getenv('CONFIG_API_PORT','5057')}"))
                    except Exception:
                        pass
                    # Inject LiveKit and OpenAI creds from defaults if present
                    try:
                        d = _load_defaults() or {}
                    except Exception:
                        d = {}
                    lk_def = (d or {}).get("livekit", {}) or {}
                    env["LIVEKIT_URL"] = str(lk_def.get("url") or os.getenv("LIVEKIT_URL", ""))
                    env["LIVEKIT_API_KEY"] = str(lk_def.get("api_key") or os.getenv("LIVEKIT_API_KEY", ""))
                    env["LIVEKIT_API_SECRET"] = str(lk_def.get("api_secret") or os.getenv("LIVEKIT_API_SECRET", ""))
                    openai_def = (d or {}).get("openai", {}) or {}
                    if openai_def.get("api_key"):
                        env.setdefault("OPENAI_API_KEY", str(openai_def.get("api_key")))
                    if openai_def.get("base_url"):
                        env.setdefault("OPENAI_BASE_URL", str(openai_def.get("base_url")))
                    # Start process
                    agent_path = str((Path(__file__).parent / "agent.py").resolve())
                    try:
                        logs_dir = Path(__file__).parent / "logs"
                        logs_dir.mkdir(parents=True, exist_ok=True)
                        # Unique logfile: number + timestamp + short id
                        base = (env.get("AGENT_NAME") or str(room_name)).strip()
                        ts = time.strftime("%Y%m%d_%H%M%S")
                        short = _uuid.uuid4().hex[:6]
                        log_fp = open(str(logs_dir / f"play_{base}_{ts}_{short}.log"), "wb")
                    except Exception:
                        log_fp = None
                    if log_fp is not None:
                        proc = subprocess.Popen([sys.executable, agent_path], env=env, stdout=log_fp, stderr=subprocess.STDOUT, bufsize=1, universal_newlines=False, preexec_fn=_preexec_child)
                    else:
                        proc = subprocess.Popen([sys.executable, agent_path], env=env, preexec_fn=_preexec_child)
                    _bind_worker(room_name, proc, log_fp, agent_name=agent_worker_name)
                    if not AUTOSPAWN_DIRECT:
                        try:
                            dispatch_meta = {
                                "call_id": room_name,
                                "dialed": dialed_number,
                                "caller": caller_number,
                                "agent_id": agent_id,
                                "agent_name": env.get("AGENT_NAME"),
                                "agent_e164": env.get("AGENT_NAME_E164") or env.get("AGENT_PHONE_NUMBER"),
                            }
                            _create_agent_dispatch(room_name, env.get("AGENT_NAME"), dispatch_meta)
                        except Exception as exc:
                            print(f"[dispatch] Failed to create agent dispatch room={room_name}: {exc}", file=sys.stderr, flush=True)
                except Exception:
                    pass
                finally:
                    with _SPAWN_LOCK:
                        _SPAWN_IN_PROGRESS.discard(room_name)
        # 2) Cloud explicit-dispatch fallback: ensure a Cloud Agent joins the room by agent_name (phone number)
        # Enabled when ENABLE_CLOUD_DISPATCH=1 (defaults to on). Safe even if the SIP rule already has agents configured.
        ENABLE_CLOUD_DISPATCH = (os.getenv("ENABLE_CLOUD_DISPATCH", "1") == "1")
        if (not ENABLE_WEBHOOK_AUTOSPAWN) and ENABLE_CLOUD_DISPATCH and should_spawn and room_name:
            try:
                # Determine the agent_name to dispatch using the same routing resolution as local spawn
                routes = _load_routes_union()
                agent_id = None
                match_route_key_cloud = None
                if dialed_number:
                    for method, candidate in _candidate_route_keys(dialed_number):
                        entry = (routes or {}).get(candidate)
                        if entry and entry.get("agent_id"):
                            agent_id = entry.get("agent_id")
                            match_route_key_cloud = candidate
                            break
                    if not agent_id:
                        try:
                            called_tail = _digits_only(dialed_number).lstrip("0")
                            if called_tail:
                                for rk, rv in (routes or {}).items():
                                    try:
                                        kdigits = _digits_only(rk)
                                        if not kdigits:
                                            continue
                                        if kdigits.endswith(called_tail) or called_tail.endswith(kdigits):
                                            agent_id = (rv or {}).get("agent_id")
                                            if agent_id:
                                                match_route_key_cloud = rk
                                                break
                                    except Exception:
                                        continue
                        except Exception:
                            pass
                # Resolve the phone number key assigned to this agent
                def _digits_only(raw: str) -> str:
                    return re.sub(r"[^0-9]", "", str(raw or ""))
                def _normalize_number(raw: str) -> str:
                    return re.sub(r"[^0-9]", "", str(raw or ""))

                def _format_e164(raw: str) -> str:
                    digits = _normalize_number(raw)
                    return f"+{digits}" if digits else ""
                assigned_num_key = match_route_key_cloud
                called_digits = _digits_only(dialed_number) if dialed_number else None
                if agent_id:
                    for rk, rv in (routes or {}).items():
                        try:
                            if str((rv or {}).get("agent_id")) == str(agent_id):
                                if called_digits and _digits_only(rk) == called_digits:
                                    assigned_num_key = rk; break
                        except Exception:
                            continue
                    if not assigned_num_key:
                        for rk, rv in (routes or {}).items():
                            try:
                                if str((rv or {}).get("agent_id")) == str(agent_id):
                                    assigned_num_key = rk; break
                            except Exception:
                                continue
                if not assigned_num_key and dialed_number:
                    assigned_num_key = dialed_number
                agent_name_for_dispatch = _normalize_number(assigned_num_key) if assigned_num_key else None
                agent_name_e164 = _format_e164(assigned_num_key) if assigned_num_key else None
                try:
                    routing_info = call.setdefault("routing", {})
                    if agent_id:
                        routing_info.setdefault("agent_id", agent_id)
                    if agent_name_e164:
                        routing_info.setdefault("agent_name", agent_name_e164)
                except Exception:
                    pass
                # Prepare LiveKit API client (convert ws(s) to http(s))
                if agent_name_for_dispatch:
                    try:
                        d = _load_defaults() or {}
                    except Exception:
                        d = {}
                    lk_def = (d or {}).get("livekit", {}) or {}
                    lk_url = str(lk_def.get("url") or os.getenv("LIVEKIT_URL", ""))
                    lk_key = str(lk_def.get("api_key") or os.getenv("LIVEKIT_API_KEY", ""))
                    lk_secret = str(lk_def.get("api_secret") or os.getenv("LIVEKIT_API_SECRET", ""))
                    if lk_url.startswith("wss://"):
                        lk_url_http = "https://" + lk_url[len("wss://"):]
                    elif lk_url.startswith("ws://"):
                        lk_url_http = "http://" + lk_url[len("ws://"):]
                    else:
                        lk_url_http = lk_url
                    if lk_url_http and lk_key and lk_secret:
                        async def _ensure_dispatch():
                            async with lk_api.LiveKitAPI(
                                url=lk_url_http,
                                api_key=lk_key,
                                api_secret=lk_secret,
                            ) as client:
                                try:
                                    existing = await client.agent_dispatch.list_dispatch(room_name)
                                except Exception as exc:
                                    print(
                                        f"[dispatch] list failed room={room_name} agent={agent_name_for_dispatch}: {exc}",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                    existing = []
                                already = False
                                for dpo in (existing or []):
                                    if getattr(dpo, "agent_name", None) == agent_name_for_dispatch:
                                        already = True
                                        break
                                if already:
                                    return
                                try:
                                    req = lk_api.CreateAgentDispatchRequest(
                                        agent_name=agent_name_for_dispatch,
                                        room=room_name,
                                    )
                                except Exception:
                                    from livekit.protocol.agent_dispatch import CreateAgentDispatchRequest as _Req
                                    req = _Req(agent_name=agent_name_for_dispatch, room=room_name)
                                await client.agent_dispatch.create_dispatch(req)
                                print(
                                    f"[dispatch] Created room={room_name} agent={agent_name_for_dispatch}",
                                    file=sys.stderr,
                                    flush=True,
                                )

                        try:
                            asyncio.run(_ensure_dispatch())
                        except RuntimeError as exc:
                            if "asyncio.run()" in str(exc):
                                loop = asyncio.new_event_loop()
                                try:
                                    loop.run_until_complete(_ensure_dispatch())
                                finally:
                                    loop.close()
                            else:
                                raise
                        except Exception as exc:
                            print(
                                f"[dispatch] Failed to create room={room_name} agent={agent_name_for_dispatch}: {exc}",
                                file=sys.stderr,
                                flush=True,
                            )
            except Exception:
                pass
    except Exception:
        pass

    try:
        _save_livekit_store(store)
    except Exception:
        pass

    return jsonify({"ok": True})


def _normalize_last_received(raw: Any) -> int:
    """Convert mixed timestamp inputs to ints so sorting never raises."""
    if raw is None:
        return 0
    if isinstance(raw, (int, float)):
        return int(raw)
    try:
        return int(float(raw))
    except Exception:
        return 0


@app.get("/livekit/calls")
@require_auth()
def list_lk_calls():
    store = _load_livekit_store()
    rows = []
    for call_id, v in (store.get("calls", {}) or {}).items():
        if str(call_id).startswith(("EV_", "EG_")):
            continue
        routing = v.get("routing") or {}
        timeline = v.get("timeline") or []
        duration = None
        if isinstance(timeline, list) and len(timeline) >= 2:
            try:
                duration = max(0, int(timeline[-1].get("t", 0)) - int(timeline[0].get("t", 0)))
            except Exception:
                duration = None
        rows.append({
            "id": call_id,
            "room": {"name": v.get("room_name") or (v.get("room") or {}).get("name")},
            "last_event": v.get("last_event"),
            "last_received_at": v.get("last_received_at"),
            "agent_id": routing.get("agent_id"),
            "agent_name": routing.get("agent_name"),
            "call_type": routing.get("call_type"),
            "duration": duration,
        })
    # basic summary sorting, newest first
    rows.sort(key=lambda x: _normalize_last_received(x.get("last_received_at")), reverse=True)
    return jsonify({"calls": rows, "agent_name_hint": os.getenv("AGENT_NAME", "")})


# ------------------ INBOUND NUMBER → AGENT ROUTING ------------------

@app.get("/numbers")
def list_numbers():
    """Return mapping of E.164/extension numbers to agent_ids and trunk labels.
    Shape: { routes: { "0523368120": { agent_id: "...", trunk: "Voice Agent V2 - Local" }, ... } }
    """
    return jsonify(_load_numbers())


@app.put("/numbers")
@require_auth(admin=True)
def upsert_numbers():
    """Replace or merge the number routing table.
    Payload: { routes: { number: { agent_id, trunk } } }
    """
    payload = request.json or {}
    cur = _load_numbers()
    routes = cur.get("routes", {})
    for k, v in (payload.get("routes") or {}).items():
        if not isinstance(v, dict):
            continue
        # enforce one agent per number (overwrite) and uniqueness across numbers is naturally satisfied
        routes[str(k)] = { "agent_id": v.get("agent_id"), "trunk": v.get("trunk") }
    cur["routes"] = routes
    # Enforce uniqueness across numbers: only one number may hold a given agent_id
    seen: set[str] = set()
    for n in list(cur["routes"].keys()):
        rid = (cur["routes"][n] or {}).get("agent_id") or ""
        rid = str(rid)
        if rid:
            if rid in seen:
                cur["routes"][n]["agent_id"] = ""
            else:
                seen.add(rid)
    # Optional UI-driven selection of active/worker number
    selected_number = payload.get("selected_number")
    if isinstance(selected_number, str) and selected_number:
        # Allow setting even if not in current routes yet (may be included in payload routes)
        cur["selected_number"] = selected_number
    _save_numbers(cur)
    return jsonify(cur)


@app.delete("/numbers/<number>")
@require_auth(admin=True)
def delete_number(number: str):
    cur = _load_numbers()
    routes = cur.get("routes", {})
    if number in routes:
        del routes[number]
        cur["routes"] = routes
        # If the deleted number was selected, clear the selection
        try:
            if cur.get("selected_number") == number:
                del cur["selected_number"]
        except Exception:
            pass
        _save_numbers(cur)
        return ("", 204)
    return jsonify({"error": "not_found"}), 404


@app.get("/livekit/calls/<call_id>")
@require_auth()
def get_lk_call(call_id: str):
    store = _load_livekit_store()
    data = store.get("calls", {}).get(call_id)
    if not data:
        return jsonify({"error": "not_found"}), 404
    svc = _analytics_service()
    if svc:
        try:
            analysis = svc.get_call_analysis(call_id)
            if analysis:
                data["analysis"] = analysis
        except Exception:
            pass
    return jsonify(data)


# ------------------ LIVEKIT RECORDINGS & TRANSCRIPTS ------------------

def _recording_s3_env() -> dict:
    return {
        "bucket": os.getenv("LIVEKIT_RECORDING_S3_BUCKET", "").strip(),
        "region": os.getenv("LIVEKIT_RECORDING_S3_REGION", "").strip(),
        "endpoint": os.getenv("LIVEKIT_RECORDING_S3_ENDPOINT", "").strip(),
        "access_key": os.getenv("LIVEKIT_RECORDING_S3_ACCESS_KEY", "").strip(),
        "secret_key": os.getenv("LIVEKIT_RECORDING_S3_SECRET_KEY", "").strip(),
        "force_path_style": os.getenv("LIVEKIT_RECORDING_S3_FORCE_PATH_STYLE", "0") == "1",
    }


@lru_cache(maxsize=1)
def _recording_s3_client():
    cfg = _recording_s3_env()
    if not cfg["bucket"] or not cfg["access_key"] or not cfg["secret_key"]:
        return None
    session = boto3.session.Session(
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=cfg["region"] or None,
    )
    boto_cfg = None
    if cfg["force_path_style"]:
        boto_cfg = BotoConfig(s3={"addressing_style": "path"})
    return session.client(
        "s3",
        endpoint_url=cfg["endpoint"] or None,
        config=boto_cfg,
    )


def _recordings_list_for_call(call: dict) -> list[dict]:
    return call.setdefault("recordings", [])


def _touch_room_mapping(store: dict, call_id: str, room_name: str | None) -> None:
    if not room_name:
        return
    room_map = store.setdefault("room_to_call", {})
    call_map = store.setdefault("call_to_room", {})
    room_map[str(room_name)] = str(call_id)
    call_map[str(call_id)] = str(room_name)


def _update_recording_entry(call: dict, egress_id: str | None, payload: dict) -> dict:
    recs = _recordings_list_for_call(call)
    rec_id = str(egress_id or payload.get("id") or uuid.uuid4().hex)
    target = None
    for rec in recs:
        if rec.get("egress_id") == egress_id and egress_id:
            target = rec
            break
        if rec.get("id") == rec_id:
            target = rec
            break
    if target is None:
        target = {"id": rec_id}
        if egress_id:
            target["egress_id"] = egress_id
        recs.append(target)
    target.update({k: v for k, v in payload.items() if v is not None})
    target.setdefault("egress_id", egress_id)
    target.setdefault("updated_at", int(time.time()))
    return target


def _find_recording(call: dict, rec_id: str) -> dict | None:
    for rec in call.get("recordings", []) or []:
        if str(rec.get("id")) == rec_id or str(rec.get("egress_id")) == rec_id:
            return rec
    return None


def _handle_egress_webhook_event(store: dict, event_name: str, payload: dict) -> None:
    try:
        if not event_name.startswith("egress_"):
            return
        egress = payload.get("egressInfo") or payload.get("egress") or {}
        egress_id = egress.get("egressId") or egress.get("egress_id") or payload.get("egress_id")
        if not egress_id:
            print(f"[egress-webhook] No egress_id found in payload, skipping", file=sys.stderr)
            return
        
        # Extract room_name from various possible locations in the egress payload
        room_name = (
            egress.get("roomName") or 
            egress.get("room_name") or 
            (payload.get("room") or {}).get("name") or
            egress.get("roomId")  # Sometimes only roomId is provided
        )
        
        print(f"[egress-webhook] Processing {event_name}: egress_id={egress_id} room_name={room_name}", file=sys.stderr)
        
        call_id = payload.get("call_id")
        if not call_id and room_name:
            call_id = (store.get("room_to_call", {}) or {}).get(str(room_name))
        if not call_id:
            # Try using room_name as call_id (common for outbound calls)
            call_id = room_name
        if not call_id:
            print(f"[egress-webhook] Could not determine call_id for egress {egress_id}", file=sys.stderr)
            return
        calls = store.setdefault("calls", {})
        if str(call_id) not in calls:
            # Create the call record if it doesn't exist (recording can be processed before call is explicitly created)
            calls[str(call_id)] = {"id": str(call_id), "room_name": room_name, "created_at": int(time.time())}
        call = calls[str(call_id)]
        if room_name:
            call["room_name"] = room_name
            _touch_room_mapping(store, call_id, room_name)
        file_path = None
        # Try various locations for file info (LiveKit uses both camelCase and snake_case)
        file_block = egress.get("file") or egress.get("fileResults") or {}
        if isinstance(file_block, dict):
            file_path = file_block.get("filepath") or file_block.get("filename") or file_block.get("filePath")
        if not file_path:
            file_results = egress.get("file_results") or egress.get("fileResults") or []
            if isinstance(file_results, list) and file_results:
                first_file = file_results[0] or {}
                file_path = first_file.get("filepath") or first_file.get("filename") or first_file.get("filePath")
        
        # Calculate duration if start and end times are available (handle both camelCase and snake_case)
        duration = None
        started_at = egress.get("started_at") or egress.get("startedAt")
        ended_at = egress.get("ended_at") or egress.get("endedAt")
        if started_at and ended_at:
            try:
                duration = int(ended_at) - int(started_at)
            except (ValueError, TypeError):
                pass
        
        # Get status (handle both camelCase and snake_case)
        egress_status_raw = egress.get("status") or ""
        
        update = {
            "status": egress_status_raw,
            "details": egress,
            "filepath": file_path,
            "ended_at": ended_at,
            "started_at": started_at,
            "duration": duration,
            "updated_at": int(time.time()),
        }
        rec = _update_recording_entry(call, egress_id, update)
        call["last_event"] = event_name
        call["last_received_at"] = int(time.time())
        event_entry = {
            "event": event_name,
            "createdAt": payload.get("created_at") or payload.get("createdAt") or int(time.time()),
            "egress": {
                "egress_id": egress_id,
                "status": egress_status_raw,
                "filepath": file_path,
                "error": egress.get("error"),
                "started_at": started_at,
                "ended_at": ended_at,
                "duration": duration,
            },
        }
        if rec.get("storage"):
            event_entry["egress"]["storage"] = rec.get("storage")
        call.setdefault("events", []).append(event_entry)
        
        # Check if this is egress_ended and recording is complete - trigger callback if registered
        egress_status = (egress_status_raw or "").upper()
        print(f"[egress-webhook] Status: {egress_status} event={event_name} call_id={call_id} file={file_path}", file=sys.stderr)
        
        if event_name == "egress_ended" or egress_status in ("EGRESS_COMPLETE", "COMPLETE", "ENDED"):
            print(f"[egress-webhook] Recording complete, checking for callback: call_id={call_id} room_name={room_name}", file=sys.stderr)
            
            # Recording is ready - check for registered callback
            # Try both call_id and room_name as the callback might be registered under either
            callback_data = _get_and_remove_callback(str(call_id)) if call_id else None
            if not callback_data and room_name and room_name != call_id:
                callback_data = _get_and_remove_callback(str(room_name))
            
            if callback_data:
                print(f"[egress-webhook] Found callback for {call_id}, triggering recording-ready callback", file=sys.stderr)
                # Send callback in a background thread to not block webhook response
                import threading
                threading.Thread(
                    target=_send_recording_ready_callback,
                    args=(callback_data, str(call_id), rec),
                    daemon=True
                ).start()
            else:
                print(f"[egress-webhook] No callback registered for call_id={call_id} room_name={room_name}", file=sys.stderr)
    except Exception as e:
        print(f"[egress-webhook] Error handling egress event: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()


@app.post("/internal/livekit/recordings")
@require_internal()
def internal_upsert_livekit_recording():
    payload = request.json or {}
    store = _load_livekit_store()
    call_id = payload.get("call_id")
    room_name = payload.get("room_name")
    if not call_id and room_name:
        call_id = (store.get("room_to_call", {}) or {}).get(str(room_name)) or str(room_name)
    if not call_id:
        return jsonify({"error": "call_id_required"}), 400
    call = _lk_get_or_create_call(store, str(call_id))
    if room_name:
        call["room_name"] = room_name
        _touch_room_mapping(store, call_id, room_name)
    record = _update_recording_entry(
        call,
        payload.get("egress_id"),
        {
            "status": payload.get("status"),
            "filepath": payload.get("filepath"),
            "storage": payload.get("storage"),
            "options": payload.get("options"),
            "agent": payload.get("agent"),
            "started_at": payload.get("started_at"),
            "updated_at": int(time.time()),
        },
    )
    _save_livekit_store(store)
    return jsonify({"recording": record})


# ------------------ RECORDING CALLBACK MANAGEMENT ------------------

def _load_callback_store() -> dict:
    """Load callback registrations from storage."""
    return _storage_backend().load_callback_store()


def _save_callback_store(data: dict) -> None:
    """Save callback registrations to storage."""
    _storage_backend().save_callback_store(data)


def _register_recording_callback(call_id: str, callback_data: dict) -> None:
    """Register a callback URL for recording-ready notifications."""
    store = _load_callback_store()
    callbacks = store.setdefault("pending_callbacks", {})
    callbacks[str(call_id)] = {
        "callback_url": callback_data.get("callback_url"),
        "interview_id": callback_data.get("interview_id"),
        "candidate_name": callback_data.get("candidate_name"),
        "phone_number": callback_data.get("phone_number"),
        "registered_at": int(time.time()),
    }
    _save_callback_store(store)


def _get_and_remove_callback(call_id: str) -> dict | None:
    """Get and remove a registered callback for a call."""
    store = _load_callback_store()
    callbacks = store.get("pending_callbacks", {})
    callback_data = callbacks.pop(str(call_id), None)
    if callback_data:
        _save_callback_store(store)
    return callback_data


def _send_recording_ready_callback(callback_data: dict, call_id: str, recording: dict) -> None:
    """Send a callback notification when recording is ready."""
    import requests as http_requests
    
    callback_url = callback_data.get("callback_url")
    if not callback_url:
        return
    
    api_base = os.getenv("CONFIG_API_BASE", "http://localhost:5057")
    rec_id = recording.get("id") or recording.get("egress_id")
    
    payload = {
        "event": "recording_ready",
        "call_id": call_id,
        "interview_id": callback_data.get("interview_id"),
        "candidate_name": callback_data.get("candidate_name"),
        "phone_number": callback_data.get("phone_number"),
        "recording": {
            "status": "available",
            "recording_id": rec_id,
            "recording_url": f"{api_base}/api/livekit/calls/{call_id}/recordings/{rec_id}/file",
            "duration_seconds": recording.get("duration"),
            "filepath": recording.get("filepath"),
            "started_at": recording.get("started_at"),
            "ended_at": recording.get("ended_at"),
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    
    # Save callback result to database BEFORE sending
    result_id = None
    try:
        store = _storage_backend()
        result_id = store.save_callback_result(
            call_id=call_id,
            callback_type="recording_ready",
            payload=payload,
            callback_url=callback_url,
            interview_id=callback_data.get("interview_id"),
        )
        print(f"[recording-callback] Saved recording_ready payload to DB: result_id={result_id}", file=sys.stderr)
    except Exception as save_err:
        print(f"[recording-callback] Failed to save callback payload to DB: {save_err}", file=sys.stderr)
    
    http_status = None
    http_response = None
    success = False
    
    try:
        resp = http_requests.post(
            callback_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        http_status = resp.status_code
        success = 200 <= resp.status_code < 300
        if not success:
            http_response = resp.text[:500]
        print(f"[recording-callback] Sent recording_ready to {callback_url}: {resp.status_code}", file=sys.stderr)
    except Exception as e:
        print(f"[recording-callback] Failed to send recording_ready callback: {e}", file=sys.stderr)
        http_response = str(e)[:500]
    
    # Update callback result with response info
    if result_id:
        try:
            store.update_callback_result(
                result_id=result_id,
                http_status=http_status,
                http_response=http_response,
                success=success,
            )
        except Exception as upd_err:
            print(f"[recording-callback] Failed to update callback result: {upd_err}", file=sys.stderr)


@app.post("/internal/callback/register")
@require_internal()
def internal_register_callback():
    """Register a callback URL for recording-ready notifications.
    
    Called by the agent when a call ends to register for recording notification.
    """
    payload = request.json or {}
    call_id = payload.get("call_id")
    callback_url = payload.get("callback_url")
    
    if not call_id:
        return jsonify({"error": "call_id_required"}), 400
    if not callback_url:
        return jsonify({"error": "callback_url_required"}), 400
    
    _register_recording_callback(call_id, payload)
    print(f"[callback-register] Registered callback for call {call_id}", file=sys.stderr)
    
    return jsonify({"status": "registered", "call_id": call_id})


@app.post("/internal/callback/save")
@require_internal()
def internal_save_callback():
    """Save a callback payload to the database for auditing.
    
    Called by the agent before sending a questionnaire callback.
    """
    payload = request.json or {}
    call_id = payload.get("call_id")
    callback_type = payload.get("callback_type")
    callback_payload = payload.get("payload")
    
    if not call_id or not callback_type or not callback_payload:
        return jsonify({"error": "missing_fields"}), 400
    
    try:
        store = _storage_backend()
        result_id = store.save_callback_result(
            call_id=call_id,
            callback_type=callback_type,
            payload=callback_payload,
            callback_url=payload.get("callback_url"),
            interview_id=payload.get("interview_id"),
        )
        return jsonify({"status": "saved", "id": result_id})
    except Exception as e:
        print(f"[callback-save] Failed to save callback: {e}", file=sys.stderr)
        return jsonify({"error": str(e)}), 500


@app.post("/internal/callback/update")
@require_internal()
def internal_update_callback():
    """Update a callback result with HTTP response info.
    
    Called by the agent after sending a callback to record the response.
    """
    payload = request.json or {}
    result_id = payload.get("id")
    
    if not result_id:
        return jsonify({"error": "id_required"}), 400
    
    try:
        store = _storage_backend()
        store.update_callback_result(
            result_id=result_id,
            http_status=payload.get("http_status"),
            http_response=payload.get("http_response"),
            success=payload.get("success"),
        )
        return jsonify({"status": "updated"})
    except Exception as e:
        print(f"[callback-update] Failed to update callback: {e}", file=sys.stderr)
        return jsonify({"error": str(e)}), 500


@app.get("/api/livekit/calls/<call_id>/callbacks")
@require_auth()
def list_call_callbacks(call_id: str):
    """Retrieve callback results for a specific call.
    
    Returns all callback payloads (questionnaire results, recording notifications)
    that were sent for this call, along with HTTP response status.
    """
    try:
        store = _storage_backend()
        results = store.get_callback_results(call_id=call_id)
        return jsonify({"callbacks": results})
    except Exception as e:
        print(f"[callbacks] Failed to retrieve callbacks: {e}", file=sys.stderr)
        return jsonify({"error": str(e)}), 500


@app.get("/api/callbacks")
@require_auth()
def list_all_callbacks():
    """Retrieve callback results with optional filters.
    
    Query params:
    - call_id: Filter by call ID
    - interview_id: Filter by external interview ID
    - callback_type: Filter by type ('questionnaire' or 'recording_ready')
    - limit: Maximum results (default 100)
    """
    try:
        store = _storage_backend()
        results = store.get_callback_results(
            call_id=request.args.get("call_id"),
            interview_id=request.args.get("interview_id"),
            callback_type=request.args.get("callback_type"),
            limit=int(request.args.get("limit", 100)),
        )
        return jsonify({"callbacks": results, "count": len(results)})
    except Exception as e:
        print(f"[callbacks] Failed to retrieve callbacks: {e}", file=sys.stderr)
        return jsonify({"error": str(e)}), 500


@app.get("/livekit/calls/<call_id>/recordings")
@require_auth()
def list_call_recordings(call_id: str):
    call = (_load_livekit_store().get("calls") or {}).get(call_id) or {}
    return jsonify({"recordings": call.get("recordings") or []})


@app.get("/livekit/calls/<call_id>/recordings/<rec_id>/file")
@require_auth()
def stream_call_recording(call_id: str, rec_id: str):
    store = _load_livekit_store()
    call = (store.get("calls") or {}).get(call_id)
    if not call:
        return jsonify({"error": "not_found"}), 404
    rec = _find_recording(call, rec_id)
    if not rec:
        return jsonify({"error": "recording_not_found"}), 404
    storage = rec.get("storage") or {}
    if storage.get("kind", "s3") != "s3":
        return jsonify({"error": "unsupported_storage"}), 400
    key = rec.get("filepath") or storage.get("key")
    if not key:
        return jsonify({"error": "missing_filepath"}), 400
    bucket = storage.get("bucket") or _recording_s3_env().get("bucket")
    if not bucket:
        return jsonify({"error": "bucket_not_configured"}), 503
    client = _recording_s3_client()
    if client is None:
        return jsonify({"error": "storage_not_configured"}), 503
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 500)
        return jsonify({"error": "storage_error", "message": str(exc)}), status
    body = obj.get("Body")
    if body is None:
        return jsonify({"error": "empty_body"}), 500
    content_type = obj.get("ContentType") or ("audio/ogg" if rec.get("options", {}).get("audio_only") else "video/mp4")
    content_length = obj.get("ContentLength")
    download_name = rec.get("download_name") or f"{rec_id}.{ 'ogg' if rec.get('options', {}).get('audio_only') else 'mp4'}"

    def _generate():
        try:
            while True:
                chunk = body.read(8192)
                if not chunk:
                    break
                yield chunk
        finally:
            try:
                body.close()
            except Exception:
                pass

    headers = {"Content-Disposition": f'inline; filename="{download_name}"'}
    if content_length:
        headers["Content-Length"] = str(content_length)
    return Response(_generate(), headers=headers, mimetype=content_type)


@app.post("/internal/livekit/transcript")
@require_internal()
def internal_upsert_transcript():
    payload = request.json or {}
    call_id = payload.get("call_id")
    if not call_id:
        return jsonify({"error": "call_id_required"}), 400

    transcript_data = {
        "entries": payload.get("entries") or [],
        "agent": payload.get("agent") or {},
        "room_name": payload.get("room_name"),
        "started_at": payload.get("started_at"),
        "ended_at": payload.get("ended_at"),
        "saved_at": int(time.time()),
    }

    def mutator(store):
        call = _lk_get_or_create_call(store, str(call_id))
        call["transcript"] = transcript_data
        if payload.get("room_name"):
            _touch_room_mapping(store, call_id, payload.get("room_name"))

    _with_livekit_store(mutator)
    
    # Trigger analysis on full transcript upload
    if call_id:
        _trigger_analytics(str(call_id), retry_for_transcript=False)
    return jsonify({"transcript": transcript_data})


@app.post("/internal/livekit/transcript/events")
@require_internal()
def internal_transcript_event():
    payload = request.json or {}
    call_id = payload.get("call_id")
    segment = payload.get("segment") or {}
    if not call_id or not segment.get("text"):
        return jsonify({"error": "invalid_payload"}), 400

    new_entry = {
            "role": segment.get("role"),
            "text": segment.get("text"),
            "timestamp_ms": segment.get("timestamp_ms") or int(time.time() * 1000),
            "confidence": segment.get("confidence"),
        }

    def mutator(store):
        call = _lk_get_or_create_call(store, str(call_id))
        transcript = call.setdefault("transcript", {})
        entries = transcript.setdefault("entries", [])
        entries.append(new_entry)
        transcript["agent"] = payload.get("agent") or transcript.get("agent") or {}
        transcript["room_name"] = transcript.get("room_name") or call.get("room_name")
        transcript["last_event_at"] = int(time.time())
        call["transcript"] = transcript

    _with_livekit_store(mutator)
    return jsonify({"ok": True})


@app.get("/livekit/calls/<call_id>/transcript")
@require_auth()
def get_call_transcript(call_id: str):
    call = (_load_livekit_store().get("calls") or {}).get(call_id)
    if not call or "transcript" not in call:
        return jsonify({"transcript": None})
    return jsonify({"transcript": call.get("transcript")})


# ------------------ PBX RECORDINGS (ON-DEMAND PROXY) ------------------

def _recordings_api_cfg() -> dict:
    """Return configuration for the external FreePBX Recordings API.
    Uses env vars:
      - RECORDINGS_API_BASE (e.g., http://154.26.136.196:8080)
      - RECORDINGS_API_KEY  (optional X-API-Key header)
    """
    base = os.getenv("RECORDINGS_API_BASE") or os.getenv("FREEPBX_RECORDINGS_API_BASE")
    key = os.getenv("RECORDINGS_API_KEY") or os.getenv("FREEPBX_RECORDINGS_API_KEY")
    return {"base": (base or ""), "key": (key or "")}


def _extract_numbers_from_call(call: dict) -> set[str]:
    """Heuristically extract phone numbers from a stored LiveKit call record.
    Looks at participant identities and SIP metadata within events.
    """
    nums: set[str] = set()
    try:
        # 1) Participant identities often include SIP numbers like "sip_+97148341451"
        for p in (call.get("participants") or {}).values():
            ident = str((p or {}).get("identity") or (p or {}).get("sid") or "")
            m = re.findall(r"[+]?\d{6,}", ident)
            for n in (m or []):
                nums.add(n)
    except Exception:
        pass
    try:
        # 2) SIP metadata in webhook events
        for ev in (call.get("events") or []):
            sip = (ev.get("sip") or {}).get("call") or {}
            for k in ("from", "to", "phoneNumber", "trunkPhoneNumber", "callerNumber", "calleeNumber"):
                v = sip.get(k)
                if isinstance(v, str) and v.strip():
                    m = re.findall(r"[+]?\d{6,}", v)
                    for n in (m or []):
                        nums.add(n)
    except Exception:
        pass
    # Basic normalization: keep as-is and digits-only variant for robust filename matching
    out: set[str] = set()
    for n in nums:
        s = str(n).strip()
        out.add(s)
        try:
            out.add(re.sub(r"[^0-9]", "", s))
        except Exception:
            pass
    return out


def _score_candidates_vs_call_time(items: list[dict], call_epoch: int | None) -> list[dict]:
    if not items:
        return []
    scored = []
    for it in items:
        d = dict(it)
        try:
            mod = float(d.get("modified_ts"))
        except Exception:
            mod = None
        delta = None
        if call_epoch is not None and mod is not None:
            try:
                delta = abs(float(mod) - float(call_epoch))
            except Exception:
                delta = None
        d["delta_seconds"] = delta
        scored.append(d)
    scored.sort(key=lambda x: (x.get("delta_seconds") if x.get("delta_seconds") is not None else 9e18))
    return scored


# ------------------ PBX RECORDINGS INDEXER (BACKGROUND) ------------------

_REC_INDEX_LOCK = threading.Lock()
_REC_INDEXER_STARTED = False


def _rec_log(event: str, **fields) -> None:
    """Structured logging for recordings features (enabled by RECORDINGS_DEBUG=1)."""
    try:
        enabled = (os.getenv("RECORDINGS_DEBUG", "1") == "1")
    except Exception:
        enabled = True
    if not enabled:
        return
    try:
        record = {"ts": int(time.time()), "event": str(event)}
        for k, v in (fields or {}).items():
            # Ensure values are JSON-serializable
            try:
                json.dumps(v)
                record[k] = v
            except Exception:
                record[k] = str(v)
        line = json.dumps(record, ensure_ascii=False)
    except Exception:
        try:
            line = f"{event} {fields}"
        except Exception:
            line = str(event)
    # stdout (captured by pm2/systemd)
    try:
        print(f"[recordings] {line}", flush=True)
    except Exception:
        pass
    # file
    try:
        logs_dir = Path(__file__).parent / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        with open(str(logs_dir / "recordings.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _recordings_index_path() -> Path:
    return _agents_dir() / "recordings_index.json"


def _load_recordings_index() -> dict:
    """Load the local recordings index JSON, creating it if needed."""
    _ensure_storage()
    idx = _storage_backend().load_recordings_index()
    if not isinstance(idx, dict):
        return {"items": [], "last_scan_ts": 0, "scan_cursor": None, "total": 0}
    idx.setdefault("items", [])
    idx.setdefault("last_scan_ts", 0)
    idx.setdefault("scan_cursor", None)
    idx.setdefault("total", len(idx.get("items") or []))
    return idx


def _save_recordings_index(data: dict) -> None:
    """Persist the local recordings index JSON atomically under a lock."""
    try:
        with _REC_INDEX_LOCK:
            _storage_backend().save_recordings_index(data or {"items": [], "last_scan_ts": 0, "scan_cursor": None, "total": 0})
    except Exception:
        pass


def _update_index_with_items(index: dict, items: list[dict]) -> dict:
    """Merge a batch of items into the index keyed by relative_path and sort by modified_ts desc."""
    if not isinstance(index, dict):
        index = {}
    existing = {}
    for it in (index.get("items") or []):
        try:
            rp = str(it.get("relative_path") or "")
        except Exception:
            rp = ""
        if rp:
            existing[rp] = {
                "filename": it.get("filename"),
                "relative_path": rp,
                "size_bytes": it.get("size_bytes"),
                "modified_ts": it.get("modified_ts"),
            }
    for it in (items or []):
        try:
            rp = str(it.get("relative_path") or "")
        except Exception:
            rp = ""
        if not rp:
            continue
        existing[rp] = {
            "filename": it.get("filename"),
            "relative_path": rp,
            "size_bytes": it.get("size_bytes"),
            "modified_ts": it.get("modified_ts"),
        }
    merged = list(existing.values())
    try:
        merged.sort(key=lambda x: float(x.get("modified_ts") or 0.0), reverse=True)
    except Exception:
        pass
    index["items"] = merged
    index["total"] = len(merged)
    return index


def _search_index_for_numbers(numbers: set[str], limit: int = 200) -> list[dict]:
    """Find index items whose filename contains any of the provided number variants."""
    idx = _load_recordings_index()
    items = idx.get("items") or []
    results: list[dict] = []
    seen_paths: set[str] = set()
    variants: set[str] = set()
    for n in (numbers or []):
        s = str(n or "").strip()
        if not s:
            continue
        variants.add(s)
        try:
            digits = re.sub(r"[^0-9]", "", s)
            if digits:
                variants.add(digits)
                variants.add("+" + digits)
        except Exception:
            pass
    for it in items:
        fn = str(it.get("filename") or "")
        rp = str(it.get("relative_path") or "")
        if not rp or rp in seen_paths:
            continue
        for v in variants:
            if v and (v in fn):
                results.append(it)
                seen_paths.add(rp)
                break
        if len(results) >= int(limit or 0):
            break
    return results


def _recordings_indexer_tick(pages_per_cycle: int = 3, page_size: int = 500) -> None:
    """One indexer cycle: refresh head page and deepen scan for older pages."""
    cfg = _recordings_api_cfg()
    base = (cfg.get("base") or "").rstrip("/")
    if not base:
        return
    headers = {"Accept": "application/json"}
    if cfg.get("key"):
        headers["X-API-Key"] = cfg.get("key")
    index = _load_recordings_index()
    now = int(time.time())
    # Always refresh head (newest-first)
    try:
        r = requests.get(f"{base}/recordings", headers=headers, params={"page_size": page_size}, timeout=12)
        if r.ok:
            payload = r.json() or {}
            items = payload.get("items") or []
            index = _update_index_with_items(index, items)
            index["last_scan_ts"] = now
            head_next = payload.get("next_offset")
            if index.get("scan_cursor") is None and head_next is not None:
                index["scan_cursor"] = head_next
            _save_recordings_index(index)
            try:
                _rec_log("index_head_ok", count=len(items), page_size=page_size, next_offset=head_next)
            except Exception:
                pass
        else:
            index["last_error"] = f"head_status_{r.status_code}"
            _save_recordings_index(index)
            _rec_log("index_head_error", status=r.status_code)
    except Exception as e:
        index["last_error"] = f"head_exc_{e}"
        _save_recordings_index(index)
        _rec_log("index_head_exception", error=str(e))
        return
    # Deepen scan for older pages
    cursor = index.get("scan_cursor")
    pages_done = 0
    while pages_done < int(pages_per_cycle or 0):
        if cursor is None:
            break
        try:
            r = requests.get(f"{base}/recordings", headers=headers, params={"offset": cursor, "page_size": page_size}, timeout=20)
            if not r.ok:
                index["last_error"] = f"page_status_{r.status_code}"
                _rec_log("index_page_error", status=r.status_code, cursor=cursor)
                break
            payload = r.json() or {}
            items = payload.get("items") or []
            index = _update_index_with_items(index, items)
            index["last_scan_ts"] = now
            cursor = payload.get("next_offset")
            index["scan_cursor"] = cursor
            pages_done += 1
            _rec_log("index_page_ok", page_count=len(items), next_offset=cursor, pages_done=pages_done)
        except Exception as e:
            index["last_error"] = f"page_exc_{e}"
            _rec_log("index_page_exception", error=str(e), cursor=cursor)
            break
    _save_recordings_index(index)


def _ensure_recordings_indexer() -> None:
    """Start the indexer background thread if configured and not already started."""
    global _REC_INDEXER_STARTED
    if _REC_INDEXER_STARTED:
        return
    base = (_recordings_api_cfg().get("base") or "").strip()
    if not base:
        _rec_log("indexer_skip_no_base")
        return
    interval = int(os.getenv("RECORDINGS_INDEX_POLL_SECONDS", "15"))
    pages_per_cycle = int(os.getenv("RECORDINGS_INDEX_PAGES_PER_CYCLE", "3"))
    page_size = int(os.getenv("RECORDINGS_INDEX_PAGE_SIZE", "500"))
    _rec_log("indexer_started", interval=interval, pages_per_cycle=pages_per_cycle, page_size=page_size)

    def _loop() -> None:
        while True:
            try:
                _recordings_indexer_tick(pages_per_cycle=pages_per_cycle, page_size=page_size)
            except Exception:
                pass
            time.sleep(interval)

    try:
        threading.Thread(target=_loop, daemon=True).start()
        _REC_INDEXER_STARTED = True
    except Exception:
        pass


@app.get("/pbx/recordings/index")
@require_auth()
def recordings_index_summary():
    """Return a lightweight summary of the local recordings index (optionally first N items)."""
    idx = _load_recordings_index()
    try:
        limit = int(request.args.get("limit") or "0")
    except Exception:
        limit = 0
    resp = {
        "total": int(idx.get("total") or len(idx.get("items") or [])),
        "last_scan_ts": idx.get("last_scan_ts"),
        "scan_cursor": idx.get("scan_cursor"),
        "error": idx.get("last_error") or "",
    }
    if limit and isinstance(idx.get("items"), list):
        resp["items"] = (idx.get("items") or [])[:limit]
    return jsonify(resp)

@app.get("/pbx/recordings/for_call/<call_id>")
@require_auth()
def recordings_for_call(call_id: str):
    """Return candidate PBX recordings for a given call and best-guess selection.
    Does not persist any audio; only queries the external Recordings API.
    """
    cfg = _recordings_api_cfg()
    base = (cfg.get("base") or "").rstrip("/")
    if not base:
        return jsonify({"candidates": [], "selected": None, "error": "recordings_api_unconfigured"}), 200

    store = _load_livekit_store()
    call = (store.get("calls") or {}).get(call_id)
    if not call:
        return jsonify({"error": "not_found"}), 404

    # Call reference time: prefer created_at; fallback to first event time
    call_epoch = None
    try:
        call_epoch = int(call.get("created_at"))
    except Exception:
        call_epoch = None
    if call_epoch is None:
        try:
            ets = [int(e.get("created_at")) for e in (call.get("events") or []) if isinstance(e.get("created_at"), (int, float))]
            if ets:
                call_epoch = min(ets)
        except Exception:
            pass

    _rec_log("for_call_start", call_id=call_id)
    numbers = _extract_numbers_from_call(call)
    try:
        _rec_log("for_call_numbers", call_id=call_id, numbers=sorted(list(numbers)))
    except Exception:
        pass
    seen: dict[str, dict] = {}
    # 1) Consult local index first for fast offline-ish results
    try:
        index_before = len(seen)
        for it in _search_index_for_numbers(numbers, limit=500):
            rp = str(it.get("relative_path") or "")
            if rp and rp not in seen:
                seen[rp] = {
                    "filename": it.get("filename"),
                    "relative_path": rp,
                    "size_bytes": it.get("size_bytes"),
                    "modified_ts": it.get("modified_ts"),
                    "source": "index",
                }
        _rec_log("for_call_index_hits", call_id=call_id, hits=(len(seen) - index_before))
    except Exception:
        pass

    # Index-only selection per requirement (no runtime upstream search here)
    _rec_log("for_call_index_only", call_id=call_id, candidates=len(seen))

    candidates = list(seen.values())
    scored = _score_candidates_vs_call_time(candidates, call_epoch)
    selected = scored[0] if scored else None
    try:
        if selected:
            _rec_log("for_call_selected", call_id=call_id, relative_path=selected.get("relative_path"), filename=selected.get("filename"), delta_seconds=selected.get("delta_seconds"))
        else:
            _rec_log("for_call_no_candidates", call_id=call_id)
    except Exception:
        pass
    return jsonify({"candidates": scored, "selected": selected, "call_time": call_epoch})


@app.get("/pbx/recordings/for_call/<call_id>/file")
@require_auth()
def recordings_stream_for_call(call_id: str):
    """Resolve the best recording for a call and stream it directly.

    This wraps recordings_for_call + proxy_recording_file to provide a
    single call for the frontend. Returns 404 if no candidate is found.
    """
    try:
        data = recordings_for_call(call_id)  # Flask response
        # When called internally, jsonify(...) returns a Response; extract JSON
        if hasattr(data, "get_json"):
            payload = data.get_json() or {}
            status = getattr(data, "status_code", 200)
            if status != 200:
                return data
        else:
            # Already a dict
            payload = data or {}
        sel = (payload or {}).get("selected") or {}
        rp = str(sel.get("relative_path") or "")
        if not rp:
            _rec_log("for_call_stream_not_found", call_id=call_id)
            return jsonify({"error": "recording_not_found"}), 404
        _rec_log("for_call_stream_resolve", call_id=call_id, relative_path=rp)
        # Delegate to proxy endpoint
        return proxy_recording_file(rp)
    except Exception as e:
        _rec_log("for_call_stream_exception", call_id=call_id, error=str(e))
        return jsonify({"error": str(e)}), 500


@app.get("/pbx/recordings/file/<path:relative_path>")
@require_auth()
def proxy_recording_file(relative_path: str):
    """Stream a recording file from the PBX Recordings API through this server."""
    cfg = _recordings_api_cfg()
    base = (cfg.get("base") or "").rstrip("/")
    if not base:
        return jsonify({"error": "recordings_api_unconfigured"}), 503
    # Build upstream URL without double-encoding; requests will handle encoding
    url = f"{base}/recordings/file/{relative_path}"
    headers = {}
    if cfg.get("key"):
        headers["X-API-Key"] = cfg.get("key")
    try:
        _rec_log("file_proxy_request", relative_path=relative_path)
        upstream = requests.get(url, headers=headers, stream=True, timeout=20)
        if not upstream.ok:
            try:
                _rec_log("file_proxy_upstream_error", status=upstream.status_code, relative_path=relative_path)
                return jsonify({"error": "upstream_error", "status": upstream.status_code}), upstream.status_code
            finally:
                upstream.close()
        # Pass through content headers
        resp_headers = {}
        ct = upstream.headers.get("Content-Type")
        if ct:
            resp_headers["Content-Type"] = ct
        cd = upstream.headers.get("Content-Disposition")
        if cd:
            resp_headers["Content-Disposition"] = cd
        _rec_log("file_proxy_ok", relative_path=relative_path, content_type=(ct or ""))
        return Response(upstream.iter_content(chunk_size=8192), headers=resp_headers)
    except Exception as e:
        _rec_log("file_proxy_exception", relative_path=relative_path, error=str(e))
        return jsonify({"error": str(e)}), 500


ALLOWED_AUDIO_EXT = {".mp3", ".wav", ".ogg", ".m4a", ".aac", ".flac", ".opus"}


def _is_allowed_audio(filename: str) -> bool:
    ext = Path(filename).suffix.lower()
    return ext in ALLOWED_AUDIO_EXT


def _duplicate_routes_with_prefix(app_obj, prefix: str):
    """Duplicate all registered routes under the given prefix.
    Executed at import-time so it works under WSGI servers too.
    """
    try:
        if not prefix or prefix == "/":
            return
        rules = list(app_obj.url_map.iter_rules())
        for rule in rules:
            if rule.rule.startswith(prefix):
                continue
            if rule.endpoint == 'static':
                continue
            view = app_obj.view_functions.get(rule.endpoint)
            if view is None:
                continue
            try:
                app_obj.add_url_rule(
                    prefix + rule.rule,
                    endpoint=f"{rule.endpoint}__prefixed",
                    view_func=view,
                    methods=rule.methods,
                    defaults=rule.defaults,
                )
            except Exception:
                pass
    except Exception:
        pass

# Always duplicate at import time as well
_duplicate_routes_with_prefix(app, API_PREFIX)
# ------------------ TWILIO INBOUND VOICE WEBHOOK ------------------

def _twilio_validate(req_body: str, url: str, headers, override_token: str | None = None) -> bool:
    # Optional signature validation. Prefer a per-number override token if provided
    token = override_token or os.getenv("TWILIO_AUTH_TOKEN")
    if not token or TwilioRequestValidator is None:
        return True
    try:
        validator = TwilioRequestValidator(token)
        signature = headers.get("X-Twilio-Signature") or headers.get("x-twilio-signature")
        return bool(validator.validate(url, dict(request.form), signature))
    except Exception:
        return False


@app.post("/api/webhooks/beyond")
def beyond_presence_webhook():
    """Receive webhook events from Beyond Presence for avatar sessions.
    
    Configure this URL in Beyond Presence dashboard:
    https://your-domain.com/api/webhooks/beyond
    """
    try:
        payload = request.get_json(force=True, silent=True) or {}
        event_type = payload.get("type") or payload.get("event") or "unknown"
        session_id = payload.get("session_id") or payload.get("id")
        
        # Log the event with full payload for debugging
        print(f"[beyond-webhook] event={event_type} session={session_id}", file=sys.stderr, flush=True)
        print(f"[beyond-webhook] payload: {json.dumps(payload, indent=2)}", file=sys.stderr, flush=True)
        
        # Store recent events for debugging
        if not hasattr(beyond_presence_webhook, "_events"):
            beyond_presence_webhook._events = []
        beyond_presence_webhook._events.append({
            "ts": int(time.time()),
            "type": event_type,
            "session_id": session_id,
            "payload": payload
        })
        # Keep only last 50 events
        beyond_presence_webhook._events = beyond_presence_webhook._events[-50:]
        
        return jsonify({"status": "ok"})
    except Exception as e:
        print(f"[beyond-webhook] error: {e}", file=sys.stderr, flush=True)
        return jsonify({"error": str(e)}), 500


@app.get("/api/webhooks/beyond/events")
def beyond_presence_events():
    """Get recent Beyond Presence webhook events for debugging."""
    try:
        events = getattr(beyond_presence_webhook, "_events", [])
        return jsonify({"events": events, "count": len(events)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/twilio/voice")
def twilio_voice_webhook():
    """Minimal Twilio inbound voice handler that bridges to LiveKit SIP trunk.

    Uses number→agent routing from numbers.json to choose an agent. The actual
    media is handled by LiveKit SIP trunk; this endpoint returns TwiML to
    redirect Twilio to the LiveKit SIP domain with the original called number.
    """
    try:
        # Resolve per-number auth token for signature validation (multi-tenant)
        called_raw = (request.form.get("To") or request.values.get("To") or "").strip()
        called = _normalize_e164(called_raw)
        per_number_token = _twilio_auth_token_for_number(called)
        if not _twilio_validate(request.get_data(as_text=True) or "", request.url, request.headers, override_token=per_number_token):
            return jsonify({"error": "invalid_signature"}), 401
        
        from_num = (request.form.get("From") or request.values.get("From") or "").strip()
        # Lookup routing (user-scoped routes take precedence over global)
        routes = _load_routes_union()
        route = routes.get(called) or routes.get(_digits_only(called)) or {}
        trunk_label = str(route.get("trunk") or "")
        # LiveKit SIP domain and credentials from env
        sip_domain = os.getenv("LIVEKIT_SIP_DOMAIN") or os.getenv("LIVEKIT_SIP_URI")
        sip_username = os.getenv("LIVEKIT_SIP_USERNAME") or ""
        sip_password = os.getenv("LIVEKIT_SIP_PASSWORD") or ""
        if VoiceResponse is None or not sip_domain:
            # Fallback: simple <Say> if SIP not configured
            vr = VoiceResponse()
            vr.say("Voice service is not configured.")
            return Response(str(vr), mimetype="text/xml")
        # Build TwiML Dial to SIP with authentication
        vr = VoiceResponse()
        dial = vr.dial(caller_id=from_num if from_num else None)
        # Pass called number via user part so LiveKit rules can route
        sip_uri = f"sip:{called}@{sip_domain}"
        # Optionally pass trunk label in SIP headers
        headers = {}
        if trunk_label:
            headers["X-Trunk-Label"] = trunk_label
        # Include SIP credentials for LiveKit authentication
        if sip_username and sip_password:
            dial.sip(sip_uri, username=sip_username, password=sip_password, headers=headers if headers else None)
        else:
            dial.sip(sip_uri, headers=headers if headers else None)
        return Response(str(vr), mimetype="text/xml")
    except Exception as e:
        try:
            vr = VoiceResponse()
            vr.say("An error occurred.")
            return Response(str(vr), mimetype="text/xml")
        except Exception:
            return jsonify({"error": str(e)}), 500


# ------------------ OUTBOUND CALLING ------------------

def _trigger_outbound_call(agent_id, to_number, from_number, context=None):
    """Trigger outbound PSTN call via LiveKit SIP CreateSIPParticipant."""
    lk_url = (os.getenv("LIVEKIT_URL") or "").strip()
    lk_api_key = (os.getenv("LIVEKIT_API_KEY") or "").strip()
    lk_api_secret = (os.getenv("LIVEKIT_API_SECRET") or "").strip()
    trunk_id = (os.getenv("LIVEKIT_SIP_TRUNK_ID") or "").strip()

    # Attempt to resolve user-specific trunk credentials first if context has user_id
    user_trunk_id = None
    if context and isinstance(context, dict) and context.get("user_id"):
        # Future: Lookup user-specific trunk ID from a user_trunks table or similar
        # For now, we only support user-specific Twilio credentials for the number lookup, 
        # but the SIP Trunk ID is usually global for the LiveKit instance unless configured otherwise.
        # If users bring their own Twilio, we still route through the same LiveKit Trunk 
        # which points to our TwiML handler, which then uses the user's Twilio creds to dial.
        pass

    if not all([lk_url, lk_api_key, lk_api_secret, trunk_id]):
        raise Exception("LiveKit credentials or LIVEKIT_SIP_TRUNK_ID missing")

    if not to_number:
        raise Exception("Destination number required")
    
    to_number = str(to_number).strip()
    if from_number:
        from_number = str(from_number).strip()

    # Resolve Twilio credentials for the 'from_number' to verify ownership/account
    # This logic is mainly for the TwiML handler later, but we can pre-check here.
    # The actual dialing happens when LiveKit calls our SIP URI, which calls Twilio, 
    # which calls our TwiML webhook, which dials the PSTN number.
    # To support user-specific Twilio, the TwiML webhook needs to know which account to use.
    # We pass this via the SIP header or context.
    
    # Find the account associated with the from_number
    twilio_store = _load_twilio_store()
    account_sid = None
    auth_token = None
    
    # 1. Check numbers.json mapping
    number_map = (twilio_store.get("numbers") or {})
    # Normalize from_number
    normalized_from = _normalize_e164(from_number)
    
    if normalized_from in number_map:
        meta = number_map[normalized_from]
        acc_id = meta.get("account_id")
        if acc_id:
            # Look up account
            for acc in twilio_store.get("accounts", []):
                if acc.get("id") == acc_id:
                    account_sid = acc.get("account_sid")
                    auth_token = acc.get("auth_token")
                    break
    
    # 2. If not found, check if this is an admin request/system number usage
    # Fallback to env ONLY if no specific user mapping was attempted but failed.
    # Actually, we should only fallback to env if the number is NOT mapped to a user.
    # The check above (if normalized_from in number_map) handles the user mapping.
    
    is_user_mapped = normalized_from in number_map
    if not account_sid and not is_user_mapped:
        # Only use system credentials if this number is NOT claimed by a user
        # This allows admins to use unmapped numbers via system env vars
        account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        auth_token = os.getenv("TWILIO_AUTH_TOKEN")

    if is_user_mapped and not (account_sid and auth_token):
        # User mapped this number but account lookup failed (e.g. account deleted)
        raise Exception(f"Twilio account not found for number {from_number}. Please check your Twilio settings.")

    room_name = f"outbound-{agent_id}-{uuid.uuid4().hex[:8]}"
    metadata = json.dumps(context or {})
    
    # Debug: Log the context being passed to the room
    print(f"[outbound] Room context: candidate_name={context.get('candidate_name') if context else None}, questionnaire_len={len(context.get('questionnaire', [])) if context and context.get('questionnaire') else 0}", file=sys.stderr)

    # Load workspace defaults for LiveKit config (SIP URI, credentials, etc.)
    try:
        _defaults = _load_defaults() or {}
    except Exception:
        _defaults = {}
    _lk_def = (_defaults or {}).get("livekit", {}) or {}
    
    # Get SIP config for OUTBOUND calls
    # LIVEKIT_SIP_URI_OUTBOUND format: username:password@domain (e.g., admin:zainlee@4o12xf0u8qx.sip.livekit.cloud)
    # This is different from LIVEKIT_URL which uses Project URL slug!
    sip_uri_outbound = str(os.getenv("LIVEKIT_SIP_URI_OUTBOUND") or "").strip()
    sip_domain = ""
    sip_username = ""
    sip_password = ""
    
    if sip_uri_outbound and "@" in sip_uri_outbound:
        # Parse username:password@domain format
        try:
            creds_part, domain_part = sip_uri_outbound.rsplit("@", 1)
            sip_domain = domain_part.strip()
            if ":" in creds_part:
                sip_username, sip_password = creds_part.split(":", 1)
                sip_username = sip_username.strip()
                sip_password = sip_password.strip()
            print(f"[outbound] Parsed SIP URI: domain={sip_domain} username={sip_username}", file=sys.stderr)
        except Exception as e:
            print(f"[outbound] Failed to parse LIVEKIT_SIP_URI_OUTBOUND: {e}", file=sys.stderr)
    
    # Fallback to separate env vars if LIVEKIT_SIP_URI_OUTBOUND not set
    if not sip_domain:
        sip_domain = str(os.getenv("LIVEKIT_SIP_URI") or "").strip()
    if not sip_username:
        sip_username = str(os.getenv("LIVEKIT_SIP_USERNAME") or "").strip()
    if not sip_password:
        sip_password = str(os.getenv("LIVEKIT_SIP_PASSWORD") or "").strip()
    
    if not sip_domain:
        # Fallback to workspace defaults if env not set
        sip_domain = str(_lk_def.get("sip_uri") or "").strip()

    # Pre-spawn agent worker BEFORE making any call so it's ready when user answers
    def _pre_spawn_agent_for_outbound():
        """Spawn agent worker before the call so it's ready when user answers."""
        try:
            # CRITICAL: Mark room as spawning FIRST to prevent webhook from double-spawning
            # This MUST happen before any async operations or subprocess creation
            with _SPAWN_LOCK:
                if room_name in _SPAWN_IN_PROGRESS:
                    print(f"[outbound] Room {room_name} already in spawn progress, skipping", file=sys.stderr)
                    return None
                _SPAWN_IN_PROGRESS.add(room_name)
                print(f"[outbound] Marked room {room_name} as spawn-in-progress", file=sys.stderr)
            
            # Get the assigned phone number for this agent
            routes = _load_routes_union()
            assigned_num_key = None
            
            # Find the phone number assigned to this agent
            for rk, rv in (routes or {}).items():
                if str((rv or {}).get("agent_id")) == str(agent_id):
                    assigned_num_key = rk
                    break
            
            if not assigned_num_key:
                # Use from_number as fallback
                assigned_num_key = from_number
            
            # Normalize the agent name (phone number without formatting)
            import re as _re
            agent_worker_name = _re.sub(r"[^0-9]", "", str(assigned_num_key or from_number or ""))
            if not agent_worker_name:
                agent_worker_name = f"agent-{uuid.uuid4().hex[:6]}"
            
            print(f"[outbound] Pre-spawning agent worker: agent_name={agent_worker_name} agent_id={agent_id}", file=sys.stderr)
            
            # Check if worker already running for this agent
            with _AGENT_WORKER_LOCK:
                existing_room = _AGENT_NAME_TO_ROOM.get(agent_worker_name)
                if existing_room and existing_room != room_name:
                    print(f"[outbound] Agent worker already running for {agent_worker_name} in room {existing_room}", file=sys.stderr)
                    # Don't remove from SPAWN_IN_PROGRESS - let cleanup handle it
                    return agent_worker_name
            
            # Build environment for the agent worker
            env = os.environ.copy()
            env["RUN_AS_WORKER"] = "1"
            env["AGENT_NAME"] = agent_worker_name
            env["AGENT_PROFILE_ID"] = str(agent_id)
            env["DIALED_NUMBER"] = from_number or agent_worker_name
            env["AGENT_PHONE_NUMBER"] = from_number or agent_worker_name
            
            # CRITICAL: Load LiveKit and OpenAI credentials from defaults (same as webhook spawn)
            try:
                d = _load_defaults() or {}
            except Exception:
                d = {}
            lk_def = (d or {}).get("livekit", {}) or {}
            env["LIVEKIT_URL"] = str(lk_def.get("url") or os.getenv("LIVEKIT_URL", ""))
            env["LIVEKIT_API_KEY"] = str(lk_def.get("api_key") or os.getenv("LIVEKIT_API_KEY", ""))
            env["LIVEKIT_API_SECRET"] = str(lk_def.get("api_secret") or os.getenv("LIVEKIT_API_SECRET", ""))
            
            openai_def = (d or {}).get("openai", {}) or {}
            if openai_def.get("api_key"):
                env.setdefault("OPENAI_API_KEY", str(openai_def.get("api_key")))
            if openai_def.get("base_url"):
                env.setdefault("OPENAI_BASE_URL", str(openai_def.get("base_url")))
            
            # Provide config API base for number routing and credential lookups
            try:
                env.setdefault("CONFIG_API_BASE", os.getenv("CONFIG_API_BASE", f"http://localhost:{os.getenv('CONFIG_API_PORT','5057')}"))
            except Exception:
                pass
            
            # Silence GPU/VAAPI and multimedia noise
            env.setdefault("LIBVA_MESSAGING_LEVEL", "0")
            env.setdefault("FFMPEG_LOG_LEVEL", "error")
            env.setdefault("GLOG_minloglevel", "2")
            env.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
            env.setdefault("CUDA_VISIBLE_DEVICES", "")
            env.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", os.getenv("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python"))
            env.setdefault("PYTHONUNBUFFERED", "1")
            
            # Spawn the worker process
            agent_path = str((Path(__file__).parent / "agent.py").resolve())
            try:
                logs_dir = Path(__file__).parent / "logs"
                logs_dir.mkdir(parents=True, exist_ok=True)
                ts = time.strftime("%Y%m%d_%H%M%S")
                short = uuid.uuid4().hex[:6]
                log_fp = open(str(logs_dir / f"outbound_{agent_worker_name}_{ts}_{short}.log"), "wb")
            except Exception:
                log_fp = None
            
            if log_fp is not None:
                proc = subprocess.Popen([sys.executable, agent_path], env=env, stdout=log_fp, stderr=subprocess.STDOUT, bufsize=1, universal_newlines=False)
            else:
                proc = subprocess.Popen([sys.executable, agent_path], env=env)
            
            _bind_worker(room_name, proc, log_fp, agent_name=agent_worker_name)
            
            # Give the worker a moment to start registering
            time.sleep(1.5)
            
            print(f"[outbound] Agent worker spawned: pid={proc.pid} agent_name={agent_worker_name}", file=sys.stderr)
            return agent_worker_name
            
        except Exception as e:
            print(f"[outbound] Failed to pre-spawn agent: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            # On failure, remove from spawn-in-progress so cleanup can happen
            with _SPAWN_LOCK:
                _SPAWN_IN_PROGRESS.discard(room_name)
            return None

    # --- STANDARD FLOW (LiveKit SIP Trunk via CreateSIPParticipant API) ---
    # This uses the OUTBOUND trunk to dial directly from LiveKit to the phone
    print(f"[outbound] Using LiveKit SIP Trunk for {to_number}", file=sys.stderr)
    
    # Pre-spawn the agent worker
    spawned_agent_name = _pre_spawn_agent_for_outbound()

    async def _create_participant():
        # Ensure sip_number is valid. If empty/None, use default trunk number.
        effective_from = from_number
        if not effective_from:
            effective_from = "+12568713652"
        
        # Create request inside async function
        request = sip_proto.CreateSIPParticipantRequest(
            sip_trunk_id=trunk_id,
            sip_call_to=to_number,
            sip_number=effective_from,
            room_name=room_name,
            participant_identity=f"phone-{_digits_only(to_number) or uuid.uuid4().hex[:6]}",
            participant_name=to_number,
            participant_metadata=metadata,
            participant_attributes={"agent_id": agent_id},
        )

        print(f"[outbound] SIP Params: To={to_number} From={effective_from} Trunk={_mask_secret(trunk_id)}", file=sys.stderr)

        client = lk_api.LiveKitAPI(lk_url, lk_api_key, lk_api_secret)
        try:
            # Pre-create room to ensure metadata (context) is available to the agent immediately
            try:
                await client.room.create_room(
                    room_proto.CreateRoomRequest(
                        name=room_name,
                        metadata=metadata,
                        empty_timeout=300, # 5 min timeout
                        max_participants=10,  # Allow agent + SIP participant + any others
                    )
                )
                print(f"[outbound] Pre-created room {room_name} with context (metadata_len={len(metadata)})", file=sys.stderr)
                print(f"[outbound] Room metadata: {metadata[:500]}...", file=sys.stderr)
            except Exception as e:
                print(f"[outbound] Warning: Failed to pre-create room: {e}", file=sys.stderr)

            # Create agent dispatch so the pre-spawned worker joins this room
            if spawned_agent_name:
                try:
                    dispatch_meta = {
                        "call_id": room_name,
                        "agent_id": agent_id,
                        "to_number": to_number,
                        "from_number": from_number,
                        "outbound": True,
                    }
                    try:
                        req = lk_api.CreateAgentDispatchRequest(
                            agent_name=spawned_agent_name,
                            room=room_name,
                            metadata=json.dumps(dispatch_meta),
                        )
                    except Exception:
                        from livekit.protocol.agent_dispatch import CreateAgentDispatchRequest as _Req
                        req = _Req(agent_name=spawned_agent_name, room=room_name, metadata=json.dumps(dispatch_meta))
                    
                    await client.agent_dispatch.create_dispatch(req)
                    print(f"[outbound] Created agent dispatch: room={room_name} agent={spawned_agent_name}", file=sys.stderr)
                except Exception as e:
                    print(f"[outbound] Warning: Failed to create agent dispatch: {e}", file=sys.stderr)

            # Debug log for trunk ID
            print(f"[outbound] Using Trunk ID: {_mask_secret(trunk_id)} for {to_number}", file=sys.stderr)
            print(f"[outbound] Sending SIP create request: to={request.sip_call_to}, from={request.sip_number}, room={request.room_name}", file=sys.stderr)

            return await client.sip.create_sip_participant(request)
        finally:
            await client.aclose()

    try:
        participant_info = asyncio.run(_create_participant())
    except Exception as exc:
        # Surface SIP provider status if available
        sip_code = getattr(exc, "metadata", {}).get("sip_status_code") if hasattr(exc, "metadata") else None
        sip_msg = getattr(exc, "metadata", {}).get("sip_status") if hasattr(exc, "metadata") else None
        error_text = f"LiveKit SIP Create failed: {exc}"
        if sip_code or sip_msg:
            error_text += f" (carrier_status={sip_code} {sip_msg})"
        raise Exception(error_text)

    return participant_info.participant_id or participant_info.sip_call_id, participant_info.sip_call_id or "initiated"

def _outbound_scheduler_loop():
    print("[scheduler] Starting outbound scheduler loop", file=sys.stderr)
    while True:
        try:
            store = _storage_backend()
            if hasattr(store, "list_scheduled_calls"):
                pending = store.list_scheduled_calls(status="pending")
                now = datetime.now(timezone.utc)
                
                for call in pending:
                    try:
                        scheduled_at_str = call.get("scheduled_at")
                        if not scheduled_at_str:
                            continue
                        s_dt = datetime.fromisoformat(scheduled_at_str)
                        if s_dt.tzinfo is None:
                            s_dt = s_dt.replace(tzinfo=timezone.utc)
                        else:
                            s_dt = s_dt.astimezone(timezone.utc)
                            
                        if s_dt <= now:
                            print(f"[scheduler] Triggering call {call['id']}", file=sys.stderr)
                            store.update_scheduled_call_status(call["id"], "initiated")
                            
                            try:
                                sid, status = _trigger_outbound_call(
                                    call.get("agent_id"),
                                    call.get("to_number"),
                                    call.get("from_number"),
                                    call.get("context")
                                )
                                store.update_scheduled_call_status(call["id"], "completed", sid=sid)
                            except Exception as e:
                                print(f"[scheduler] Failed call {call['id']}: {e}", file=sys.stderr)
                                store.update_scheduled_call_status(call["id"], "failed", error=str(e))
                                
                    except Exception as e:
                        print(f"[scheduler] Error processing call {call.get('id')}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[scheduler] Loop error: {e}", file=sys.stderr)
        time.sleep(60)

# Start scheduler in background
threading.Thread(target=_outbound_scheduler_loop, daemon=True).start()

@app.post("/api/outbound/call")
@require_auth()
def outbound_call_trigger():
    """Immediately trigger an outbound call.
    
    Accepts optional callback_url for receiving questionnaire results when call completes.
    The callback will receive a POST with:
    {
        "call_id": "outbound-xxx",
        "candidate_name": "...",
        "phone_number": "+...",
        "questionnaire_results": [{"question": "...", "answer": "..."}, ...],
        "transcript": [...],
        "call_duration_seconds": 120,
        "call_status": "completed" | "no_answer" | "failed"
    }
    """
    payload = request.json or {}
    agent_id = payload.get("agent_id")
    to_number = payload.get("to_number")
    from_number = payload.get("from_number") or "+12568713652"
    context = payload.get("context") or {}
    
    # Extract callback_url from top-level or context
    callback_url = payload.get("callback_url") or context.get("callback_url")
    if callback_url:
        context["callback_url"] = callback_url

    if not agent_id or not to_number:
        return jsonify({"error": "missing_fields"}), 400

    try:
        sid, status = _trigger_outbound_call(agent_id, to_number, from_number, context)
        return jsonify({"call_sid": sid, "status": status, "callback_url": callback_url or None})
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[outbound] Error triggering call: {e}", file=sys.stderr)
        return jsonify({"error": str(e)}), 500

@app.post("/twilio/twiml/outbound")
def twilio_twiml_outbound():
    """Return TwiML to dial the SIP URI with authentication."""
    sip_uri = request.args.get("sip_uri")
    ctx = request.args.get("ctx")
    
    if not sip_uri or not VoiceResponse:
        return Response("<Response><Say>Invalid configuration.</Say></Response>", mimetype="text/xml")
    
    # Get SIP credentials from env
    sip_username = os.getenv("LIVEKIT_SIP_USERNAME") or ""
    sip_password = os.getenv("LIVEKIT_SIP_PASSWORD") or ""
    
    vr = VoiceResponse()
    dial = vr.dial()
    # Include SIP credentials for LiveKit authentication
    if sip_username and sip_password:
        sip = dial.sip(sip_uri, username=sip_username, password=sip_password)
    else:
        sip = dial.sip(sip_uri)
    if ctx:
        sip.add_header("X-Outbound-Context", ctx)
        
    return Response(str(vr), mimetype="text/xml")

@app.post("/api/outbound/schedule")
@require_auth()
def outbound_schedule_add():
    payload = request.json or {}
    agent_id = payload.get("agent_id")
    to_number = payload.get("to_number")
    from_number = payload.get("from_number") or "+12568713652"
    # scheduled_at is expected in ISO format (from UI, likely GST/Local)
    scheduled_at_str = payload.get("scheduled_at")
    context = payload.get("context")
    
    if not agent_id or not to_number or not scheduled_at_str:
        return jsonify({"error": "missing_fields"}), 400
        
    try:
        # Parse ISO string
        dt = datetime.fromisoformat(scheduled_at_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
    except Exception:
        return jsonify({"error": "invalid_date_format"}), 400

    store = _storage_backend()
    if not hasattr(store, "save_scheduled_call"):
        return jsonify({"error": "storage_not_supported"}), 501

    cid = store.save_scheduled_call({
        "agent_id": agent_id,
        "to_number": to_number,
        "from_number": from_number,
        "scheduled_at": dt,
        "status": "pending",
        "context": context
    })
    return jsonify({"id": cid, "status": "pending"})

@app.get("/api/outbound/schedule")
@require_auth()
def outbound_schedule_list():
    store = _storage_backend()
    if not hasattr(store, "list_scheduled_calls"):
        return jsonify({"calls": []})
    calls = store.list_scheduled_calls()
    return jsonify({"calls": calls})


@app.post("/api/outbound/schedule/cleanup")
@require_auth(admin=True)
def outbound_schedule_cleanup():
    """Remove duplicate pending scheduled calls.
    
    Duplicates are calls with the same agent_id, to_number, and scheduled_at.
    Only the first one is kept.
    """
    store = _storage_backend()
    if not hasattr(store, "cleanup_duplicate_scheduled_calls"):
        return jsonify({"error": "not_supported"}), 501
    
    removed = store.cleanup_duplicate_scheduled_calls()
    print(f"[scheduler] Cleanup removed {removed} duplicate scheduled calls", file=sys.stderr)
    return jsonify({"removed": removed, "status": "success"})


@app.delete("/api/outbound/schedule/<int:call_id>")
@require_auth()
def outbound_schedule_delete(call_id: int):
    """Cancel/delete a scheduled call."""
    store = _storage_backend()
    if not hasattr(store, "update_scheduled_call_status"):
        return jsonify({"error": "not_supported"}), 501
    
    store.update_scheduled_call_status(call_id, "cancelled")
    return jsonify({"status": "cancelled", "id": call_id})


@app.post("/api/outbound/upload")
@require_auth()
def outbound_csv_upload():
    if 'file' not in request.files:
        return jsonify({"error": "no_file"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "no_file"}), 400
    
    # Simple CSV parsing
    import csv
    import io
    
    try:
        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        csv_input = csv.DictReader(stream)
    except Exception:
        return jsonify({"error": "invalid_csv"}), 400
    
    added = []
    store = _storage_backend()
    if not hasattr(store, "save_scheduled_call"):
        return jsonify({"error": "storage_not_supported"}), 501
    
    for row in csv_input:
        # Expect: agent_id, to_number, scheduled_at, from_number (optional), context (optional json)
        if not row.get("agent_id") or not row.get("to_number") or not row.get("scheduled_at"):
            continue
        
        try:
            dt = datetime.fromisoformat(row.get("scheduled_at"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            
            ctx = {}
            if row.get("context"):
                try:
                    ctx = json.loads(row.get("context"))
                except:
                    ctx = {"raw": row.get("context")}

            cid = store.save_scheduled_call({
                "agent_id": row.get("agent_id"),
                "to_number": row.get("to_number"),
                "from_number": row.get("from_number") or "+12568713652",
                "scheduled_at": dt,
                "status": "pending",
                "context": ctx
            })
            added.append(cid)
        except Exception:
            continue
            
    return jsonify({"added_count": len(added), "ids": added})


# ------------------ TWILIO MULTI-TENANT API ------------------
def _mask_secret(value: str) -> str:
    try:
        s = str(value or "")
        if len(s) <= 6:
            return "*" * len(s)
        return s[:2] + ("*" * (len(s) - 6)) + s[-4:]
    except Exception:
        return ""


@app.get("/twilio/accounts")
@require_auth()
def twilio_list_accounts():
    user = g.current_user
    rows = []
    for acc in _twilio_get_accounts_for_user(user.get("id")):
        rows.append({
            "id": acc.get("id"),
            "label": acc.get("label") or acc.get("account_sid"),
            "account_sid": acc.get("account_sid"),
            "auth_token_masked": _mask_secret(acc.get("auth_token")),
            "created_at": acc.get("created_at"),
        })
    return jsonify({"accounts": rows})


@app.post("/twilio/accounts")
@require_auth()
def twilio_add_account():
    user = g.current_user
    payload = request.json or {}
    sid = (payload.get("account_sid") or "").strip()
    token = (payload.get("auth_token") or "").strip()
    label = (payload.get("label") or "").strip()
    if not sid or not token:
        return jsonify({"error": "missing_fields"}), 400
    store = _load_twilio_store()
    # Disallow duplicates for the same user+sid
    for acc in store.get("accounts", []):
        if acc.get("user_id") == user.get("id") and acc.get("account_sid") == sid:
            return jsonify({"error": "account_exists"}), 409
    rec = {
        "id": uuid.uuid4().hex,
        "user_id": user.get("id"),
        "account_sid": sid,
        "auth_token": token,
        "label": label,
        "created_at": int(time.time()),
    }
    store.setdefault("accounts", []).append(rec)
    _save_twilio_store(store)
    return jsonify({
        "id": rec["id"],
        "label": rec.get("label") or rec.get("account_sid"),
        "account_sid": rec.get("account_sid"),
        "auth_token_masked": _mask_secret(rec.get("auth_token")),
        "created_at": rec.get("created_at"),
    }), 201


@app.delete("/twilio/accounts/<account_id>")
@require_auth()
def twilio_delete_account(account_id: str):
    user = g.current_user
    store = _load_twilio_store()
    before = len(store.get("accounts", []))
    owned = [a for a in store.get("accounts", []) if a.get("id") == account_id and a.get("user_id") == user.get("id")]
    if not owned:
        return jsonify({"error": "not_found_or_forbidden"}), 404
    store["accounts"] = [a for a in store.get("accounts", []) if not (a.get("id") == account_id and a.get("user_id") == user.get("id"))]
    # Remove bound numbers for this account owned by this user
    nums = store.get("numbers", {}) or {}
    for k in list(nums.keys()):
        v = nums.get(k) or {}
        if v.get("user_id") == user.get("id") and v.get("account_id") == account_id:
            try:
                del nums[k]
            except Exception:
                pass
    store["numbers"] = nums
    _save_twilio_store(store)
    return ("", 204) if len(store.get("accounts", [])) < before else (jsonify({"error": "not_found_or_forbidden"}), 404)


@app.get("/twilio/numbers")
@require_auth()
def twilio_list_numbers():
    user = g.current_user
    store = _load_twilio_store()
    rows = []
    for num, meta in (store.get("numbers") or {}).items():
        try:
            if meta.get("user_id") == user.get("id"):
                rows.append({
                    "number": num,
                    "agent_id": meta.get("agent_id"),
                    "trunk": meta.get("trunk"),
                    "account_id": meta.get("account_id"),
                })
        except Exception:
            continue
    return jsonify({"numbers": rows})


@app.put("/twilio/numbers")
@require_auth()
def twilio_upsert_numbers():
    """Upsert per-user number→agent mappings under a specific Twilio account.
    Payload: { numbers: { "+14155550123": { account_id, agent_id, trunk? }, ... } }
    """
    user = g.current_user
    payload = request.json or {}
    entries = (payload.get("numbers") or {})
    if not isinstance(entries, dict) or not entries:
        return jsonify({"error": "invalid_payload"}), 400
    store = _load_twilio_store()
    # Validate all first
    accounts = {a.get("id"): a for a in store.get("accounts", []) if a.get("user_id") == user.get("id")}
    to_write = {}
    for raw_num, conf in entries.items():
        try:
            number_norm = _normalize_e164(raw_num)
            if not isinstance(conf, dict):
                return jsonify({"error": "invalid_entry", "number": raw_num}), 400
            account_id = (conf.get("account_id") or "").strip()
            agent_id = (conf.get("agent_id") or "").strip()
            trunk = (conf.get("trunk") or "").strip()
            if not account_id or not agent_id:
                return jsonify({"error": "missing_fields", "number": raw_num}), 400
            if account_id not in accounts:
                return jsonify({"error": "account_not_owned", "account_id": account_id, "number": raw_num}), 403
            # Ownership check if number already claimed by a different user
            existing = (store.get("numbers", {}) or {}).get(number_norm)
            if existing and existing.get("user_id") != user.get("id") and user.get("role") not in ["admin", "subscriber", "convix"]:
                return jsonify({"error": "number_in_use", "number": number_norm}), 409
            to_write[number_norm] = {"agent_id": agent_id, "trunk": trunk, "account_id": account_id}
        except Exception:
            return jsonify({"error": "invalid_entry", "number": raw_num}), 400
    # Apply writes
    nums = store.setdefault("numbers", {})
    for n, v in to_write.items():
        nums[n] = {
            "user_id": user.get("id"),
            "account_id": v.get("account_id"),
            "agent_id": v.get("agent_id"),
            "trunk": v.get("trunk"),
        }
    store["numbers"] = nums
    _save_twilio_store(store)
    return jsonify({"ok": True, "numbers": list(to_write.keys())})


@app.delete("/twilio/numbers/<number>")
@require_auth()
def twilio_delete_number(number: str):
    user = g.current_user
    store = _load_twilio_store()
    n = _normalize_e164(number)
    meta = (store.get("numbers", {}) or {}).get(n)
    if not meta:
        return jsonify({"error": "not_found"}), 404
    if meta.get("user_id") != user.get("id") and user.get("role") not in ["admin", "subscriber", "convix"]:
        return jsonify({"error": "forbidden"}), 403
    try:
        del store.setdefault("numbers", {})[n]
    except Exception:
        pass
    _save_twilio_store(store)
    return ("", 204)


_duplicate_routes_with_prefix(app, API_PREFIX)

# ------------------ VOICES API ------------------
@app.get("/voices")
@require_auth()
def list_voices():
    return jsonify(_load_voices())


@app.post("/voices")
@require_auth(admin=True)
def add_voice():
    payload = request.json or {}
    name = (payload.get("name") or "").strip()
    vid = (payload.get("id") or "").strip()
    if not name or not vid:
        return jsonify({"error": "missing_fields"}), 400
    data = _load_voices()
    # ensure unique id and name
    if any(v.get("id") == vid for v in data.get("voices", [])):
        return jsonify({"error": "id_exists"}), 409
    if any((v.get("name") or "").lower() == name.lower() for v in data.get("voices", [])):
        return jsonify({"error": "name_exists"}), 409
    data.setdefault("voices", []).append({"name": name, "id": vid})
    _save_voices(data)
    return jsonify({"name": name, "id": vid}), 201


@app.delete("/voices/<voice_id>")
@require_auth(admin=True)
def delete_voice(voice_id):
    data = _load_voices()
    before = len(data.get("voices", []))
    data["voices"] = [v for v in data.get("voices", []) if v.get("id") != voice_id]
    if len(data.get("voices", [])) == before:
        return jsonify({"error": "not_found"}), 404
    _save_voices(data)
    return ("", 204)

# Duplicate again after registering new routes so they are available under API_PREFIX
_duplicate_routes_with_prefix(app, API_PREFIX)


# ------------------ MODELS (GENERIC) ------------------
@app.post("/models/<provider_id>/discover")
@require_auth(admin=True)
def discover_provider_models(provider_id: str):
    """Fetch all available models from the provider's API."""
    provider_id = provider_id.lower()
    payload = request.json or {}
    api_key = payload.get("api_key")
    
    # If no key provided, try to load from defaults
    if not api_key:
        defaults = _load_defaults()
        print(f"[discovery] No key in payload, checking storage for {provider_id}...", file=sys.stderr, flush=True)
        print(f"[discovery] Sections found in defaults: {list(defaults.keys())}", file=sys.stderr, flush=True)
        
        # 1. Check 'env' section FIRST (new unified storage)
        env_section = defaults.get("env", {}) or {}
        print(f"[discovery] Env section keys: {list(env_section.keys())}", file=sys.stderr, flush=True)
        env_key = f"{provider_id.upper()}_API_KEY"
        # Prefer encrypted in env
        enc_val = env_section.get(f"{env_key}_encrypted")
        if enc_val:
            try: 
                api_key = decrypt_value(enc_val)
                if api_key: print(f"[discovery] Found encrypted key in 'env' section for {provider_id}", file=sys.stderr, flush=True)
            except Exception as e:
                print(f"[discovery] Failed to decrypt env key for {provider_id}: {e}", file=sys.stderr, flush=True)
        if not api_key:
            api_key = env_section.get(env_key)
            if api_key: print(f"[discovery] Found plaintext key in 'env' section for {provider_id}", file=sys.stderr, flush=True)
            
        # 2. Check provider-specific section (legacy storage) if still not found
        if not api_key:
            p_sec = defaults.get(provider_id, {})
            if isinstance(p_sec, dict):
                print(f"[discovery] Checking legacy section '{provider_id}' keys: {list(p_sec.keys())}", file=sys.stderr, flush=True)
                # Prefer encrypted
                enc_val = p_sec.get("api_key_encrypted")
                if enc_val:
                    try: 
                        api_key = decrypt_value(enc_val)
                        if api_key: print(f"[discovery] Found encrypted key in legacy '{provider_id}' section", file=sys.stderr, flush=True)
                    except Exception as e:
                        print(f"[discovery] Failed to decrypt legacy key for {provider_id}: {e}", file=sys.stderr, flush=True)
                if not api_key:
                    api_key = p_sec.get("api_key")
                    if api_key: print(f"[discovery] Found plaintext key in legacy '{provider_id}' section", file=sys.stderr, flush=True)
                
    if not api_key:
        print(f"[discovery] No API key found for {provider_id} in any section", file=sys.stderr, flush=True)
        return jsonify({"error": "No API key found. Please add a key first."}), 400

    # Ensure key is a string and stripped
    api_key = str(api_key).strip()
    
    results = {"llm": [], "stt": [], "tts": [], "realtime": [], "raw": []}
    print(f"[discovery] Fetching models for {provider_id} using key: {api_key[:8]}...{api_key[-4:] if len(api_key)>8 else '***'}", file=sys.stderr, flush=True)
    
    try:
        if provider_id == "openai":
            # Check if there's a base_url in defaults
            defaults = _load_defaults()
            base_url = defaults.get("openai", {}).get("base_url") or os.getenv("OPENAI_BASE_URL", "https://api.openai.com")
            url = base_url.rstrip("/") + "/v1/models"
            
            r = requests.get(url, headers={"Authorization": f"Bearer {api_key}"}, timeout=10)
            if r.ok:
                models = r.json().get("data", [])
                for m in models:
                    mid = m["id"]
                    results["raw"].append(mid)
                    # Simple heuristic categorization
                    if mid.startswith(("gpt-", "o1", "text-davinci")):
                        if "realtime" in mid: results["realtime"].append({"id": mid, "label": mid})
                        else: results["llm"].append({"id": mid, "label": mid})
                    elif mid.startswith(("whisper", "tts")):
                        if mid.startswith("whisper"): results["stt"].append({"id": mid, "label": mid})
                        else: results["tts"].append({"id": mid, "label": mid})
            else:
                app.logger.error(f"[discovery] OpenAI API error: {r.status_code} {r.text}")
                err_msg = "Invalid API key" if r.status_code == 401 else f"API Error: {r.status_code}"
                try: 
                    json_err = r.json()
                    if "error" in json_err and "message" in json_err["error"]:
                        err_msg = json_err["error"]["message"]
                except: pass
                return jsonify({"error": err_msg}), 400
        
        elif provider_id == "anthropic":
            r = requests.get("https://api.anthropic.com/v1/models", headers={"x-api-key": api_key, "anthropic-version": "2023-06-01"}, timeout=10)
            if r.ok:
                models = r.json().get("data", [])
                for m in models:
                    mid = m["id"]
                    results["raw"].append(mid)
                    results["llm"].append({"id": mid, "label": mid})
            else:
                app.logger.error(f"[discovery] Anthropic API error: {r.status_code} {r.text}")
                return jsonify({"error": f"Anthropic error: {r.status_code}"}), 400
                    
        elif provider_id == "elevenlabs":
            r = requests.get("https://api.elevenlabs.io/v1/models", headers={"xi-api-key": api_key}, timeout=10)
            if r.ok:
                models = r.json()
                for m in models:
                    mid = m["model_id"]
                    results["raw"].append(mid)
                    results["tts"].append({"id": mid, "label": m.get("name", mid)})
            else:
                app.logger.error(f"[discovery] ElevenLabs API error: {r.status_code} {r.text}")
                return jsonify({"error": f"ElevenLabs error: {r.status_code}"}), 400

        elif provider_id == "google":
            try:
                # Try a direct request to Gemini API list models
                # URL: https://generativelanguage.googleapis.com/v1beta/models?key=YOUR_API_KEY
                print(f"[discovery] Calling Google API...", file=sys.stderr, flush=True)
                r = requests.get(f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}", timeout=10)
                if r.ok:
                    models = r.json().get("models", [])
                    print(f"[discovery] Google API returned {len(models)} models", file=sys.stderr, flush=True)
                    for m in models:
                        mid = m["name"].split("/")[-1] # models/gemini-pro -> gemini-pro
                        results["raw"].append(mid)
                        if "gemini" in mid:
                            if "live" in mid.lower() or "realtime" in mid.lower():
                                results["realtime"].append({"id": mid, "label": m.get("displayName", mid)})
                            else:
                                results["llm"].append({"id": mid, "label": m.get("displayName", mid)})
                else:
                    print(f"[discovery] Google API error: {r.status_code} {r.text}", file=sys.stderr, flush=True)
                    return jsonify({"error": f"Google API error: {r.status_code}. {r.text[:100]}"}), 400
            except Exception as ge:
                print(f"[discovery] Google discovery failed: {ge}", file=sys.stderr, flush=True)
                return jsonify({"error": str(ge)}), 500
            
        return jsonify(results)
    except Exception as e:
        app.logger.error(f"[discovery] Exception: {str(e)}")
        return jsonify({"error": f"Internal error: {str(e)}"}), 500

@app.get("/models/<provider_id>")
def list_provider_models(provider_id: str):
    """Return available models for a specific provider (built-in + custom).
    
    Admin users see all models including hidden ones.
    Non-admin users only see non-hidden, non-deleted models.
    """
    provider_id = provider_id.lower()
    
    # Check if user is admin
    is_admin = False
    try:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            user_id = payload.get("sub")
            users = _load_users()
            user = next((u for u in users.get("users", []) if u.get("id") == user_id), None)
            if user and user.get("role") in ("admin", "convix", "subscriber"):
                is_admin = True
    except:
        pass
    
    # 1. Get built-in models from registry
    built_in_llm = []
    built_in_stt = []
    built_in_tts = []
    built_in_realtime = []
    
    from providers import LLM_PROVIDERS, STT_PROVIDERS, TTS_PROVIDERS, REALTIME_PROVIDERS
    
    if provider_id in LLM_PROVIDERS:
        built_in_llm = LLM_PROVIDERS[provider_id].get("models") or []
    if provider_id in STT_PROVIDERS:
        built_in_stt = STT_PROVIDERS[provider_id].get("models") or []
    if provider_id in TTS_PROVIDERS:
        built_in_tts = TTS_PROVIDERS[provider_id].get("models") or []
    if provider_id in REALTIME_PROVIDERS:
        built_in_realtime = REALTIME_PROVIDERS[provider_id].get("models") or []
        
    # 2. Load custom models and model settings from defaults
    d = _load_defaults()
    custom_store = d.get("custom_models", {})
    provider_custom = custom_store.get(provider_id, {})
    
    # Load deleted and hidden model lists
    model_settings = d.get("model_settings", {})
    provider_settings = model_settings.get(provider_id, {})
    deleted_models = provider_settings.get("deleted", {})  # {kind: [model_ids]}
    hidden_models = provider_settings.get("hidden", {})    # {kind: [model_ids]}
    
    custom_llm = provider_custom.get("llm") or []
    custom_stt = provider_custom.get("stt") or []
    custom_tts = provider_custom.get("tts") or []
    custom_realtime = provider_custom.get("realtime") or []
    
    # Backward compatibility for old openai-only keys
    if provider_id == "openai":
        custom_llm = _dedup_models(custom_llm + (d.get("llm_models") or []))
        custom_stt = _dedup_models(custom_stt + (d.get("stt_models") or []))

    def _merge(built_in, custom, kind):
        seen = set()
        out = []
        deleted_list = deleted_models.get(kind, [])
        hidden_list = hidden_models.get(kind, [])
        
        # Built-in first
        for m in built_in:
            mid = m.get("id")
            if mid and mid not in seen:
                # Skip deleted models (unless admin viewing for management)
                is_deleted = mid in deleted_list
                is_hidden = mid in hidden_list
                
                # For non-admin users: skip deleted and hidden models
                if not is_admin and (is_deleted or is_hidden):
                    continue
                
                m_copy = copy.deepcopy(m)
                m_copy["is_builtin"] = True
                m_copy["is_deleted"] = is_deleted
                m_copy["is_hidden"] = is_hidden
                out.append(m_copy)
                seen.add(mid)
                
        # Custom second
        for m in custom:
            mid = m.get("id") if isinstance(m, dict) else str(m)
            if mid and mid not in seen:
                is_deleted = mid in deleted_list
                is_hidden = mid in hidden_list
                
                # For non-admin users: skip deleted and hidden models
                if not is_admin and (is_deleted or is_hidden):
                    continue
                    
                label = m.get("label") if isinstance(m, dict) else mid
                out.append({
                    "id": mid, 
                    "label": label, 
                    "is_builtin": False,
                    "is_deleted": is_deleted,
                    "is_hidden": is_hidden,
                })
                seen.add(mid)
        return out

    return jsonify({
        "llm": _merge(built_in_llm, custom_llm, "llm"),
        "stt": _merge(built_in_stt, custom_stt, "stt"),
        "tts": _merge(built_in_tts, custom_tts, "tts"),
        "realtime": _merge(built_in_realtime, custom_realtime, "realtime"),
    })

def _dedup_models(models_list):
    seen = set()
    out = []
    for m in models_list:
        mid = m.get("id") if isinstance(m, dict) else str(m)
        if mid and mid not in seen:
            out.append(m)
            seen.add(mid)
    return out

@app.post("/models/<provider_id>")
@require_auth(admin=True)
def add_custom_model(provider_id: str):
    provider_id = provider_id.lower()
    payload = request.json or {}
    kind = (payload.get("kind") or "llm").lower()
    mid = (payload.get("id") or "").strip()
    label = (payload.get("label") or mid).strip()
    
    if kind not in {"llm", "stt", "tts", "realtime"} or not mid:
        return jsonify({"error": "invalid"}), 400
        
    d = _load_defaults()
    custom_store = d.setdefault("custom_models", {})
    provider_custom = custom_store.setdefault(provider_id, {})
    arr = provider_custom.setdefault(kind, [])
    
    # Avoid dupes
    if not any((isinstance(x, dict) and x.get("id") == mid) or (not isinstance(x, dict) and str(x) == mid) for x in arr):
        arr.append({"id": mid, "label": label})
        
    _save_defaults(d)
    return jsonify(d)

@app.delete("/models/<provider_id>/<kind>/<model_id>")
@require_auth(admin=True)
def delete_custom_model(provider_id: str, kind: str, model_id: str):
    provider_id = provider_id.lower()
    kind = kind.lower()
    
    if kind not in {"llm", "stt", "tts", "realtime"}:
        return jsonify({"error": "invalid"}), 400
        
    d = _load_defaults()
    custom_store = d.get("custom_models", {})
    provider_custom = custom_store.get(provider_id, {})
    if not provider_custom:
        # Check legacy keys for openai
        if provider_id == "openai":
            key = "llm_models" if kind == "llm" else "stt_models"
            arr = d.get(key) or []
            d[key] = [x for x in arr if (isinstance(x, dict) and x.get("id") != model_id) and (not (not isinstance(x, dict) and str(x) == model_id))]
            _save_defaults(d)
            return jsonify(d)
        return jsonify({"error": "not_found"}), 404
        
    arr = provider_custom.get(kind) or []
    provider_custom[kind] = [x for x in arr if (isinstance(x, dict) and x.get("id") != model_id) and (not (not isinstance(x, dict) and str(x) == model_id))]
    
    _save_defaults(d)
    return jsonify(d)


@app.post("/models/<provider_id>/<kind>/<model_id>/delete")
@require_auth(admin=True)
def mark_model_deleted(provider_id: str, kind: str, model_id: str):
    """Mark a model as deleted (removes from user selection, admin can still see)."""
    provider_id = provider_id.lower()
    kind = kind.lower()
    
    if kind not in {"llm", "stt", "tts", "realtime"}:
        return jsonify({"error": "invalid kind"}), 400
    
    d = _load_defaults()
    model_settings = d.setdefault("model_settings", {})
    provider_settings = model_settings.setdefault(provider_id, {})
    deleted_list = provider_settings.setdefault("deleted", {})
    kind_deleted = deleted_list.setdefault(kind, [])
    
    if model_id not in kind_deleted:
        kind_deleted.append(model_id)
    
    _save_defaults(d)
    return jsonify({"ok": True, "deleted": True})


@app.post("/models/<provider_id>/<kind>/<model_id>/hide")
@require_auth(admin=True)
def mark_model_hidden(provider_id: str, kind: str, model_id: str):
    """Mark a model as hidden from non-admin users."""
    provider_id = provider_id.lower()
    kind = kind.lower()
    
    if kind not in {"llm", "stt", "tts", "realtime"}:
        return jsonify({"error": "invalid kind"}), 400
    
    d = _load_defaults()
    model_settings = d.setdefault("model_settings", {})
    provider_settings = model_settings.setdefault(provider_id, {})
    hidden_list = provider_settings.setdefault("hidden", {})
    kind_hidden = hidden_list.setdefault(kind, [])
    
    if model_id not in kind_hidden:
        kind_hidden.append(model_id)
    
    _save_defaults(d)
    return jsonify({"ok": True, "hidden": True})


@app.post("/models/<provider_id>/<kind>/<model_id>/restore")
@require_auth(admin=True)
def restore_model(provider_id: str, kind: str, model_id: str):
    """Restore a deleted or hidden model."""
    provider_id = provider_id.lower()
    kind = kind.lower()
    
    if kind not in {"llm", "stt", "tts", "realtime"}:
        return jsonify({"error": "invalid kind"}), 400
    
    d = _load_defaults()
    model_settings = d.get("model_settings", {})
    provider_settings = model_settings.get(provider_id, {})
    
    # Remove from deleted list
    deleted_list = provider_settings.get("deleted", {})
    if kind in deleted_list and model_id in deleted_list[kind]:
        deleted_list[kind].remove(model_id)
    
    # Remove from hidden list
    hidden_list = provider_settings.get("hidden", {})
    if kind in hidden_list and model_id in hidden_list[kind]:
        hidden_list[kind].remove(model_id)
    
    _save_defaults(d)
    return jsonify({"ok": True, "restored": True})


@app.get("/models/openai")
def list_openai_models_legacy():
    return list_provider_models("openai")

@app.post("/models/openai")
@require_auth(admin=True)
def add_openai_model_legacy():
    return add_custom_model("openai")

@app.delete("/models/openai/<kind>/<model_id>")
@require_auth(admin=True)
def delete_openai_model_legacy(kind, model_id):
    return delete_custom_model("openai", kind, model_id)

_duplicate_routes_with_prefix(app, API_PREFIX)


# ------------------ SIMPLE BROWSER TEST AGENT TURN ------------------
@app.post("/testagent/turn")
@require_auth()
def test_agent_turn():
    """Lightweight chat turn for browser testing (no LiveKit).

    Payload: {
      agent_id?: string,
      messages: [ { role: 'user'|'assistant'|'system', content: string } ]
    }

    Returns: { text: string }
    """
    payload = request.json or {}
    agent_id = payload.get("agent_id")
    msgs = payload.get("messages") or []
    # Load agent profile (by id or active)
    agents = _load_all().get("agents", [])
    agent = None
    if agent_id:
        for a in agents:
            if a.get("id") == agent_id:
                agent = a
                break
    if agent is None:
        agent = _load_active_global() or {}
    # Compose system instructions
    system_prompt = (agent.get("instructions") if isinstance(agent, dict) else None) or "You are a helpful voice agent. Keep replies concise."

    # Only OpenAI provider supported in this simple test path
    llm = (agent.get("llm") if isinstance(agent, dict) else {}) or {}
    # Optional overrides from payload
    overrides = payload.get("llm") or {}
    model = overrides.get("model") or llm.get("model") or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    base_url = (llm.get("base_url") or os.getenv("OPENAI_BASE_URL") or "https://api.openai.com")
    openai_key = os.getenv("OPENAI_API_KEY")
    if not openai_key:
        return jsonify({"error": "missing_openai_key"}), 400

    # Build messages with system at the top
    chat_messages = [{"role": "system", "content": system_prompt}]
    for m in msgs:
        role = (m or {}).get("role") or "user"
        content = (m or {}).get("content") or ""
        chat_messages.append({"role": role, "content": content})

    try:
        resp = requests.post(
            f"{base_url}/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {openai_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "messages": chat_messages,
                "temperature": float(overrides.get("temperature", llm.get("temperature", 0.7))) if isinstance(llm, dict) else 0.7,
                "top_p": float(overrides.get("top_p", llm.get("top_p", 0.9))) if isinstance(llm, dict) else 0.9,
                "max_tokens": 300,
            },
            timeout=30,
        )
        data = resp.json()
        if resp.status_code >= 400:
            return jsonify({"error": data.get("error") or data}), resp.status_code
        text = (((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or ""
        return jsonify({"text": text})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/testagent/transcribe")
@require_auth()
def test_agent_transcribe():
    """Transcribe uploaded audio using OpenAI Whisper.

    Accepts multipart/form-data with field 'file'. Returns { text }.
    """
    try:
        if 'file' not in request.files:
            return jsonify({"error":"file_missing"}), 400
        f = request.files['file']
        openai_key = os.getenv("OPENAI_API_KEY")
        if not openai_key:
            return jsonify({"error":"missing_openai_key"}), 400
        files = {
            'file': (f.filename or 'audio.webm', f.stream, f.mimetype or 'audio/webm')
        }
        data = { 'model': 'whisper-1' }
        resp = requests.post(
            'https://api.openai.com/v1/audio/transcriptions',
            headers={'Authorization': f'Bearer {openai_key}'},
            files=files,
            data=data,
            timeout=60
        )
        out = resp.json()
        if resp.status_code >= 400:
            return jsonify({"error": out.get('error') or out}), resp.status_code
        text = out.get('text') or ''
        return jsonify({ 'text': text })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ------------------ LIVE PLAYGROUND: spawn agent + browser token ------------------
_PLAYGROUND_PROCS: dict[str, subprocess.Popen] = {}
_PLAYGROUND_LOGFILES: dict[str, Any] = {}
# Prevent duplicate spawns for the same room when multiple webhooks arrive in quick succession
_SPAWN_IN_PROGRESS: set[str] = set()
_SPAWN_LOCK = threading.Lock()
# Track workers by agent name so we can enforce a single live worker per phone number.
_AGENT_NAME_TO_ROOM: dict[str, str] = {}
_ROOM_TO_AGENT_NAME: dict[str, str] = {}
_AGENT_NAME_TO_PROC: dict[str, subprocess.Popen] = {}
_AGENT_WORKER_LOCK = threading.Lock()

# Ensure spawned workers die if parent dies, and start their own process group
def _preexec_child():
    # Keep preexec simple and async-signal-safe: start a new session/group only
    try:
        import os
        os.setsid()
    except Exception:
        pass

# Reaper thread to clean up finished child records and close log files
def _terminate_child(proc: subprocess.Popen) -> None:
    try:
        if proc.poll() is not None:
            return
        proc.terminate()
        for _ in range(20):
            if proc.poll() is not None:
                break
            time.sleep(0.2)
        if proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
        try:
            proc.wait(timeout=1)
        except Exception:
            pass
    except Exception:
        pass


_WORKER_START_TIMES: dict[str, float] = {}  # room -> start timestamp
_WORKER_MAX_IDLE_SECONDS = int(os.getenv("WORKER_MAX_IDLE_SECONDS", "180"))  # 3 minutes default

def _bind_worker(room: str, proc: subprocess.Popen, log_fp: Any | None, *, agent_name: str | None = None) -> None:
    if not room or proc is None:
        return
    with _AGENT_WORKER_LOCK:
        _PLAYGROUND_PROCS[room] = proc
        if log_fp is not None:
            _PLAYGROUND_LOGFILES[room] = log_fp
        if agent_name:
            _ROOM_TO_AGENT_NAME[room] = agent_name
            _AGENT_NAME_TO_ROOM[agent_name] = room
            _AGENT_NAME_TO_PROC[agent_name] = proc
        _WORKER_START_TIMES[room] = time.time()


def _stop_egress_for_room(room: str, call_id: str = None) -> None:
    """Stop any active egress recordings for a room."""
    if not room:
        return
    
    try:
        lk_url = (os.getenv("LIVEKIT_URL") or "").strip()
        lk_api_key = (os.getenv("LIVEKIT_API_KEY") or "").strip()
        lk_api_secret = (os.getenv("LIVEKIT_API_SECRET") or "").strip()
        
        if not all([lk_url, lk_api_key, lk_api_secret]):
            return
        
        # Convert ws URL to http
        if lk_url.startswith("wss://"):
            http_url = "https://" + lk_url[len("wss://"):]
        elif lk_url.startswith("ws://"):
            http_url = "http://" + lk_url[len("ws://"):]
        else:
            http_url = lk_url
        
        async def _stop_room_egress():
            client = lk_api.LiveKitAPI(url=http_url, api_key=lk_api_key, api_secret=lk_api_secret)
            try:
                # List active egress for this room
                list_req = lk_api.ListEgressRequest(room_name=room)
                egress_list = await client.egress.list_egress(list_req)
                
                for egress_info in (egress_list.items if hasattr(egress_list, 'items') else egress_list or []):
                    egress_id = getattr(egress_info, 'egress_id', None)
                    status = getattr(egress_info, 'status', None)
                    
                    # Only stop if actively recording
                    if egress_id and status in (None, 0, 1, 2):  # EGRESS_STARTING, EGRESS_ACTIVE
                        try:
                            stop_req = lk_api.StopEgressRequest(egress_id=egress_id)
                            await client.egress.stop_egress(stop_req)
                            print(f"[cleanup] Stopped egress {egress_id} for room {room}", file=sys.stderr)
                        except Exception as e:
                            print(f"[cleanup] Failed to stop egress {egress_id}: {e}", file=sys.stderr)
            finally:
                await client.aclose()
        
        try:
            asyncio.run(_stop_room_egress())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(_stop_room_egress())
            finally:
                loop.close()
                
    except Exception as e:
        print(f"[cleanup] Error stopping egress for room {room}: {e}", file=sys.stderr)


def _cleanup_worker_for_room(room: str | None, reason: str = "cleanup") -> None:
    if not room:
        return
    proc = None
    log_fp = None
    agent_name = None
    with _AGENT_WORKER_LOCK:
        proc = _PLAYGROUND_PROCS.pop(room, None)
        log_fp = _PLAYGROUND_LOGFILES.pop(room, None)
        agent_name = _ROOM_TO_AGENT_NAME.pop(room, None)
        _WORKER_START_TIMES.pop(room, None)  # Remove start time tracking
        if agent_name:
            if _AGENT_NAME_TO_ROOM.get(agent_name) == room:
                _AGENT_NAME_TO_ROOM.pop(agent_name, None)
            stored_proc = _AGENT_NAME_TO_PROC.get(agent_name)
            if stored_proc is proc or stored_proc is None:
                _AGENT_NAME_TO_PROC.pop(agent_name, None)
    
    # Stop any active egress recordings for this room
    try:
        _stop_egress_for_room(room)
    except Exception as e:
        print(f"[cleanup] Warning: egress stop failed for {room}: {e}", file=sys.stderr)
    
    if proc is not None:
        _terminate_child(proc)
        print(f"[cleanup] Terminated worker process for room {room}", file=sys.stderr)
    if log_fp is not None:
        try:
            log_fp.flush()
            log_fp.close()
        except Exception:
            pass
    with _SPAWN_LOCK:
        _SPAWN_IN_PROGRESS.discard(room)
    if agent_name:
        try:
            print(f"[cleanup] Cleaned up room {room} agent={agent_name} reason={reason}", file=sys.stderr, flush=True)
        except Exception:
            pass


def _cleanup_worker_for_agent(agent_name: str | None, reason: str = "cleanup") -> None:
    if not agent_name:
        return
    with _AGENT_WORKER_LOCK:
        room = _AGENT_NAME_TO_ROOM.get(agent_name)
    if room:
        _cleanup_worker_for_room(room, reason=reason)


def _ensure_single_worker_for_agent(agent_name: str | None, reason: str = "pre_spawn", target_room: str | None = None) -> None:
    """Ensure only one worker exists for an agent. If target_room matches existing room, skip cleanup."""
    if not agent_name:
        return
    with _AGENT_WORKER_LOCK:
        existing_room = _AGENT_NAME_TO_ROOM.get(agent_name)
    
    # If worker already exists for the SAME room we're trying to spawn into, don't kill it
    if existing_room and target_room and existing_room == target_room:
        print(f"[cleanup] Skipping cleanup for agent {agent_name} - already bound to target room {target_room}", file=sys.stderr)
        return
    
    # For outbound rooms, be extra careful - check if the existing room is an outbound pre-spawn
    if existing_room and existing_room.startswith("outbound-"):
        with _SPAWN_LOCK:
            if existing_room in _SPAWN_IN_PROGRESS:
                print(f"[cleanup] Skipping cleanup for agent {agent_name} - outbound room {existing_room} still spawning", file=sys.stderr)
                return
    
    if existing_room:
        _cleanup_worker_for_room(existing_room, reason=reason)
    _kill_orphan_agent_workers(agent_name, reason=reason)


def _read_proc_env_value(proc_dir: Path, key: str) -> str | None:
    try:
        with open(proc_dir / "environ", "rb") as fp:
            data = fp.read().split(b"\x00")
        key_bytes = (key + "=").encode()
        for item in data:
            if item.startswith(key_bytes):
                return item[len(key_bytes):].decode(errors="ignore")
    except Exception:
        return None
    return None


def _terminate_pid(pid: int, timeout: float = 2.0) -> bool:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return False
    except PermissionError:
        return False
    end = time.time() + max(0.2, timeout)
    while time.time() < end:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        time.sleep(0.1)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return True


def _kill_orphan_agent_workers(agent_name: str, reason: str = "cleanup") -> None:
    if not agent_name:
        return
    proc_root = Path("/proc")
    killed = 0
    for entry in proc_root.iterdir():
        if not entry.is_dir():
            continue
        name = entry.name
        if not name.isdigit():
            continue
        pid = int(name)
        if pid == os.getpid():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes()
        except Exception:
            continue
        if b"Livekit-Agent/agent.py" not in cmdline:
            continue
        env_agent = _read_proc_env_value(entry, "AGENT_NAME")
        if not env_agent or env_agent != agent_name:
            continue
        # Skip the process if we're already tracking it (handled elsewhere)
        with _AGENT_WORKER_LOCK:
            tracked_proc = _AGENT_NAME_TO_PROC.get(agent_name)
        if tracked_proc is not None and tracked_proc.pid == pid:
            continue
        if _terminate_pid(pid):
            killed += 1
    if killed:
        try:
            print(f"[webhook] Killed {killed} orphan workers for agent {agent_name} reason={reason}", file=sys.stderr, flush=True)
        except Exception:
            pass


def _check_room_has_participants(room_name: str) -> bool:
    """Check if a room has any participants via LiveKit API."""
    try:
        lk_url = (os.getenv("LIVEKIT_URL") or "").strip()
        lk_api_key = (os.getenv("LIVEKIT_API_KEY") or "").strip()
        lk_api_secret = (os.getenv("LIVEKIT_API_SECRET") or "").strip()
        
        if not all([lk_url, lk_api_key, lk_api_secret]):
            return True  # Assume has participants if can't check
        
        # Convert ws URL to http
        if lk_url.startswith("wss://"):
            http_url = "https://" + lk_url[len("wss://"):]
        elif lk_url.startswith("ws://"):
            http_url = "http://" + lk_url[len("ws://"):]
        else:
            http_url = lk_url
        
        async def _check():
            client = lk_api.LiveKitAPI(url=http_url, api_key=lk_api_key, api_secret=lk_api_secret)
            try:
                rooms = await client.room.list_rooms(room_proto.ListRoomsRequest(names=[room_name]))
                if not rooms or not rooms.rooms:
                    return False  # Room doesn't exist
                room = rooms.rooms[0]
                return room.num_participants > 0
            finally:
                await client.aclose()
        
        try:
            return asyncio.run(_check())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(_check())
            finally:
                loop.close()
    except Exception:
        return True  # Assume has participants on error


def _start_reaper_thread():
    def _reap():
        while True:
            try:
                now = time.time()
                rooms_to_cleanup = []
                
                for room, proc in list(_PLAYGROUND_PROCS.items()):
                    try:
                        # Check if process has exited
                        if proc.poll() is not None:
                            rooms_to_cleanup.append((room, "worker_exit"))
                            continue
                        
                        # Check for idle workers (running too long)
                        start_time = _WORKER_START_TIMES.get(room, now)
                        running_seconds = now - start_time
                        
                        # After max idle time, check if room is actually empty
                        if running_seconds > _WORKER_MAX_IDLE_SECONDS:
                            if not _check_room_has_participants(room):
                                rooms_to_cleanup.append((room, f"idle_timeout_{int(running_seconds)}s"))
                                print(f"[reaper] Worker for room {room} idle for {int(running_seconds)}s, marking for cleanup", file=sys.stderr)
                    except Exception as e:
                        print(f"[reaper] Error checking room {room}: {e}", file=sys.stderr)
                        continue
                
                # Cleanup marked rooms
                for room, reason in rooms_to_cleanup:
                    try:
                        _cleanup_worker_for_room(room, reason=reason)
                    except Exception as e:
                        print(f"[reaper] Error cleaning up room {room}: {e}", file=sys.stderr)
                        
            except Exception as e:
                print(f"[reaper] Error in reaper loop: {e}", file=sys.stderr)
            
            time.sleep(15)  # Check every 15 seconds
    
    try:
        t = threading.Thread(target=_reap, daemon=True, name="worker_reaper")
        t.start()
        print("[reaper] Worker reaper thread started (max idle: {}s)".format(_WORKER_MAX_IDLE_SECONDS), file=sys.stderr)
    except Exception as e:
        print(f"[reaper] Failed to start reaper thread: {e}", file=sys.stderr)

_start_reaper_thread()
_clear_dispatch_rule_agents_async()


# ------------------ ADMIN: WORKER MANAGEMENT ------------------

@app.get("/api/admin/workers")
@require_auth(admin=True)
def list_active_workers():
    """List all active agent workers (admin only)."""
    workers = []
    now = time.time()
    with _AGENT_WORKER_LOCK:
        for room, proc in _PLAYGROUND_PROCS.items():
            try:
                agent_name = _ROOM_TO_AGENT_NAME.get(room)
                start_time = _WORKER_START_TIMES.get(room, now)
                running_seconds = int(now - start_time)
                workers.append({
                    "room": room,
                    "agent_name": agent_name,
                    "pid": proc.pid if proc else None,
                    "running_seconds": running_seconds,
                    "status": "running" if proc and proc.poll() is None else "exited",
                })
            except Exception:
                continue
    return jsonify({
        "workers": workers,
        "count": len(workers),
        "max_idle_seconds": _WORKER_MAX_IDLE_SECONDS,
    })


@app.post("/api/admin/workers/cleanup")
@require_auth(admin=True)
def cleanup_all_workers():
    """Force cleanup all active workers (admin only)."""
    cleaned = []
    with _AGENT_WORKER_LOCK:
        rooms = list(_PLAYGROUND_PROCS.keys())
    
    for room in rooms:
        try:
            _cleanup_worker_for_room(room, reason="admin_cleanup")
            cleaned.append(room)
        except Exception as e:
            print(f"[admin] Failed to cleanup room {room}: {e}", file=sys.stderr)
    
    return jsonify({
        "status": "ok",
        "cleaned_rooms": cleaned,
        "count": len(cleaned),
    })


@app.delete("/api/admin/workers/<room>")
@require_auth(admin=True)
def cleanup_worker_by_room(room: str):
    """Force cleanup a specific worker by room name (admin only)."""
    try:
        with _AGENT_WORKER_LOCK:
            if room not in _PLAYGROUND_PROCS:
                return jsonify({"error": "worker_not_found"}), 404
        
        _cleanup_worker_for_room(room, reason="admin_delete")
        return jsonify({"status": "ok", "room": room})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ------------------ LOCAL AGENT SUPERVISOR (PER-NUMBER WORKERS) ------------------
# When enabled, keep a long-lived local supervisor running that registers
# LiveKit Agents workers named by phone number. SIP dispatch rules that target
# those agent names will then dispatch locally without requiring webhook spawn.
_NUMBERS_SUPERVISOR: subprocess.Popen | None = None
_NUMBERS_SUP_LOCK = threading.Lock()

def _start_numbers_supervisor_if_needed() -> None:
    try:
        if os.getenv("ENABLE_LOCAL_SUPERVISOR", "0") != "1":
            return
        global _NUMBERS_SUPERVISOR
        with _NUMBERS_SUP_LOCK:
            if _NUMBERS_SUPERVISOR is not None and _NUMBERS_SUPERVISOR.poll() is None:
                return
            env = os.environ.copy()
            # Provide CONFIG_API_BASE for numbers.json routing lookups
            try:
                env.setdefault("CONFIG_API_BASE", os.getenv("CONFIG_API_BASE", f"http://localhost:{os.getenv('CONFIG_API_PORT','5057')}"))
            except Exception:
                pass
            # Inject LiveKit creds from defaults as a convenience
            try:
                d = _load_defaults() or {}
            except Exception:
                d = {}
            lk_def = (d or {}).get("livekit", {}) or {}
            env["LIVEKIT_URL"] = str(lk_def.get("url") or os.getenv("LIVEKIT_URL", ""))
            env["LIVEKIT_API_KEY"] = str(lk_def.get("api_key") or os.getenv("LIVEKIT_API_KEY", ""))
            env["LIVEKIT_API_SECRET"] = str(lk_def.get("api_secret") or os.getenv("LIVEKIT_API_SECRET", ""))
            # Start agent.py without AGENT_NAME so it runs the per-number supervisor
            agent_path = str((Path(__file__).parent / "agent.py").resolve())
            try:
                logs_dir = Path(__file__).parent / "logs"
                logs_dir.mkdir(parents=True, exist_ok=True)
                log_fp = open(str(logs_dir / f"supervisor.log"), "wb")
            except Exception:
                log_fp = None
            try:
                if log_fp is not None:
                    _NUMBERS_SUPERVISOR = subprocess.Popen([sys.executable, agent_path], env=env, stdout=log_fp, stderr=subprocess.STDOUT, preexec_fn=_preexec_child)
                else:
                    _NUMBERS_SUPERVISOR = subprocess.Popen([sys.executable, agent_path], env=env, preexec_fn=_preexec_child)
            except Exception:
                _NUMBERS_SUPERVISOR = None
    except Exception:
        pass

def _monitor_numbers_supervisor():
    while True:
        try:
            _start_numbers_supervisor_if_needed()
        except Exception:
            pass
        time.sleep(5)

try:
    t = threading.Thread(target=_monitor_numbers_supervisor, daemon=True)
    t.start()
except Exception:
    pass

@app.post("/playground/start")
@require_auth()
def playground_start():
    payload = request.json or {}
    
    # Log full payload (redact sensitive fields)
    debug_payload = {k: v if k not in ['token', 'jwt', 'password'] else '***REDACTED***' for k, v in payload.items()}
    print(f"[playground/start] Full payload received:", file=sys.stderr, flush=True)
    print(json.dumps(debug_payload, indent=2), file=sys.stderr, flush=True)
    
    agent_id = payload.get("agent_id")
    if not agent_id:
        return jsonify({"error":"agent_id_required"}), 400
    
    # Validate and extract tools
    tools = payload.get("tools", [])
    print(f"[playground/start] Received request with {len(tools) if tools else 0} tools in payload", file=sys.stderr, flush=True)
    if tools and not isinstance(tools, list):
        return jsonify({"error": "tools must be an array"}), 400
    
    # Extract global webhook configuration (caller handles routing)
    tools_webhook = payload.get("tools_webhook", {})
    webhook_url = tools_webhook.get("url") if isinstance(tools_webhook, dict) else None
    webhook_headers = tools_webhook.get("headers", {}) if isinstance(tools_webhook, dict) else {}
    webhook_method = tools_webhook.get("method", "POST") if isinstance(tools_webhook, dict) else "POST"
    webhook_timeout = tools_webhook.get("timeout", 10) if isinstance(tools_webhook, dict) else 10
    
    # Normalize tool format: support both OpenAI format and direct format
    normalized_tools = []
    for tool in tools:
        if not isinstance(tool, dict):
            return jsonify({"error": "each tool must be an object"}), 400
        
        # Check if it's OpenAI format with "function" wrapper
        if "type" in tool and tool.get("type") == "function" and "function" in tool:
            # Extract function definition
            func = tool.get("function", {})
            normalized = {
                "name": func.get("name"),
                "description": func.get("description"),
                "parameters": func.get("parameters", {}),
            }
            # Use tool-specific implementation if provided, otherwise use global webhook
            if "implementation" in tool:
                normalized["implementation"] = tool.get("implementation")
            elif webhook_url:
                # Apply global webhook config
                normalized["implementation"] = {
                    "type": "http_callback",
                    "url": webhook_url,
                    "method": webhook_method,
                    "headers": webhook_headers,
                    "timeout": webhook_timeout
                }
            normalized_tools.append(normalized)
        else:
            # Direct format (already normalized)
            if "name" not in tool or "description" not in tool:
                return jsonify({"error": "tool missing name or description"}), 400
            # Apply global webhook if no implementation
            if "implementation" not in tool and webhook_url:
                tool["implementation"] = {
                    "type": "http_callback",
                    "url": webhook_url,
                    "method": webhook_method,
                    "headers": webhook_headers,
                    "timeout": webhook_timeout
                }
            normalized_tools.append(tool)
    
    tools = normalized_tools
    print(f"[playground/start] After normalization: {len(tools)} tools ready", file=sys.stderr, flush=True)
    
    # Extract and validate avatar configuration (Beyond Presence)
    avatar_config = payload.get("avatar", {})
    avatar_enabled = False
    avatar_id = None
    if isinstance(avatar_config, dict) and avatar_config.get("enabled"):
        # Validate BEY_API_KEY is configured
        if not os.getenv("BEY_API_KEY"):
            return jsonify({
                "error": "avatar_missing_api_key",
                "hint": "Set BEY_API_KEY in server environment to use Beyond Presence avatars."
            }), 400
        avatar_enabled = True
        avatar_id = avatar_config.get("avatar_id") or os.getenv("BEY_AVATAR_ID")
        print(f"[playground/start] Avatar enabled, avatar_id={avatar_id or 'default'}", file=sys.stderr, flush=True)
    
    room = payload.get("room") or f"play-{_uuid.uuid4().hex[:8]}"
    identity = f"user-{_uuid.uuid4().hex[:6]}"
    
    # Get room_config from frontend (official LiveKit pattern)
    room_config_payload = payload.get("room_config")
    agent_display_name = None
    try:
        if isinstance(room_config_payload, dict):
            agents_cfg = room_config_payload.get("agents")
            if isinstance(agents_cfg, list) and agents_cfg:
                agent_display_name = agents_cfg[0].get("agent_name") or agents_cfg[0].get("name")
    except Exception:
        pass

    try:
        data = _load_all()
        agent_conf = next((a for a in data.get("agents", []) if a.get("id") == agent_id), {})
        d = _load_defaults()
        lk_cfg = (agent_conf or {}).get("livekit", {}) if isinstance(agent_conf, dict) else {}
        lk_def = (d or {}).get("livekit", {})
        lk_url = str(lk_cfg.get("url") or lk_def.get("url") or os.getenv("LIVEKIT_URL", ""))
        lk_key = str(lk_cfg.get("api_key") or lk_def.get("api_key") or os.getenv("LIVEKIT_API_KEY", ""))
        lk_secret = str(lk_cfg.get("api_secret") or lk_def.get("api_secret") or os.getenv("LIVEKIT_API_SECRET", ""))
        if not (lk_url and lk_key and lk_secret):
            return jsonify({"error":"missing_livekit_env", "hint":"Set LIVEKIT_URL/KEY/SECRET in server env or in the agent's livekit section."}), 500
        if not agent_display_name:
            agent_display_name = (agent_conf or {}).get("name") or (agent_conf or {}).get("title")
        # Pre-populate call metadata for web/playground sessions
        try:
            store = _load_livekit_store()
            call_entry = _lk_get_or_create_call(store, room)
            call_entry["room_name"] = room
            routing_meta = call_entry.setdefault("routing", {})
            routing_meta.setdefault("call_type", "web")
            routing_meta.setdefault("agent_id", str(agent_id))
            if agent_display_name:
                routing_meta.setdefault("agent_name", agent_display_name)
            # Track participants baseline as empty dict to avoid KeyErrors
            call_entry.setdefault("participants", {})
            call_entry.setdefault("numbers", {})
            call_entry.setdefault("events", [])
            call_entry.setdefault("timeline", []).append({"t": int(time.time()), "event": "playground_start"})
            call_entry["last_event"] = "playground_start"
            call_entry["last_received_at"] = int(time.time())
            agent_rt = call_entry.setdefault("agent_runtime", {})
            if agent_display_name and not agent_rt.get("agent_name"):
                agent_rt["agent_name"] = agent_display_name
            _save_livekit_store(store)
        except Exception:
            pass
        
        token = lk_api.AccessToken(lk_key, lk_secret)
        token.with_identity(identity)
        token.with_name(identity)
        grants = lk_api.VideoGrants(
            room=room,
            room_join=True,
            can_publish=True,
            can_subscribe=True,
        )
        token.with_grants(grants)
        jwt = token.to_jwt()
        
        # Store tools in session cache before returning
        if tools:
            _playground_set_tools(room, tools)
            print(f"[playground] Stored {len(tools)} tools for room {room}", file=sys.stderr, flush=True)
        
        # Spawn agent worker on-demand for this playground session
        try:
            # Thread-safe deduplication check (same as webhook handler)
            should_spawn_web = False
            with _SPAWN_LOCK:
                if room not in _PLAYGROUND_PROCS and room not in _SPAWN_IN_PROGRESS:
                    _SPAWN_IN_PROGRESS.add(room)
                    should_spawn_web = True
                    print(f"[playground] Spawning worker for room {room}", file=sys.stderr, flush=True)
                else:
                    print(f"[playground] Skipping duplicate spawn for room {room}", file=sys.stderr, flush=True)
            
            if should_spawn_web:
                def _spawn_playground_worker():
                    try:
                        env = os.environ.copy()
                        env.setdefault("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", os.getenv("PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION", "python"))
                        env.setdefault("PYTHONUNBUFFERED", "1")
                        env["RUN_AS_WORKER"] = "1"
                        env["PLAYGROUND_ROOM_NAME"] = str(room)
                        env["PLAYGROUND_FORCE_DIRECT"] = "1"
                        env.setdefault("PLAYGROUND_IDENTITY", f"agent-{_uuid.uuid4().hex[:6]}")
                        env["AGENT_PROFILE_ID"] = str(agent_id)
                        env["LIVEKIT_URL"] = lk_url
                        env["LIVEKIT_API_KEY"] = lk_key
                        env["LIVEKIT_API_SECRET"] = lk_secret
                        
                        # Inject OpenAI credentials from defaults if present
                        try:
                            openai_def = (d or {}).get("openai", {})
                            if openai_def.get("api_key"):
                                env.setdefault("OPENAI_API_KEY", str(openai_def.get("api_key")))
                            if openai_def.get("base_url"):
                                env.setdefault("OPENAI_BASE_URL", str(openai_def.get("base_url")))
                        except Exception:
                            pass
                        
                        env.setdefault("AGENT_NAME", f"playground-{agent_id}")
                        
                        # Avatar configuration (Beyond Presence)
                        if avatar_enabled:
                            env["AVATAR_ENABLED"] = "1"
                            if avatar_id:
                                env["AVATAR_ID"] = str(avatar_id)
                            # Pass BEY_API_KEY to worker (already validated above)
                            env.setdefault("BEY_API_KEY", os.getenv("BEY_API_KEY", ""))
                        
                        # Provide CONFIG_API_BASE for tool resolution
                        try:
                            env.setdefault("CONFIG_API_BASE", os.getenv("CONFIG_API_BASE", f"http://localhost:{os.getenv('CONFIG_API_PORT','5057')}"))
                        except Exception:
                            pass
                        
                        agent_path = str((Path(__file__).parent / "agent.py").resolve())
                        
                        # Create log file
                        try:
                            logs_dir = Path(__file__).parent / "logs"
                            logs_dir.mkdir(parents=True, exist_ok=True)
                            ts = time.strftime("%Y%m%d_%H%M%S")
                            short = _uuid.uuid4().hex[:6]
                            log_fp = open(str(logs_dir / f"play_{room}_{ts}_{short}.log"), "wb")
                        except Exception:
                            log_fp = None
                        
                        if log_fp is not None:
                            proc = subprocess.Popen([sys.executable, agent_path], env=env, stdout=log_fp, stderr=subprocess.STDOUT, preexec_fn=_preexec_child)
                        else:
                            proc = subprocess.Popen([sys.executable, agent_path], env=env, preexec_fn=_preexec_child)
                        
                        _bind_worker(room, proc, log_fp)
                        print(f"[playground] Spawned worker for room {room} (PID: {proc.pid})", file=sys.stderr, flush=True)
                    except Exception as e:
                        print(f"[playground] Failed to spawn worker: {e}", file=sys.stderr, flush=True)
                    finally:
                        # Clear spawn-in-progress flag
                        with _SPAWN_LOCK:
                            _SPAWN_IN_PROGRESS.discard(room)
        
                # Spawn in background thread to avoid blocking response
                threading.Thread(target=_spawn_playground_worker, daemon=True).start()
        except Exception as e:
            print(f"[playground] Worker spawn error: {e}", file=sys.stderr, flush=True)
        
        response_data = {"server_url": lk_url, "room": room, "token": jwt, "identity": identity}
        if avatar_enabled:
            response_data["avatar"] = {"enabled": True, "avatar_id": avatar_id}
        return jsonify(response_data)
    except Exception as e:
        return jsonify({"error": f"token_error: {e}"}), 500


@app.post("/playground/stop")
@require_auth()
def playground_stop():
    payload = request.json or {}
    room = payload.get("room")
    if not room:
        return jsonify({"error":"room_required"}), 400
    _cleanup_worker_for_room(room, reason="playground_stop")
    # Clear tools cache for this room
    _playground_clear_tools(room)
    print(f"[playground] Cleared tools cache for room {room}", file=sys.stderr, flush=True)
    return jsonify({"ok": True})


@app.get("/internal/playground/<room>/tools")
@require_internal()
def get_playground_tools(room: str):
    """Return tools configured for a playground session (agent worker use only)."""
    tools = _playground_get_tools(room)
    return jsonify({"tools": tools})


# Ensure the new playground routes are also available under API_PREFIX (e.g., /api)
_duplicate_routes_with_prefix(app, API_PREFIX)

if __name__ == "__main__":
    port = int(os.getenv("CONFIG_API_PORT", "5057"))
    app.run(host="0.0.0.0", port=port, debug=True)



