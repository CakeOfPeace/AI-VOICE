"""Postgres-backed storage implementation."""

from __future__ import annotations

import copy
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict

from sqlalchemy import delete, select, text as _sa_text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

try:  # psycopg is optional in some environments, so guard the import
    from psycopg.errors import DeadlockDetected
except Exception:  # pragma: no cover - fallback when psycopg isn't installed
    DeadlockDetected = None

from db.core import get_session_factory
from db import models
from .base import StorageBackend


def _deepcopy(data: Dict[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(data)


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            try:
                return datetime.fromtimestamp(float(s), tz=timezone.utc)
            except Exception:
                return None
    return None


_DEADLOCK_MAX_RETRIES = 5
_DEADLOCK_BASE_DELAY = 0.1  # seconds


def _is_deadlock_error(exc: OperationalError) -> bool:
    """Detect postgres deadlock errors without depending on concrete driver types."""
    orig = getattr(exc, "orig", None)
    if DeadlockDetected and isinstance(orig, DeadlockDetected):
        return True
    message = str(orig or exc).lower()
    return "deadlock detected" in message


class PostgresStorageBackend(StorageBackend):
    def __init__(self):
        self.SessionFactory = get_session_factory()
        self._livekit_lock = threading.RLock()

    def _upsert_call_transcript(self, session: Session, call_id: str, payload: Dict[str, Any]):
        """Insert or update a transcript row atomically to avoid duplicate key races."""
        entries = payload.get("entries") or []
        agent = payload.get("agent")
        started_at = _to_datetime(payload.get("started_at"))
        ended_at = _to_datetime(payload.get("ended_at"))
        stmt = (
            insert(models.CallTranscript)
            .values(
                call_id=call_id,
                entries=entries,
                agent=agent,
                started_at=started_at,
                ended_at=ended_at,
                saved_at=datetime.now(timezone.utc),
            )
            .on_conflict_do_update(
                index_elements=[models.CallTranscript.call_id],
                set_={
                    "entries": entries,
                    "agent": agent,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "saved_at": datetime.now(timezone.utc),
                },
            )
            .returning(models.CallTranscript.id)
        )
        transcript_id = session.execute(stmt).scalar_one()
        return session.get(models.CallTranscript, transcript_id)

    # Generic helpers ------------------------------------------------
    def _load_kv(self, namespace: str, key: str, default: Dict[str, Any]) -> Dict[str, Any]:
        with self.SessionFactory() as session:
            row = session.get(models.KVStore, {"namespace": namespace, "key": key})
            if row and row.data is not None:
                return copy.deepcopy(row.data)
        return _deepcopy(default)

    def _save_kv(self, namespace: str, key: str, data: Dict[str, Any]) -> None:
        payload = data or {}
        with self.SessionFactory() as session:
            stmt = insert(models.KVStore).values(namespace=namespace, key=key, data=payload)
            stmt = stmt.on_conflict_do_update(
                index_elements=[models.KVStore.namespace, models.KVStore.key],
                set_={"data": stmt.excluded.data},
            )
            session.execute(stmt)
            session.commit()

    # Agents ---------------------------------------------------------
    def load_agents(self) -> Dict[str, Any]:
        return self._load_kv("config", "agents", {"agents": [], "active_agent_id": None})

    def save_agents(self, data: Dict[str, Any]) -> None:
        self._save_kv("config", "agents", data)

    def load_active_agent(self) -> Dict[str, Any]:
        return self._load_kv("config", "active", {})

    def save_active_agent(self, data: Dict[str, Any]) -> None:
        self._save_kv("config", "active", data)

    def load_users(self) -> Dict[str, Any]:
        return self._load_kv("config", "users", {"users": []})

    def save_users(self, data: Dict[str, Any]) -> None:
        self._save_kv("config", "users", data)

    def load_sessions(self) -> Dict[str, Any]:
        return self._load_kv("config", "sessions", {"sessions": {}, "resets": {}})

    def save_sessions(self, data: Dict[str, Any]) -> None:
        self._save_kv("config", "sessions", data)

    # Config ---------------------------------------------------------
    def load_numbers(self) -> Dict[str, Any]:
        return self._load_kv("config", "numbers", {"routes": {}})

    def save_numbers(self, data: Dict[str, Any]) -> None:
        self._save_kv("config", "numbers", data)

    def load_twilio_store(self) -> Dict[str, Any]:
        return self._load_kv("config", "twilio_accounts", {"accounts": [], "numbers": {}})

    def save_twilio_store(self, data: Dict[str, Any]) -> None:
        self._save_kv("config", "twilio_accounts", data)

    def load_defaults(self) -> Dict[str, Any]:
        return self._load_kv("config", "defaults", {"llm": {}, "stt": {}, "tts": {}, "realtime": {}, "openai": {}, "env": {}})

    def save_defaults(self, data: Dict[str, Any]) -> None:
        self._save_kv("config", "defaults", data)

    def load_voices(self) -> Dict[str, Any]:
        return self._load_kv("config", "voices", {"voices": []})

    def save_voices(self, data: Dict[str, Any]) -> None:
        self._save_kv("config", "voices", data)

    def load_usage(self) -> Dict[str, Any]:
        return self._load_kv("metrics", "usage", {"usage": {}})

    def save_usage(self, data: Dict[str, Any]) -> None:
        self._save_kv("metrics", "usage", data)

    def load_subscriptions(self) -> Dict[str, Any]:
        return self._load_kv("metrics", "subscriptions", {"subs": []})

    def save_subscriptions(self, data: Dict[str, Any]) -> None:
        self._save_kv("metrics", "subscriptions", data)

    # LiveKit --------------------------------------------------------
    def load_livekit_store(self) -> Dict[str, Any]:
        meta = self._load_kv("livekit", "meta", {"calls": {}, "room_to_call": {}, "call_to_room": {}})
        with self.SessionFactory() as session:
            calls: Dict[str, Any] = {}
            rows = session.scalars(select(models.LivekitCall)).all()
            for row in rows:
                payload = copy.deepcopy(row.data or {})
                if row.transcript:
                    payload.setdefault(
                        "transcript",
                        {
                            "entries": row.transcript.entries or [],
                            "agent": row.transcript.agent,
                            "started_at": row.transcript.started_at.isoformat() if row.transcript.started_at else None,
                            "ended_at": row.transcript.ended_at.isoformat() if row.transcript.ended_at else None,
                            "saved_at": row.transcript.saved_at.isoformat() if row.transcript.saved_at else None,
                        },
                    )
                calls[row.call_id] = payload
        meta["calls"] = calls
        return meta

    def save_livekit_store(self, data: Dict[str, Any]) -> None:
        data = data or {}
        calls = copy.deepcopy(data.get("calls") or {})
        meta = {k: copy.deepcopy(v) for k, v in data.items() if k != "calls"}
        self._save_kv("livekit", "meta", meta)
        for attempt in range(1, _DEADLOCK_MAX_RETRIES + 1):
            with self.SessionFactory() as session:
                try:
                    existing_ids = set(session.scalars(select(models.LivekitCall.call_id)))
                    for call_id, payload in calls.items():
                        transcript_payload = payload.get("transcript")
                        transcript_obj = None
                        if transcript_payload:
                            transcript_obj = self._upsert_call_transcript(session, call_id, transcript_payload)

                        call_row = session.get(models.LivekitCall, call_id)
                        if call_row is None:
                            call_row = models.LivekitCall(call_id=call_id)
                            session.add(call_row)
                        call_row.room_name = payload.get("room_name")
                        agent_id = None
                        runtime = payload.get("agent_runtime") or {}
                        routing = payload.get("routing") or {}
                        if isinstance(runtime, dict):
                            agent_id = runtime.get("agent_id") or runtime.get("profile_id")
                        if not agent_id and isinstance(routing, dict):
                            agent_id = routing.get("agent_id")
                        call_row.agent_id = agent_id
                        call_row.created_at = _to_datetime(payload.get("created_at"))
                        call_row.ended_at = _to_datetime(payload.get("ended_at"))
                        call_row.last_received_at = payload.get("last_received_at")
                        call_row.data = payload
                        if transcript_obj:
                            call_row.transcript = transcript_obj
                        existing_ids.discard(call_id)
                    if existing_ids:
                        session.execute(delete(models.LivekitCall).where(models.LivekitCall.call_id.in_(list(existing_ids))))
                    session.commit()
                    return
                except OperationalError as exc:
                    session.rollback()
                    if _is_deadlock_error(exc) and attempt < _DEADLOCK_MAX_RETRIES:
                        backoff = min(_DEADLOCK_BASE_DELAY * (2 ** (attempt - 1)), 1.0)
                        time.sleep(backoff)
                        continue
                    raise

    def mutate_livekit_store(self, mutate_fn):
        with self._livekit_lock:
            store = self.load_livekit_store()
            result = mutate_fn(store)
            if result is not None:
                store = result
            self.save_livekit_store(store)
            return store

    def load_recordings_index(self) -> Dict[str, Any]:
        return self._load_kv("livekit", "recordings_index", {"items": [], "last_scan_ts": 0, "scan_cursor": None, "total": 0})

    def save_recordings_index(self, data: Dict[str, Any]) -> None:
        self._save_kv("livekit", "recordings_index", data)

    # Outbound Calling -----------------------------------------------
    def list_scheduled_calls(self, status: str | None = None) -> list[Dict[str, Any]]:
        with self.SessionFactory() as session:
            stmt = select(models.ScheduledCall)
            if status:
                stmt = stmt.where(models.ScheduledCall.status == status)
            stmt = stmt.order_by(models.ScheduledCall.scheduled_at)
            rows = session.scalars(stmt).all()
            out = []
            for r in rows:
                out.append({
                    "id": r.id,
                    "agent_id": r.agent_id,
                    "to_number": r.to_number,
                    "from_number": r.from_number,
                    "scheduled_at": r.scheduled_at.isoformat() if r.scheduled_at else None,
                    "status": r.status,
                    "context": r.context,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "call_sid": r.call_sid,
                    "error_message": r.error_message,
                })
            return out

    def save_scheduled_call(self, call: Dict[str, Any]) -> int:
        from sqlalchemy import and_
        with self.SessionFactory() as session:
            if call.get("id"):
                # Update
                row = session.get(models.ScheduledCall, call["id"])
                if row:
                    row.agent_id = call.get("agent_id")
                    row.to_number = call.get("to_number")
                    row.from_number = call.get("from_number")
                    if call.get("scheduled_at"):
                        row.scheduled_at = _to_datetime(call.get("scheduled_at"))
                    if call.get("status"):
                        row.status = call.get("status")
                    if call.get("context") is not None:
                        row.context = call.get("context")
                    session.commit()
                    return row.id
            
            # Check for duplicate: same agent, number, and time (pending only)
            scheduled_at = _to_datetime(call.get("scheduled_at"))
            agent_id = call.get("agent_id")
            to_number = call.get("to_number")
            
            if scheduled_at and agent_id and to_number:
                existing = session.scalars(
                    select(models.ScheduledCall).where(
                        and_(
                            models.ScheduledCall.agent_id == agent_id,
                            models.ScheduledCall.to_number == to_number,
                            models.ScheduledCall.scheduled_at == scheduled_at,
                            models.ScheduledCall.status == "pending",
                        )
                    )
                ).first()
                if existing:
                    # Return existing ID instead of creating duplicate
                    return existing.id
            
            # Insert
            obj = models.ScheduledCall(
                agent_id=agent_id,
                to_number=to_number,
                from_number=call.get("from_number"),
                scheduled_at=scheduled_at,
                status=call.get("status", "pending"),
                context=call.get("context"),
            )
            session.add(obj)
            session.commit()
            return obj.id

    def cleanup_duplicate_scheduled_calls(self) -> int:
        """Remove duplicate pending scheduled calls, keeping only the first one.
        
        Returns the number of duplicates removed.
        """
        from sqlalchemy import and_, func
        removed = 0
        with self.SessionFactory() as session:
            # Find duplicates: same agent_id, to_number, scheduled_at with status=pending
            subq = (
                select(
                    models.ScheduledCall.agent_id,
                    models.ScheduledCall.to_number,
                    models.ScheduledCall.scheduled_at,
                    func.min(models.ScheduledCall.id).label("keep_id"),
                    func.count(models.ScheduledCall.id).label("cnt"),
                )
                .where(models.ScheduledCall.status == "pending")
                .group_by(
                    models.ScheduledCall.agent_id,
                    models.ScheduledCall.to_number,
                    models.ScheduledCall.scheduled_at,
                )
                .having(func.count(models.ScheduledCall.id) > 1)
            ).subquery()
            
            # Get all pending calls that match duplicates but aren't the "keep" one
            dups = session.execute(
                select(models.ScheduledCall.id, subq.c.keep_id)
                .join(
                    subq,
                    and_(
                        models.ScheduledCall.agent_id == subq.c.agent_id,
                        models.ScheduledCall.to_number == subq.c.to_number,
                        models.ScheduledCall.scheduled_at == subq.c.scheduled_at,
                    ),
                )
                .where(
                    and_(
                        models.ScheduledCall.status == "pending",
                        models.ScheduledCall.id != subq.c.keep_id,
                    )
                )
            ).all()
            
            for dup_id, _ in dups:
                row = session.get(models.ScheduledCall, dup_id)
                if row:
                    session.delete(row)
                    removed += 1
            
            session.commit()
        return removed

    def update_scheduled_call_status(self, call_id: int, status: str, sid: str | None = None, error: str | None = None) -> None:
        with self.SessionFactory() as session:
            row = session.get(models.ScheduledCall, call_id)
            if row:
                row.status = status
                if sid:
                    row.call_sid = sid
                if error:
                    row.error_message = error
                session.commit()

    # User Credentials -----------------------------------------------
    def load_user_credentials(self) -> Dict[str, Any]:
        return self._load_kv("config", "user_credentials", {"credentials": {}})

    def save_user_credentials(self, data: Dict[str, Any]) -> None:
        self._save_kv("config", "user_credentials", data)

    def get_user_credential(self, user_id: str, provider: str) -> Dict[str, Any] | None:
        data = self.load_user_credentials()
        user_creds = data.get("credentials", {}).get(user_id, {})
        return user_creds.get(provider)

    def set_user_credential(self, user_id: str, provider: str, cred_data: Dict[str, Any]) -> None:
        data = self.load_user_credentials()
        if "credentials" not in data:
            data["credentials"] = {}
        if user_id not in data["credentials"]:
            data["credentials"][user_id] = {}
        data["credentials"][user_id][provider] = cred_data
        self.save_user_credentials(data)

    def delete_user_credential(self, user_id: str, provider: str) -> bool:
        data = self.load_user_credentials()
        user_creds = data.get("credentials", {}).get(user_id, {})
        if provider in user_creds:
            del data["credentials"][user_id][provider]
            # Clean up empty user entry
            if not data["credentials"][user_id]:
                del data["credentials"][user_id]
            self.save_user_credentials(data)
            return True
        return False

    def get_all_credentials_for_user(self, user_id: str) -> Dict[str, Dict[str, Any]]:
        data = self.load_user_credentials()
        return data.get("credentials", {}).get(user_id, {})

    # Recording Callbacks -----------------------------------------------
    def load_callback_store(self) -> Dict[str, Any]:
        return self._load_kv("config", "callback_store", {"pending_callbacks": {}})

    def save_callback_store(self, data: Dict[str, Any]) -> None:
        self._save_kv("config", "callback_store", data)

    # Callback Results (permanent storage) --------------------------------
    def save_callback_result(
        self,
        call_id: str,
        callback_type: str,
        payload: Dict[str, Any],
        callback_url: str | None = None,
        interview_id: str | None = None,
        http_status: int | None = None,
        http_response: str | None = None,
        success: bool = False,
    ) -> int:
        """Save a callback result for auditing and debugging."""
        from datetime import datetime, timezone
        with self.SessionFactory() as session:
            row = models.CallbackResult(
                call_id=call_id,
                interview_id=interview_id,
                callback_type=callback_type,
                callback_url=callback_url,
                payload=payload,
                http_status=http_status,
                http_response=http_response[:2048] if http_response else None,
                sent_at=datetime.now(timezone.utc),
                success=success,
            )
            session.add(row)
            session.commit()
            return row.id

    def get_callback_results(
        self,
        call_id: str | None = None,
        interview_id: str | None = None,
        callback_type: str | None = None,
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        """Retrieve callback results for debugging/auditing."""
        with self.SessionFactory() as session:
            stmt = select(models.CallbackResult).order_by(models.CallbackResult.sent_at.desc())
            if call_id:
                stmt = stmt.where(models.CallbackResult.call_id == call_id)
            if interview_id:
                stmt = stmt.where(models.CallbackResult.interview_id == interview_id)
            if callback_type:
                stmt = stmt.where(models.CallbackResult.callback_type == callback_type)
            stmt = stmt.limit(limit)
            rows = session.scalars(stmt).all()
            return [
                {
                    "id": r.id,
                    "call_id": r.call_id,
                    "interview_id": r.interview_id,
                    "callback_type": r.callback_type,
                    "callback_url": r.callback_url,
                    "payload": r.payload,
                    "http_status": r.http_status,
                    "http_response": r.http_response,
                    "sent_at": r.sent_at.isoformat() if r.sent_at else None,
                    "success": r.success,
                }
                for r in rows
            ]

    def update_callback_result(
        self,
        result_id: int,
        http_status: int | None = None,
        http_response: str | None = None,
        success: bool | None = None,
    ) -> None:
        """Update a callback result after sending (to record response)."""
        with self.SessionFactory() as session:
            row = session.get(models.CallbackResult, result_id)
            if row:
                if http_status is not None:
                    row.http_status = http_status
                if http_response is not None:
                    row.http_response = http_response[:2048]
                if success is not None:
                    row.success = success
                session.commit()

    # Agent Integrations / Workflows / Memory Config -------------------
    # These use a generic JSON key-value pattern backed by a single table.
    # For Postgres, we store them as JSONB in the agent_metadata table.

    def _get_agent_meta(self, agent_id: str, key: str, default=None):
        """Generic helper: read a JSON blob from agent_metadata."""
        import json as _json
        with self.SessionFactory() as session:
            row = session.execute(
                _sa_text("SELECT value FROM agent_metadata WHERE agent_id = :aid AND key = :k"),
                {"aid": agent_id, "k": key},
            ).first()
            if row:
                val = row[0]
                return _json.loads(val) if isinstance(val, str) else val
        return default

    def _set_agent_meta(self, agent_id: str, key: str, value):
        """Generic helper: upsert a JSON blob into agent_metadata."""
        import json as _json
        val_str = _json.dumps(value)
        with self.SessionFactory() as session:
            session.execute(
                _sa_text(
                    "INSERT INTO agent_metadata (agent_id, key, value) VALUES (:aid, :k, CAST(:v AS JSONB)) "
                    "ON CONFLICT (agent_id, key) DO UPDATE SET value = CAST(:v AS JSONB), updated_at = NOW()"
                ),
                {"aid": agent_id, "k": key, "v": val_str},
            )
            session.commit()

    def load_agent_integrations(self, agent_id: str) -> list:
        return self._get_agent_meta(agent_id, "integrations", [])

    def save_agent_integrations(self, agent_id: str, data: list) -> None:
        self._set_agent_meta(agent_id, "integrations", data)

    def load_agent_workflow(self, agent_id: str) -> dict:
        return self._get_agent_meta(agent_id, "workflow", {})

    def save_agent_workflow(self, agent_id: str, data: dict) -> None:
        self._set_agent_meta(agent_id, "workflow", data)

    def load_agent_memory_config(self, agent_id: str) -> dict:
        return self._get_agent_meta(agent_id, "memory_config",
                                     {"enabled": False, "provider": "mem0", "config": {}})

    def save_agent_memory_config(self, agent_id: str, data: dict) -> None:
        self._set_agent_meta(agent_id, "memory_config", data)

