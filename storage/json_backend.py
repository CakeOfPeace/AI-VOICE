"""File-based storage backend (legacy JSON files)."""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Dict

from .base import StorageBackend


def _default_agents() -> Dict[str, Any]:
    return {"agents": [], "active_agent_id": None}


class JsonStorageBackend(StorageBackend):
    def __init__(self, base_path: Path):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._livekit_lock = threading.RLock()

    # Helpers --------------------------------------------------------
    def _path(self, filename: str) -> Path:
        return self.base_path / filename

    def _read(self, filename: str, default: Dict[str, Any]) -> Dict[str, Any]:
        path = self._path(filename)
        if not path.exists():
            path.write_text(json.dumps(default, indent=2), encoding="utf-8")
            return json.loads(json.dumps(default))
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return json.loads(json.dumps(default))

    def _write(self, filename: str, data: Dict[str, Any]) -> None:
        path = self._path(filename)
        path.write_text(json.dumps(data or {}, indent=2), encoding="utf-8")

    # Agents ---------------------------------------------------------
    def load_agents(self) -> Dict[str, Any]:
        return self._read("agents.json", _default_agents())

    def save_agents(self, data: Dict[str, Any]) -> None:
        self._write("agents.json", data)

    def load_active_agent(self) -> Dict[str, Any]:
        return self._read("active.json", {})

    def save_active_agent(self, data: Dict[str, Any]) -> None:
        self._write("active.json", data)

    def load_users(self) -> Dict[str, Any]:
        return self._read("users.json", {"users": []})

    def save_users(self, data: Dict[str, Any]) -> None:
        self._write("users.json", data)

    def load_sessions(self) -> Dict[str, Any]:
        return self._read("sessions.json", {"sessions": {}, "resets": {}})

    def save_sessions(self, data: Dict[str, Any]) -> None:
        self._write("sessions.json", data)

    # Config ---------------------------------------------------------
    def load_numbers(self) -> Dict[str, Any]:
        return self._read("numbers.json", {"routes": {}})

    def save_numbers(self, data: Dict[str, Any]) -> None:
        self._write("numbers.json", data)

    def load_twilio_store(self) -> Dict[str, Any]:
        return self._read("twilio_accounts.json", {"accounts": [], "numbers": {}})

    def save_twilio_store(self, data: Dict[str, Any]) -> None:
        self._write("twilio_accounts.json", data)

    def load_defaults(self) -> Dict[str, Any]:
        return self._read("default.json", {"llm": {}, "stt": {}, "tts": {}, "realtime": {}, "openai": {}, "env": {}})

    def save_defaults(self, data: Dict[str, Any]) -> None:
        self._write("default.json", data)

    def load_voices(self) -> Dict[str, Any]:
        return self._read("voices.json", {"voices": []})

    def save_voices(self, data: Dict[str, Any]) -> None:
        self._write("voices.json", data)

    def load_usage(self) -> Dict[str, Any]:
        return self._read("usage.json", {"usage": {}})

    def save_usage(self, data: Dict[str, Any]) -> None:
        self._write("usage.json", data)

    def load_subscriptions(self) -> Dict[str, Any]:
        return self._read("subscriptions.json", {"subs": []})

    def save_subscriptions(self, data: Dict[str, Any]) -> None:
        self._write("subscriptions.json", data)

    # LiveKit --------------------------------------------------------
    def load_livekit_store(self) -> Dict[str, Any]:
        return self._read("livekit_store.json", {"calls": {}})

    def save_livekit_store(self, data: Dict[str, Any]) -> None:
        self._write("livekit_store.json", data)

    def mutate_livekit_store(self, mutate_fn):
        with self._livekit_lock:
            store = self.load_livekit_store()
            result = mutate_fn(store)
            if result is not None:
                store = result
            self.save_livekit_store(store)
            return store

    def load_recordings_index(self) -> Dict[str, Any]:
        return self._read("recordings_index.json", {"items": [], "last_scan_ts": 0, "scan_cursor": None, "total": 0})

    def save_recordings_index(self, data: Dict[str, Any]) -> None:
        self._write("recordings_index.json", data)

    # Outbound Calling -----------------------------------------------
    def list_scheduled_calls(self, status: str | None = None) -> list[Dict[str, Any]]:
        data = self._read("scheduled_calls.json", {"calls": []})
        calls = data.get("calls", [])
        if status:
            return [c for c in calls if c.get("status") == status]
        return calls

    def save_scheduled_call(self, call: Dict[str, Any]) -> int:
        data = self._read("scheduled_calls.json", {"calls": []})
        calls = data.get("calls", [])
        
        if not call.get("id"):
            # Check for duplicate: same agent, number, and time (pending only)
            agent_id = call.get("agent_id")
            to_number = call.get("to_number")
            scheduled_at = call.get("scheduled_at")
            # Normalize scheduled_at to string for comparison
            if hasattr(scheduled_at, 'isoformat'):
                scheduled_at_str = scheduled_at.isoformat()
            else:
                scheduled_at_str = str(scheduled_at) if scheduled_at else None
            
            for c in calls:
                if (c.get("status") == "pending" and 
                    c.get("agent_id") == agent_id and
                    c.get("to_number") == to_number):
                    # Compare times (normalize both)
                    c_time = c.get("scheduled_at")
                    if hasattr(c_time, 'isoformat'):
                        c_time_str = c_time.isoformat()
                    else:
                        c_time_str = str(c_time) if c_time else None
                    if c_time_str == scheduled_at_str:
                        # Duplicate found, return existing ID
                        return c["id"]
            
            # Generate simple ID
            max_id = 0
            for c in calls:
                if isinstance(c.get("id"), int) and c["id"] > max_id:
                    max_id = c["id"]
            call["id"] = max_id + 1
            call["created_at"] = int(time.time())
            # Convert datetime to string for JSON storage
            if hasattr(call.get("scheduled_at"), 'isoformat'):
                call["scheduled_at"] = call["scheduled_at"].isoformat()
            calls.append(call)
        else:
            # Update existing
            for i, c in enumerate(calls):
                if c.get("id") == call["id"]:
                    calls[i] = call
                    break
            else:
                calls.append(call)
        
        self._write("scheduled_calls.json", {"calls": calls})
        return call["id"]

    def update_scheduled_call_status(self, call_id: int, status: str, sid: str | None = None, error: str | None = None) -> None:
        data = self._read("scheduled_calls.json", {"calls": []})
        calls = data.get("calls", [])
        found = False
        for c in calls:
            if c.get("id") == call_id:
                c["status"] = status
                if sid:
                    c["call_sid"] = sid
                if error:
                    c["error_message"] = error
                found = True
                break
        if found:
            self._write("scheduled_calls.json", {"calls": calls})

    def cleanup_duplicate_scheduled_calls(self) -> int:
        """Remove duplicate pending scheduled calls, keeping only the first one."""
        data = self._read("scheduled_calls.json", {"calls": []})
        calls = data.get("calls", [])
        
        # Group by (agent_id, to_number, scheduled_at)
        seen = {}  # key -> first call id
        to_remove = []
        
        for c in calls:
            if c.get("status") != "pending":
                continue
            key = (c.get("agent_id"), c.get("to_number"), str(c.get("scheduled_at")))
            if key in seen:
                to_remove.append(c.get("id"))
            else:
                seen[key] = c.get("id")
        
        if to_remove:
            calls = [c for c in calls if c.get("id") not in to_remove]
            self._write("scheduled_calls.json", {"calls": calls})
        
        return len(to_remove)

    # User Credentials -----------------------------------------------
    def load_user_credentials(self) -> Dict[str, Any]:
        return self._read("user_credentials.json", {"credentials": {}})

    def save_user_credentials(self, data: Dict[str, Any]) -> None:
        self._write("user_credentials.json", data)

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
        return self._read("callback_store.json", {"pending_callbacks": {}})

    def save_callback_store(self, data: Dict[str, Any]) -> None:
        self._write("callback_store.json", data)

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
        data = self._read("callback_results.json", {"results": [], "next_id": 1})
        result_id = data.get("next_id", 1)
        data["results"].append({
            "id": result_id,
            "call_id": call_id,
            "interview_id": interview_id,
            "callback_type": callback_type,
            "callback_url": callback_url,
            "payload": payload,
            "http_status": http_status,
            "http_response": http_response[:2048] if http_response else None,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "success": success,
        })
        data["next_id"] = result_id + 1
        self._write("callback_results.json", data)
        return result_id

    def get_callback_results(
        self,
        call_id: str | None = None,
        interview_id: str | None = None,
        callback_type: str | None = None,
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        """Retrieve callback results for debugging/auditing."""
        data = self._read("callback_results.json", {"results": []})
        results = data.get("results", [])
        # Filter
        if call_id:
            results = [r for r in results if r.get("call_id") == call_id]
        if interview_id:
            results = [r for r in results if r.get("interview_id") == interview_id]
        if callback_type:
            results = [r for r in results if r.get("callback_type") == callback_type]
        # Sort by sent_at desc, limit
        results.sort(key=lambda x: x.get("sent_at", ""), reverse=True)
        return results[:limit]

    def update_callback_result(
        self,
        result_id: int,
        http_status: int | None = None,
        http_response: str | None = None,
        success: bool | None = None,
    ) -> None:
        """Update a callback result after sending (to record response)."""
        data = self._read("callback_results.json", {"results": []})
        for r in data.get("results", []):
            if r.get("id") == result_id:
                if http_status is not None:
                    r["http_status"] = http_status
                if http_response is not None:
                    r["http_response"] = http_response[:2048]
                if success is not None:
                    r["success"] = success
                break
        self._write("callback_results.json", data)

    # Agent Integrations -----------------------------------------------
    def _integrations_file(self, agent_id: str) -> str:
        return f"integrations_{agent_id}.json"

    def load_agent_integrations(self, agent_id: str) -> list:
        data = self._read(self._integrations_file(agent_id), {"integrations": []})
        return data.get("integrations", [])

    def save_agent_integrations(self, agent_id: str, data: list) -> None:
        self._write(self._integrations_file(agent_id), {"integrations": data})

    # Agent Workflows --------------------------------------------------
    def _workflow_file(self, agent_id: str) -> str:
        return f"workflow_{agent_id}.json"

    def load_agent_workflow(self, agent_id: str) -> Dict[str, Any]:
        return self._read(self._workflow_file(agent_id), {})

    def save_agent_workflow(self, agent_id: str, data: Dict[str, Any]) -> None:
        self._write(self._workflow_file(agent_id), data)

    # Agent Memory Config ----------------------------------------------
    def _memory_config_file(self, agent_id: str) -> str:
        return f"memory_{agent_id}.json"

    def load_agent_memory_config(self, agent_id: str) -> Dict[str, Any]:
        return self._read(self._memory_config_file(agent_id),
                          {"enabled": False, "provider": "mem0", "config": {}})

    def save_agent_memory_config(self, agent_id: str, data: Dict[str, Any]) -> None:
        self._write(self._memory_config_file(agent_id), data)

