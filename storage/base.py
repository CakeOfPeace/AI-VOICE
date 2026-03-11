"""Abstract storage backend interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Callable


class StorageBackend(ABC):
    """Interface implemented by JSON and Postgres backends."""

    # Agents / users -------------------------------------------------
    @abstractmethod
    def load_agents(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_agents(self, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def load_active_agent(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_active_agent(self, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def load_users(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_users(self, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def load_sessions(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_sessions(self, data: Dict[str, Any]) -> None:
        ...

    # Config ---------------------------------------------------------
    @abstractmethod
    def load_numbers(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_numbers(self, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def load_twilio_store(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_twilio_store(self, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def load_defaults(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_defaults(self, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def load_voices(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_voices(self, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def load_usage(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_usage(self, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def load_subscriptions(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_subscriptions(self, data: Dict[str, Any]) -> None:
        ...

    # LiveKit --------------------------------------------------------
    @abstractmethod
    def load_livekit_store(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_livekit_store(self, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def load_recordings_index(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_recordings_index(self, data: Dict[str, Any]) -> None:
        ...

    @abstractmethod
    def mutate_livekit_store(self, mutate_fn: Callable[[Dict[str, Any]], Dict[str, Any] | None]) -> Dict[str, Any]:
        ...

    # Outbound Calling -----------------------------------------------
    @abstractmethod
    def list_scheduled_calls(self, status: str | None = None) -> list[Dict[str, Any]]:
        ...

    @abstractmethod
    def save_scheduled_call(self, call: Dict[str, Any]) -> int:
        """Save or update a scheduled call. Returns the ID."""
        ...

    @abstractmethod
    def update_scheduled_call_status(self, call_id: int, status: str, sid: str | None = None, error: str | None = None) -> None:
        ...

    @abstractmethod
    def cleanup_duplicate_scheduled_calls(self) -> int:
        """Remove duplicate pending scheduled calls, keeping only the first one.
        
        Returns the number of duplicates removed.
        """
        ...

    # User Credentials -----------------------------------------------
    @abstractmethod
    def load_user_credentials(self) -> Dict[str, Any]:
        """Load all user credentials (encrypted)."""
        ...

    @abstractmethod
    def save_user_credentials(self, data: Dict[str, Any]) -> None:
        """Save all user credentials (encrypted)."""
        ...

    @abstractmethod
    def get_user_credential(self, user_id: str, provider: str) -> Dict[str, Any] | None:
        """Get a specific user's credential for a provider (encrypted)."""
        ...

    @abstractmethod
    def set_user_credential(self, user_id: str, provider: str, data: Dict[str, Any]) -> None:
        """Set a specific user's credential for a provider (encrypted)."""
        ...

    @abstractmethod
    def delete_user_credential(self, user_id: str, provider: str) -> bool:
        """Delete a specific user's credential for a provider. Returns True if deleted."""
        ...

    @abstractmethod
    def get_all_credentials_for_user(self, user_id: str) -> Dict[str, Dict[str, Any]]:
        """Get all credentials for a specific user (encrypted). Returns {provider: {data}}."""
        ...

    # Recording Callbacks -----------------------------------------------
    @abstractmethod
    def load_callback_store(self) -> Dict[str, Any]:
        """Load recording callback registrations."""
        ...

    @abstractmethod
    def save_callback_store(self, data: Dict[str, Any]) -> None:
        """Save recording callback registrations."""
        ...

    # Callback Results (permanent storage) --------------------------------
    @abstractmethod
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
        """Save a callback result for auditing and debugging.
        
        Args:
            call_id: The call identifier
            callback_type: 'questionnaire' or 'recording_ready'
            payload: The full callback payload that was/will be sent
            callback_url: The target URL
            interview_id: External system's identifier
            http_status: HTTP response status code (if sent)
            http_response: HTTP response body truncated (if sent)
            success: Whether the callback was acknowledged (2xx response)
            
        Returns:
            The ID of the saved callback result
        """
        ...

    @abstractmethod
    def get_callback_results(
        self,
        call_id: str | None = None,
        interview_id: str | None = None,
        callback_type: str | None = None,
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        """Retrieve callback results for debugging/auditing.
        
        Args:
            call_id: Filter by call ID
            interview_id: Filter by external interview ID
            callback_type: Filter by type ('questionnaire' or 'recording_ready')
            limit: Maximum results to return
            
        Returns:
            List of callback result records
        """
        ...

    @abstractmethod
    def update_callback_result(
        self,
        result_id: int,
        http_status: int | None = None,
        http_response: str | None = None,
        success: bool | None = None,
    ) -> None:
        """Update a callback result after sending (to record response).
        
        Args:
            result_id: The callback result ID
            http_status: HTTP response status code
            http_response: HTTP response body (truncated)
            success: Whether the callback was acknowledged
        """
        ...

    # Agent Integrations -----------------------------------------------
    @abstractmethod
    def load_agent_integrations(self, agent_id: str) -> list:
        """Load enabled integrations for an agent.
        
        Returns list of dicts:
        [{"integration_id": "cal_com", "credentials": {...}, "actions": [...]}]
        """
        ...

    @abstractmethod
    def save_agent_integrations(self, agent_id: str, data: list) -> None:
        """Save enabled integrations for an agent."""
        ...

    # Agent Workflows --------------------------------------------------
    @abstractmethod
    def load_agent_workflow(self, agent_id: str) -> Dict[str, Any]:
        """Load workflow definition for an agent.
        
        Returns dict with 'nodes' and 'edges' keys, or empty dict.
        """
        ...

    @abstractmethod
    def save_agent_workflow(self, agent_id: str, data: Dict[str, Any]) -> None:
        """Save workflow definition for an agent."""
        ...

    # Agent Memory Config ----------------------------------------------
    @abstractmethod
    def load_agent_memory_config(self, agent_id: str) -> Dict[str, Any]:
        """Load memory configuration for an agent.
        
        Returns dict like {"enabled": True, "provider": "mem0", "config": {}}.
        """
        ...

    @abstractmethod
    def save_agent_memory_config(self, agent_id: str, data: Dict[str, Any]) -> None:
        """Save memory configuration for an agent."""
        ...

