"""
Abstract base class for all integrations.

Each integration defines:
- Metadata (id, name, description, category, icon)
- Required credential fields
- Available actions (which become agent function_tools)
- Action execution logic
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any
import logging
import json

logger = logging.getLogger("integrations")


@dataclass
class IntegrationCredentialField:
    """Defines a single credential field the user must provide."""
    name: str
    label: str
    field_type: str = "text"  # "text", "password", "url", "oauth"
    required: bool = True
    placeholder: str = ""
    help_text: str = ""


@dataclass
class ActionParameter:
    """Defines a single parameter for an integration action."""
    name: str
    param_type: str  # "string", "number", "boolean", "object", "array"
    description: str
    required: bool = True
    enum: list = field(default_factory=list)


@dataclass
class IntegrationAction:
    """Defines one callable action exposed by an integration."""
    name: str
    description: str
    parameters: list  # list[ActionParameter]

    def to_json_schema(self) -> dict:
        """Convert parameters to JSON Schema for LLM tool definition."""
        properties = {}
        required = []
        for p in self.parameters:
            prop = {"type": p.param_type, "description": p.description}
            if p.enum:
                prop["enum"] = p.enum
            properties[p.name] = prop
            if p.required:
                required.append(p.name)
        return {
            "type": "object",
            "properties": properties,
            "required": required,
        }


class IntegrationBase(ABC):
    """Base class every integration must extend."""

    @property
    @abstractmethod
    def id(self) -> str:
        """Unique slug, e.g. 'cal_com', 'sendgrid'."""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name, e.g. 'Cal.com'."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Short description shown in catalog."""
        ...

    @property
    @abstractmethod
    def category(self) -> str:
        """Category: 'calendar', 'email', 'sms', 'crm', 'data', 'notification', 'webhook'."""
        ...

    @property
    def icon(self) -> str:
        """Optional icon name (matches frontend icon set)."""
        return self.id

    @property
    @abstractmethod
    def credential_fields(self) -> list:
        """List of IntegrationCredentialField the user must provide."""
        ...

    @property
    @abstractmethod
    def actions(self) -> list:
        """List of IntegrationAction this integration exposes."""
        ...

    @abstractmethod
    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        """Run a specific action. Returns a JSON string the LLM can reason about."""
        ...

    async def test_credentials(self, credentials: dict) -> dict:
        """Validate that the provided credentials work.
        
        Returns: {"ok": True/False, "message": "..."}
        Default implementation always returns ok; override for real checks.
        """
        return {"ok": True, "message": "Credentials accepted (not verified)."}

    # ------------------------------------------------------------------
    # Tool generation
    # ------------------------------------------------------------------

    def to_catalog_entry(self) -> dict:
        """Serialize metadata for the frontend integration catalog."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "icon": self.icon,
            "credential_fields": [
                {
                    "name": f.name,
                    "label": f.label,
                    "field_type": f.field_type,
                    "required": f.required,
                    "placeholder": f.placeholder,
                    "help_text": f.help_text,
                }
                for f in self.credential_fields
            ],
            "actions": [
                {
                    "name": a.name,
                    "description": a.description,
                    "parameters": a.to_json_schema(),
                }
                for a in self.actions
            ],
        }

    def to_function_tools(self, credentials: dict) -> list:
        """Generate LiveKit function_tools backed by this integration."""
        tools = []
        for action in self.actions:
            tool_fn = self._make_tool_fn(action, credentials)
            tools.append(tool_fn)
        return tools

    def _make_tool_fn(self, action: IntegrationAction, credentials: dict):
        """Create a single function_tool for an action using raw_schema.

        raw_schema gives the LLM the full parameter definitions (names, types,
        descriptions, required list) so it knows exactly what arguments to pass.
        """
        from livekit.agents import function_tool

        integration = self
        tool_name = f"{self.id}_{action.name}"

        async def _execute(**kwargs):
            logger.info(f"[{integration.id}] Executing action '{action.name}' with params: {list(kwargs.keys())}")
            try:
                result = await integration.execute_action(action.name, credentials, kwargs)
                logger.info(f"[{integration.id}] Action '{action.name}' completed")
                return result
            except Exception as exc:
                logger.error(f"[{integration.id}] Action '{action.name}' failed: {exc}")
                return json.dumps({"error": str(exc), "integration": integration.id, "action": action.name})

        _execute.__name__ = tool_name

        schema = {
            "name": tool_name,
            "description": action.description,
            "parameters": action.to_json_schema(),
        }

        return function_tool(raw_schema=schema)(_execute)


async def _http_request(method: str, url: str, headers: dict = None,
                        json_body: dict = None, params: dict = None,
                        timeout: int = 15) -> dict:
    """Shared async HTTP helper used by integration implementations."""
    import aiohttp
    async with aiohttp.ClientSession() as session:
        kwargs = {"headers": headers or {}, "timeout": aiohttp.ClientTimeout(total=timeout)}
        if json_body is not None:
            kwargs["json"] = json_body
        if params is not None:
            kwargs["params"] = params

        async with session.request(method, url, **kwargs) as resp:
            body = await resp.text()
            try:
                data = json.loads(body)
            except (json.JSONDecodeError, ValueError):
                data = {"raw": body}
            return {"status": resp.status, "ok": resp.ok, "data": data}
