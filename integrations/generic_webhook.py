"""Generic HTTP Webhook integration."""

import json
from .base import (
    IntegrationBase, IntegrationCredentialField, IntegrationAction,
    ActionParameter, _http_request,
)
from . import register


@register
class GenericWebhookIntegration(IntegrationBase):

    @property
    def id(self) -> str:
        return "generic_webhook"

    @property
    def name(self) -> str:
        return "Webhook"

    @property
    def description(self) -> str:
        return "Send data to any HTTP endpoint. Use this to connect to custom APIs, Zapier, Make, or n8n."

    @property
    def category(self) -> str:
        return "webhook"

    @property
    def credential_fields(self) -> list:
        return [
            IntegrationCredentialField(
                name="webhook_url",
                label="Webhook URL",
                field_type="url",
                required=True,
                placeholder="https://hooks.example.com/your-endpoint",
                help_text="The URL that will receive POST requests with event data.",
            ),
            IntegrationCredentialField(
                name="auth_header",
                label="Authorization Header",
                field_type="password",
                required=False,
                placeholder="Bearer your-token",
                help_text="Optional. Sent as the Authorization header on every request.",
            ),
        ]

    @property
    def actions(self) -> list:
        return [
            IntegrationAction(
                name="send_webhook",
                description=(
                    "Send data to the configured webhook URL. "
                    "Use this to notify external systems about events or pass collected information."
                ),
                parameters=[
                    ActionParameter(name="event_type", param_type="string",
                                    description="Short label for the event, e.g. 'appointment_booked', 'lead_captured'."),
                    ActionParameter(name="data", param_type="string",
                                    description="JSON string containing the event payload to send."),
                ],
            ),
        ]

    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        if action_name != "send_webhook":
            return json.dumps({"error": f"Unknown action: {action_name}"})

        url = credentials.get("webhook_url", "").strip()
        if not url:
            return json.dumps({"error": "No webhook URL configured."})

        headers = {"Content-Type": "application/json"}
        auth = credentials.get("auth_header", "").strip()
        if auth:
            headers["Authorization"] = auth

        try:
            payload_data = json.loads(params.get("data", "{}"))
        except (json.JSONDecodeError, TypeError):
            payload_data = {"raw": params.get("data", "")}

        body = {
            "event": params.get("event_type", "agent_event"),
            "data": payload_data,
        }

        resp = await _http_request("POST", url, headers=headers, json_body=body)
        if resp["ok"]:
            return json.dumps({"success": True, "status": resp["status"]})
        return json.dumps({"success": False, "status": resp["status"], "detail": resp["data"]})

    async def test_credentials(self, credentials: dict) -> dict:
        url = credentials.get("webhook_url", "").strip()
        if not url:
            return {"ok": False, "message": "Webhook URL is required."}
        try:
            resp = await _http_request("POST", url, json_body={"test": True}, timeout=10)
            return {"ok": resp["ok"], "message": f"HTTP {resp['status']}"}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
