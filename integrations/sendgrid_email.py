"""SendGrid email integration.

SendGrid API docs: https://www.twilio.com/docs/sendgrid/api-reference
Authentication: Bearer token via API key.
Free tier: 100 emails/day.
"""

import json
from .base import (
    IntegrationBase, IntegrationCredentialField, IntegrationAction,
    ActionParameter, _http_request,
)
from . import register

SENDGRID_API_BASE = "https://api.sendgrid.com/v3"


@register
class SendGridIntegration(IntegrationBase):

    @property
    def id(self) -> str:
        return "sendgrid"

    @property
    def name(self) -> str:
        return "SendGrid"

    @property
    def description(self) -> str:
        return "Send emails via SendGrid. Useful for confirmations, follow-ups, and notifications."

    @property
    def category(self) -> str:
        return "email"

    @property
    def credential_fields(self) -> list:
        return [
            IntegrationCredentialField(
                name="api_key",
                label="SendGrid API Key",
                field_type="password",
                required=True,
                placeholder="SG.xxxx...",
                help_text="Create at Settings > API Keys in the SendGrid dashboard.",
            ),
            IntegrationCredentialField(
                name="from_email",
                label="From Email",
                field_type="text",
                required=True,
                placeholder="noreply@yourdomain.com",
                help_text="Verified sender email address.",
            ),
            IntegrationCredentialField(
                name="from_name",
                label="From Name",
                field_type="text",
                required=False,
                placeholder="Your Company",
                help_text="Display name for the sender.",
            ),
        ]

    @property
    def actions(self) -> list:
        return [
            IntegrationAction(
                name="send_email",
                description=(
                    "Send an email to the specified recipient. "
                    "Use this for confirmations, follow-ups, or delivering requested information."
                ),
                parameters=[
                    ActionParameter(name="to_email", param_type="string",
                                    description="Recipient email address."),
                    ActionParameter(name="subject", param_type="string",
                                    description="Email subject line."),
                    ActionParameter(name="body", param_type="string",
                                    description="Plain-text email body content."),
                    ActionParameter(name="to_name", param_type="string",
                                    description="Recipient display name.", required=False),
                ],
            ),
        ]

    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        if action_name != "send_email":
            return json.dumps({"error": f"Unknown action: {action_name}"})

        api_key = credentials.get("api_key", "").strip()
        from_email = credentials.get("from_email", "").strip()
        from_name = credentials.get("from_name", "").strip()

        if not api_key or not from_email:
            return json.dumps({"error": "SendGrid API key and from_email are required."})

        to_email = params.get("to_email", "").strip()
        subject = params.get("subject", "")
        body_text = params.get("body", "")

        if not to_email:
            return json.dumps({"error": "Recipient email (to_email) is required."})

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        sender = {"email": from_email}
        if from_name:
            sender["name"] = from_name

        recipient = {"email": to_email}
        if params.get("to_name"):
            recipient["name"] = params["to_name"]

        payload = {
            "personalizations": [{"to": [recipient]}],
            "from": sender,
            "subject": subject,
            "content": [{"type": "text/plain", "value": body_text}],
        }

        resp = await _http_request("POST", f"{SENDGRID_API_BASE}/mail/send",
                                   headers=headers, json_body=payload)

        if resp["status"] in (200, 201, 202):
            return json.dumps({
                "success": True,
                "message": f"Email sent to {to_email} with subject '{subject}'.",
            })
        return json.dumps({
            "success": False,
            "error": f"SendGrid API returned HTTP {resp['status']}",
            "detail": resp["data"],
        })

    async def test_credentials(self, credentials: dict) -> dict:
        api_key = credentials.get("api_key", "").strip()
        if not api_key:
            return {"ok": False, "message": "API key is required."}

        headers = {"Authorization": f"Bearer {api_key}"}
        try:
            resp = await _http_request("GET", f"{SENDGRID_API_BASE}/user/profile",
                                       headers=headers, timeout=10)
            if resp["ok"]:
                return {"ok": True, "message": "API key is valid."}
            return {"ok": False, "message": f"HTTP {resp['status']}"}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
