"""Twilio SMS integration.

Uses existing Twilio credentials (shared with SIP trunking) or per-integration overrides.
Twilio REST API: https://www.twilio.com/docs/messaging/api
"""

import json
import base64
from .base import (
    IntegrationBase, IntegrationCredentialField, IntegrationAction,
    ActionParameter, _http_request,
)
from . import register


@register
class TwilioSMSIntegration(IntegrationBase):

    @property
    def id(self) -> str:
        return "twilio_sms"

    @property
    def name(self) -> str:
        return "Twilio SMS"

    @property
    def description(self) -> str:
        return "Send SMS messages via Twilio. Useful for confirmations, reminders, and follow-ups."

    @property
    def category(self) -> str:
        return "sms"

    @property
    def credential_fields(self) -> list:
        return [
            IntegrationCredentialField(
                name="account_sid",
                label="Account SID",
                field_type="text",
                required=True,
                placeholder="ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                help_text="Found on your Twilio Console dashboard.",
            ),
            IntegrationCredentialField(
                name="auth_token",
                label="Auth Token",
                field_type="password",
                required=True,
                placeholder="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            ),
            IntegrationCredentialField(
                name="from_number",
                label="From Phone Number",
                field_type="text",
                required=True,
                placeholder="+15551234567",
                help_text="A Twilio phone number or Messaging Service SID.",
            ),
        ]

    @property
    def actions(self) -> list:
        return [
            IntegrationAction(
                name="send_sms",
                description="Send an SMS text message to a phone number.",
                parameters=[
                    ActionParameter(name="to_number", param_type="string",
                                    description="Recipient phone number in E.164 format, e.g. '+15551234567'."),
                    ActionParameter(name="message", param_type="string",
                                    description="The text message body to send."),
                ],
            ),
        ]

    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        if action_name != "send_sms":
            return json.dumps({"error": f"Unknown action: {action_name}"})

        sid = credentials.get("account_sid", "").strip()
        token = credentials.get("auth_token", "").strip()
        from_num = credentials.get("from_number", "").strip()

        if not all([sid, token, from_num]):
            return json.dumps({"error": "Twilio account_sid, auth_token, and from_number are required."})

        to_num = params.get("to_number", "").strip()
        body = params.get("message", "")
        if not to_num or not body:
            return json.dumps({"error": "to_number and message are required."})

        url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
        auth_str = base64.b64encode(f"{sid}:{token}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth_str}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        import aiohttp
        async with aiohttp.ClientSession() as session:
            form_data = aiohttp.FormData()
            form_data.add_field("To", to_num)
            form_data.add_field("From", from_num)
            form_data.add_field("Body", body)

            async with session.post(url, data=form_data,
                                    headers={"Authorization": f"Basic {auth_str}"},
                                    timeout=aiohttp.ClientTimeout(total=15)) as resp:
                result = await resp.json()
                if resp.status in (200, 201):
                    return json.dumps({
                        "success": True,
                        "message_sid": result.get("sid", ""),
                        "message": f"SMS sent to {to_num}.",
                    })
                return json.dumps({
                    "success": False,
                    "error": f"Twilio API returned HTTP {resp.status}",
                    "detail": result.get("message", str(result)),
                })

    async def test_credentials(self, credentials: dict) -> dict:
        sid = credentials.get("account_sid", "").strip()
        token = credentials.get("auth_token", "").strip()
        if not sid or not token:
            return {"ok": False, "message": "Account SID and Auth Token are required."}
        url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}.json"
        auth_str = base64.b64encode(f"{sid}:{token}".encode()).decode()
        try:
            resp = await _http_request("GET", url,
                                       headers={"Authorization": f"Basic {auth_str}"}, timeout=10)
            if resp["ok"]:
                return {"ok": True, "message": "Twilio credentials are valid."}
            return {"ok": False, "message": f"HTTP {resp['status']}"}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
