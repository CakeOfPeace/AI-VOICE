"""Slack incoming webhook integration.

No OAuth or approval needed -- just an Incoming Webhook URL from Slack.
Docs: https://api.slack.com/messaging/webhooks
"""

import json
from .base import (
    IntegrationBase, IntegrationCredentialField, IntegrationAction,
    ActionParameter, _http_request,
)
from . import register


@register
class SlackWebhookIntegration(IntegrationBase):

    @property
    def id(self) -> str:
        return "slack"

    @property
    def name(self) -> str:
        return "Slack"

    @property
    def description(self) -> str:
        return "Send notifications to a Slack channel via Incoming Webhook."

    @property
    def category(self) -> str:
        return "notification"

    @property
    def credential_fields(self) -> list:
        return [
            IntegrationCredentialField(
                name="webhook_url",
                label="Slack Webhook URL",
                field_type="url",
                required=True,
                placeholder="https://hooks.slack.com/services/T.../B.../xxx",
                help_text="Create at https://api.slack.com/apps > Incoming Webhooks.",
            ),
        ]

    @property
    def actions(self) -> list:
        return [
            IntegrationAction(
                name="send_slack_message",
                description="Send a message to the configured Slack channel.",
                parameters=[
                    ActionParameter(name="text", param_type="string",
                                    description="The message text to send to Slack."),
                ],
            ),
        ]

    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        if action_name != "send_slack_message":
            return json.dumps({"error": f"Unknown action: {action_name}"})

        url = credentials.get("webhook_url", "").strip()
        if not url:
            return json.dumps({"error": "Slack webhook URL is required."})

        text = params.get("text", "")
        if not text:
            return json.dumps({"error": "Message text is required."})

        resp = await _http_request("POST", url, json_body={"text": text})
        if resp["ok"]:
            return json.dumps({"success": True, "message": "Message sent to Slack."})
        return json.dumps({"success": False, "error": f"Slack returned HTTP {resp['status']}"})

    async def test_credentials(self, credentials: dict) -> dict:
        url = credentials.get("webhook_url", "").strip()
        if not url:
            return {"ok": False, "message": "Webhook URL is required."}
        if "hooks.slack.com" not in url and "hooks.slack-gov.com" not in url:
            return {"ok": False, "message": "URL does not look like a Slack webhook."}
        return {"ok": True, "message": "URL format is valid."}
