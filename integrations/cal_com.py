"""Cal.com scheduling integration.

Cal.com API v2 docs: https://cal.com/docs/api-reference/v2
Authentication: Bearer token via API key.
Free tier: self-hosted unlimited, cloud has generous free plan.
"""

import json
from .base import (
    IntegrationBase, IntegrationCredentialField, IntegrationAction,
    ActionParameter, _http_request,
)
from . import register

CAL_API_BASE = "https://api.cal.com/v2"


@register
class CalComIntegration(IntegrationBase):

    @property
    def id(self) -> str:
        return "cal_com"

    @property
    def name(self) -> str:
        return "Cal.com"

    @property
    def description(self) -> str:
        return "Schedule, check availability, and manage appointments via Cal.com."

    @property
    def category(self) -> str:
        return "calendar"

    @property
    def credential_fields(self) -> list:
        return [
            IntegrationCredentialField(
                name="api_key",
                label="Cal.com API Key",
                field_type="password",
                required=True,
                placeholder="cal_live_...",
                help_text="Found in Cal.com Settings > Security > API Keys.",
            ),
            IntegrationCredentialField(
                name="base_url",
                label="API Base URL",
                field_type="url",
                required=False,
                placeholder=CAL_API_BASE,
                help_text="Override for self-hosted instances. Leave empty for cloud.",
            ),
            IntegrationCredentialField(
                name="event_type_id",
                label="Default Event Type ID",
                field_type="text",
                required=False,
                placeholder="123456",
                help_text="Default event type to use for bookings. Agent can override per-call.",
            ),
        ]

    @property
    def actions(self) -> list:
        return [
            IntegrationAction(
                name="check_availability",
                description="Check available time slots for booking an appointment.",
                parameters=[
                    ActionParameter(name="date_from", param_type="string",
                                    description="Start date in ISO-8601 format, e.g. '2026-02-20'."),
                    ActionParameter(name="date_to", param_type="string",
                                    description="End date in ISO-8601 format, e.g. '2026-02-21'."),
                    ActionParameter(name="event_type_id", param_type="string",
                                    description="Cal.com event type ID. Use the default if not specified.",
                                    required=False),
                ],
            ),
            IntegrationAction(
                name="create_booking",
                description="Book an appointment for the caller at a specific date and time.",
                parameters=[
                    ActionParameter(name="start_time", param_type="string",
                                    description="Start time in ISO-8601, e.g. '2026-02-20T14:00:00Z'."),
                    ActionParameter(name="name", param_type="string",
                                    description="Full name of the person booking."),
                    ActionParameter(name="email", param_type="string",
                                    description="Email address for the booking confirmation."),
                    ActionParameter(name="event_type_id", param_type="string",
                                    description="Cal.com event type ID.", required=False),
                    ActionParameter(name="notes", param_type="string",
                                    description="Additional notes for the booking.", required=False),
                ],
            ),
            IntegrationAction(
                name="cancel_booking",
                description="Cancel an existing booking by its booking UID.",
                parameters=[
                    ActionParameter(name="booking_uid", param_type="string",
                                    description="The UID of the booking to cancel."),
                    ActionParameter(name="reason", param_type="string",
                                    description="Reason for cancellation.", required=False),
                ],
            ),
        ]

    def _headers(self, credentials: dict) -> dict:
        return {
            "Authorization": f"Bearer {credentials.get('api_key', '')}",
            "Content-Type": "application/json",
            "cal-api-version": "2024-08-13",
        }

    def _base(self, credentials: dict) -> str:
        return (credentials.get("base_url") or CAL_API_BASE).rstrip("/")

    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        handlers = {
            "check_availability": self._check_availability,
            "create_booking": self._create_booking,
            "cancel_booking": self._cancel_booking,
        }
        handler = handlers.get(action_name)
        if not handler:
            return json.dumps({"error": f"Unknown action: {action_name}"})
        return await handler(credentials, params)

    async def _check_availability(self, creds: dict, params: dict) -> str:
        event_type_id = params.get("event_type_id") or creds.get("event_type_id", "")
        if not event_type_id:
            return json.dumps({"error": "event_type_id is required to check availability."})

        url = f"{self._base(creds)}/slots"
        query = {
            "startTime": params["date_from"],
            "endTime": params["date_to"],
            "eventTypeId": event_type_id,
        }
        resp = await _http_request("GET", url, headers=self._headers(creds), params=query)
        if resp["ok"]:
            slots = resp["data"]
            return json.dumps({"available_slots": slots})
        return json.dumps({"error": f"Cal.com API error: HTTP {resp['status']}", "detail": resp["data"]})

    async def _create_booking(self, creds: dict, params: dict) -> str:
        event_type_id = params.get("event_type_id") or creds.get("event_type_id", "")
        if not event_type_id:
            return json.dumps({"error": "event_type_id is required to create a booking."})

        url = f"{self._base(creds)}/bookings"
        body = {
            "eventTypeId": int(event_type_id),
            "start": params["start_time"],
            "attendee": {
                "name": params["name"],
                "email": params["email"],
                "timeZone": "UTC",
            },
        }
        if params.get("notes"):
            body["notes"] = params["notes"]

        resp = await _http_request("POST", url, headers=self._headers(creds), json_body=body)
        if resp["ok"]:
            booking = resp["data"]
            uid = booking.get("data", {}).get("uid", booking.get("uid", "unknown"))
            return json.dumps({
                "success": True,
                "booking_uid": uid,
                "message": f"Appointment booked for {params['name']} at {params['start_time']}.",
            })
        return json.dumps({"error": f"Booking failed: HTTP {resp['status']}", "detail": resp["data"]})

    async def _cancel_booking(self, creds: dict, params: dict) -> str:
        uid = params.get("booking_uid", "")
        if not uid:
            return json.dumps({"error": "booking_uid is required."})

        url = f"{self._base(creds)}/bookings/{uid}/cancel"
        body = {}
        if params.get("reason"):
            body["cancellationReason"] = params["reason"]

        resp = await _http_request("POST", url, headers=self._headers(creds), json_body=body)
        if resp["ok"]:
            return json.dumps({"success": True, "message": f"Booking {uid} cancelled."})
        return json.dumps({"error": f"Cancel failed: HTTP {resp['status']}", "detail": resp["data"]})

    async def test_credentials(self, credentials: dict) -> dict:
        url = f"{self._base(credentials)}/me"
        try:
            resp = await _http_request("GET", url, headers=self._headers(credentials), timeout=10)
            if resp["ok"]:
                return {"ok": True, "message": "API key is valid."}
            return {"ok": False, "message": f"HTTP {resp['status']}: {resp['data']}"}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
