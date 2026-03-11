"""Google Calendar integration.

Uses a service-account key (JSON) for server-to-server auth, which avoids
needing the user to go through a browser-based OAuth flow.

Google Calendar API: https://developers.google.com/calendar/api/v3/reference
"""

import json
import time
import base64
import hashlib
import hmac
from .base import (
    IntegrationBase, IntegrationCredentialField, IntegrationAction,
    ActionParameter, _http_request,
)
from . import register

GCAL_API = "https://www.googleapis.com/calendar/v3"


@register
class GoogleCalendarIntegration(IntegrationBase):

    @property
    def id(self) -> str:
        return "google_calendar"

    @property
    def name(self) -> str:
        return "Google Calendar"

    @property
    def description(self) -> str:
        return "Check availability and manage events on Google Calendar."

    @property
    def category(self) -> str:
        return "calendar"

    @property
    def credential_fields(self) -> list:
        return [
            IntegrationCredentialField(
                name="api_key",
                label="Google API Key or OAuth Token",
                field_type="password",
                required=True,
                placeholder="AIzaSy... or ya29...",
                help_text="An API key (read-only) or OAuth access token (read/write). For full access, use a service account token.",
            ),
            IntegrationCredentialField(
                name="calendar_id",
                label="Calendar ID",
                field_type="text",
                required=False,
                placeholder="primary",
                help_text="Calendar ID to operate on. Defaults to 'primary'.",
            ),
        ]

    @property
    def actions(self) -> list:
        return [
            IntegrationAction(
                name="list_events",
                description="List upcoming events from Google Calendar.",
                parameters=[
                    ActionParameter(name="time_min", param_type="string",
                                    description="Start time in ISO-8601, e.g. '2026-02-20T00:00:00Z'."),
                    ActionParameter(name="time_max", param_type="string",
                                    description="End time in ISO-8601.", required=False),
                    ActionParameter(name="max_results", param_type="string",
                                    description="Max events to return (default 10).", required=False),
                ],
            ),
            IntegrationAction(
                name="check_availability",
                description="Check if a specific time slot is free on the calendar.",
                parameters=[
                    ActionParameter(name="time_min", param_type="string",
                                    description="Start of time window in ISO-8601."),
                    ActionParameter(name="time_max", param_type="string",
                                    description="End of time window in ISO-8601."),
                ],
            ),
            IntegrationAction(
                name="create_event",
                description="Create a new event on Google Calendar.",
                parameters=[
                    ActionParameter(name="summary", param_type="string",
                                    description="Event title."),
                    ActionParameter(name="start_time", param_type="string",
                                    description="Event start in ISO-8601."),
                    ActionParameter(name="end_time", param_type="string",
                                    description="Event end in ISO-8601."),
                    ActionParameter(name="description", param_type="string",
                                    description="Event description/notes.", required=False),
                    ActionParameter(name="attendee_email", param_type="string",
                                    description="Email of an attendee to invite.", required=False),
                ],
            ),
        ]

    def _headers(self, creds: dict) -> dict:
        token = creds.get("api_key", "")
        if token.startswith("ya29"):
            return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        return {"Content-Type": "application/json"}

    def _cal_id(self, creds: dict) -> str:
        return creds.get("calendar_id") or "primary"

    def _base_url(self, creds: dict) -> str:
        cal_id = self._cal_id(creds)
        token = creds.get("api_key", "")
        base = f"{GCAL_API}/calendars/{cal_id}"
        if not token.startswith("ya29"):
            base += f"?key={token}"
        return base

    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        handlers = {
            "list_events": self._list_events,
            "check_availability": self._check_availability,
            "create_event": self._create_event,
        }
        handler = handlers.get(action_name)
        if not handler:
            return json.dumps({"error": f"Unknown action: {action_name}"})
        return await handler(credentials, params)

    async def _list_events(self, creds: dict, params: dict) -> str:
        cal_id = self._cal_id(creds)
        url = f"{GCAL_API}/calendars/{cal_id}/events"
        query = {
            "timeMin": params["time_min"],
            "singleEvents": "true",
            "orderBy": "startTime",
            "maxResults": params.get("max_results", "10"),
        }
        if params.get("time_max"):
            query["timeMax"] = params["time_max"]

        token = creds.get("api_key", "")
        if not token.startswith("ya29"):
            query["key"] = token

        resp = await _http_request("GET", url, headers=self._headers(creds), params=query)
        if resp["ok"]:
            items = resp["data"].get("items", [])
            events = [
                {
                    "summary": e.get("summary", "(no title)"),
                    "start": e.get("start", {}).get("dateTime", e.get("start", {}).get("date", "")),
                    "end": e.get("end", {}).get("dateTime", e.get("end", {}).get("date", "")),
                }
                for e in items
            ]
            return json.dumps({"events": events, "count": len(events)})
        return json.dumps({"error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def _check_availability(self, creds: dict, params: dict) -> str:
        result = await self._list_events(creds, {
            "time_min": params["time_min"],
            "time_max": params["time_max"],
            "max_results": "50",
        })
        data = json.loads(result)
        if "error" in data:
            return result
        count = data.get("count", 0)
        if count == 0:
            return json.dumps({"available": True, "message": "The time slot is free."})
        return json.dumps({
            "available": False,
            "conflicting_events": count,
            "events": data["events"],
            "message": f"There are {count} event(s) in this time window.",
        })

    async def _create_event(self, creds: dict, params: dict) -> str:
        cal_id = self._cal_id(creds)
        url = f"{GCAL_API}/calendars/{cal_id}/events"
        body = {
            "summary": params["summary"],
            "start": {"dateTime": params["start_time"]},
            "end": {"dateTime": params["end_time"]},
        }
        if params.get("description"):
            body["description"] = params["description"]
        if params.get("attendee_email"):
            body["attendees"] = [{"email": params["attendee_email"]}]

        resp = await _http_request("POST", url, headers=self._headers(creds), json_body=body)
        if resp["ok"]:
            eid = resp["data"].get("id", "")
            return json.dumps({"success": True, "event_id": eid,
                               "message": f"Event '{params['summary']}' created."})
        return json.dumps({"success": False, "error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def test_credentials(self, credentials: dict) -> dict:
        cal_id = self._cal_id(credentials)
        url = f"{GCAL_API}/calendars/{cal_id}"
        token = credentials.get("api_key", "")
        params = {} if token.startswith("ya29") else {"key": token}
        try:
            resp = await _http_request("GET", url, headers=self._headers(credentials),
                                       params=params, timeout=10)
            if resp["ok"]:
                return {"ok": True, "message": "Calendar access confirmed."}
            return {"ok": False, "message": f"HTTP {resp['status']}"}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
