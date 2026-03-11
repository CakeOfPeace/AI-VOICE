"""Airtable data integration.

Airtable API: https://airtable.com/developers/web/api/introduction
Authentication: Bearer token via personal access token.
Free tier: 1,000 API calls/month.
"""

import json
from .base import (
    IntegrationBase, IntegrationCredentialField, IntegrationAction,
    ActionParameter, _http_request,
)
from . import register

AT_API_BASE = "https://api.airtable.com/v0"


@register
class AirtableIntegration(IntegrationBase):

    @property
    def id(self) -> str:
        return "airtable"

    @property
    def name(self) -> str:
        return "Airtable"

    @property
    def description(self) -> str:
        return "Read and write records in Airtable bases. Great for storing call data, leads, or form responses."

    @property
    def category(self) -> str:
        return "data"

    @property
    def credential_fields(self) -> list:
        return [
            IntegrationCredentialField(
                name="api_key",
                label="Personal Access Token",
                field_type="password",
                required=True,
                placeholder="patXXXXXXXXXXXXXX.xxxxxxxxxxxxxxxx",
                help_text="Create at https://airtable.com/create/tokens",
            ),
            IntegrationCredentialField(
                name="base_id",
                label="Base ID",
                field_type="text",
                required=True,
                placeholder="appXXXXXXXXXXXXXX",
                help_text="Found in the Airtable API docs for your base.",
            ),
            IntegrationCredentialField(
                name="table_name",
                label="Default Table Name",
                field_type="text",
                required=True,
                placeholder="Leads",
                help_text="The table to read/write records in.",
            ),
        ]

    @property
    def actions(self) -> list:
        return [
            IntegrationAction(
                name="list_records",
                description="List records from the Airtable table. Returns up to 10 records.",
                parameters=[
                    ActionParameter(name="filter_formula", param_type="string",
                                    description="Airtable formula to filter records, e.g. \"{Status}='Active'\". Leave empty for all.",
                                    required=False),
                    ActionParameter(name="table_name", param_type="string",
                                    description="Override the default table name.", required=False),
                ],
            ),
            IntegrationAction(
                name="create_record",
                description="Create a new record in the Airtable table.",
                parameters=[
                    ActionParameter(name="fields", param_type="string",
                                    description="JSON string of field names and values, e.g. '{\"Name\":\"John\",\"Email\":\"john@example.com\"}'."),
                    ActionParameter(name="table_name", param_type="string",
                                    description="Override the default table name.", required=False),
                ],
            ),
            IntegrationAction(
                name="update_record",
                description="Update an existing record in Airtable by record ID.",
                parameters=[
                    ActionParameter(name="record_id", param_type="string",
                                    description="The Airtable record ID (starts with 'rec')."),
                    ActionParameter(name="fields", param_type="string",
                                    description="JSON string of field names and values to update."),
                    ActionParameter(name="table_name", param_type="string",
                                    description="Override the default table name.", required=False),
                ],
            ),
        ]

    def _headers(self, creds: dict) -> dict:
        return {
            "Authorization": f"Bearer {creds.get('api_key', '')}",
            "Content-Type": "application/json",
        }

    def _table_url(self, creds: dict, params: dict) -> str:
        base_id = creds.get("base_id", "")
        table = params.get("table_name") or creds.get("table_name", "")
        return f"{AT_API_BASE}/{base_id}/{table}"

    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        handlers = {
            "list_records": self._list_records,
            "create_record": self._create_record,
            "update_record": self._update_record,
        }
        handler = handlers.get(action_name)
        if not handler:
            return json.dumps({"error": f"Unknown action: {action_name}"})
        return await handler(credentials, params)

    async def _list_records(self, creds: dict, params: dict) -> str:
        url = self._table_url(creds, params)
        query = {"maxRecords": 10}
        formula = params.get("filter_formula", "").strip()
        if formula:
            query["filterByFormula"] = formula

        resp = await _http_request("GET", url, headers=self._headers(creds), params=query)
        if resp["ok"]:
            records = resp["data"].get("records", [])
            simplified = [
                {"id": r["id"], "fields": r.get("fields", {})}
                for r in records
            ]
            return json.dumps({"records": simplified, "count": len(simplified)})
        return json.dumps({"error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def _create_record(self, creds: dict, params: dict) -> str:
        try:
            fields = json.loads(params.get("fields", "{}"))
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"error": "fields must be a valid JSON string."})

        url = self._table_url(creds, params)
        resp = await _http_request(
            "POST", url, headers=self._headers(creds),
            json_body={"records": [{"fields": fields}]},
        )
        if resp["ok"]:
            records = resp["data"].get("records", [])
            rid = records[0]["id"] if records else ""
            return json.dumps({"success": True, "record_id": rid, "message": "Record created."})
        return json.dumps({"success": False, "error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def _update_record(self, creds: dict, params: dict) -> str:
        record_id = params.get("record_id", "")
        if not record_id:
            return json.dumps({"error": "record_id is required."})
        try:
            fields = json.loads(params.get("fields", "{}"))
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"error": "fields must be a valid JSON string."})

        url = self._table_url(creds, params)
        resp = await _http_request(
            "PATCH", url, headers=self._headers(creds),
            json_body={"records": [{"id": record_id, "fields": fields}]},
        )
        if resp["ok"]:
            return json.dumps({"success": True, "message": f"Record {record_id} updated."})
        return json.dumps({"success": False, "error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def test_credentials(self, credentials: dict) -> dict:
        url = self._table_url(credentials, {})
        try:
            resp = await _http_request("GET", url, headers=self._headers(credentials),
                                       params={"maxRecords": 1}, timeout=10)
            if resp["ok"]:
                return {"ok": True, "message": "Credentials are valid."}
            return {"ok": False, "message": f"HTTP {resp['status']}"}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
