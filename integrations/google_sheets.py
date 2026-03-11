"""Google Sheets integration.

Google Sheets API v4: https://developers.google.com/sheets/api/reference/rest
Authentication: API key (read-only) or OAuth token (read/write).
Free tier: 300 read / 300 write requests per minute per project.
"""

import json
from .base import (
    IntegrationBase, IntegrationCredentialField, IntegrationAction,
    ActionParameter, _http_request,
)
from . import register

SHEETS_API = "https://sheets.googleapis.com/v4/spreadsheets"


@register
class GoogleSheetsIntegration(IntegrationBase):

    @property
    def id(self) -> str:
        return "google_sheets"

    @property
    def name(self) -> str:
        return "Google Sheets"

    @property
    def description(self) -> str:
        return "Read and write data in Google Sheets. Use for logging call data, reading lookup tables, etc."

    @property
    def category(self) -> str:
        return "data"

    @property
    def credential_fields(self) -> list:
        return [
            IntegrationCredentialField(
                name="api_key",
                label="Google API Key or OAuth Token",
                field_type="password",
                required=True,
                placeholder="AIzaSy... or ya29...",
                help_text="API key (read-only) or OAuth token (read/write).",
            ),
            IntegrationCredentialField(
                name="spreadsheet_id",
                label="Spreadsheet ID",
                field_type="text",
                required=True,
                placeholder="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms",
                help_text="Found in the spreadsheet URL between /d/ and /edit.",
            ),
            IntegrationCredentialField(
                name="sheet_name",
                label="Default Sheet (Tab) Name",
                field_type="text",
                required=False,
                placeholder="Sheet1",
                help_text="Tab name to operate on. Defaults to 'Sheet1'.",
            ),
        ]

    @property
    def actions(self) -> list:
        return [
            IntegrationAction(
                name="read_rows",
                description="Read rows from a Google Sheet.",
                parameters=[
                    ActionParameter(name="range", param_type="string",
                                    description="Cell range, e.g. 'A1:D10' or 'Sheet1!A:D'. If omitted, reads all.",
                                    required=False),
                ],
            ),
            IntegrationAction(
                name="append_row",
                description="Append a new row to the bottom of a Google Sheet.",
                parameters=[
                    ActionParameter(name="values", param_type="string",
                                    description="JSON array of cell values, e.g. '[\"John\",\"john@example.com\",\"2026-02-20\"]'."),
                    ActionParameter(name="range", param_type="string",
                                    description="Target range, e.g. 'Sheet1!A:D'. Defaults to sheet name.",
                                    required=False),
                ],
            ),
            IntegrationAction(
                name="update_row",
                description="Update specific cells in a Google Sheet.",
                parameters=[
                    ActionParameter(name="range", param_type="string",
                                    description="Cell range to update, e.g. 'Sheet1!A2:D2'."),
                    ActionParameter(name="values", param_type="string",
                                    description="JSON array of cell values for the range."),
                ],
            ),
        ]

    def _headers(self, creds: dict) -> dict:
        token = creds.get("api_key", "")
        if token.startswith("ya29"):
            return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        return {"Content-Type": "application/json"}

    def _sheet(self, creds: dict) -> str:
        return creds.get("sheet_name") or "Sheet1"

    def _sid(self, creds: dict) -> str:
        return creds.get("spreadsheet_id", "")

    def _key_param(self, creds: dict) -> dict:
        token = creds.get("api_key", "")
        if not token.startswith("ya29"):
            return {"key": token}
        return {}

    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        handlers = {
            "read_rows": self._read_rows,
            "append_row": self._append_row,
            "update_row": self._update_row,
        }
        handler = handlers.get(action_name)
        if not handler:
            return json.dumps({"error": f"Unknown action: {action_name}"})
        return await handler(credentials, params)

    async def _read_rows(self, creds: dict, params: dict) -> str:
        sid = self._sid(creds)
        cell_range = params.get("range") or self._sheet(creds)
        url = f"{SHEETS_API}/{sid}/values/{cell_range}"
        resp = await _http_request("GET", url, headers=self._headers(creds),
                                   params=self._key_param(creds))
        if resp["ok"]:
            rows = resp["data"].get("values", [])
            return json.dumps({"rows": rows, "count": len(rows)})
        return json.dumps({"error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def _append_row(self, creds: dict, params: dict) -> str:
        sid = self._sid(creds)
        cell_range = params.get("range") or self._sheet(creds)
        url = f"{SHEETS_API}/{sid}/values/{cell_range}:append"

        try:
            values = json.loads(params.get("values", "[]"))
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"error": "values must be a valid JSON array."})

        if not isinstance(values, list):
            values = [values]

        query = {**self._key_param(creds), "valueInputOption": "USER_ENTERED",
                 "insertDataOption": "INSERT_ROWS"}
        resp = await _http_request(
            "POST", url, headers=self._headers(creds),
            params=query,
            json_body={"values": [values]},
        )
        if resp["ok"]:
            return json.dumps({"success": True, "message": "Row appended."})
        return json.dumps({"success": False, "error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def _update_row(self, creds: dict, params: dict) -> str:
        sid = self._sid(creds)
        cell_range = params.get("range", "")
        if not cell_range:
            return json.dumps({"error": "range is required for update."})

        try:
            values = json.loads(params.get("values", "[]"))
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"error": "values must be a valid JSON array."})
        if not isinstance(values, list):
            values = [values]

        url = f"{SHEETS_API}/{sid}/values/{cell_range}"
        query = {**self._key_param(creds), "valueInputOption": "USER_ENTERED"}
        resp = await _http_request(
            "PUT", url, headers=self._headers(creds),
            params=query,
            json_body={"values": [values]},
        )
        if resp["ok"]:
            return json.dumps({"success": True, "message": f"Range {cell_range} updated."})
        return json.dumps({"success": False, "error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def test_credentials(self, credentials: dict) -> dict:
        sid = self._sid(credentials)
        if not sid:
            return {"ok": False, "message": "Spreadsheet ID is required."}
        url = f"{SHEETS_API}/{sid}"
        try:
            resp = await _http_request("GET", url, headers=self._headers(credentials),
                                       params=self._key_param(credentials), timeout=10)
            if resp["ok"]:
                title = resp["data"].get("properties", {}).get("title", "")
                return {"ok": True, "message": f"Connected to '{title}'."}
            return {"ok": False, "message": f"HTTP {resp['status']}"}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
