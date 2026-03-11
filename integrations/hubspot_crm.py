"""HubSpot CRM integration.

HubSpot API: https://developers.hubspot.com/docs/api/overview
Authentication: Bearer token via private app access token.
Free CRM tier available.
"""

import json
from .base import (
    IntegrationBase, IntegrationCredentialField, IntegrationAction,
    ActionParameter, _http_request,
)
from . import register

HS_API_BASE = "https://api.hubapi.com"


@register
class HubSpotIntegration(IntegrationBase):

    @property
    def id(self) -> str:
        return "hubspot"

    @property
    def name(self) -> str:
        return "HubSpot"

    @property
    def description(self) -> str:
        return "Create and manage contacts and deals in HubSpot CRM."

    @property
    def category(self) -> str:
        return "crm"

    @property
    def credential_fields(self) -> list:
        return [
            IntegrationCredentialField(
                name="access_token",
                label="Private App Access Token",
                field_type="password",
                required=True,
                placeholder="pat-na1-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                help_text="Create a Private App at Settings > Integrations > Private Apps.",
            ),
        ]

    @property
    def actions(self) -> list:
        return [
            IntegrationAction(
                name="create_contact",
                description="Create a new contact in HubSpot CRM.",
                parameters=[
                    ActionParameter(name="email", param_type="string",
                                    description="Contact email address."),
                    ActionParameter(name="firstname", param_type="string",
                                    description="Contact first name.", required=False),
                    ActionParameter(name="lastname", param_type="string",
                                    description="Contact last name.", required=False),
                    ActionParameter(name="phone", param_type="string",
                                    description="Contact phone number.", required=False),
                ],
            ),
            IntegrationAction(
                name="update_contact",
                description="Update an existing contact's properties by their email address.",
                parameters=[
                    ActionParameter(name="email", param_type="string",
                                    description="Email of the contact to update."),
                    ActionParameter(name="properties", param_type="string",
                                    description="JSON string of properties to update, e.g. '{\"phone\":\"+1555...\", \"company\":\"Acme\"}'."),
                ],
            ),
            IntegrationAction(
                name="search_contacts",
                description="Search for contacts in HubSpot by name, email, or phone.",
                parameters=[
                    ActionParameter(name="query", param_type="string",
                                    description="Search term (name, email, or phone number)."),
                ],
            ),
            IntegrationAction(
                name="create_deal",
                description="Create a new deal/opportunity in HubSpot.",
                parameters=[
                    ActionParameter(name="deal_name", param_type="string",
                                    description="Name of the deal."),
                    ActionParameter(name="amount", param_type="string",
                                    description="Deal amount.", required=False),
                    ActionParameter(name="stage", param_type="string",
                                    description="Deal stage, e.g. 'appointmentscheduled', 'qualifiedtobuy'.",
                                    required=False),
                ],
            ),
        ]

    def _headers(self, creds: dict) -> dict:
        return {
            "Authorization": f"Bearer {creds.get('access_token', '')}",
            "Content-Type": "application/json",
        }

    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        handlers = {
            "create_contact": self._create_contact,
            "update_contact": self._update_contact,
            "search_contacts": self._search_contacts,
            "create_deal": self._create_deal,
        }
        handler = handlers.get(action_name)
        if not handler:
            return json.dumps({"error": f"Unknown action: {action_name}"})
        return await handler(credentials, params)

    async def _create_contact(self, creds: dict, params: dict) -> str:
        props = {"email": params["email"]}
        for key in ("firstname", "lastname", "phone"):
            if params.get(key):
                props[key] = params[key]

        resp = await _http_request(
            "POST", f"{HS_API_BASE}/crm/v3/objects/contacts",
            headers=self._headers(creds), json_body={"properties": props},
        )
        if resp["ok"]:
            cid = resp["data"].get("id", "")
            return json.dumps({"success": True, "contact_id": cid,
                               "message": f"Contact created for {params['email']}."})
        if resp["status"] == 409:
            return json.dumps({"success": False, "error": "Contact already exists.",
                               "detail": resp["data"]})
        return json.dumps({"success": False, "error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def _update_contact(self, creds: dict, params: dict) -> str:
        email = params.get("email", "")
        search_resp = await _http_request(
            "POST", f"{HS_API_BASE}/crm/v3/objects/contacts/search",
            headers=self._headers(creds),
            json_body={"filterGroups": [{"filters": [
                {"propertyName": "email", "operator": "EQ", "value": email}
            ]}]},
        )
        if not search_resp["ok"]:
            return json.dumps({"error": f"Search failed: HTTP {search_resp['status']}"})

        results = search_resp["data"].get("results", [])
        if not results:
            return json.dumps({"error": f"No contact found with email {email}."})

        contact_id = results[0]["id"]
        try:
            props = json.loads(params.get("properties", "{}"))
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"error": "properties must be a valid JSON string."})

        resp = await _http_request(
            "PATCH", f"{HS_API_BASE}/crm/v3/objects/contacts/{contact_id}",
            headers=self._headers(creds), json_body={"properties": props},
        )
        if resp["ok"]:
            return json.dumps({"success": True, "message": f"Contact {email} updated."})
        return json.dumps({"success": False, "error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def _search_contacts(self, creds: dict, params: dict) -> str:
        query = params.get("query", "")
        resp = await _http_request(
            "POST", f"{HS_API_BASE}/crm/v3/objects/contacts/search",
            headers=self._headers(creds),
            json_body={"query": query, "limit": 5,
                       "properties": ["email", "firstname", "lastname", "phone"]},
        )
        if resp["ok"]:
            results = resp["data"].get("results", [])
            contacts = [
                {k: r.get("properties", {}).get(k, "") for k in ("email", "firstname", "lastname", "phone")}
                for r in results
            ]
            return json.dumps({"contacts": contacts, "total": len(contacts)})
        return json.dumps({"error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def _create_deal(self, creds: dict, params: dict) -> str:
        props = {"dealname": params["deal_name"]}
        if params.get("amount"):
            props["amount"] = params["amount"]
        if params.get("stage"):
            props["dealstage"] = params["stage"]

        resp = await _http_request(
            "POST", f"{HS_API_BASE}/crm/v3/objects/deals",
            headers=self._headers(creds), json_body={"properties": props},
        )
        if resp["ok"]:
            did = resp["data"].get("id", "")
            return json.dumps({"success": True, "deal_id": did,
                               "message": f"Deal '{params['deal_name']}' created."})
        return json.dumps({"success": False, "error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def test_credentials(self, credentials: dict) -> dict:
        try:
            resp = await _http_request(
                "GET", f"{HS_API_BASE}/crm/v3/objects/contacts?limit=1",
                headers=self._headers(credentials), timeout=10,
            )
            if resp["ok"]:
                return {"ok": True, "message": "Access token is valid."}
            return {"ok": False, "message": f"HTTP {resp['status']}"}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
