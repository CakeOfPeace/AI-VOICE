"""Notion integration.

Notion API: https://developers.notion.com/reference/intro
Authentication: Bearer token via internal integration token.
Free tier: unlimited API calls on free Notion plan.
"""

import json
from .base import (
    IntegrationBase, IntegrationCredentialField, IntegrationAction,
    ActionParameter, _http_request,
)
from . import register

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"


@register
class NotionIntegration(IntegrationBase):

    @property
    def id(self) -> str:
        return "notion"

    @property
    def name(self) -> str:
        return "Notion"

    @property
    def description(self) -> str:
        return "Search, create, and update pages and databases in Notion."

    @property
    def category(self) -> str:
        return "data"

    @property
    def credential_fields(self) -> list:
        return [
            IntegrationCredentialField(
                name="api_key",
                label="Integration Token",
                field_type="password",
                required=True,
                placeholder="ntn_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                help_text="Create at https://www.notion.so/my-integrations",
            ),
            IntegrationCredentialField(
                name="database_id",
                label="Default Database ID",
                field_type="text",
                required=False,
                placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                help_text="The database to use for record operations. Found in the database URL.",
            ),
        ]

    @property
    def actions(self) -> list:
        return [
            IntegrationAction(
                name="search_pages",
                description="Search for pages and databases in Notion by title or content.",
                parameters=[
                    ActionParameter(name="query", param_type="string",
                                    description="Search query text."),
                ],
            ),
            IntegrationAction(
                name="create_page",
                description="Create a new page in a Notion database.",
                parameters=[
                    ActionParameter(name="title", param_type="string",
                                    description="Page title."),
                    ActionParameter(name="properties", param_type="string",
                                    description="JSON string of additional properties, e.g. '{\"Status\":{\"select\":{\"name\":\"New\"}}}'.",
                                    required=False),
                    ActionParameter(name="content", param_type="string",
                                    description="Plain-text content for the page body.", required=False),
                    ActionParameter(name="database_id", param_type="string",
                                    description="Target database ID. Uses default if omitted.",
                                    required=False),
                ],
            ),
            IntegrationAction(
                name="update_page",
                description="Update properties of an existing Notion page.",
                parameters=[
                    ActionParameter(name="page_id", param_type="string",
                                    description="The Notion page ID to update."),
                    ActionParameter(name="properties", param_type="string",
                                    description="JSON string of properties to update."),
                ],
            ),
        ]

    def _headers(self, creds: dict) -> dict:
        return {
            "Authorization": f"Bearer {creds.get('api_key', '')}",
            "Content-Type": "application/json",
            "Notion-Version": NOTION_VERSION,
        }

    async def execute_action(self, action_name: str, credentials: dict, params: dict) -> str:
        handlers = {
            "search_pages": self._search,
            "create_page": self._create_page,
            "update_page": self._update_page,
        }
        handler = handlers.get(action_name)
        if not handler:
            return json.dumps({"error": f"Unknown action: {action_name}"})
        return await handler(credentials, params)

    async def _search(self, creds: dict, params: dict) -> str:
        resp = await _http_request(
            "POST", f"{NOTION_API}/search",
            headers=self._headers(creds),
            json_body={"query": params.get("query", ""), "page_size": 5},
        )
        if resp["ok"]:
            results = resp["data"].get("results", [])
            pages = []
            for r in results:
                title_parts = r.get("properties", {}).get("title", {}).get("title", []) or \
                              r.get("properties", {}).get("Name", {}).get("title", [])
                title = "".join(t.get("plain_text", "") for t in title_parts) if title_parts else r.get("id", "")
                pages.append({
                    "id": r.get("id", ""),
                    "title": title,
                    "type": r.get("object", ""),
                    "url": r.get("url", ""),
                })
            return json.dumps({"results": pages, "count": len(pages)})
        return json.dumps({"error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def _create_page(self, creds: dict, params: dict) -> str:
        db_id = params.get("database_id") or creds.get("database_id", "")
        if not db_id:
            return json.dumps({"error": "database_id is required to create a page."})

        properties = {"Name": {"title": [{"text": {"content": params["title"]}}]}}
        if params.get("properties"):
            try:
                extra = json.loads(params["properties"])
                properties.update(extra)
            except (json.JSONDecodeError, TypeError):
                pass

        body = {
            "parent": {"database_id": db_id},
            "properties": properties,
        }

        if params.get("content"):
            body["children"] = [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": params["content"]}}]
                    },
                }
            ]

        resp = await _http_request("POST", f"{NOTION_API}/pages",
                                   headers=self._headers(creds), json_body=body)
        if resp["ok"]:
            pid = resp["data"].get("id", "")
            return json.dumps({"success": True, "page_id": pid,
                               "message": f"Page '{params['title']}' created."})
        return json.dumps({"success": False, "error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def _update_page(self, creds: dict, params: dict) -> str:
        page_id = params.get("page_id", "")
        if not page_id:
            return json.dumps({"error": "page_id is required."})

        try:
            properties = json.loads(params.get("properties", "{}"))
        except (json.JSONDecodeError, TypeError):
            return json.dumps({"error": "properties must be a valid JSON string."})

        resp = await _http_request(
            "PATCH", f"{NOTION_API}/pages/{page_id}",
            headers=self._headers(creds), json_body={"properties": properties},
        )
        if resp["ok"]:
            return json.dumps({"success": True, "message": f"Page {page_id} updated."})
        return json.dumps({"success": False, "error": f"HTTP {resp['status']}", "detail": resp["data"]})

    async def test_credentials(self, credentials: dict) -> dict:
        try:
            resp = await _http_request("GET", f"{NOTION_API}/users/me",
                                       headers=self._headers(credentials), timeout=10)
            if resp["ok"]:
                return {"ok": True, "message": "Integration token is valid."}
            return {"ok": False, "message": f"HTTP {resp['status']}"}
        except Exception as exc:
            return {"ok": False, "message": str(exc)}
