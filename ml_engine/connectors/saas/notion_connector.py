from typing import Any
from ..base import BaseConnector

NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

class NotionConnector(BaseConnector):
    """
    Connects to Notion via integration API key.
    config keys: { "api_key": str, "database_id": str }
    """

    def _headers(self, api_key: str) -> dict:
        return {
            "Authorization": f"Bearer {api_key}",
            "Notion-Version": NOTION_VERSION,
            "Content-Type": "application/json",
        }

    def _extract_property_value(self, prop: dict) -> Any:
        """Flatten a Notion property object to a plain Python value."""
        ptype = prop.get("type")
        val = prop.get(ptype)

        if val is None:
            return None
        if ptype == "title" or ptype == "rich_text":
            return " ".join(t.get("plain_text", "") for t in val)
        if ptype in ("number", "checkbox", "url", "email", "phone_number"):
            return val
        if ptype == "select":
            return val.get("name") if val else None
        if ptype == "multi_select":
            return ", ".join(o.get("name", "") for o in val)
        if ptype == "date":
            return val.get("start") if val else None
        if ptype == "people":
            return ", ".join(p.get("name", "") for p in val)
        return str(val)

    def validate(self, config: dict[str, Any]) -> dict:
        import requests
        try:
            api_key = config["api_key"]
            database_id = config["database_id"]
            resp = requests.get(
                f"{NOTION_API_BASE}/databases/{database_id}",
                headers=self._headers(api_key),
                timeout=10,
            )
            resp.raise_for_status()
            db = resp.json()
            title = "".join(t.get("plain_text", "") for t in db.get("title", []))
            properties = list(db.get("properties", {}).keys())
            return {
                "status": "ok",
                "sources": [
                    {
                        "id": "default",
                        "label": title or database_id,
                        "meta": {"columns": properties},
                    }
                ],
                "message": f"Connected to '{title}'. {len(properties)} properties found",
            }
        except Exception as e:
            return {"status": "error", "sources": [], "message": str(e)}

    def fetch(self, config: dict[str, Any], source: str):
        import requests
        import pandas as pd
        api_key = config["api_key"]
        database_id = config["database_id"]
        headers = self._headers(api_key)

        rows = []
        start_cursor = None

        while True:
            body: dict = {"page_size": 100}
            if start_cursor:
                body["start_cursor"] = start_cursor

            resp = requests.post(
                f"{NOTION_API_BASE}/databases/{database_id}/query",
                headers=headers,
                json=body,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            for page in data.get("results", []):
                row = {"_notion_id": page["id"]}
                for prop_name, prop_val in page.get("properties", {}).items():
                    row[prop_name] = self._extract_property_value(prop_val)
                rows.append(row)

            if not data.get("has_more"):
                break
            start_cursor = data.get("next_cursor")

        return pd.DataFrame(rows)

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "fields": [
                {
                    "name": "database_id", 
                    "type": "text", 
                    "label": "Database ID", 
                    "placeholder": "8a5f2d3e-...", 
                    "required": True,
                    "hint": "From the database URL: notion.so/username/DATABASE_ID?v=..."
                },
                {
                    "name": "api_key", 
                    "type": "password", 
                    "label": "Internal Integration Token", 
                    "required": True,
                    "hint": "Create at notion.so/my-integrations, then share the DB with it"
                },
            ]
        }
