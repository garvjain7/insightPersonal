from typing import Any
from ..base import BaseConnector

AIRTABLE_API_BASE = "https://api.airtable.com/v0"
META_API_BASE = "https://api.airtable.com/v0/meta/bases"

class AirtableConnector(BaseConnector):
    """
    Connects to Airtable via API key.
    config keys: { "api_key": str, "base_id": str }
    """

    def _headers(self, api_key: str) -> dict:
        return {"Authorization": f"Bearer {api_key}"}

    def validate(self, config: dict[str, Any]) -> dict:
        import requests
        try:
            api_key = config["api_key"]
            base_id = config["base_id"]
            resp = requests.get(
                f"{META_API_BASE}/{base_id}/tables",
                headers=self._headers(api_key),
                timeout=10,
            )
            resp.raise_for_status()
            tables = resp.json().get("tables", [])
            sources = [
                {
                    "id": t["name"],
                    "label": t["name"],
                    "meta": {"table_id": t["id"]},
                }
                for t in tables
            ]
            return {
                "status": "ok",
                "sources": sources,
                "message": f"Connected. Found {len(sources)} table(s)",
            }
        except Exception as e:
            return {"status": "error", "sources": [], "message": str(e)}

    def fetch(self, config: dict[str, Any], source: str):
        import requests
        import pandas as pd
        api_key = config["api_key"]
        base_id = config["base_id"]
        headers = self._headers(api_key)

        records = []
        offset = None

        while True:
            params = {"pageSize": 100}
            if offset:
                params["offset"] = offset

            resp = requests.get(
                f"{AIRTABLE_API_BASE}/{base_id}/{source}",
                headers=headers,
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            for record in data.get("records", []):
                row = {"_airtable_id": record["id"]}
                row.update(record.get("fields", {}))
                records.append(row)

            offset = data.get("offset")
            if not offset:
                break

        return pd.DataFrame(records)

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "fields": [
                {
                    "name": "base_id", 
                    "type": "text", 
                    "label": "Base ID", 
                    "placeholder": "appXXXXXXXXXXXXXX", 
                    "required": True,
                    "hint": "Found in the URL: airtable.com/appXXX/..."
                },
                {
                    "name": "api_key", 
                    "type": "password", 
                    "label": "Personal Access Token", 
                    "required": True,
                    "hint": "Generate at airtable.com/create/tokens with data.records:read scope"
                },
            ]
        }
