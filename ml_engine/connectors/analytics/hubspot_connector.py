from typing import Any
from ..base import BaseConnector

HUBSPOT_API_BASE = "https://api.hubapi.com"
HUBSPOT_OBJECTS = [
    {"id": "contacts",   "label": "Contacts"},
    {"id": "companies",  "label": "Companies"},
    {"id": "deals",      "label": "Deals"},
    {"id": "tickets",    "label": "Tickets"},
    {"id": "products",   "label": "Products"},
]

class HubSpotConnector(BaseConnector):
    """
    Connects to HubSpot via Private App API key.
    config keys: { "api_key": str }
    """

    def _headers(self, api_key: str) -> dict:
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def validate(self, config: dict[str, Any]) -> dict:
        import requests
        try:
            api_key = config["api_key"]
            resp = requests.get(
                f"{HUBSPOT_API_BASE}/account-info/v3/details",
                headers=self._headers(api_key),
                timeout=10,
            )
            resp.raise_for_status()
            portal_id = resp.json().get("portalId", "")
            return {
                "status": "ok",
                "sources": [
                    {"id": obj["id"], "label": obj["label"], "meta": {}}
                    for obj in HUBSPOT_OBJECTS
                ],
                "message": f"Connected to HubSpot portal {portal_id}",
            }
        except Exception as e:
            return {"status": "error", "sources": [], "message": str(e)}

    def fetch(self, config: dict[str, Any], source: str):
        import requests
        import pandas as pd
        api_key = config["api_key"]
        headers = self._headers(api_key)

        records = []
        after = None

        while True:
            params = {"limit": 100}
            if after:
                params["after"] = after

            resp = requests.get(
                f"{HUBSPOT_API_BASE}/crm/v3/objects/{source}",
                headers=headers,
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            for result in data.get("results", []):
                row = {"_hubspot_id": result["id"]}
                row.update(result.get("properties", {}))
                records.append(row)

            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break

        return pd.DataFrame(records)

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "fields": [
                {
                    "name": "api_key", 
                    "type": "password", 
                    "label": "Private App Access Token", 
                    "required": True,
                    "hint": "Create a Private App in HubSpot Settings → Integrations → Private Apps"
                },
            ]
        }
