from typing import Any
from ..base import BaseConnector

class JSONConnector(BaseConnector):
    """Fetches data from a REST API endpoint returning JSON array."""

    def validate(self, config: dict[str, Any]) -> dict:
        import requests
        import pandas as pd
        try:
            url = config.get("url")
            headers = config.get("headers", {})
            if isinstance(headers, str):
                import json
                headers = json.loads(headers)
                
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            data = res.json()
            
            if not isinstance(data, list):
                # Try to find a list in the object
                if isinstance(data, dict):
                    for k, v in data.items():
                        if isinstance(v, list):
                            data = v
                            break
                else:
                    return {"status": "error", "message": "Endpoint did not return a JSON list"}

            df = pd.DataFrame(data).head(5)
            return {
                "status": "ok",
                "sources": [{"id": "api_endpoint", "label": "API Result"}],
                "message": f"Successfully parsed JSON. Found {len(df.columns)} fields.",
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def fetch(self, config: dict[str, Any], source: str):
        import requests
        import pandas as pd
        url = config.get("url")
        headers = config.get("headers", {})
        if isinstance(headers, str):
            import json
            headers = json.loads(headers)
            
        res = requests.get(url, headers=headers)
        data = res.json()
        if isinstance(data, dict):
             for k, v in data.items():
                if isinstance(v, list):
                    data = v
                    break
        return pd.DataFrame(data)

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "fields": [
                {"name": "url", "type": "text", "label": "JSON URL", "placeholder": "https://api.example.com/data", "required": True},
                {"name": "headers", "type": "text", "label": "Headers (JSON String)", "placeholder": '{"Authorization": "Bearer ..."}', "required": False},
            ]
        }
