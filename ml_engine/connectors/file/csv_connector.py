from io import BytesIO
from typing import Any
from ..base import BaseConnector

class CSVConnector(BaseConnector):
    """config keys: { "file_bytes": bytes, "filename": str }; source: \"default\" """

    def validate(self, config: dict[str, Any]) -> dict:
        import pandas as pd
        try:
            file_bytes: bytes = config["file_bytes"]
            df = pd.read_csv(BytesIO(file_bytes), nrows=5)
            return {
                "status": "ok",
                "sources": [
                    {
                        "id": "default",
                        "label": config.get("filename", "file.csv"),
                        "meta": {
                            "preview_columns": list(df.columns),
                            "preview_rows": len(df),
                        },
                    }
                ],
                "message": f"Found {len(df.columns)} columns",
            }
        except Exception as e:
            return {"status": "error", "sources": [], "message": str(e)}

    def fetch(self, config: dict[str, Any], source: str):
        import pandas as pd
        file_bytes: bytes = config["file_bytes"]
        return pd.read_csv(BytesIO(file_bytes))

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "fields": [
                {"name": "file", "type": "file", "label": "CSV File", "required": True},
            ]
        }
