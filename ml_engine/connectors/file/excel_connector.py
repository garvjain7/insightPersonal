from io import BytesIO
from typing import Any
from ..base import BaseConnector

class ExcelConnector(BaseConnector):
    def validate(self, config: dict[str, Any]) -> dict:
        import pandas as pd
        try:
            file_bytes: bytes = config["file_bytes"]
            # Load sheet names without reading whole file
            xl = pd.ExcelFile(BytesIO(file_bytes))
            sources = []
            for sheet in xl.sheet_names:
                sources.append({"id": sheet, "label": sheet})
            
            return {
                "status": "ok",
                "sources": sources,
                "message": f"Found {len(sources)} sheets",
            }
        except Exception as e:
            return {"status": "error", "sources": [], "message": str(e)}

    def fetch(self, config: dict[str, Any], source: str):
        import pandas as pd
        file_bytes: bytes = config["file_bytes"]
        return pd.read_excel(BytesIO(file_bytes), sheet_name=source)

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "fields": [
                {"name": "file", "type": "file", "label": "Excel File", "required": True},
            ]
        }
