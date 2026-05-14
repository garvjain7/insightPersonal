from typing import Any
from ..base import BaseConnector

class GoogleSheetsConnector(BaseConnector):
    """
    Connects to Google Sheets via a Service Account.
    config keys: {
        "spreadsheet_id": str,
        "service_account_json": dict
    }
    """

    def _get_client(self, service_account_json: dict):
        try:
            import gspread
            from google.oauth2.service_account import Credentials
        except ImportError:
            raise RuntimeError("gspread and google-auth are not installed")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = Credentials.from_service_account_info(service_account_json, scopes=scopes)
        return gspread.authorize(creds)

    def validate(self, config: dict[str, Any]) -> dict:
        try:
            client = self._get_client(config["service_account_json"])
            spreadsheet = client.open_by_key(config["spreadsheet_id"])
            worksheets = spreadsheet.worksheets()
            sources = [
                {
                    "id": ws.title,
                    "label": ws.title,
                    "meta": {
                        "rows": ws.row_count,
                        "cols": ws.col_count,
                    },
                }
                for ws in worksheets
            ]
            return {
                "status": "ok",
                "sources": sources,
                "message": f"Connected to '{spreadsheet.title}'. Found {len(sources)} sheet(s)",
            }
        except Exception as e:
            return {"status": "error", "sources": [], "message": str(e)}

    def fetch(self, config: dict[str, Any], source: str):
        import pandas as pd
        client = self._get_client(config["service_account_json"])
        spreadsheet = client.open_by_key(config["spreadsheet_id"])
        worksheet = spreadsheet.worksheet(source)
        records = worksheet.get_all_records()
        return pd.DataFrame(records)

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "fields": [
                {
                    "name": "spreadsheet_id", 
                    "type": "text", 
                    "label": "Spreadsheet ID", 
                    "placeholder": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms", 
                    "required": True,
                    "hint": "Found in the spreadsheet URL between /d/ and /edit"
                },
                {
                    "name": "service_account_file", 
                    "type": "file", 
                    "label": "Service Account JSON", 
                    "required": True,
                    "hint": "Download from Google Cloud Console → IAM → Service Accounts"
                },
            ]
        }
