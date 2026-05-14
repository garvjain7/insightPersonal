from typing import Any
from ..base import BaseConnector

class PostgreSQLConnector(BaseConnector):
    def validate(self, config: dict[str, Any]) -> dict:
        import pandas as pd
        import psycopg2
        try:
            conn = psycopg2.connect(
                host=config.get("host"),
                database=config.get("database"),
                user=config.get("user"),
                password=config.get("password"),
                port=config.get("port", 5432)
            )
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            conn.close()
            
            sources = [{"id": t[0], "label": t[0]} for t in tables]
            return {
                "status": "ok",
                "sources": sources,
                "message": f"Connected! Found {len(sources)} tables in public schema.",
            }
        except Exception as e:
            return {"status": "error", "sources": [], "message": str(e)}

    def fetch(self, config: dict[str, Any], source: str):
        import pandas as pd
        import psycopg2
        conn_str = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config.get('port', 5432)}/{config['database']}"
        return pd.read_sql(f"SELECT * FROM {source} LIMIT 50000", conn_str)

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "fields": [
                {"name": "host", "type": "text", "label": "Host", "placeholder": "localhost", "required": True},
                {"name": "database", "type": "text", "label": "Database", "required": True},
                {"name": "user", "type": "text", "label": "User", "required": True},
                {"name": "password", "type": "password", "label": "Password", "required": True},
                {"name": "port", "type": "number", "label": "Port", "placeholder": "5432", "required": False},
            ]
        }
