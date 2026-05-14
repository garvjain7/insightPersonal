from typing import Any
from ..base import BaseConnector

class MySQLConnector(BaseConnector):
    """
    Connects to a MySQL or MariaDB database.
    config keys: { host, port, database, username, password }
    """

    def _get_conn(self, config: dict[str, Any]):
        try:
            import pymysql
        except ImportError:
            raise RuntimeError("pymysql is not installed")

        return pymysql.connect(
            host=config["host"],
            port=int(config.get("port", 3306)),
            db=config["database"],
            user=config["username"],
            password=config["password"],
            connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def validate(self, config: dict[str, Any]) -> dict:
        try:
            conn = self._get_conn(config)
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES")
                rows = cur.fetchall()
            conn.close()

            key = list(rows[0].keys())[0] if rows else None
            sources = [
                {
                    "id": row[key],
                    "label": row[key],
                    "meta": {"database": config["database"]},
                }
                for row in rows
            ] if key else []

            return {
                "status": "ok",
                "sources": sources,
                "message": f"Connected. Found {len(sources)} table(s)",
            }
        except Exception as e:
            return {"status": "error", "sources": [], "message": str(e)}

    def fetch(self, config: dict[str, Any], source: str):
        import pandas as pd
        conn = self._get_conn(config)
        try:
            df = pd.read_sql(f"SELECT * FROM `{source}`", conn)
        finally:
            conn.close()
        return df

    @classmethod
    def get_config_schema(cls) -> dict:
        return {
            "fields": [
                {"name": "host", "type": "text", "label": "Host", "placeholder": "localhost", "required": True},
                {"name": "database", "type": "text", "label": "Database", "required": True},
                {"name": "username", "type": "text", "label": "Username", "required": True},
                {"name": "password", "type": "password", "label": "Password", "required": True},
                {"name": "port", "type": "number", "label": "Port", "placeholder": "3306", "required": False},
            ]
        }
