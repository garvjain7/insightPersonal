import sys
import warnings

# Suppress warnings IMMEDIATELY before any other imports
warnings.filterwarnings("ignore", category=UserWarning, module="requests")
warnings.filterwarnings("ignore", message=".*urllib3.*")

import base64
import json
import uuid

def _build_creds(payload: dict) -> dict:
    creds = dict(payload.get("credentials") or {})
    fb = payload.get("file_base64")
    if fb:
        creds["file_bytes"] = base64.b64decode(fb)
        creds["filename"] = payload.get("filename") or "upload.csv"
    sb = payload.get("service_account_base64")
    if sb:
        raw = base64.b64decode(sb)
        creds["service_account_json"] = json.loads(raw.decode("utf-8"))
    return creds

def main() -> None:
    # Force UTF-8 for reliable JSON communication on Windows
    if hasattr(sys.stdin, "reconfigure"):
        sys.stdin.reconfigure(encoding="utf-8")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    try:
        raw = sys.stdin.read()
        if not raw.strip():
            return
        
        payload = json.loads(raw)
        action = payload.get("action")

        if action == "catalog":
            from ml_engine.connectors.manager import get_catalog
            print(json.dumps({"success": True, "data": get_catalog()}, ensure_ascii=False), flush=True)
            return

        if action == "validate":
            from ml_engine.connectors.manager import get_connector
            conn_id = payload.get("connector")
            if not conn_id:
                print(json.dumps({"success": False, "message": "Missing connector"}), flush=True)
                return
            creds = _build_creds(payload)
            conn = get_connector(conn_id)
            result = conn.validate(creds)
            print(json.dumps({"success": True, "data": result}, ensure_ascii=False), flush=True)
            return

        if action == "fetch":
            from ml_engine.connectors.manager import get_connector
            from ml_engine.connectors.core import normalizer, storage
            
            conn_id = payload.get("connector")
            source = payload.get("source")
            if not conn_id or not source:
                print(json.dumps({"success": False, "message": "Missing connector or source"}), flush=True)
                return

            did = payload.get("dataset_id") or str(uuid.uuid4())
            hint = (payload.get("output_name_hint") or "dataset.csv").strip()
            
            creds = _build_creds(payload)
            conn = get_connector(conn_id)
            df = conn.fetch(creds, source)
            
            if df.empty:
                print(json.dumps({"success": False, "message": "Connector returned an empty dataset"}), flush=True)
                return

            dataset = normalizer.normalize(df, connector=conn_id, source=source, dataset_id=did)
            raw_path = storage.save_raw(df, did, hint)

            print(json.dumps({
                "success": True,
                "dataset_id": did,
                "file_name": raw_path.name,
                "dataset_display_name": f"{conn_id}: {source}"[:240]
            }, ensure_ascii=False), flush=True)
            return

        print(json.dumps({"success": False, "message": f"Unknown action: {action}"}), flush=True)
    except Exception as e:
        print(json.dumps({"success": False, "message": str(e)}, ensure_ascii=False), flush=True)

if __name__ == "__main__":
    main()
