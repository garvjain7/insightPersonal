"""Raw CSV lives in repo uploads/raw/ (same contract as Node uploadController)."""

from pathlib import Path

import pandas as pd

from .filename_sanitize import sanitize_filename

# ml_engine/connectors/core/storage.py → parents[3] == repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = REPO_ROOT / "uploads" / "raw"


def save_raw(df: pd.DataFrame, dataset_id: str, output_name_hint: str) -> Path:
    safe_suffix = sanitize_filename(output_name_hint)
    path = RAW_DIR / f"{dataset_id}_{safe_suffix}"
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path
