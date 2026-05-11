"""
cleaner.py — Workspace Initializer
===================================
Called ONCE by Node.js immediately after a dataset is uploaded.

Responsibilities:
  1. Detect the real header row (skip metadata/junk rows at the top)
  2. Strip leading/trailing whitespace from all string values and column names
  3. Copy the sanitized raw CSV → /uploads/temp/{dataset_id}/current_working.csv
  4. Compute the baseline Data Quality Score (missing, duplicate, outlier ratios)
  5. Write state.json  — all 5 steps initialised to "pending"
  6. Write metadata.json — empty per-step record
  7. Return a JSON summary to Node.js via stdout

This file does NOT apply any cleaning transformation.
Cleaning is entirely the user's job through the wizard.
"""

import argparse
import json
import logging
import os
import shutil
import sys

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [cleaner] %(levelname)s — %(message)s",
)
logger = logging.getLogger("cleaner")

# ── Constants ─────────────────────────────────────────────────────────────────

STEPS = ["null_values", "duplicates", "data_types", "outliers", "feature_engineering"]

# How many rows to scan when searching for the real header
HEADER_SCAN_ROWS = 30

# A column is considered "mostly populated" if ≥ this fraction of scan rows are non-empty
HEADER_POPULATION_THRESHOLD = 0.5


# ── Header Detection ──────────────────────────────────────────────────────────

def detect_header_row(file_path: str) -> int:
    """
    Return the 0-based index of the row that is most likely the real header.

    Strategy:
      - Read up to HEADER_SCAN_ROWS raw rows (no parsing assumptions).
      - Score each row: a good header has many non-empty, short, string-like cells
        and the rows BELOW it have a high data-population rate.
      - Row 0 wins unless a later row scores significantly better.
    """
    try:
        # Read raw without any header assumption
        raw = pd.read_csv(file_path, header=None, nrows=HEADER_SCAN_ROWS,
                          dtype=str, skip_blank_lines=False)
    except Exception as exc:
        logger.warning(f"Header scan failed, defaulting to row 0: {exc}")
        return 0

    n_rows, n_cols = raw.shape
    if n_rows == 0:
        return 0

    best_row = 0
    best_score = -1.0

    for i in range(min(10, n_rows)):          # only check first 10 rows as candidate headers
        candidate = raw.iloc[i].fillna("")
        below = raw.iloc[i + 1:] if i + 1 < n_rows else pd.DataFrame()

        # Header score: fraction of cells that look like column names
        # (non-numeric, reasonably short, non-empty)
        non_empty = candidate[candidate != ""]
        if len(non_empty) == 0:
            continue

        string_like = non_empty[
            non_empty.apply(lambda v: not _is_numeric_string(v) and len(str(v)) <= 60)
        ]
        header_score = len(string_like) / n_cols

        # Data-below score: fraction of cells in rows below that are non-empty
        if len(below) > 0:
            data_score = below.notna().values.sum() / (below.shape[0] * below.shape[1])
        else:
            data_score = 0.0

        combined = header_score * 0.6 + data_score * 0.4

        if combined > best_score:
            best_score = combined
            best_row = i

    return best_row


def _is_numeric_string(value: str) -> bool:
    try:
        float(str(value).replace(",", "").strip())
        return True
    except ValueError:
        return False


# ── Data Loading ──────────────────────────────────────────────────────────────

def load_raw_dataset(file_path: str) -> tuple[pd.DataFrame, int]:
    """
    Load a CSV respecting the detected header row.
    Returns (dataframe, header_row_index).
    Strips whitespace from column names and string values.
    """
    header_row = detect_header_row(file_path)
    logger.info(f"Detected header at row {header_row}")

    df = pd.read_csv(
        file_path,
        header=header_row,
        dtype=str,           # read everything as str first — we don't mutate types here
        skip_blank_lines=True,
    )

    # ── Strip whitespace ──────────────────────────────────────────────────────
    # Column names
    df.columns = [str(c).strip() for c in df.columns]

    # Remove completely unnamed/empty columns that sneak in from trailing commas
    df = df.loc[:, df.columns.str.strip() != ""]
    df = df.loc[:, ~df.columns.str.fullmatch(r"Unnamed:.*")]

    # Cell values
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # Replace empty strings with NaN for consistent null handling downstream
    df.replace("", np.nan, inplace=True)

    # ── Drop trailing footer rows ─────────────────────────────────────────────
    # Footer rows are rows where > 80 % of columns are null (common in Excel exports)
    null_frac = df.isnull().mean(axis=1)
    df = df[null_frac <= 0.80].copy()
    df.reset_index(drop=True, inplace=True)

    return df, header_row


# ── Quality Scoring ───────────────────────────────────────────────────────────

def _outlier_ratio(df: pd.DataFrame) -> float:
    numeric = df.select_dtypes(include=[np.number])
    if numeric.empty:
        return 0.0
    Q1 = numeric.quantile(0.25)
    Q3 = numeric.quantile(0.75)
    IQR = Q3 - Q1
    mask = (numeric < (Q1 - 1.5 * IQR)) | (numeric > (Q3 + 1.5 * IQR))
    total_cells = numeric.shape[0] * numeric.shape[1]
    return float(mask.values.sum() / total_cells) if total_cells > 0 else 0.0


def compute_quality_score(df: pd.DataFrame) -> dict:
    """
    Returns a dict with:
      - score          0-100
      - missing_ratio
      - duplicate_ratio
      - outlier_ratio
      - column_nulls   {col: count}
    """
    total_cells = df.shape[0] * df.shape[1]
    missing_ratio = float(df.isnull().sum().sum() / total_cells) if total_cells > 0 else 0.0
    duplicate_ratio = float(df.duplicated().sum() / len(df)) if len(df) > 0 else 0.0
    outlier_ratio = _outlier_ratio(df)

    score = 100.0
    score -= min(missing_ratio * 100 * 0.50, 30.0)   # up to –30 for missing
    score -= min(duplicate_ratio * 100 * 1.00, 20.0)  # up to –20 for dupes
    score -= min(outlier_ratio * 100 * 0.80, 20.0)    # up to –20 for outliers
    score = max(0.0, round(score, 2))

    column_nulls = df.isnull().sum().to_dict()

    return {
        "score": score,
        "missing_ratio": round(missing_ratio, 4),
        "duplicate_ratio": round(duplicate_ratio, 4),
        "outlier_ratio": round(outlier_ratio, 4),
        "total_rows": int(len(df)),
        "total_cols": int(len(df.columns)),
        "total_nulls": int(df.isnull().sum().sum()),
        "total_duplicates": int(df.duplicated().sum()),
        "column_nulls": {k: int(v) for k, v in column_nulls.items()},
    }


# ── Column Profile (for UI) ────────────────────────────────────────────────────

def build_column_profile(df: pd.DataFrame) -> list:
    """
    Return a lightweight per-column profile the UI can display immediately.
    We coerce types here for profiling only — the working dataset stays as-is.
    """
    profile = []
    for col in df.columns:
        series = df[col]
        null_count = int(series.isnull().sum())
        non_null = series.dropna()
        nunique = int(non_null.nunique())
        sample = non_null.head(3).tolist()

        # Infer dtype
        numeric_attempt = pd.to_numeric(non_null, errors="coerce")
        numeric_rate = float(numeric_attempt.notna().sum() / len(non_null)) if len(non_null) > 0 else 0.0

        if numeric_rate >= 0.80:
            inferred = "numeric"
        else:
            date_attempt = pd.to_datetime(non_null, errors="coerce", infer_datetime_format=True)
            date_rate = float(date_attempt.notna().sum() / len(non_null)) if len(non_null) > 0 else 0.0
            if date_rate >= 0.70:
                inferred = "datetime"
            elif nunique <= max(10, int(len(df) * 0.05)):
                inferred = "categorical"
            else:
                inferred = "text"

        profile.append({
            "name": col,
            "inferred_type": inferred,
            "null_count": null_count,
            "null_pct": round(null_count / len(df) * 100, 2) if len(df) > 0 else 0.0,
            "nunique": nunique,
            "sample": [str(v) for v in sample],
        })

    return profile


# ── Workspace Setup ────────────────────────────────────────────────────────────

def build_initial_state() -> dict:
    return {
        "steps": {step: "pending" for step in STEPS},
        "active_preview": None,   # which step currently has an un-applied preview
        "current_step": None,     # last step the user was on (for session recovery)
    }


def build_initial_metadata() -> dict:
    return {step: {} for step in STEPS}


# ── Main ──────────────────────────────────────────────────────────────────────

def initialise_workspace(raw_path: str, dataset_id: str, uploads_root: str) -> dict:
    """
    Full workspace initialisation pipeline.
    Returns a result dict that Node.js reads from stdout.
    """
    # ── Resolve paths ─────────────────────────────────────────────────────────
    temp_dir = os.path.join(uploads_root, "temp", str(dataset_id))
    logs_dir = os.path.join(temp_dir, "logs")
    working_path = os.path.join(temp_dir, "current_working.csv")
    state_path = os.path.join(temp_dir, "state.json")
    metadata_path = os.path.join(temp_dir, "metadata.json")

    os.makedirs(logs_dir, exist_ok=True)

    # ── Load + sanitise ───────────────────────────────────────────────────────
    try:
        df, header_row = load_raw_dataset(raw_path)
    except Exception as exc:
        logger.error(f"Failed to load raw dataset: {exc}")
        return {"status": "error", "message": f"Failed to read raw dataset: {str(exc)}"}

    if df.empty:
        return {"status": "error", "message": "Dataset is empty after loading"}

    # ── Write current_working.csv ─────────────────────────────────────────────
    try:
        df.to_csv(working_path, index=False)
        logger.info(f"Written current_working.csv ({len(df)} rows × {len(df.columns)} cols)")
    except Exception as exc:
        return {"status": "error", "message": f"Failed to write working dataset: {str(exc)}"}

    # ── Quality score ─────────────────────────────────────────────────────────
    # Re-read with proper type inference for scoring
    try:
        df_typed = pd.read_csv(working_path)
    except Exception:
        df_typed = df.copy()

    quality = compute_quality_score(df_typed)
    column_profile = build_column_profile(df_typed)

    # ── state.json ────────────────────────────────────────────────────────────
    state = build_initial_state()
    # Cache baseline stats in state.json
    state["stats"] = {
        "rows": quality["total_rows"],
        "cols": quality["total_cols"],
        "total_nulls": quality["total_nulls"],
        "is_preview": False
    }
    with open(state_path, "w") as fh:
        json.dump(state, fh, indent=2)

    # ── metadata.json ─────────────────────────────────────────────────────────
    metadata = build_initial_metadata()
    with open(metadata_path, "w") as fh:
        json.dump(metadata, fh, indent=2)

    logger.info(f"[{dataset_id}] Workspace initialised. Quality score: {quality['score']}")

    return {
        "status": "success",
        "dataset_id": dataset_id,
        "working_path": working_path,
        "header_row_detected": header_row,
        "quality": quality,
        "columns": column_profile,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialise a cleaning workspace")
    parser.add_argument("--raw_path", required=True, help="Absolute path to the raw CSV file")
    parser.add_argument("--dataset_id", required=True, help="Dataset ID (used for temp folder name)")
    parser.add_argument("--uploads_root", required=True, help="Absolute path to the uploads/ root directory")
    args = parser.parse_args()

    result = initialise_workspace(args.raw_path, args.dataset_id, args.uploads_root)
    print(json.dumps(result))
    sys.exit(0 if result.get("status") == "success" else 1)