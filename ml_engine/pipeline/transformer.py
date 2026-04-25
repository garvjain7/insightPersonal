"""
transformer.py — Per-Step Cleaning Worker
==========================================
Called by Node.js for every wizard action during the cleaning flow.

CLI:
    python transformer.py
        --dataset_dir   /uploads/temp/{dataset_id}/
        --config        '{"type":"null_fill","params":{...}}'
        --mode          preview | skip | apply | finalize | get_stats | get_state
        --step          1-5   (required for preview / skip / apply)
        --dataset_id    used only for finalize (to build cleaned_ filename)
        --uploads_root  used only for finalize

Modes
─────
preview   Run transform on current_working.csv → write preview_output.csv
          Update state[step] = "previewed", write metadata entry
          Do NOT touch current_working.csv

skip      Copy current_working.csv → preview_output.csv (no-op transform)
          Update state[step] = "previewed", metadata mode = "skip"
          Overrides any previous preview for that step

apply     Copy preview_output.csv → current_working.csv
          Update state[step] = "committed"
          Invalidate all steps AFTER this step (reset to "pending", clear metadata)
          Returns { invalidated_steps: [...] } so Node.js can warn the UI

finalize  Copy current_working.csv → /uploads/cleaned/cleaned_{dataset_id}.csv
          Returns the path of the saved file

get_stats Return live statistics from current_working.csv

get_state Return state.json + metadata.json for session recovery
"""

import argparse
import json
import logging
import os
import shutil
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [transformer] %(levelname)s — %(message)s",
)
logger = logging.getLogger("transformer")

# ── Step ordering ──────────────────────────────────────────────────────────────
STEPS = ["null_values", "duplicates", "data_types", "outliers", "feature_engineering"]
STEP_INDEX = {name: i for i, name in enumerate(STEPS)}   # 0-based


def step_name(step_number: int) -> str:
    """Convert 1-based step number to canonical step name."""
    idx = int(step_number) - 1
    if idx < 0 or idx >= len(STEPS):
        raise ValueError(f"Invalid step number: {step_number}. Must be 1–{len(STEPS)}")
    return STEPS[idx]


# ── State / Metadata helpers ───────────────────────────────────────────────────

def read_json(path: str) -> dict:
    try:
        with open(path, "r") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning(f"Could not read {path}: {exc}")
        return {}


def write_json(path: str, data: dict) -> None:
    with open(path, "w") as fh:
        json.dump(data, fh, indent=2)


def _paths(dataset_dir: str) -> dict:
    return {
        "working": os.path.join(dataset_dir, "current_working.csv"),
        "preview": os.path.join(dataset_dir, "preview_output.csv"),
        "state": os.path.join(dataset_dir, "state.json"),
        "metadata": os.path.join(dataset_dir, "metadata.json"),
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Transformation logic ───────────────────────────────────────────────────────

def apply_null_fill(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    params = { col_name: strategy }
    Strategies: "Fill with 0" | "Fill with mean" | "Fill with median" |
                "Fill with mode" | "Drop rows" | "Fill with empty string"
    """
    for col, strategy in params.items():
        if col not in df.columns:
            continue
        if strategy == "Fill with 0":
            df[col] = df[col].fillna(0)
        elif strategy == "Fill with mean":
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(df[col].mean())
        elif strategy == "Fill with median":
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(df[col].median())
        elif strategy == "Fill with mode":
            mode_val = df[col].mode()
            if not mode_val.empty:
                df[col] = df[col].fillna(mode_val[0])
        elif strategy == "Drop rows":
            df = df.dropna(subset=[col])
        elif strategy == "Fill with empty string":
            df[col] = df[col].fillna("")
    return df


def apply_ai_null_fill(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    AI Decide for null values:
    - Numeric columns → fill with median (robust to outliers)
    - Categorical/object columns → fill with mode
    - Drop columns with > 70 % missing
    Returns (df, decisions) where decisions records what was done per column.
    """
    decisions = {}
    # Drop columns that are almost entirely empty
    high_null_cols = [c for c in df.columns if df[c].isnull().mean() > 0.70]
    if high_null_cols:
        df = df.drop(columns=high_null_cols)
        for c in high_null_cols:
            decisions[c] = "dropped_column (>70% missing)"

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].isnull().any():
            df[col] = df[col].fillna(df[col].median())
            decisions[col] = "Fill with median"

    cat_cols = df.select_dtypes(include=["object", "category"]).columns
    for col in cat_cols:
        if df[col].isnull().any():
            mode_val = df[col].mode()
            if not mode_val.empty:
                df[col] = df[col].fillna(mode_val[0])
                decisions[col] = "Fill with mode"

    return df, decisions


def apply_drop_duplicates(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    params = { strategy: "Keep first" | "Keep last" | "Drop all" }
    """
    strategy = params.get("strategy", "Keep first")
    if strategy == "Keep first":
        df = df.drop_duplicates(keep="first")
    elif strategy == "Keep last":
        df = df.drop_duplicates(keep="last")
    elif strategy == "Drop all":
        df = df.drop_duplicates(keep=False)
    return df


def apply_type_conversion(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    params = { col_name: target_type }
    target_type: "Integer" | "Float" | "String" | "Boolean" | "Date"
    """
    for col, target_type in params.items():
        if col not in df.columns:
            continue
        try:
            if target_type == "Integer":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            elif target_type == "Float":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)
            elif target_type == "String":
                df[col] = df[col].astype(str)
            elif target_type == "Boolean":
                bool_map = {
                    "true": True, "false": False,
                    "1": True, "0": False,
                    1: True, 0: False,
                    "yes": True, "no": False,
                    "y": True, "n": False,
                }
                df[col] = df[col].map(lambda v: bool_map.get(str(v).lower().strip(), np.nan))
            elif target_type == "Date":
                df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
        except Exception as exc:
            logger.warning(f"Type conversion failed for column '{col}': {exc}")
    return df


def apply_ai_type_conversion(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    AI Decide for data types:
    - Try to convert object columns that look numeric → float
    - Try to convert object columns that look like dates → datetime
    """
    decisions = {}
    obj_cols = df.select_dtypes(include=["object"]).columns

    for col in obj_cols:
        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue

        # Numeric check
        numeric_attempt = pd.to_numeric(non_null, errors="coerce")
        numeric_rate = numeric_attempt.notna().sum() / len(non_null)
        if numeric_rate >= 0.85:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            decisions[col] = "converted_to_numeric"
            continue

        # Datetime check
        date_attempt = pd.to_datetime(non_null, errors="coerce", infer_datetime_format=True)
        date_rate = date_attempt.notna().sum() / len(non_null)
        if date_rate >= 0.75:
            df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
            decisions[col] = "converted_to_datetime"

    return df, decisions


def apply_outlier_handling(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    params = { col_name: strategy }
    strategy: "Remove rows" | "IQR capping" | "Z-score capping"
    """
    for col, strategy in params.items():
        if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
            continue
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR

        if strategy == "Remove rows":
            df = df[(df[col] >= lower) & (df[col] <= upper)]
        elif strategy == "IQR capping":
            df[col] = df[col].clip(lower=lower, upper=upper)
        elif strategy == "Z-score capping":
            mean = df[col].mean()
            std = df[col].std()
            if std > 0:
                z_lower = mean - 3 * std
                z_upper = mean + 3 * std
                df[col] = df[col].clip(lower=z_lower, upper=z_upper)
    return df


def apply_ai_outlier_handling(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    AI Decide for outliers:
    - Use IQR capping on all numeric columns (preserves row count, less aggressive)
    """
    decisions = {}
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        outlier_count = int(((df[col] < lower) | (df[col] > upper)).sum())
        if outlier_count > 0:
            df[col] = df[col].clip(lower=lower, upper=upper)
            decisions[col] = f"iqr_capping ({outlier_count} outliers capped)"

    return df, decisions


def apply_feature_engineering(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """
    params = { features: [ { col, operation, inputs, ... } ] }
    """
    features = params.get("features", [])

    for feat in features:
        new_col = feat.get("col") or feat.get("output") or feat.get("newCol")
        operation = feat.get("operation") or feat.get("kind") or feat.get("type")
        inputs = feat.get("inputs") or []
        orig = feat.get("originalCol")

        if not inputs and orig:
            inputs = [orig]

        # Validate columns exist
        inputs = [c for c in inputs if c in df.columns]
        if not new_col or not inputs:
            logger.warning(f"Skipping feature '{new_col}': missing inputs or name")
            continue

        numeric_inputs = [
            pd.to_numeric(df[c], errors="coerce").fillna(0) if pd.api.types.is_numeric_dtype(df[c]) 
            else pd.Series(0, index=df.index) 
            for c in inputs
        ]

        try:
            if operation == "normalize":
                src = numeric_inputs[0]
                max_val = src.max()
                df[new_col] = src / max_val if max_val and max_val != 0 else 0.0

            elif operation == "boolean_flag":
                src = df[inputs[0]]
                df[new_col] = (src.notna() & (src.astype(str).str.strip() != "")).astype(int)

            elif operation == "product":
                result = numeric_inputs[0].copy()
                for s in numeric_inputs[1:]:
                    result = result * s
                df[new_col] = result

            elif operation == "sum":
                df[new_col] = sum(numeric_inputs)

            elif operation == "difference":
                first = numeric_inputs[0]
                second = numeric_inputs[1] if len(numeric_inputs) > 1 else pd.Series(0, index=df.index)
                df[new_col] = first - second

            elif operation == "ratio":
                numerator = numeric_inputs[0]
                denominator = numeric_inputs[1] if len(numeric_inputs) > 1 else pd.Series(1, index=df.index)
                df[new_col] = np.where(denominator != 0, numerator / denominator, np.nan)

            elif operation == "log":
                src = numeric_inputs[0].clip(lower=0)
                df[new_col] = np.log1p(src)

            elif operation == "zscore":
                src = numeric_inputs[0]
                mean, std = src.mean(), src.std()
                df[new_col] = (src - mean) / std if std > 0 else 0.0

            elif operation == "binning":
                src = numeric_inputs[0]
                n_bins = int(feat.get("bins", 5))
                df[new_col] = pd.cut(src, bins=n_bins, labels=False)

            else:
                # Fallback: normalize
                src = numeric_inputs[0]
                max_val = src.max()
                df[new_col] = src / max_val if max_val and max_val != 0 else 0.0
                logger.warning(f"Unknown operation '{operation}' — applied normalize as fallback")

        except Exception as exc:
            logger.error(f"Feature engineering failed for '{new_col}' (op={operation}): {exc}")

    return df


# ── Dispatch: run the right transform ─────────────────────────────────────────

def run_transform(df: pd.DataFrame, step_no: int, config: dict, is_ai: bool) -> tuple[pd.DataFrame, dict]:
    """
    Apply the transformation for the given step.
    Returns (transformed_df, decisions_dict).
    decisions_dict is populated by AI mode; manual mode returns empty dict.
    """
    t_type = config.get("type", "")
    params = config.get("params", {})
    decisions = {}

    step = step_name(step_no)

    if step == "null_values":
        if is_ai:
            df, decisions = apply_ai_null_fill(df)
        else:
            df = apply_null_fill(df, params)

    elif step == "duplicates":
        if is_ai:
            before = len(df)
            df = df.drop_duplicates(keep="first")
            after = len(df)
            if before > after:
                decisions["strategy"] = "Keep first"
                decisions["rows_removed"] = before - after
        else:
            df = apply_drop_duplicates(df, params)

    elif step == "data_types":
        if is_ai:
            df, decisions = apply_ai_type_conversion(df)
        else:
            df = apply_type_conversion(df, params)

    elif step == "outliers":
        if is_ai:
            df, decisions = apply_ai_outlier_handling(df)
        else:
            df = apply_outlier_handling(df, params)

    elif step == "feature_engineering":
        # No "AI decide" for feature engineering — must be manual
        df = apply_feature_engineering(df, params)

    else:
        raise ValueError(f"Unknown step type: {t_type} for step {step_no}")

    return df, decisions


# ── get_stats ──────────────────────────────────────────────────────────────────

def get_stats(working_path: str) -> dict:
    try:
        df = pd.read_csv(working_path)
    except Exception as exc:
        return {"status": "error", "message": str(exc)}

    numeric = df.select_dtypes(include=[np.number])
    Q1 = numeric.quantile(0.25)
    Q3 = numeric.quantile(0.75)
    IQR = Q3 - Q1
    outlier_mask = (numeric < (Q1 - 1.5 * IQR)) | (numeric > (Q3 + 1.5 * IQR))

    return {
        "status": "success",
        "total_rows": int(len(df)),
        "total_cols": int(len(df.columns)),
        "total_nulls": int(df.isnull().sum().sum()),
        "total_duplicates": int(df.duplicated().sum()),
        "total_outliers": int(outlier_mask.values.sum()) if not outlier_mask.empty else 0,
        "column_nulls": df.isnull().sum().to_dict(),
        "columns": list(df.columns),
    }


# ── get_state ──────────────────────────────────────────────────────────────────

def get_state(dataset_dir: str) -> dict:
    p = _paths(dataset_dir)
    state = read_json(p["state"])
    metadata = read_json(p["metadata"])

    working_exists = os.path.exists(p["working"])
    preview_exists = os.path.exists(p["preview"])

    # Use cached stats if available to avoid full CSV read
    stats = state.get("stats")

    return {
        "status": "success",
        "state": state,
        "metadata": metadata,
        "working_exists": working_exists,
        "preview_exists": preview_exists,
        "stats": stats
    }


# ── Mode handlers ──────────────────────────────────────────────────────────────

def handle_preview(dataset_dir: str, step_no: int, config: dict, is_ai: bool) -> dict:
    """
    Run transform on current_working → write preview_output.
    Do NOT touch current_working.
    """
    p = _paths(dataset_dir)

    if not os.path.exists(p["working"]):
        return {"status": "error", "message": "current_working.csv not found. Initialise workspace first."}

    try:
        df = pd.read_csv(p["working"])
    except Exception as exc:
        return {"status": "error", "message": f"Failed to read working dataset: {str(exc)}"}

    rows_before = len(df)
    cols_before = len(df.columns)
    nulls_before = int(df.isnull().sum().sum())
    dupes_before = int(df.duplicated().sum())

    try:
        df_out, decisions = run_transform(df, step_no, config, is_ai)
    except Exception as exc:
        return {"status": "error", "message": f"Transform failed: {str(exc)}"}

    try:
        df_out.to_csv(p["preview"], index=False)
    except Exception as exc:
        return {"status": "error", "message": f"Failed to write preview: {str(exc)}"}

    # Update state
    state = read_json(p["state"])
    step = step_name(step_no)
    state["steps"][step] = "previewed"
    state["active_preview"] = step
    state["current_step"] = step
    
    # Store preview stats in state for UI
    stats = {
        "rows": len(df_out),
        "cols": len(df_out.columns),
        "total_nulls": int(df_out.isnull().sum().sum()),
        "total_duplicates": int(df_out.duplicated().sum()),
        "column_nulls": df_out.isnull().sum().to_dict(),
        "is_preview": True
    }
    state["preview_stats"] = stats
    write_json(p["state"], state)

    # Update metadata
    metadata = read_json(p["metadata"])
    metadata[step] = {
        "mode": "ai" if is_ai else "manual",
        "params": config.get("params", {}),
        "decisions": decisions,
        "applied": False,
        "previewed_at": _now(),
        "rows_before": rows_before,
        "rows_after": len(df_out),
        "cols_before": cols_before,
        "cols_after": len(df_out.columns),
        "nulls_before": nulls_before,
        "nulls_after": int(df_out.isnull().sum().sum()),
        "dupes_before": dupes_before,
        "dupes_after": int(df_out.duplicated().sum())
    }
    write_json(p["metadata"], metadata)

    return {
        "status": "success",
        "mode": "preview",
        "step": step,
        "rows_before": rows_before,
        "rows_after": len(df_out),
        "cols_before": len(df.columns),
        "cols_after": len(df_out.columns),
        "decisions": decisions,
        "stats": stats
    }


def handle_skip(dataset_dir: str, step_no: int) -> dict:
    """
    Copy current_working → preview_output unchanged.
    Overrides any previous preview for this step.
    """
    p = _paths(dataset_dir)

    if not os.path.exists(p["working"]):
        return {"status": "error", "message": "current_working.csv not found."}

    try:
        shutil.copy2(p["working"], p["preview"])
    except Exception as exc:
        return {"status": "error", "message": f"Failed to copy for skip: {str(exc)}"}

    step = step_name(step_no)

    # Update state
    state = read_json(p["state"])
    step = step_name(step_no)
    state["steps"][step] = "previewed"
    state["active_preview"] = step
    state["current_step"] = step

    df = pd.read_csv(p["working"])
    stats = {
        "rows": len(df),
        "cols": len(df.columns),
        "total_nulls": int(df.isnull().sum().sum()),
        "total_duplicates": int(df.duplicated().sum()),
        "column_nulls": df.isnull().sum().to_dict(),
        "is_preview": True
    }
    state["preview_stats"] = stats
    write_json(p["state"], state)

    # Update metadata
    metadata = read_json(p["metadata"])
    metadata[step] = {
        "mode": "skip",
        "params": {},
        "decisions": {},
        "applied": False,
        "previewed_at": _now(),
        "rows_before": len(df),
        "rows_after": len(df),
        "cols_before": len(df.columns),
        "cols_after": len(df.columns),
        "nulls_before": int(df.isnull().sum().sum()),
        "nulls_after": int(df.isnull().sum().sum()),
        "dupes_before": int(df.duplicated().sum()),
        "dupes_after": int(df.duplicated().sum())
    }
    write_json(p["metadata"], metadata)

    return {
        "status": "success",
        "mode": "skip",
        "step": step,
        "message": "Step will be skipped. Click Apply Changes to confirm.",
        "stats": stats
    }


def handle_apply(dataset_dir: str, step_no: int) -> dict:
    """
    Copy preview_output → current_working (commit).
    Invalidate all steps AFTER this one.
    """
    p = _paths(dataset_dir)

    if not os.path.exists(p["preview"]):
        return {"status": "error", "message": "No preview found. Generate a preview first."}

    # ── Copy preview → working ────────────────────────────────────────────────
    try:
        shutil.copy2(p["preview"], p["working"])
    except Exception as exc:
        return {"status": "error", "message": f"Failed to commit preview: {str(exc)}"}

    step = step_name(step_no)
    step_idx = STEP_INDEX[step]

    # ── Invalidate downstream steps ───────────────────────────────────────────
    downstream = STEPS[step_idx + 1:]
    state = read_json(p["state"])
    state["steps"][step] = "committed"
    state["active_preview"] = None
    state["preview_stats"] = None  # Clear preview stats on commit

    invalidated = []
    for ds in downstream:
        if state["steps"].get(ds) in ("previewed", "committed", "skipped"):
            state["steps"][ds] = "pending"
            invalidated.append(ds)

    # Read final row/col count from working to cache as committed stats
    try:
        df = pd.read_csv(p["working"])
        stats = {
            "rows": len(df),
            "cols": len(df.columns),
            "total_nulls": int(df.isnull().sum().sum()),
            "total_duplicates": int(df.duplicated().sum()),
            "column_nulls": df.isnull().sum().to_dict(),
        }
        state["stats"] = stats
    except Exception as exc:
        logger.warning(f"Failed to cache stats after apply: {exc}")
        stats = None

    write_json(p["state"], state)

    # Clear metadata for invalidated steps
    metadata = read_json(p["metadata"])
    for ds in invalidated:
        metadata[ds] = {}
    metadata[step]["applied"] = True
    metadata[step]["applied_at"] = _now()
    write_json(p["metadata"], metadata)

    return {
        "status": "success",
        "mode": "apply",
        "step": step,
        "committed": True,
        "invalidated_steps": invalidated,
        "working_rows": stats["rows"] if stats else 0,
        "working_cols": stats["cols"] if stats else 0,
        "stats": stats
    }


def handle_finalize(dataset_dir: str, dataset_id: str, uploads_root: str) -> dict:
    """
    Copy current_working → /uploads/cleaned/cleaned_{dataset_id}.csv
    """
    p = _paths(dataset_dir)

    if not os.path.exists(p["working"]):
        return {"status": "error", "message": "current_working.csv not found. Cannot finalise."}

    cleaned_dir = os.path.join(uploads_root, "cleaned")
    os.makedirs(cleaned_dir, exist_ok=True)
    cleaned_path = os.path.join(cleaned_dir, f"cleaned_{dataset_id}.csv")

    try:
        shutil.copy2(p["working"], cleaned_path)
    except Exception as exc:
        return {"status": "error", "message": f"Failed to write cleaned dataset: {str(exc)}"}

    # Read final stats
    try:
        df = pd.read_csv(cleaned_path)
        rows, cols = len(df), len(df.columns)
    except Exception:
        rows, cols = None, None

    logger.info(f"[{dataset_id}] Finalised → {cleaned_path}")

    return {
        "status": "success",
        "mode": "finalize",
        "cleaned_path": cleaned_path,
        "final_rows": rows,
        "final_cols": cols,
    }


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="DataInsights cleaning transformer")
    parser.add_argument("--dataset_dir", required=True,
                        help="Path to /uploads/temp/{dataset_id}/")
    parser.add_argument("--mode", required=True,
                        choices=["preview", "skip", "apply", "finalize",
                                 "get_stats", "get_state"],
                        help="Action to perform")
    parser.add_argument("--step", type=int, default=None,
                        help="Step number 1-5 (required for preview/skip/apply)")
    parser.add_argument("--config", default="{}",
                        help="JSON config string for the transformation")
    parser.add_argument("--dataset_id", default=None,
                        help="Dataset ID (required for finalize)")
    parser.add_argument("--uploads_root", default=None,
                        help="Absolute path to uploads/ root (required for finalize)")
    args = parser.parse_args()

    try:
        config = json.loads(args.config)
    except json.JSONDecodeError as exc:
        print(json.dumps({"status": "error", "message": f"Invalid config JSON: {str(exc)}"}))
        sys.exit(1)

    p = _paths(args.dataset_dir)
    mode = args.mode

    # ── Route ─────────────────────────────────────────────────────────────────
    if mode == "get_stats":
        result = get_stats(p["working"])

    elif mode == "get_state":
        result = get_state(args.dataset_dir)

    elif mode in ("preview", "skip", "apply"):
        if args.step is None:
            result = {"status": "error", "message": f"--step is required for mode='{mode}'"}
        elif mode == "preview":
            is_ai = config.get("ai", False)
            result = handle_preview(args.dataset_dir, args.step, config, is_ai)
        elif mode == "skip":
            result = handle_skip(args.dataset_dir, args.step)
        elif mode == "apply":
            result = handle_apply(args.dataset_dir, args.step)

    elif mode == "finalize":
        if not args.dataset_id or not args.uploads_root:
            result = {"status": "error",
                      "message": "--dataset_id and --uploads_root are required for finalize"}
        else:
            result = handle_finalize(args.dataset_dir, args.dataset_id, args.uploads_root)

    else:
        result = {"status": "error", "message": f"Unknown mode: {mode}"}

    print(json.dumps(result, default=str))
    sys.exit(0 if result.get("status") == "success" else 1)


if __name__ == "__main__":
    main()