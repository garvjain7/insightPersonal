"""
core/dataset.py
===============
Loads a CSV from /uploads/cleaned/, extracts a rich column-level schema,
and registers the DataFrame with DuckDB for SQL execution.

The loaded dataset is cached in memory (per dataset_id) so repeated
questions on the same file do not re-read disk.

What is extracted per column (sent to RAG / LLM):
  - name, dtype, inferred_type
  - null_count, null_pct, nunique
  - sample_values (N distinct, non-null values)
  - stats: min/max/mean/median for numeric columns
"""

from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import duckdb
import numpy as np
import pandas as pd

import re
from config import (
    RAG_MIN_SCORE,
    SAMPLE_VALUES_N,
    STATS_FOR_NUMERIC,
    UPLOADS_DIR,
)
from core.persistence import (
    get_artifact_dir,
    is_artifact_valid,
    ArtifactLock,
    atomic_save_json,
    load_json
)
from models.schemas import ColumnMeta, DatasetInfo

logger = logging.getLogger("chatbot.dataset")


# ── In-memory cache: dataset_id → LoadedDataset ──────────────────────────────

_cache: dict[str, "LoadedDataset"] = {}


class LoadedDataset:
    """
    Holds everything about a loaded dataset:
      - df          : the raw pandas DataFrame
      - conn        : a DuckDB connection with the DataFrame registered as a table
      - schema      : list of ColumnMeta (used by RAG + prompts)
      - dataset_id  : filename key
      - table_name  : DuckDB table name (always "dataset")
    """

    def __init__(self, dataset_id: str, df: pd.DataFrame, schema: list[ColumnMeta]):
        self.dataset_id  = dataset_id
        self.df          = df
        self.schema      = schema
        self.table_name  = "dataset"

        # Register with DuckDB — in-memory connection per dataset
        self.conn = duckdb.connect(database=":memory:")
        self.conn.register(self.table_name, df)
        logger.info(
            f"[{dataset_id}] Registered with DuckDB. "
            f"Rows: {len(df)}, Cols: {len(df.columns)}"
        )

    def info(self) -> DatasetInfo:
        # For DataInsights.ai, the path is uploads/cleaned/cleaned_{dataset_id}.csv
        cleaned_dir = UPLOADS_DIR / "cleaned"
        path = cleaned_dir / f"cleaned_{self.dataset_id}.csv"
        size_mb = round(path.stat().st_size / 1_048_576, 3) if path.exists() else 0.0
        return DatasetInfo(
            dataset_id=self.dataset_id,
            total_rows=len(self.df),
            total_cols=len(self.df.columns),
            columns=self.schema,
            file_size_mb=size_mb,
        )


# ── Public API ────────────────────────────────────────────────────────────────

def load(dataset_id: str) -> LoadedDataset:
    """
    Load a dataset by dataset_id from /uploads/cleaned/.
    Returns cached instance if already loaded.
    Uses schema caching to speed up repeated loads.
    """
    if dataset_id in _cache:
        logger.debug(f"[{dataset_id}] Returning cached dataset")
        return _cache[dataset_id]

    cleaned_dir = UPLOADS_DIR / "cleaned"
    actual_filename = f"cleaned_{dataset_id}.csv"
    path = cleaned_dir / actual_filename
    if not path.exists():
        raise FileNotFoundError(f"Dataset '{dataset_id}' not found at {path}")

    # Artifact paths
    art_dir = get_artifact_dir(str(UPLOADS_DIR), dataset_id)
    schema_path = str(art_dir / "schema_cache.json")
    lock_path = str(art_dir / "schema.lock")

    schema = None
    # 1. Try to load cached schema
    if is_artifact_valid(schema_path, str(path)):
        cached_data = load_json(schema_path)
        if cached_data:
            schema = [ColumnMeta(**c) for c in cached_data]
            logger.info(f"[{dataset_id}] Loaded schema from cache")

    # 2. Read CSV (Always needed for DuckDB registration)
    logger.info(f"[{dataset_id}] Loading CSV from disk...")
    df = _read_csv(path)

    # 3. Build schema if not loaded from cache
    if not schema:
        with ArtifactLock(lock_path):
            # Double-check after lock
            if is_artifact_valid(schema_path, str(path)):
                cached_data = load_json(schema_path)
                if cached_data: schema = [ColumnMeta(**c) for c in cached_data]

            if not schema:
                logger.info(f"[{dataset_id}] Inferring schema (cache miss/stale)...")
                schema = _build_schema(df)
                atomic_save_json(schema_path, [c.dict() for c in schema])

    loaded = LoadedDataset(dataset_id, df, schema)
    _cache[dataset_id] = loaded
    return loaded


def unload(dataset_id: str) -> None:
    """Remove a dataset from the in-memory cache."""
    if dataset_id in _cache:
        _cache[dataset_id].conn.close()
        del _cache[dataset_id]
        logger.info(f"[{dataset_id}] Unloaded from cache")


def list_available() -> list[str]:
    """
    Return all available dataset IDs.
    Looks for cleaned_*.csv files and returns the dataset_id (without prefix).
    Example: cleaned_abc123.csv → abc123
    """
    cleaned_dir = UPLOADS_DIR / "cleaned"
    if not cleaned_dir.exists():
        return []

    datasets = []
    for path in cleaned_dir.glob("cleaned_*.csv"):
        # Extract dataset_id from cleaned_{dataset_id}.csv
        name = path.name.replace("cleaned_", "", 1).replace(".csv", "")
        datasets.append(name)

    return sorted(datasets)


# ── CSV reading ───────────────────────────────────────────────────────────────

def _read_csv(path: Path) -> pd.DataFrame:
    """
    Read a CSV with header detection and basic sanitisation.
    - Strips whitespace from column names and string values
    - Replaces empty strings with NaN
    - Drops trailing footer rows (>80% null)
    """
    # Detect encoding
    try:
        df = pd.read_csv(path, dtype=str, skip_blank_lines=True)
    except UnicodeDecodeError:
        df = pd.read_csv(path, dtype=str, skip_blank_lines=True, encoding="latin-1")

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Strip whitespace from string values and replace empty strings with NaN
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].str.strip()
            df[col] = df[col].replace('', np.nan)

    # Drop rows that are >80% null (likely footer rows)
    null_pct = df.isnull().mean(axis=1)
    df = df[null_pct <= 0.8]

    return df


# ── Schema building ───────────────────────────────────────────────────────────

def _build_schema(df: pd.DataFrame) -> list[ColumnMeta]:
    """
    Extract rich metadata for each column.
    This is used by RAG to find relevant columns for a question.
    """
    schema = []

    for col_name in df.columns:
        series = df[col_name]
        null_count = series.isnull().sum()
        null_pct = (null_count / len(series)) * 100
        nunique = series.nunique(dropna=True)

        # Sample values (distinct, non-null, up to SAMPLE_VALUES_N)
        sample_values = series.dropna().unique()[:SAMPLE_VALUES_N].tolist()

        # Infer type
        inferred_type = _infer_type(series, col_name)

        # Stats for numeric columns
        stats = None
        if STATS_FOR_NUMERIC and inferred_type == "numeric":
            numeric_series = pd.to_numeric(series, errors='coerce').dropna()
            if len(numeric_series) > 0:
                stats = {
                    "min":   float(numeric_series.min()),
                    "max":   float(numeric_series.max()),
                    "mean":  float(numeric_series.mean()),
                }

        schema.append(ColumnMeta(
            name=col_name,
            dtype=str(series.dtype),
            inferred_type=inferred_type,
            null_count=int(null_count),
            null_pct=round(null_pct, 1),
            nunique=int(nunique),
            sample_values=sample_values,
            stats=stats,
        ))

    return schema


def _infer_type(series: pd.Series, col_name: str) -> str:
    """
    Infer the semantic type of a column.
    Optimized: only attempts expensive datetime parsing on likely columns.
    """
    # 1. Try numeric (fastest/most common)
    numeric_series = pd.to_numeric(series, errors='coerce')
    if numeric_series.notna().sum() / (len(numeric_series) + 1e-10) > 0.8:
        return "numeric"

    # 2. Check for datetime ONLY if name suggests it
    date_patterns = r"(date|time|timestamp|created|updated|dob|year|month|day)"
    if re.search(date_patterns, col_name.lower()):
        try:
            # Using errors='coerce' to avoid hard failures on messy data
            parsed = pd.to_datetime(series, errors='coerce')
            if parsed.notna().sum() / (len(series) + 1e-10) > 0.6: # Relaxed threshold for messy dates
                return "datetime"
        except:
            pass

    # 3. Check uniqueness ratio (categorical if low)
    unique_ratio = series.nunique() / (len(series) + 1e-10)
    if unique_ratio < 0.1:  # Less than 10% unique values
        return "categorical"

    # 4. Default to text
    return "text"