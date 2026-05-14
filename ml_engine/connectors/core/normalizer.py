from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from .models import ColumnMeta, DatasetMeta, NormalizedDataset

ROW_STORAGE_LIMIT = 10_000


def _infer_dtype(series: pd.Series) -> str:
    dtype = str(series.dtype)
    if dtype.startswith("int"):
        return "integer"
    if dtype.startswith("float"):
        return "float"
    if dtype == "bool":
        return "boolean"
    if dtype.startswith("datetime"):
        return "datetime"
    return "string"


def _clean_value(val: Any) -> Any:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, (pd.Timestamp, datetime)):
        return val.isoformat()
    if isinstance(val, (np.bool_,)):
        return bool(val)
    return val


def normalize(
    df: pd.DataFrame,
    connector: str,
    source: str,
    dataset_id: str,
) -> NormalizedDataset:
    total_rows = len(df)
    truncated = total_rows > ROW_STORAGE_LIMIT

    if truncated:
        df_stored = df.head(ROW_STORAGE_LIMIT).copy()
    else:
        df_stored = df.copy()

    columns = []
    for col in df_stored.columns:
        columns.append(
            ColumnMeta(
                name=str(col),
                dtype=_infer_dtype(df_stored[col]),
                nullable=bool(df_stored[col].isnull().any()),
            )
        )

    rows = []
    for _, row in df_stored.iterrows():
        rows.append({str(k): _clean_value(v) for k, v in row.items()})

    meta = DatasetMeta(
        dataset_id=dataset_id,
        connector=connector,
        source=source,
        imported_at=datetime.now(timezone.utc).isoformat(),
        row_count_total=total_rows,
        row_count_stored=len(rows),
        column_count=len(columns),
        truncated=truncated,
        truncation_limit=ROW_STORAGE_LIMIT,
    )

    return NormalizedDataset(meta=meta, columns=columns, rows=rows)
