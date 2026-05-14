from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class ConnectorInfo(BaseModel):
    id: str
    label: str
    category: str
    tier: str
    description: str
    auth_type: str
    available: bool


class ColumnMeta(BaseModel):
    name: str
    dtype: str
    nullable: bool


class DatasetMeta(BaseModel):
    dataset_id: str
    connector: str
    source: str
    imported_at: str
    row_count_total: int
    row_count_stored: int
    column_count: int
    truncated: bool
    truncation_limit: int


class NormalizedDataset(BaseModel):
    meta: DatasetMeta
    columns: list[ColumnMeta]
    rows: list[dict[str, Any]]


class DatasetSummary(BaseModel):
    dataset_id: str
    connector: str
    source: str
    imported_at: str
    row_count_stored: int
    column_count: int
    truncated: bool
