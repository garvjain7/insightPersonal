"""
models/schemas.py
=================
All Pydantic request/response models used across the API.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ── Enums ──────────────────────────────────────────────────────────────────────

class MessageRole(str, Enum):
    user      = "user"
    assistant = "assistant"
    system    = "system"


class IntentType(str, Enum):
    data_query   = "data_query"    # Requires SQL + data
    analytical   = "analytical"   # Requires SQL + deep reasoning
    general      = "general"       # Pure LLM, no SQL needed
    clarification = "clarification" # LLM asks user to clarify


# ── Chat message (stored in session history) ──────────────────────────────────

class ChatMessage(BaseModel):
    role:    MessageRole
    content: str


# ── API request bodies ────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    session_id: str  = Field(...,  description="Unique session identifier")
    dataset_id: str  = Field(...,  description="Dataset filename (without path)")
    question:   str  = Field(...,  min_length=1, max_length=2000)


class LoadDatasetRequest(BaseModel):
    dataset_id: str = Field(..., description="Filename of the dataset in /uploads/")


class ClearSessionRequest(BaseModel):
    session_id: str


# ── API response bodies ───────────────────────────────────────────────────────

class DatasetInfo(BaseModel):
    dataset_id:  str
    total_rows:  int
    total_cols:  int
    columns:     list[ColumnMeta]
    file_size_mb: float


class ColumnMeta(BaseModel):
    name:          str
    dtype:         str
    inferred_type: str                       # numeric / categorical / datetime / text
    null_count:    int
    null_pct:      float
    nunique:       int
    sample_values: list[Any]
    stats:         dict[str, Any] | None = None  # min/max/mean for numeric


class ChatResponse(BaseModel):
    session_id:    str
    answer:        str
    intent:        IntentType
    sql_executed:  str | None        = None
    rows_returned: int | None        = None
    columns_used:  list[str]         = Field(default_factory=list)
    error:         str | None        = None


class SessionInfo(BaseModel):
    session_id:    str
    dataset_id:    str | None
    message_count: int
    created_at:    str
    last_active:   str


class HealthResponse(BaseModel):
    status:      str
    ollama:      bool
    chat_model:  str
    embed_model: str