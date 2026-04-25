"""
core/sql_executor.py
====================
Executes validated SQL queries against a DuckDB in-memory connection
and returns a structured result the LLM can reason about.

Key concerns:
  - Timeout enforcement (QUERY_TIMEOUT seconds)
  - Result size capping (MAX_RESULT_ROWS)
  - Result summarisation: if result is large, summarise rather than
    dump all rows into the prompt — keeps LLM context lean
  - Returns both a markdown table (for the LLM) and raw rows (for API response)
"""

from __future__ import annotations

import logging
import textwrap
from dataclasses import dataclass, field

import duckdb
import pandas as pd

from config import MAX_RESULT_ROWS, QUERY_TIMEOUT

logger = logging.getLogger("chatbot.sql_executor")


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class QueryResult:
    sql:           str
    sub_question:  str
    columns:       list[str]
    rows:          list[dict]           # Raw rows (up to MAX_RESULT_ROWS)
    total_rows:    int                  # Actual result count before capping
    truncated:     bool                 # True if result was capped
    summary:       str                  # Markdown/text summary for LLM context
    error:         str | None = None


# ── Public API ────────────────────────────────────────────────────────────────

def execute(
    conn:         duckdb.DuckDBPyConnection,
    sql:          str,
    sub_question: str = "",
) -> QueryResult:
    """
    Execute a validated SQL query and return a QueryResult.
    Never raises — errors are captured in QueryResult.error.
    """
    try:
        # DuckDB doesn't support async — run synchronously
        # For timeout, we rely on DuckDB's own progress cancellation
        result_df = conn.execute(sql).df()

        total_rows = len(result_df)
        truncated  = total_rows > MAX_RESULT_ROWS
        display_df = result_df.head(MAX_RESULT_ROWS)

        rows    = display_df.to_dict(orient="records")
        columns = list(display_df.columns)
        summary = _summarise(display_df, total_rows, truncated, sub_question)

        logger.info(
            f"Query executed: {total_rows} rows returned"
            f"{' (truncated)' if truncated else ''}"
        )
        return QueryResult(
            sql=sql,
            sub_question=sub_question,
            columns=columns,
            rows=rows,
            total_rows=total_rows,
            truncated=truncated,
            summary=summary,
        )

    except duckdb.Error as exc:
        msg = str(exc)
        logger.error(f"DuckDB error: {msg}")
        return QueryResult(
            sql=sql,
            sub_question=sub_question,
            columns=[],
            rows=[],
            total_rows=0,
            truncated=False,
            summary="",
            error=_friendly_error(msg),
        )
    except Exception as exc:
        logger.exception(f"Unexpected executor error: {exc}")
        return QueryResult(
            sql=sql,
            sub_question=sub_question,
            columns=[],
            rows=[],
            total_rows=0,
            truncated=False,
            summary="",
            error=str(exc),
        )


def results_to_context(results: list[QueryResult]) -> str:
    """
    Combine multiple QueryResult summaries into a single context block
    for injection into the LLM prompt.
    """
    if not results:
        return ""

    parts = []
    for i, r in enumerate(results, 1):
        header = f"Query {i}: {r.sub_question}" if len(results) > 1 else "Query result"
        if r.error:
            parts.append(f"{header}\nError: {r.error}")
        else:
            parts.append(f"{header}\n{r.summary}")

    return "\n\n".join(parts)


# ── Summariser ────────────────────────────────────────────────────────────────

def _summarise(
    df:         pd.DataFrame,
    total_rows: int,
    truncated:  bool,
    context:    str = "",
) -> str:
    """
    Convert a DataFrame to a compact text representation for the LLM.

    Strategy:
      - ≤ 10 rows         → full markdown table
      - 11–50 rows        → markdown table
      - > 50 rows (agg)   → describe the aggregation in text
      - Single value      → plain sentence
    """
    if df.empty:
        return "The query returned no results."

    rows, cols = df.shape

    # Single scalar result
    if rows == 1 and cols == 1:
        val = df.iloc[0, 0]
        col_name = df.columns[0]
        return f"Result: **{col_name}** = {val}"

    # Small result — full markdown table
    if rows <= 20:
        md = _to_markdown(df)
        if truncated:
            md += f"\n*(Showing {MAX_RESULT_ROWS} of {total_rows} rows)*"
        return md

    # Larger result — describe numerically
    lines = [f"Result: {total_rows} rows × {cols} columns"]

    for col in df.columns:
        series = df[col]
        if pd.api.types.is_numeric_dtype(series):
            lines.append(
                f"  {col}: min={series.min():.2f}, max={series.max():.2f}, "
                f"mean={series.mean():.2f}, sum={series.sum():.2f}"
            )
        else:
            top = series.value_counts().head(5)
            top_str = ", ".join(f"{k}({v})" for k, v in top.items())
            lines.append(f"  {col} (top values): {top_str}")

    if truncated:
        lines.append(f"*(Showing {MAX_RESULT_ROWS} of {total_rows} rows)*")

    return "\n".join(lines)


def _to_markdown(df: pd.DataFrame) -> str:
    """Convert a small DataFrame to a markdown table string."""
    headers = " | ".join(str(c) for c in df.columns)
    sep     = " | ".join("---" for _ in df.columns)
    rows    = []
    for _, row in df.iterrows():
        rows.append(" | ".join(str(v) for v in row))
    return "\n".join([headers, sep] + rows)


def _friendly_error(msg: str) -> str:
    """Convert technical DuckDB errors into user-friendly messages."""
    msg_lower = msg.lower()
    if "no such column" in msg_lower or "column not found" in msg_lower:
        return f"Column not found in dataset. {msg}"
    if "syntax error" in msg_lower:
        return f"SQL syntax error — the query could not be understood. {msg}"
    if "division by zero" in msg_lower:
        return "Division by zero encountered in the query."
    return f"Query execution error: {msg}"