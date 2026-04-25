"""
core/sql_generator.py
=====================
Converts a natural language question into one or more DuckDB SQL queries.

For analytical questions (why/compare/trend), generates a query plan
— a sequence of up to 4 focused queries — so each query answers one
sub-question. Results from each are passed forward for reasoning.

For data queries, generates a single focused SELECT.

Only the relevant columns (from RAG) are included in the schema context.
The table is always named "dataset".
"""

from __future__ import annotations

import json
import logging
import re

from core.llm import chat
from models.schemas import ColumnMeta, IntentType

logger = logging.getLogger("chatbot.sql_generator")


# ── System prompts ────────────────────────────────────────────────────────────

_BASE_RULES = """
Rules:
- Table name is always: dataset
- Only write SELECT queries. No INSERT, UPDATE, DELETE, DROP, CREATE, ALTER.
- Use DuckDB SQL syntax (supports strftime, date_diff, QUALIFY, etc.)
- Column names with spaces must be quoted: "Column Name"
- For date/time columns use: strftime('%Y-%m', date_col) for month grouping
- Always LIMIT results to 100 rows maximum unless doing aggregations
- Return only SQL — no explanation, no markdown fences, no extra text
"""

_SINGLE_QUERY_SYSTEM = f"""You are a SQL expert. Convert the user question into a single DuckDB SQL query.
{_BASE_RULES}"""

_PLAN_SYSTEM = f"""You are a SQL expert and data analyst. The user has an analytical question that needs
multiple SQL queries to answer properly.

Break the question into up to 4 focused sub-questions, and write one SQL query for each.
Return a JSON array of objects with this exact structure:
[
  {{"sub_question": "What is the monthly sales for Product A?", "sql": "SELECT ..."}},
  {{"sub_question": "What products launched in the last 2 months?", "sql": "SELECT ..."}}
]

{_BASE_RULES}
Return only the JSON array — no explanation, no markdown."""


# ── Public API ────────────────────────────────────────────────────────────────

async def generate(
    question:      str,
    columns:       list[ColumnMeta],
    intent:        IntentType,
    table_name:    str = "dataset",
) -> list[dict[str, str]]:
    """
    Generate SQL queries for the given question.

    Returns a list of dicts:
      [{ "sub_question": str, "sql": str }, ...]

    For data_query: always returns exactly one item.
    For analytical: may return 1-4 items (query plan).
    """
    schema_context = _build_schema_context(columns)

    if intent == IntentType.analytical:
        return await _generate_plan(question, schema_context)
    else:
        return await _generate_single(question, schema_context)


# ── Single query ──────────────────────────────────────────────────────────────

async def _generate_single(
    question:       str,
    schema_context: str,
) -> list[dict[str, str]]:
    prompt = f"{schema_context}\n\nQuestion: {question}"
    try:
        sql = await chat(
            messages=[
                {"role": "system", "content": _SINGLE_QUERY_SYSTEM},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.1,
        )
        sql = _clean_sql(sql)
        if not sql:
            return []
        return [{"sub_question": question, "sql": sql}]
    except Exception as exc:
        logger.error(f"SQL generation failed: {exc}")
        return []


# ── Query plan (analytical) ───────────────────────────────────────────────────

async def _generate_plan(
    question:       str,
    schema_context: str,
) -> list[dict[str, str]]:
    prompt = f"{schema_context}\n\nAnalytical question: {question}"
    try:
        raw = await chat(
            messages=[
                {"role": "system", "content": _PLAN_SYSTEM},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.1,
        )
        # Strip any markdown code fences Ollama might add
        raw = re.sub(r"```(?:json)?|```", "", raw).strip()
        plan = json.loads(raw)

        if not isinstance(plan, list):
            raise ValueError("Expected a JSON array")

        validated = []
        for item in plan[:4]:                        # cap at 4 sub-queries
            sql = _clean_sql(item.get("sql", ""))
            sub = item.get("sub_question", question)
            if sql:
                validated.append({"sub_question": sub, "sql": sql})

        logger.info(f"Query plan generated: {len(validated)} sub-queries")
        return validated

    except (json.JSONDecodeError, ValueError, KeyError) as exc:
        logger.warning(f"Plan parsing failed ({exc}). Falling back to single query.")
        return await _generate_single(question, schema_context)
    except Exception as exc:
        logger.error(f"Plan generation failed: {exc}")
        return []


# ── Schema context builder ────────────────────────────────────────────────────

def _build_schema_context(columns: list[ColumnMeta]) -> str:
    """
    Build a compact schema description to inject into the SQL prompt.
    Only includes the columns retrieved by RAG — not the full schema.
    """
    lines = ["Dataset schema (relevant columns only):"]
    for col in columns:
        samples = ", ".join(str(v) for v in col.sample_values[:4])
        line = f"  - {col.name} ({col.inferred_type})"
        if samples:
            line += f"  →  samples: [{samples}]"
        if col.stats:
            line += f"  range: {col.stats['min']}–{col.stats['max']}"
        lines.append(line)
    return "\n".join(lines)


# ── SQL cleaner ───────────────────────────────────────────────────────────────

def _clean_sql(raw: str) -> str:
    """
    Strip markdown fences, leading/trailing whitespace.
    Returns empty string if the result doesn't look like SQL.
    """
    sql = re.sub(r"```(?:sql)?|```", "", raw).strip()
    # Must start with SELECT
    if not re.match(r"^\s*SELECT\b", sql, re.IGNORECASE):
        logger.warning(f"Generated SQL does not start with SELECT: {sql[:80]!r}")
        return ""
    return sql