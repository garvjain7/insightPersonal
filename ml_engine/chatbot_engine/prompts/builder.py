"""
prompts/builder.py
==================
Builds the final message list sent to Ollama for the answer generation step.

The prompt is assembled from these layers (in order):
  1. System message — role, dataset context, behaviour rules
  2. Chat history   — rolling window of past messages (HISTORY_WINDOW pairs)
  3. Data context   — SQL results formatted for LLM reasoning (if any)
  4. User message   — the current question

Nothing outside this file decides what goes into the prompt.
This makes it easy to tune without touching business logic.
"""

from __future__ import annotations

from models.schemas import ChatMessage, ColumnMeta, IntentType, MessageRole


# ── System prompt ─────────────────────────────────────────────────────────────

_SYSTEM_TEMPLATE = """You are a data analyst assistant with direct access to a dataset.
Your job is to answer questions about the data clearly and accurately.

Dataset: {dataset_id}
Total rows: {total_rows:,}
Total columns: {total_cols}

Relevant columns for this question:
{schema_section}

Behaviour rules:
- Answer in plain English. Be concise but complete.
- If you ran SQL queries, explain the results — do not just repeat raw numbers.
- For analytical questions (why/trend/compare), reason through the data step by step.
- If the data is insufficient to answer fully, say so clearly.
- Never make up data. Only use what the query results show.
- Format numbers with commas for readability (e.g. 1,234,567).
- If results suggest an interesting follow-up insight, briefly mention it.
"""

_SYSTEM_WITH_DATA_SECTION = """
Query results:
{data_context}
"""


# ── Public API ────────────────────────────────────────────────────────────────

def build(
    question:       str,
    dataset_id:     str,
    total_rows:     int,
    total_cols:     int,
    relevant_cols:  list[ColumnMeta],
    history:        list[ChatMessage],
    data_context:   str | None,
    intent:         IntentType,
) -> list[dict]:
    """
    Assemble the full messages list for Ollama.

    Returns a list of {"role": ..., "content": ...} dicts.
    """
    messages: list[dict] = []

    # 1. System message
    schema_section = _format_schema(relevant_cols)
    system_content = _SYSTEM_TEMPLATE.format(
        dataset_id=dataset_id,
        total_rows=total_rows,
        total_cols=total_cols,
        schema_section=schema_section,
    )

    if data_context:
        system_content += _SYSTEM_WITH_DATA_SECTION.format(data_context=data_context)

    messages.append({"role": "system", "content": system_content})

    # 2. Chat history (rolling window)
    for msg in history:
        messages.append({"role": msg.role.value, "content": msg.content})

    # 3. Current user question
    messages.append({"role": "user", "content": question})

    return messages


# ── Schema formatter ──────────────────────────────────────────────────────────

def _format_schema(columns: list[ColumnMeta]) -> str:
    """
    Format the relevant columns for the system prompt.
    Compact but informative — enough for the LLM to reason about the data.
    """
    if not columns:
        return "  (no specific columns identified)"

    lines = []
    for col in columns:
        samples = ", ".join(str(v) for v in col.sample_values[:4])
        line = f"  • {col.name} ({col.inferred_type})"
        if samples:
            line += f"  —  e.g. {samples}"
        if col.stats:
            line += f"  [range: {col.stats['min']} → {col.stats['max']}]"
        if col.null_pct > 0:
            line += f"  [{col.null_pct:.1f}% null]"
        lines.append(line)

    return "\n".join(lines)