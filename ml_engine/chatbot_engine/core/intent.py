"""
core/intent.py
==============
Classifies the user's question before any SQL or LLM work happens.

Intent types:
  data_query    → Question about specific values, counts, filters, rankings
  analytical    → "Why" / "compare" / "trend" questions needing multi-step SQL
  general       → Dataset metadata, clarifications, greetings — no SQL needed

This is a two-stage classifier:
  Stage 1: Fast rule-based check (regex patterns, zero latency)
  Stage 2: If ambiguous, ask Ollama with a tiny prompt (< 200 tokens)

Keeping this fast is critical — it runs before every SQL generation.
"""

from __future__ import annotations

import logging
import re

from core.llm import chat
from models.schemas import IntentType

logger = logging.getLogger("chatbot.intent")


# ── Keyword patterns ──────────────────────────────────────────────────────────

_DATA_QUERY_PATTERNS = re.compile(
    r"\b(show|list|find|get|fetch|count|how many|what is|what are|top|bottom|"
    r"average|avg|sum|total|max|min|highest|lowest|filter|where|group|rank|"
    r"compare|between|greater|less|equal|select|query|rows|records)\b",
    re.IGNORECASE,
)

_ANALYTICAL_PATTERNS = re.compile(
    r"\b(why|reason|cause|because|explain|analyse|analyze|trend|pattern|"
    r"performance|affect|impact|influence|correlation|relationship|compared to|"
    r"vs|versus|over time|month|year|quarter|dropped|increased|declined|grew|"
    r"insight|unusual|anomaly|outlier|distribution|changed|difference)\b",
    re.IGNORECASE,
)

_GENERAL_PATTERNS = re.compile(
    r"\b(how many columns|how many rows|what columns|column names|dataset|"
    r"schema|structure|hello|hi|help|what can you|tell me about the dataset|"
    r"describe the dataset|overview)\b",
    re.IGNORECASE,
)


# ── Public API ────────────────────────────────────────────────────────────────

async def classify(question: str) -> IntentType:
    """
    Return the IntentType for a user question.
    Fast path first, LLM fallback only when ambiguous.
    """
    # Stage 1: rule-based
    intent = _rule_based(question)
    if intent is not None:
        logger.debug(f"Intent (rule-based): {intent.value}")
        return intent

    # Stage 2: LLM — only for ambiguous cases
    intent = await _llm_classify(question)
    logger.debug(f"Intent (LLM): {intent.value}")
    return intent


# ── Stage 1: Rule-based ───────────────────────────────────────────────────────

def _rule_based(question: str) -> IntentType | None:
    """
    Returns an IntentType if confidence is high, None if ambiguous.
    """
    q = question.strip()

    # Very short, clearly meta questions
    if len(q) < 30 and _GENERAL_PATTERNS.search(q):
        return IntentType.general

    has_analytical = bool(_ANALYTICAL_PATTERNS.search(q))
    has_data       = bool(_DATA_QUERY_PATTERNS.search(q))

    # "Why did X happen" style — always analytical even if it has data keywords
    why_pattern = re.search(r"\bwhy\b", q, re.IGNORECASE)
    if why_pattern:
        return IntentType.analytical

    if has_analytical and has_data:
        return IntentType.analytical
    if has_analytical:
        return IntentType.analytical
    if has_data:
        return IntentType.data_query

    return None  # Ambiguous — let LLM decide


# ── Stage 2: LLM classification ───────────────────────────────────────────────

_CLASSIFY_SYSTEM = """You are a query classifier. Given a user question about a dataset,
output exactly one word — the intent type — with no explanation.

Intent types:
- data_query   : wants specific data, numbers, lists, counts, averages, filters
- analytical   : wants explanation, comparison, trend analysis, reasoning about why
- general      : asking about the dataset structure, metadata, or a general question

Respond with only one of: data_query | analytical | general"""


async def _llm_classify(question: str) -> IntentType:
    try:
        response = await chat(
            messages=[
                {"role": "system",  "content": _CLASSIFY_SYSTEM},
                {"role": "user",    "content": question},
            ],
            temperature=0.0,
        )
        raw = response.strip().lower()
        if "analytical" in raw:
            return IntentType.analytical
        if "general" in raw:
            return IntentType.general
        return IntentType.data_query  # default
    except Exception as exc:
        logger.warning(f"LLM classify failed: {exc}. Defaulting to data_query.")
        return IntentType.data_query