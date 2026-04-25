"""
core/rag.py
===========
RAG layer: embeds column metadata and retrieves only the columns
relevant to a user's question.

Each "document" in the FAISS index is one column, represented as:
    "{name} | {inferred_type} | samples: {sample_values} | {stats}"

On a question:
  1. Embed the question (nomic-embed-text via Ollama)
  2. Cosine similarity search against column embeddings
  3. Return the top-K ColumnMeta objects that pass the score threshold

If embedding fails (Ollama embed model not available), falls back
to TF-IDF-style keyword matching so the system never hard-fails.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import numpy as np

from config import RAG_MIN_SCORE, RAG_TOP_K
from core.llm import embed
from models.schemas import ColumnMeta

if TYPE_CHECKING:
    pass

logger = logging.getLogger("chatbot.rag")


# ── Column index ──────────────────────────────────────────────────────────────

@dataclass
class ColumnIndex:
    """
    Holds FAISS-style in-memory vectors for all columns of one dataset.
    Built once per dataset load. Queried on every user message.
    """
    columns:    list[ColumnMeta]
    texts:      list[str]           # Human-readable doc per column
    vectors:    np.ndarray | None   # Shape (n_cols, embedding_dim), None if embedding failed
    dim:        int = 0

    @classmethod
    async def build(cls, columns: list[ColumnMeta]) -> "ColumnIndex":
        texts   = [_column_to_text(col) for col in columns]
        vectors = await _embed_batch(texts)
        dim     = vectors.shape[1] if vectors is not None else 0

        logger.info(
            f"ColumnIndex built: {len(columns)} columns, "
            f"dim={dim}, embedded={'yes' if vectors is not None else 'no (fallback)'}"
        )
        return cls(columns=columns, texts=texts, vectors=vectors, dim=dim)


# ── Public API ────────────────────────────────────────────────────────────────

async def retrieve(
    index:    ColumnIndex,
    question: str,
    top_k:    int = RAG_TOP_K,
) -> list[ColumnMeta]:
    """
    Return the most relevant ColumnMeta objects for *question*.

    If embedding is available: cosine similarity.
    If not: keyword overlap fallback.
    """
    if index.vectors is not None:
        return await _retrieve_semantic(index, question, top_k)
    else:
        return _retrieve_keyword(index, question, top_k)


# ── Semantic retrieval (FAISS-style, numpy) ───────────────────────────────────

async def _retrieve_semantic(
    index:    ColumnIndex,
    question: str,
    top_k:    int,
) -> list[ColumnMeta]:
    q_vec = await embed(question)

    if not q_vec:
        logger.warning("Question embedding returned empty — falling back to keyword")
        return _retrieve_keyword(index, question, top_k)

    q_arr = np.array(q_vec, dtype=np.float32)
    q_arr = q_arr / (np.linalg.norm(q_arr) + 1e-10)

    scores = index.vectors @ q_arr           # dot product = cosine similarity (normalised)
    ranked = np.argsort(scores)[::-1]        # descending

    results: list[ColumnMeta] = []
    for idx in ranked:
        score = float(scores[idx])
        if score < RAG_MIN_SCORE:
            break
        if len(results) >= top_k:
            break
        logger.debug(f"  RAG hit: {index.columns[idx].name} score={score:.3f}")
        results.append(index.columns[idx])

    # If nothing passes the threshold, return top-3 anyway
    if not results:
        results = [index.columns[i] for i in ranked[:3]]

    return results


# ── Keyword fallback ──────────────────────────────────────────────────────────

def _retrieve_keyword(
    index:    ColumnIndex,
    question: str,
    top_k:    int,
) -> list[ColumnMeta]:
    """
    Simple token overlap between question words and column document text.
    Used when embedding model is unavailable.
    """
    q_tokens = set(_tokenize(question))
    scored: list[tuple[int, float]] = []

    for i, text in enumerate(index.texts):
        doc_tokens = set(_tokenize(text))
        overlap = len(q_tokens & doc_tokens)
        if overlap > 0:
            # Jaccard-like score
            score = overlap / (len(q_tokens | doc_tokens) + 1e-10)
            scored.append((i, score))

    scored.sort(key=lambda x: x[1], reverse=True)
    top_indices = [i for i, _ in scored[:top_k]] or list(range(min(top_k, len(index.columns))))

    return [index.columns[i] for i in top_indices]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _column_to_text(col: ColumnMeta) -> str:
    """
    Converts a ColumnMeta to a short descriptive text for embedding.
    Keeps it compact — just enough for semantic matching.
    """
    parts = [
        f"column: {col.name}",
        f"type: {col.inferred_type}",
    ]
    if col.sample_values:
        samples = ", ".join(str(v) for v in col.sample_values[:4])
        parts.append(f"samples: {samples}")
    if col.stats:
        parts.append(
            f"range: {col.stats.get('min')} to {col.stats.get('max')}"
        )
    return " | ".join(parts)


async def _embed_batch(texts: list[str]) -> np.ndarray | None:
    """
    Embed a list of texts. Returns normalised float32 numpy array or None.
    """
    vectors: list[list[float]] = []
    for text in texts:
        vec = await embed(text)
        if not vec:
            logger.warning("Embed returned empty — disabling semantic RAG")
            return None
        vectors.append(vec)

    arr = np.array(vectors, dtype=np.float32)
    # L2 normalise each row for cosine similarity via dot product
    norms = np.linalg.norm(arr, axis=1, keepdims=True) + 1e-10
    return arr / norms


def _tokenize(text: str) -> list[str]:
    """Lowercase alphanumeric tokens, minimum 2 chars."""
    import re
    return [t for t in re.split(r"[^a-z0-9]+", text.lower()) if len(t) >= 2]