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
import os
import asyncio
import logging
import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional, Any

import numpy as np

from config import RAG_MIN_SCORE, RAG_TOP_K
from core.llm import embed
from core.persistence import (
    get_artifact_dir, 
    is_artifact_valid, 
    ArtifactLock, 
    atomic_save_json, 
    load_json
)
from models.schemas import ColumnMeta

if TYPE_CHECKING:
    pass

logger = logging.getLogger("chatbot.rag")


# ── Column index ──────────────────────────────────────────────────────────────

@dataclass
class ColumnIndex:
    """
    Holds vectors for all columns of one dataset.
    Optimized for production with disk persistence.
    """
    columns:    list[ColumnMeta]
    texts:      list[str]
    vectors:    np.ndarray | None   # Shape (n_cols, embedding_dim)
    dim:        int = 0

    @classmethod
    async def get_or_build(cls, dataset_id: str, columns: list[ColumnMeta], uploads_root: str, csv_path: str) -> "ColumnIndex":
        """
        Loads from disk if valid artifacts exist; otherwise builds and saves.
        Uses .lock and atomic rename for safety.
        """
        art_dir = get_artifact_dir(uploads_root, dataset_id)
        vec_path = str(art_dir / "column_vectors.npy")
        meta_path = str(art_dir / "column_metadata.json")
        lock_path = str(art_dir / "rag.lock")

        # 1. Quick check without lock
        if is_artifact_valid(vec_path, csv_path) and is_artifact_valid(meta_path, csv_path):
            idx = cls.load(vec_path, meta_path)
            if idx: return idx

        # 2. Acquire lock and build
        with ArtifactLock(lock_path):
            # Double-check after lock
            if is_artifact_valid(vec_path, csv_path) and is_artifact_valid(meta_path, csv_path):
                idx = cls.load(vec_path, meta_path)
                if idx: return idx

            logger.info(f"Building RAG index for dataset {dataset_id}...")
            texts = [_column_to_text(col) for col in columns]
            vectors = await _embed_batch(texts)
            dim = vectors.shape[1] if vectors is not None else 0
            
            index = cls(columns=columns, texts=texts, vectors=vectors, dim=dim)
            if vectors is not None:
                index.save(vec_path, meta_path)
            
            return index

    def save(self, vec_path: str, meta_path: str):
        """Atomic save to disk."""
        try:
            tmp_vec = f"{vec_path}.tmp"
            np.save(tmp_vec, self.vectors)
            os.replace(tmp_vec, vec_path)

            meta_data = {
                "texts": self.texts,
                "columns": [col.dict() for col in self.columns],
                "dim": self.dim
            }
            atomic_save_json(meta_path, meta_data)
            logger.info(f"RAG artifacts saved to disk.")
        except Exception as e:
            logger.error(f"Failed to save RAG artifacts: {e}")

    @classmethod
    def load(cls, vec_path: str, meta_path: str) -> Optional["ColumnIndex"]:
        """Load artifacts from disk."""
        try:
            vectors = np.load(vec_path)
            meta = load_json(meta_path)
            if not meta: return None

            columns = [ColumnMeta(**c) for c in meta["columns"]]
            return cls(
                columns=columns,
                texts=meta["texts"],
                vectors=vectors,
                dim=meta["dim"]
            )
        except Exception as e:
            logger.warning(f"Failed to load RAG artifacts: {e}")
            return None


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


async def _embed_batch(texts: list[str], batch_size: int = 4) -> np.ndarray | None:
    """
    Embed a list of texts in controlled batches.
    Returns normalised float32 numpy array or None.
    """
    vectors: list[list[float]] = []
    
    # Process in chunks to avoid overwhelming Ollama
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        logger.debug(f"Processing embedding batch {i//batch_size + 1}...")
        
        # Parallelize within the batch
        batch_results = await asyncio.gather(*[embed(text) for text in batch])
        
        for vec in batch_results:
            if not vec:
                logger.warning("Embed returned empty — disabling semantic RAG")
                return None
            vectors.append(vec)

    arr = np.array(vectors, dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True) + 1e-10
    return arr / norms


def _tokenize(text: str) -> list[str]:
    """Lowercase alphanumeric tokens, minimum 2 chars."""
    import re
    return [t for t in re.split(r"[^a-z0-9]+", text.lower()) if len(t) >= 2]