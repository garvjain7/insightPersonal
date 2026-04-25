"""
core/llm.py
===========
Thin, robust wrapper around the Ollama HTTP API.

Responsibilities:
  - chat()        → blocking chat completion (returns full string)
  - stream_chat() → async generator of token strings
  - embed()       → get embedding vector for a text string
  - health()      → check if Ollama is reachable and models are available
"""

from __future__ import annotations

import json
import logging
from typing import AsyncGenerator

import httpx

from config import (
    CHAT_MODEL,
    EMBED_MODEL,
    OLLAMA_BASE_URL,
    OLLAMA_TIMEOUT,
)

logger = logging.getLogger("chatbot.llm")


class OllamaError(Exception):
    """Raised when Ollama returns an error or is unreachable."""


# ── Internal HTTP client ──────────────────────────────────────────────────────

def _client(timeout: float = OLLAMA_TIMEOUT) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=OLLAMA_BASE_URL, timeout=timeout)


# ── Embed ─────────────────────────────────────────────────────────────────────

async def embed(text: str) -> list[float]:
    """
    Return an embedding vector for *text* using the local embedding model.
    Falls back to an empty list if the model is unavailable (RAG will
    degrade gracefully to full-schema mode).
    """
    async with _client() as client:
        try:
            resp = await client.post(
                "/api/embed",
                json={"model": EMBED_MODEL, "input": text},
            )
            resp.raise_for_status()
            data = resp.json()
            # Ollama /api/embed returns { "embeddings": [[...]] }
            embeddings = data.get("embeddings") or data.get("embedding")
            if isinstance(embeddings, list) and embeddings:
                vec = embeddings[0] if isinstance(embeddings[0], list) else embeddings
                return vec
            return []
        except Exception as exc:
            logger.warning(f"Embedding failed: {exc}. RAG will use full schema.")
            return []


# ── Blocking chat ─────────────────────────────────────────────────────────────

async def chat(
    messages: list[dict],
    model: str = CHAT_MODEL,
    temperature: float = 0.2,
) -> str:
    """
    Blocking chat completion. Returns the full assistant response as a string.
    Used for intent detection and SQL generation where we need the full output
    before proceeding to the next step.
    """
    async with _client() as client:
        try:
            resp = await client.post(
                "/api/chat",
                json={
                    "model":    model,
                    "messages": messages,
                    "stream":   False,
                    "options":  {"temperature": temperature},
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data["message"]["content"].strip()
        except httpx.HTTPStatusError as exc:
            raise OllamaError(f"Ollama HTTP {exc.response.status_code}: {exc.response.text}") from exc
        except Exception as exc:
            raise OllamaError(f"Ollama request failed: {exc}") from exc


# ── Streaming chat ────────────────────────────────────────────────────────────

async def stream_chat(
    messages: list[dict],
    model: str = CHAT_MODEL,
    temperature: float = 0.3,
) -> AsyncGenerator[str, None]:
    """
    Async generator that yields response tokens one by one.
    Caller is responsible for joining or forwarding them.
    """
    async with _client(timeout=OLLAMA_TIMEOUT) as client:
        try:
            async with client.stream(
                "POST",
                "/api/chat",
                json={
                    "model":    model,
                    "messages": messages,
                    "stream":   True,
                    "options":  {"temperature": temperature},
                },
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if not line.strip():
                        continue
                    try:
                        chunk = json.loads(line)
                        token = chunk.get("message", {}).get("content", "")
                        if token:
                            yield token
                        if chunk.get("done"):
                            break
                    except json.JSONDecodeError:
                        continue
        except httpx.HTTPStatusError as exc:
            raise OllamaError(f"Ollama stream error {exc.response.status_code}") from exc
        except Exception as exc:
            raise OllamaError(f"Ollama stream failed: {exc}") from exc


# ── Health check ──────────────────────────────────────────────────────────────

async def health() -> dict[str, bool]:
    """
    Returns { "reachable": bool, "chat_model": bool, "embed_model": bool }.
    """
    result = {"reachable": False, "chat_model": False, "embed_model": False}
    async with _client(timeout=5) as client:
        try:
            resp = await client.get("/api/tags")
            resp.raise_for_status()
            tags = [m["name"] for m in resp.json().get("models", [])]
            result["reachable"]    = True
            result["chat_model"]   = any(CHAT_MODEL  in t for t in tags)
            result["embed_model"]  = any(EMBED_MODEL in t for t in tags)
        except Exception:
            pass
    return result