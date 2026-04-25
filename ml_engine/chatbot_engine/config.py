"""
config.py
=========
All configurable settings for the chatbot system.
Edit this file to change models, paths, or limits.
"""

from pathlib import Path


# ── Ollama ────────────────────────────────────────────────────────────────────

OLLAMA_BASE_URL   = "http://localhost:11434"
CHAT_MODEL        = "llama3.2:3b"       # Model used for answering questions
EMBED_MODEL       = "nomic-embed-text"  # Model used for column embeddings (RAG)
OLLAMA_TIMEOUT    = 120                 # Seconds before request is considered dead
STREAM_ENABLED    = True                # Stream tokens back to client


# ── Paths ─────────────────────────────────────────────────────────────────────

# Point to the project root's uploads directory
# Resolving from __file__ ensures it works correctly regardless of the CWD when called from Node.js
UPLOADS_DIR = (Path(__file__).parent.parent.parent / "uploads").resolve()


# ── RAG ───────────────────────────────────────────────────────────────────────

RAG_TOP_K         = 8     # Max columns retrieved per question
RAG_MIN_SCORE     = 0.25  # Minimum cosine similarity to include a column
SAMPLE_VALUES_N   = 6     # Sample values per column sent to Ollama
STATS_FOR_NUMERIC = True  # Include min/max/mean for numeric columns


# ── SQL Execution ─────────────────────────────────────────────────────────────

MAX_RESULT_ROWS   = 50    # Max rows returned from a SQL query to the LLM
QUERY_TIMEOUT     = 30    # Seconds before a DuckDB query is killed


# ── Session / Memory ──────────────────────────────────────────────────────────

HISTORY_WINDOW    = 6     # Number of recent message pairs kept in context
                          # (each pair = 1 user + 1 assistant message)
SESSION_TTL_MINS  = 120   # Sessions inactive longer than this are purged


# ── Node.js Integration ───────────────────────────────────────────────────────

# This is called by Node.js backend, not run as standalone server
NODE_JS_MODE      = True