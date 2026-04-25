"""
core/session.py
===============
In-memory session store. Each session holds:
  - chat history (rolling window of last N message pairs)
  - the loaded dataset reference
  - the column index (RAG)
  - timestamps for TTL-based cleanup

Sessions are keyed by a session_id string (client-provided or UUID).
No persistence — sessions are lost on server restart. This is intentional
for the current phase (privacy, simplicity).

Thread safety: FastAPI runs in a single async loop, so a plain dict is safe.
The cleanup task runs periodically in the background.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from config import HISTORY_WINDOW, SESSION_TTL_MINS
from models.schemas import ChatMessage, MessageRole

if TYPE_CHECKING:
    from core.dataset import LoadedDataset
    from core.rag import ColumnIndex

logger = logging.getLogger("chatbot.session")


# ── Session dataclass ─────────────────────────────────────────────────────────

@dataclass
class Session:
    session_id:   str
    dataset_id:   str | None             = None
    dataset:      "LoadedDataset | None" = None
    column_index: "ColumnIndex | None"   = None
    history:      list[ChatMessage]      = field(default_factory=list)
    created_at:   datetime               = field(default_factory=lambda: datetime.now(timezone.utc))
    last_active:  datetime               = field(default_factory=lambda: datetime.now(timezone.utc))

    def touch(self) -> None:
        self.last_active = datetime.now(timezone.utc)

    def add_message(self, role: MessageRole, content: str) -> None:
        self.history.append(ChatMessage(role=role, content=content))
        self.touch()

    def get_history_window(self) -> list[ChatMessage]:
        """
        Return the last HISTORY_WINDOW * 2 messages (pairs of user + assistant).
        This is what gets injected into the LLM prompt.
        """
        # Each pair = 2 messages. Take last N pairs.
        max_msgs = HISTORY_WINDOW * 2
        return self.history[-max_msgs:] if len(self.history) > max_msgs else list(self.history)

    def clear_history(self) -> None:
        self.history.clear()
        self.touch()

    def is_expired(self) -> bool:
        elapsed = (datetime.now(timezone.utc) - self.last_active).total_seconds()
        return elapsed > (SESSION_TTL_MINS * 60)

    def info_dict(self) -> dict:
        return {
            "session_id":    self.session_id,
            "dataset_id":    self.dataset_id,
            "message_count": len(self.history),
            "created_at":    self.created_at.isoformat(),
            "last_active":   self.last_active.isoformat(),
        }


# ── Session store ─────────────────────────────────────────────────────────────

_store: dict[str, Session] = {}


def get_or_create(session_id: str) -> Session:
    """Return existing session or create a new one."""
    if session_id not in _store:
        _store[session_id] = Session(session_id=session_id)
        logger.info(f"New session created: {session_id}")
    return _store[session_id]


def get(session_id: str) -> Session | None:
    return _store.get(session_id)


def delete(session_id: str) -> bool:
    if session_id in _store:
        del _store[session_id]
        logger.info(f"Session deleted: {session_id}")
        return True
    return False


def all_sessions() -> list[Session]:
    return list(_store.values())


def purge_expired() -> int:
    """Remove all sessions past their TTL. Returns count of removed sessions."""
    expired = [sid for sid, s in _store.items() if s.is_expired()]
    for sid in expired:
        del _store[sid]
    if expired:
        logger.info(f"Purged {len(expired)} expired session(s)")
    return len(expired)