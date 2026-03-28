"""
DataPilot MCP — Query History Context
In-memory session store for the last N queries per session.
Each entry: {question, sql, summary, timestamp}
"""

import logging
from datetime import datetime, timezone
from typing import Any

from backend.config import QUERY_HISTORY_LIMIT

logger = logging.getLogger(__name__)

# Module-level store: session_id → list of query dicts
_store: dict[str, list[dict[str, Any]]] = {}


def add(session_id: str, question: str, sql: str, summary: str) -> None:
    """
    Add a query record to the session history.
    Automatically trims the history to QUERY_HISTORY_LIMIT entries.

    Args:
        session_id: Unique identifier for the user session.
        question: The original (or rewritten) user question.
        sql: The SQL query that was executed.
        summary: The LLM-generated business summary.
    """
    if session_id not in _store:
        _store[session_id] = []

    _store[session_id].append(
        {
            "question": question,
            "sql": sql,
            "summary": summary,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )

    # Keep only the last N entries
    if len(_store[session_id]) > QUERY_HISTORY_LIMIT:
        _store[session_id] = _store[session_id][-QUERY_HISTORY_LIMIT:]

    logger.debug(
        "History updated for session %s: %d entries",
        session_id,
        len(_store[session_id]),
    )


def get(session_id: str) -> list[dict[str, Any]]:
    """
    Retrieve the query history for a session.

    Args:
        session_id: Unique identifier for the user session.

    Returns:
        List of query dicts, oldest first.
    """
    return _store.get(session_id, [])


def get_as_text(session_id: str) -> str:
    """
    Return a compact text representation of the session history for LLM prompts.

    Example:
        Previous Q1: How many orders today? → SELECT COUNT(*) FROM orders WHERE ...
        Previous Q2: Break down by region? → SELECT region, COUNT(*) FROM orders ...
    """
    history = get(session_id)
    if not history:
        return "No previous queries in this session."
    lines = []
    for i, entry in enumerate(history, 1):
        lines.append(f"Q{i}: {entry['question']} → {entry['sql']}")
    return "\n".join(lines)


def clear(session_id: str) -> None:
    """
    Clear all history for a session.

    Args:
        session_id: Unique identifier for the user session.
    """
    if session_id in _store:
        del _store[session_id]
        logger.info("History cleared for session %s.", session_id)
