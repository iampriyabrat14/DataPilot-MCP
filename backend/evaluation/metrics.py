"""
DataPilot MCP — Evaluation Metrics
Logs per-query metrics to an in-memory list and a JSONL file.
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Any

from backend.config import EVAL_LOGS_DIR

logger = logging.getLogger(__name__)

# Module-level in-memory store
_metrics: list[dict[str, Any]] = []
_lock = threading.Lock()

_METRICS_FILE = os.path.join(EVAL_LOGS_DIR, "metrics.jsonl")


def log_metric(session_id: str, metric_dict: dict[str, Any]) -> None:
    """
    Append a metric record to the in-memory list and write a JSON line to disk.

    Expected metric_dict keys (all optional except noted):
        latency_ms        (float)  — total end-to-end latency
        llm_latency_ms    (float)  — time spent in LLM calls
        sql_latency_ms    (float)  — time spent executing SQL
        retry_count       (int)    — number of SQL retries (0 = perfect first try)
        llm_provider_used (str)    — "groq" | "openai"
        chart_type        (str)    — auto-selected chart type
        token_count       (int)    — total tokens used
        success           (bool)   — did the query succeed
        question          (str)    — user question (for debugging)

    Args:
        session_id: Session identifier.
        metric_dict: Dict of metric values.
    """
    record = {
        "session_id": session_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **metric_dict,
    }

    with _lock:
        _metrics.append(record)

    # Write to disk asynchronously (fire and forget)
    t = threading.Thread(target=_write_to_disk, args=(record,), daemon=True)
    t.start()

    logger.debug(
        "Metric logged: session=%s, success=%s, latency=%.1fms",
        session_id,
        metric_dict.get("success"),
        metric_dict.get("latency_ms", 0),
    )


def _write_to_disk(record: dict[str, Any]) -> None:
    """Append a single JSON line to the metrics JSONL file."""
    try:
        os.makedirs(EVAL_LOGS_DIR, exist_ok=True)
        with _lock:
            with open(_METRICS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, default=str) + "\n")
    except Exception as exc:
        logger.warning("Failed to write metric to disk: %s", exc)


def get_metrics(session_id: str | None = None) -> list[dict[str, Any]]:
    """
    Return all logged metrics, optionally filtered by session_id.

    Args:
        session_id: If provided, filter to this session only.

    Returns:
        List of metric dicts, most recent first.
    """
    with _lock:
        records = list(_metrics)

    if session_id:
        records = [r for r in records if r.get("session_id") == session_id]

    return list(reversed(records))


def clear_metrics() -> None:
    """Clear all in-memory metrics (disk log is preserved)."""
    with _lock:
        _metrics.clear()
    logger.info("In-memory metrics cleared.")
