"""
DataPilot MCP — Analyst Agent
Full agent loop:
  rewrite → generate SQL with retry → chart suggest → chart generate → summarize → return
"""

import logging
import time
from typing import Any

from backend.agents import chart_suggester, query_rewriter, sql_retry
from backend.mcp.context import query_history
from backend.mcp.tools import chart_generator
from backend.utils import summarizer
from backend.data.db import get_schema

logger = logging.getLogger(__name__)


def run(question: str, session_id: str) -> dict[str, Any]:
    """
    Execute the full analyst pipeline for a user question.

    Args:
        question: Raw user question.
        session_id: Client-generated session UUID.

    Returns:
        {
            "question": str,
            "rewritten_question": str,
            "sql": str,
            "columns": list[str],
            "rows": list[list],
            "row_count": int,
            "sql_latency_ms": float,
            "chart_type": str,
            "chart_config": dict | None,
            "summary": str,          # HTML <ul><li> bullets
            "attempts": int,
            "provider": str,
            "usage": dict,
            "total_latency_ms": float,
            "success": True,
        }

        On failure:
        {
            "question": str,
            "error": str,
            "success": False,
            "total_latency_ms": float,
        }
    """
    total_start = time.perf_counter()

    # ---------------------------------------------------------------- #
    # Step 1: Rewrite the question
    # ---------------------------------------------------------------- #
    try:
        rewritten = query_rewriter.rewrite(question, session_id)
    except Exception as exc:
        logger.warning("Query rewriter error: %s. Using original question.", exc)
        rewritten = question

    # ---------------------------------------------------------------- #
    # Step 2: Generate SQL with retry
    # ---------------------------------------------------------------- #
    try:
        gen_result = sql_retry.generate_and_run(rewritten, session_id)
    except RuntimeError as exc:
        total_ms = (time.perf_counter() - total_start) * 1000
        logger.error("SQL generation exhausted retries: %s", exc)
        return {
            "question": question,
            "rewritten_question": rewritten,
            "error": str(exc),
            "success": False,
            "total_latency_ms": round(total_ms, 2),
        }

    sql = gen_result["sql"]
    result = gen_result["result"]
    columns = result["columns"]
    rows = result["rows"]
    sql_latency_ms = result.get("latency_ms", 0.0)
    attempts = gen_result["attempts"]
    provider = gen_result.get("provider", "unknown")
    usage = gen_result.get("usage", {})

    # ---------------------------------------------------------------- #
    # Step 3: Suggest chart type
    # ---------------------------------------------------------------- #
    # Get column types from schema for better suggestions
    schema = get_schema()
    col_types = _get_col_types(columns, schema)

    suggested_type = chart_suggester.suggest(columns, rows, col_types)

    # ---------------------------------------------------------------- #
    # Step 4: Generate chart config
    # ---------------------------------------------------------------- #
    chart_result = chart_generator.generate(
        columns=columns,
        rows=rows,
        chart_type=suggested_type,
        col_types=col_types,
        title=rewritten[:80] if rewritten else question[:80],
    )
    chart_type = chart_result["chart_type"]
    chart_config = chart_result["config"]

    # ---------------------------------------------------------------- #
    # Step 5: Generate business summary
    # ---------------------------------------------------------------- #
    try:
        summary_html = summarizer.summarize(
            question=rewritten,
            columns=columns,
            rows=rows,
        )
    except Exception as exc:
        logger.warning("Summarizer error: %s. Using fallback summary.", exc)
        summary_html = f"<ul><li>Query returned {len(rows)} rows.</li></ul>"

    # ---------------------------------------------------------------- #
    # Step 6: Save to query history
    # ---------------------------------------------------------------- #
    query_history.add(
        session_id=session_id,
        question=rewritten,
        sql=sql,
        summary=summary_html,
    )

    total_ms = (time.perf_counter() - total_start) * 1000

    return {
        "question": question,
        "rewritten_question": rewritten,
        "sql": sql,
        "columns": columns,
        "rows": rows,
        "row_count": len(rows),
        "sql_latency_ms": sql_latency_ms,
        "chart_type": chart_type,
        "chart_config": chart_config,
        "summary": summary_html,
        "attempts": attempts,
        "provider": provider,
        "usage": usage,
        "total_latency_ms": round(total_ms, 2),
        "success": True,
    }


def _get_col_types(columns: list[str], schema: dict) -> list[str]:
    """
    Attempt to look up column types from the loaded schema.
    Falls back to 'varchar' if not found.
    """
    # Build a flat name→type map across all tables
    name_to_type: dict[str, str] = {}
    for table_cols in schema.values():
        for col in table_cols:
            name_to_type[col["name"].lower()] = col["type"]

    return [name_to_type.get(c.lower(), "varchar") for c in columns]
