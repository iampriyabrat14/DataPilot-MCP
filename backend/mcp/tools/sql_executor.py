"""
DataPilot MCP — SQL Executor Tool
Wraps db.execute_query() and returns structured results or structured errors.
"""

import logging
from typing import Any

from backend.data import db

logger = logging.getLogger(__name__)


def run(sql: str) -> dict[str, Any]:
    """
    Execute a SQL query against DuckDB.

    Args:
        sql: SQL query string.

    Returns on success:
        {
            "success": True,
            "columns": list[str],
            "rows": list[list],
            "row_count": int,
            "latency_ms": float,
            "sql": str,
        }

    Returns on error:
        {
            "success": False,
            "error": str,
            "sql": str,
        }
    """
    sql = sql.strip()
    logger.debug("Executing SQL: %s", sql[:300])

    try:
        result = db.execute_query(sql)
        return {
            "success": True,
            "columns": result["columns"],
            "rows": result["rows"],
            "row_count": result["row_count"],
            "latency_ms": result["latency_ms"],
            "sql": sql,
        }
    except Exception as exc:
        error_msg = str(exc)
        logger.warning("SQL execution failed: %s | SQL: %s", error_msg, sql[:300])
        return {
            "success": False,
            "error": error_msg,
            "sql": sql,
        }
