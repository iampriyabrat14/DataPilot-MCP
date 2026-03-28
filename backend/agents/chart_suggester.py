"""
DataPilot MCP — Chart Suggester Agent
Determines the best chart type from query result column names, types, and data shape.
Thin wrapper around mcp/tools/chart_generator.suggest_chart_type().
"""

import logging
from typing import Any

from backend.mcp.tools.chart_generator import suggest_chart_type

logger = logging.getLogger(__name__)


def suggest(
    columns: list[str],
    rows: list[list],
    col_types: list[str] | None = None,
) -> str:
    """
    Suggest the best chart type for the given query result.

    Args:
        columns: Column names from the query result.
        rows: Data rows.
        col_types: Optional DuckDB column type strings.

    Returns:
        One of: "line", "bar", "pie", "scatter", "table"
    """
    chart_type = suggest_chart_type(columns, rows, col_types)
    logger.debug(
        "Chart suggestion: %s (cols=%s, rows=%d)",
        chart_type,
        columns,
        len(rows),
    )
    return chart_type
