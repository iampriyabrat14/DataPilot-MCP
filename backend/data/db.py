"""
DataPilot MCP — DuckDB Singleton Connection
Provides query execution and file registration against an in-memory DuckDB instance.
"""

import logging
import time
from typing import Any

import duckdb

logger = logging.getLogger(__name__)

# Module-level singleton connection
_connection: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return the shared DuckDB in-memory connection, creating it if necessary."""
    global _connection
    if _connection is None:
        _connection = duckdb.connect(database=":memory:", read_only=False)
        logger.info("DuckDB in-memory connection created.")
    return _connection


def execute_query(sql: str) -> dict[str, Any]:
    """
    Execute a SQL query and return structured results.

    Returns:
        {
            "columns": list[str],
            "rows": list[list],
            "row_count": int,
            "latency_ms": float,
        }

    Raises:
        Exception — propagated as-is so callers can handle retry logic.
    """
    conn = get_connection()
    start = time.perf_counter()
    try:
        rel = conn.execute(sql)
        columns = [desc[0] for desc in rel.description] if rel.description else []
        rows = rel.fetchall()
        latency_ms = (time.perf_counter() - start) * 1000

        # Convert rows to lists (DuckDB returns tuples)
        serializable_rows = [list(row) for row in rows]

        # Convert non-JSON-serializable types (e.g. date, Decimal)
        serializable_rows = _normalize_rows(serializable_rows)

        logger.debug(
            "Query executed: %d rows, %.1f ms",
            len(serializable_rows),
            latency_ms,
        )
        return {
            "columns": columns,
            "rows": serializable_rows,
            "row_count": len(serializable_rows),
            "latency_ms": round(latency_ms, 2),
        }
    except Exception:
        latency_ms = (time.perf_counter() - start) * 1000
        logger.exception("Query failed after %.1f ms: %s", latency_ms, sql[:200])
        raise


def _normalize_rows(rows: list[list]) -> list[list]:
    """Convert non-serializable Python types to JSON-safe equivalents."""
    import decimal
    import datetime

    normalized = []
    for row in rows:
        new_row = []
        for val in row:
            if isinstance(val, decimal.Decimal):
                new_row.append(float(val))
            elif isinstance(val, (datetime.date, datetime.datetime)):
                new_row.append(val.isoformat())
            elif isinstance(val, bytes):
                new_row.append(val.decode("utf-8", errors="replace"))
            else:
                new_row.append(val)
        normalized.append(new_row)
    return normalized


def register_file(path: str, table_name: str) -> None:
    """
    Register a CSV or Parquet file as a virtual DuckDB view.

    Args:
        path: Absolute file path.
        table_name: Name to use for the DuckDB view/table.
    """
    conn = get_connection()
    path_escaped = path.replace("\\", "/")

    if path.lower().endswith(".parquet"):
        sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{path_escaped}')"
    else:
        # Default: treat as CSV
        sql = (
            f"CREATE OR REPLACE VIEW {table_name} AS "
            f"SELECT * FROM read_csv_auto('{path_escaped}')"
        )

    conn.execute(sql)
    logger.info("Registered file '%s' as DuckDB view '%s'.", path, table_name)


def get_schema() -> dict[str, list[dict[str, str]]]:
    """
    Return all tables and views with their column names and types.

    Returns:
        {
            "table_name": [
                {"name": "col1", "type": "VARCHAR"},
                ...
            ],
            ...
        }
    """
    conn = get_connection()

    # Fetch all tables and views
    tables_result = conn.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()

    schema: dict[str, list[dict[str, str]]] = {}
    for table_name, _ in tables_result:
        cols_result = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = ? "
            "ORDER BY ordinal_position",
            [table_name],
        ).fetchall()
        schema[table_name] = [
            {"name": col[0], "type": col[1]} for col in cols_result
        ]

    return schema


def schema_as_text() -> str:
    """
    Return a compact text representation of the schema suitable for LLM prompts.
    Example:
        TABLE orders: id INTEGER, customer_id INTEGER, total DOUBLE, created_at DATE
    """
    schema = get_schema()
    if not schema:
        return "No tables loaded yet."
    lines = []
    for table, cols in schema.items():
        col_str = ", ".join(f"{c['name']} {c['type']}" for c in cols)
        lines.append(f"TABLE {table}: {col_str}")
    return "\n".join(lines)
