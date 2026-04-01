"""
DataPilot MCP — DuckDB Singleton Connection
Provides query execution and file registration against an in-memory DuckDB instance.
"""

import logging
import os
import time
from typing import Any

import duckdb

logger = logging.getLogger(__name__)

# Module-level singleton connection
_connection: duckdb.DuckDBPyConnection | None = None

# Module-level SQL Server connection (pymssql)
_mssql_conn = None


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Return the shared DuckDB connection, creating it if necessary.
    - In production (DUCKDB_PATH set): uses a persistent file so data
      survives restarts and uploaded file views can be re-registered.
    - In development: uses in-memory (fast, stateless).
    """
    global _connection
    if _connection is None:
        db_path = os.getenv("DUCKDB_PATH", ":memory:")
        _connection = duckdb.connect(database=db_path, read_only=False)
        logger.info("DuckDB connection created: %s", db_path)
    return _connection


def execute_query(sql: str) -> dict[str, Any]:
    """
    Execute a SQL query and return structured results.

    Routes to SQL Server (pymssql) when an active mssql connection exists;
    otherwise executes against the local DuckDB instance.

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
    if _mssql_conn is not None:
        return _execute_mssql_query(sql)

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


def attach_postgres(host: str, port: int, database: str, username: str, password: str, alias: str = "pg") -> list[str]:
    """
    Attach a PostgreSQL database to the DuckDB session via the postgres extension.

    All Postgres tables become queryable as  <alias>.<schema>.<table>  in DuckDB.

    Args:
        host:     Postgres server hostname or IP.
        port:     Postgres port (typically 5432).
        database: Database name.
        username: Postgres user.
        password: Postgres password.
        alias:    DuckDB schema alias to use (default "pg").

    Returns:
        List of table names visible through the attached connection.

    Raises:
        Exception — propagated so the route can return a structured error.
    """
    conn = get_connection()

    # Install and load the postgres extension (no-op if already loaded)
    conn.execute("INSTALL postgres; LOAD postgres;")

    url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    conn.execute(f"ATTACH IF NOT EXISTS '{url}' AS {alias} (TYPE postgres, READ_ONLY)")

    # List tables visible through the attached alias
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = ? ORDER BY table_name",
        [alias],
    ).fetchall()

    table_names = [r[0] for r in rows]
    logger.info(
        "Attached Postgres db=%s@%s:%s/%s as alias '%s' — %d tables",
        username, host, port, database, alias, len(table_names),
    )
    return table_names


def attach_mssql(host: str, port: int, database: str, username: str, password: str) -> list[str]:
    """
    Open a pymssql connection to a SQL Server instance and store it globally.

    Subsequent calls to execute_query() will be routed through this connection
    until detach_mssql() is called.

    Returns:
        List of user table names in the connected database.

    Raises:
        Exception — propagated so the route can return a structured error.
    """
    global _mssql_conn
    import pymssql  # lazy import — only needed when SQL Server is used

    conn = pymssql.connect(
        server=host,
        port=port,
        database=database,
        user=username,
        password=password,
        as_dict=False,
        autocommit=True,
        timeout=10,
        login_timeout=10,
    )

    _mssql_conn = conn
    logger.info("SQL Server connection established: %s@%s:%s/%s", username, host, port, database)

    # List user tables
    cursor = conn.cursor()
    cursor.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = %s "
        "ORDER BY TABLE_NAME",
        (database,),
    )
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables


def detach_mssql() -> None:
    """Close and clear the active SQL Server connection."""
    global _mssql_conn
    if _mssql_conn is not None:
        try:
            _mssql_conn.close()
        except Exception:
            pass
        _mssql_conn = None
        logger.info("SQL Server connection closed.")


def get_mssql_schema() -> dict[str, list[dict[str, str]]]:
    """
    Return column metadata for all user tables in the connected SQL Server database.
    Returns the same shape as get_schema() so the rest of the pipeline is unaffected.
    """
    if _mssql_conn is None:
        return {}

    cursor = _mssql_conn.cursor()
    cursor.execute(
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "ORDER BY TABLE_NAME, ORDINAL_POSITION"
    )
    schema: dict[str, list[dict[str, str]]] = {}
    for table, col, dtype in cursor.fetchall():
        schema.setdefault(table, []).append({"name": col, "type": dtype})
    cursor.close()
    return schema


def _execute_mssql_query(sql: str) -> dict[str, Any]:
    """Execute SQL against the active SQL Server connection."""
    start = time.perf_counter()
    cursor = _mssql_conn.cursor()
    try:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = [list(row) for row in cursor.fetchall()]
        rows = _normalize_rows(rows)
        latency_ms = (time.perf_counter() - start) * 1000
        logger.debug("MSSQL query executed: %d rows, %.1f ms", len(rows), latency_ms)
        return {
            "columns":    columns,
            "rows":       rows,
            "row_count":  len(rows),
            "latency_ms": round(latency_ms, 2),
        }
    except Exception:
        latency_ms = (time.perf_counter() - start) * 1000
        logger.exception("MSSQL query failed after %.1f ms: %s", latency_ms, sql[:200])
        raise
    finally:
        cursor.close()


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

    When a SQL Server connection is active, delegates to get_mssql_schema()
    so the Schema panel reflects the connected external database.

    Returns:
        {
            "table_name": [
                {"name": "col1", "type": "VARCHAR"},
                ...
            ],
            ...
        }
    """
    if _mssql_conn is not None:
        return get_mssql_schema()

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
