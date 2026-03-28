"""
DataPilot MCP — File Reader Tool
Accepts an uploaded file path, detects csv/parquet, registers as DuckDB table, returns schema.
Feature 1: Auto Data Profile on Upload — computes per-column stats after registration.
"""

import logging
import os
import re
from typing import Any

from backend.data import db

logger = logging.getLogger(__name__)


def _sanitize_table_name(filename: str) -> str:
    """Convert a filename to a safe DuckDB table/view name."""
    name = os.path.splitext(os.path.basename(filename))[0]
    # Replace non-alphanumeric characters with underscores
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    # Ensure it starts with a letter or underscore
    if name and name[0].isdigit():
        name = "t_" + name
    if not name:
        name = "uploaded_file"
    return name.lower()


def _compute_profile(table_name: str, columns: list[dict]) -> list[dict]:
    """
    Compute a per-column data profile using DuckDB SQL.

    Returns a list of dicts:
        {column, type, null_count, null_pct, unique_count, min, max, mean, std_dev}
    """
    profile = []

    try:
        # Get total row count
        count_res = db.execute_query(f"SELECT COUNT(*) FROM {table_name}")
        total_rows = count_res["rows"][0][0] if count_res["rows"] else 0
        if total_rows == 0:
            return []

        for col_info in columns:
            col_name = col_info["name"]
            col_type = col_info["type"].upper()

            # Determine if column is numeric
            numeric_types = (
                "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT",
                "DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC",
                "HUGEINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT",
            )
            is_numeric = any(nt in col_type for nt in numeric_types)

            # Quoted column name for safety
            quoted = f'"{col_name}"'

            # Base stats: null_count, unique_count, min, max
            try:
                base_sql = (
                    f"SELECT "
                    f"  COUNT(*) - COUNT({quoted}) AS null_count, "
                    f"  COUNT(DISTINCT {quoted}) AS unique_count, "
                    f"  CAST(MIN({quoted}) AS VARCHAR) AS min_val, "
                    f"  CAST(MAX({quoted}) AS VARCHAR) AS max_val "
                    f"FROM {table_name}"
                )
                base_res = db.execute_query(base_sql)
                base_row = base_res["rows"][0] if base_res["rows"] else [0, 0, None, None]
                null_count = base_row[0] or 0
                unique_count = base_row[1] or 0
                min_val = base_row[2]
                max_val = base_row[3]
            except Exception as exc:
                logger.warning("Profile base stats failed for col '%s': %s", col_name, exc)
                null_count = 0
                unique_count = 0
                min_val = None
                max_val = None

            null_pct = round((null_count / total_rows * 100), 2) if total_rows > 0 else 0.0

            mean_val = None
            std_dev_val = None

            if is_numeric:
                try:
                    num_sql = (
                        f"SELECT "
                        f"  AVG(CAST({quoted} AS DOUBLE)) AS mean_val, "
                        f"  STDDEV(CAST({quoted} AS DOUBLE)) AS std_dev "
                        f"FROM {table_name}"
                    )
                    num_res = db.execute_query(num_sql)
                    if num_res["rows"]:
                        raw_mean = num_res["rows"][0][0]
                        raw_std = num_res["rows"][0][1]
                        mean_val = round(float(raw_mean), 4) if raw_mean is not None else None
                        std_dev_val = round(float(raw_std), 4) if raw_std is not None else None
                except Exception as exc:
                    logger.warning("Profile numeric stats failed for col '%s': %s", col_name, exc)

            profile.append({
                "column": col_name,
                "type": col_info["type"],
                "null_count": null_count,
                "null_pct": null_pct,
                "unique_count": unique_count,
                "min": min_val,
                "max": max_val,
                "mean": mean_val,
                "std_dev": std_dev_val,
            })

    except Exception as exc:
        logger.exception("Profile computation failed for table '%s': %s", table_name, exc)

    return profile


def read_file(file_path: str, table_name: str | None = None) -> dict[str, Any]:
    """
    Register a CSV or Parquet file as a DuckDB view and return its schema + profile.

    Args:
        file_path: Absolute path to the uploaded file.
        table_name: Override the auto-derived table name.

    Returns on success:
        {
            "success": True,
            "table_name": str,
            "columns": [{"name": str, "type": str}, ...],
            "row_count": int,
            "file_path": str,
            "profile": [{column, type, null_pct, unique_count, min, max, mean, std_dev}, ...]
        }

    Returns on error:
        {
            "success": False,
            "error": str,
            "file_path": str,
        }
    """
    if not os.path.exists(file_path):
        return {"success": False, "error": f"File not found: {file_path}", "file_path": file_path}

    ext = os.path.splitext(file_path)[1].lower()
    if ext not in (".csv", ".parquet"):
        return {
            "success": False,
            "error": f"Unsupported file type '{ext}'. Only .csv and .parquet are allowed.",
            "file_path": file_path,
        }

    resolved_name = table_name or _sanitize_table_name(file_path)

    try:
        db.register_file(file_path, resolved_name)

        # Fetch schema for the registered view
        schema = db.get_schema()
        columns = schema.get(resolved_name, [])

        # Count rows
        count_result = db.execute_query(f"SELECT COUNT(*) as n FROM {resolved_name}")
        row_count = count_result["rows"][0][0] if count_result["rows"] else 0

        # Compute data profile
        profile = _compute_profile(resolved_name, columns)

        logger.info(
            "File registered: %s → table '%s' (%d rows, %d cols)",
            file_path,
            resolved_name,
            row_count,
            len(columns),
        )
        return {
            "success": True,
            "table_name": resolved_name,
            "columns": columns,
            "row_count": row_count,
            "file_path": file_path,
            "profile": profile,
        }
    except Exception as exc:
        logger.exception("Failed to register file '%s': %s", file_path, exc)
        return {
            "success": False,
            "error": str(exc),
            "file_path": file_path,
        }
