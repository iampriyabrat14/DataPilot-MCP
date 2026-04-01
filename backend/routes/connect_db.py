"""
DataPilot MCP — Connect DB Route
POST /api/connect-db          — attach Postgres or SQL Server
POST /api/disconnect-db       — close the active external connection

Postgres uses DuckDB's native postgres extension (ATTACH).
SQL Server uses pymssql — a pure-Python driver, no ODBC setup required.
"""

import logging
import re

from flask import Blueprint, jsonify, request

from backend.data import db

logger = logging.getLogger(__name__)

connect_db_bp = Blueprint("connect_db", __name__)

_SAFE_ALIAS = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,30}$")

_DEFAULT_PORTS = {"postgres": 5432, "mssql": 1433}


@connect_db_bp.route("/api/connect-db", methods=["POST"])
def connect_db():
    """
    Connect to an external database.

    Request JSON:
        {
            "db_type":  "postgres" | "mssql",   // required
            "host":     "db.example.com",
            "port":     5432,                   // optional — defaults per db_type
            "database": "mydb",
            "username": "analyst",
            "password": "secret",
            "alias":    "pg"                    // Postgres only — default "pg"
        }

    Response JSON (success):
        {
            "success":     true,
            "db_type":     "postgres" | "mssql",
            "alias":       "pg",               // Postgres only
            "table_count": 12,
            "tables":      ["orders", ...]
        }

    Response JSON (error):
        { "error": "..." }
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Request body must be JSON."}), 400

    db_type  = (data.get("db_type")  or "postgres").strip().lower()
    host     = (data.get("host")     or "").strip()
    database = (data.get("database") or "").strip()
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    alias    = (data.get("alias")    or "pg").strip()

    if db_type not in _DEFAULT_PORTS:
        return jsonify({"error": f"Unsupported db_type '{db_type}'. Use 'postgres' or 'mssql'."}), 400

    try:
        port = int(data.get("port") or _DEFAULT_PORTS[db_type])
        if not (1 <= port <= 65535):
            raise ValueError
    except (ValueError, TypeError):
        return jsonify({"error": "Field 'port' must be an integer between 1 and 65535."}), 400

    missing = [f for f, v in [("host", host), ("database", database), ("username", username)] if not v]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}."}), 400

    if db_type == "postgres" and not _SAFE_ALIAS.match(alias):
        return jsonify({"error": "Field 'alias' must start with a letter and contain only letters, digits, or underscores (max 31 chars)."}), 400

    try:
        if db_type == "postgres":
            tables = db.attach_postgres(
                host=host, port=port, database=database,
                username=username, password=password, alias=alias,
            )
            return jsonify({
                "success":     True,
                "db_type":     "postgres",
                "alias":       alias,
                "table_count": len(tables),
                "tables":      tables,
            }), 200

        else:  # mssql
            tables = db.attach_mssql(
                host=host, port=port, database=database,
                username=username, password=password,
            )
            return jsonify({
                "success":     True,
                "db_type":     "mssql",
                "table_count": len(tables),
                "tables":      tables,
            }), 200

    except Exception as exc:
        err_msg = str(exc).replace(password, "***") if password else str(exc)
        logger.error("connect-db (%s) failed: %s", db_type, err_msg)
        return jsonify({"error": f"Could not connect: {err_msg}"}), 502


@connect_db_bp.route("/api/disconnect-db", methods=["POST"])
def disconnect_db():
    """
    Close the active SQL Server connection (if any).
    Postgres ATTACH persists for the DuckDB session but is transparent to the UI.
    """
    db.detach_mssql()
    return jsonify({"success": True}), 200
