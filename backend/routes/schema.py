"""
DataPilot MCP — Schema Route
GET /api/schema
Returns all loaded DuckDB tables and their columns.
"""

import logging

from flask import Blueprint, jsonify

from backend.data import db

logger = logging.getLogger(__name__)

schema_bp = Blueprint("schema", __name__)


@schema_bp.route("/api/schema", methods=["GET"])
def handle_schema():
    """
    Return the schema of all tables currently loaded in DuckDB.

    Response JSON:
        {
            "tables": {
                "table_name": [
                    {"name": "col1", "type": "VARCHAR"},
                    ...
                ],
                ...
            }
        }
    """
    try:
        schema = db.get_schema()
        return jsonify({"tables": schema}), 200
    except Exception as exc:
        logger.exception("Failed to retrieve schema: %s", exc)
        return jsonify({"error": "Failed to retrieve schema."}), 500
