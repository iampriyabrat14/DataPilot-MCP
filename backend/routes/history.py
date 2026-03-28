"""
DataPilot MCP — History Route
DELETE /api/history
Clears the query history for the current session.
"""

import logging

from flask import Blueprint, jsonify, request

from backend.mcp.context import query_history

logger = logging.getLogger(__name__)

history_bp = Blueprint("history", __name__)


@history_bp.route("/api/history", methods=["DELETE"])
def handle_clear_history():
    """
    Clear query history for a session.

    Request JSON:
        {
            "session_id": "uuid-string"
        }

    Response JSON:
        {
            "success": true,
            "session_id": str,
            "message": str
        }
    """
    data = request.get_json(silent=True)
    session_id = (data.get("session_id") or "default").strip() if data else "default"

    query_history.clear(session_id)
    logger.info("History cleared via API for session: %s", session_id)

    return jsonify({
        "success": True,
        "session_id": session_id,
        "message": f"Query history cleared for session '{session_id}'.",
    }), 200
