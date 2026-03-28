"""
DataPilot MCP — Metrics Route
GET /api/metrics
Returns the evaluation metrics log.
"""

import logging

from flask import Blueprint, jsonify, request

from backend.evaluation import metrics

logger = logging.getLogger(__name__)

metrics_bp = Blueprint("metrics", __name__)


@metrics_bp.route("/api/metrics", methods=["GET"])
def handle_metrics():
    """
    Return evaluation metrics.

    Query parameters:
        session_id (optional): Filter metrics to a specific session.

    Response JSON:
        {
            "metrics": [...],
            "count": int
        }
    """
    session_id = request.args.get("session_id")
    all_metrics = metrics.get_metrics(session_id=session_id if session_id else None)

    return jsonify({
        "metrics": all_metrics,
        "count": len(all_metrics),
    }), 200
