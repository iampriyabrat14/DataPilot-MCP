"""
DataPilot MCP — Query Route
POST /api/query
Accepts {message, session_id}, runs the analyst agent, logs metrics.
"""

import logging
import time

from flask import Blueprint, jsonify, request

from backend.agents import analyst_agent
from backend.evaluation import metrics

logger = logging.getLogger(__name__)

query_bp = Blueprint("query", __name__)


@query_bp.route("/api/query", methods=["POST"])
def handle_query():
    """
    Run a natural language query through the full analyst pipeline.

    Request JSON:
        {
            "message": "Show me total revenue by region",
            "session_id": "uuid-string"
        }

    Response JSON (success):
        {
            "question": str,
            "rewritten_question": str,
            "sql": str,
            "columns": list,
            "rows": list,
            "row_count": int,
            "sql_latency_ms": float,
            "chart_type": str,
            "chart_config": dict | null,
            "summary": str,
            "attempts": int,
            "provider": str,
            "total_latency_ms": float,
            "success": true
        }

    Response JSON (error):
        {"error": str}
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Request body must be JSON."}), 400

    message = (data.get("message") or "").strip()
    session_id = (data.get("session_id") or "default").strip()

    if not message:
        return jsonify({"error": "Field 'message' is required and cannot be empty."}), 400

    start = time.perf_counter()
    logger.info("Query received: session=%s, message='%s'", session_id, message[:100])

    result = analyst_agent.run(question=message, session_id=session_id)

    total_ms = (time.perf_counter() - start) * 1000

    # Log evaluation metrics (non-blocking)
    metrics.log_metric(
        session_id=session_id,
        metric_dict={
            "latency_ms": round(total_ms, 2),
            "sql_latency_ms": result.get("sql_latency_ms", 0),
            "retry_count": max(0, result.get("attempts", 1) - 1),
            "llm_provider_used": result.get("provider", "unknown"),
            "chart_type": result.get("chart_type", "table"),
            "token_count": result.get("usage", {}).get("total_tokens", 0),
            "success": result.get("success", False),
            "question": message,
        },
    )

    if not result.get("success"):
        return jsonify({"error": result.get("error", "Unknown error")}), 500

    return jsonify(result), 200
