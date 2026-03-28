"""
DataPilot MCP — Query Suggestions Route
POST /api/suggestions
Given a table name, asks the LLM to suggest 4 analytical questions a business analyst
would ask about that table's schema.
"""

import json
import logging

from flask import Blueprint, jsonify, request

from backend.data.db import get_schema
from backend.llm import router

logger = logging.getLogger(__name__)

suggestions_bp = Blueprint("suggestions", __name__)


@suggestions_bp.route("/api/suggestions", methods=["POST"])
def get_suggestions():
    """
    Return 4 LLM-generated analytical question suggestions for a given table.

    Request JSON:
        {
            "table_name": str,
            "session_id": str   (optional)
        }

    Response JSON:
        {
            "suggestions": ["...", "...", "...", "..."]
        }
    """
    data = request.get_json(silent=True) or {}
    table_name = (data.get("table_name") or "").strip()
    session_id = (data.get("session_id") or "default").strip()

    if not table_name:
        return jsonify({"error": "Field 'table_name' is required."}), 400

    # Build schema string for just this table
    schema = get_schema()
    table_cols = schema.get(table_name)

    if table_cols is None:
        return jsonify({"error": f"Table '{table_name}' not found in the database."}), 404

    col_str = ", ".join(f"{c['name']} {c['type']}" for c in table_cols)
    schema_text = f"TABLE {table_name}: {col_str}"

    prompt = (
        f"Given this table schema: {schema_text}, "
        f"suggest 4 specific analytical questions a business analyst would ask. "
        f"Return ONLY a JSON array of 4 question strings, nothing else."
    )

    messages = [
        {
            "role": "system",
            "content": (
                "You are a helpful data analyst. When asked for question suggestions, "
                "return ONLY a valid JSON array of strings. No markdown, no explanation."
            ),
        },
        {"role": "user", "content": prompt},
    ]

    try:
        response = router.complete(messages=messages, temperature=0.3, max_tokens=512)
        content = response.get("content", "").strip()

        # Try to extract JSON array from the response
        # The LLM sometimes wraps it in ```json ... ```
        if content.startswith("```"):
            lines = content.splitlines()
            content = "\n".join(
                l for l in lines
                if not l.strip().startswith("```")
            ).strip()

        suggestions = json.loads(content)

        if not isinstance(suggestions, list):
            raise ValueError("LLM did not return a JSON array.")

        # Ensure exactly 4 strings, trim or pad
        suggestions = [str(s) for s in suggestions[:4]]
        while len(suggestions) < 4:
            suggestions.append(f"What insights can you find in the {table_name} table?")

        return jsonify({"suggestions": suggestions}), 200

    except json.JSONDecodeError as exc:
        logger.warning("Suggestions JSON parse error: %s. Raw: %s", exc, content[:200])
        # Fallback suggestions
        fallback = [
            f"What is the total count of records in {table_name}?",
            f"Show the distribution of values in {table_name}.",
            f"What are the top 10 rows by the first numeric column in {table_name}?",
            f"Are there any null values in {table_name}?",
        ]
        return jsonify({"suggestions": fallback}), 200

    except Exception as exc:
        logger.exception("Suggestions route error: %s", exc)
        return jsonify({"error": str(exc)}), 500
