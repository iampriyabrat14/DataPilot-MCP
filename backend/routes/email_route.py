"""
DataPilot MCP — Email Route
POST /api/send-email
Validates input and sends a business summary email via the email_sender tool.
"""

import logging

from flask import Blueprint, jsonify, request

from backend.mcp.tools import email_sender

logger = logging.getLogger(__name__)

email_bp = Blueprint("email", __name__)


@email_bp.route("/api/send-email", methods=["POST"])
def handle_send_email():
    """
    Send a business summary email to the specified recipient.

    Request JSON:
        {
            "recipient": "user@example.com",
            "subject": "DataPilot Summary",
            "summary": "<ul><li>...</li></ul>",
            "chart_image": "data:image/png;base64,..."   (optional)
        }

    Response JSON (success):
        {
            "success": true,
            "message_id": str,
            "recipient": str
        }

    Response JSON (error):
        {"error": str}
    """
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"error": "Request body must be JSON."}), 400

    recipient = (data.get("recipient") or "").strip()
    subject = (data.get("subject") or "DataPilot — Business Summary").strip()
    summary_html = (data.get("summary") or "").strip()
    chart_image = data.get("chart_image")  # Optional base64 image string

    # Validate required fields
    if not recipient:
        return jsonify({"error": "Field 'recipient' is required."}), 400
    if not summary_html:
        return jsonify({"error": "Field 'summary' is required."}), 400

    logger.info("Send-email request: recipient=%s, subject='%s'", recipient, subject[:80])

    result = email_sender.send(
        recipient=recipient,
        subject=subject,
        summary_html=summary_html,
        chart_image_b64=chart_image,
    )

    if not result.get("success"):
        return jsonify({"error": result.get("error", "Failed to send email.")}), 500

    return jsonify(result), 200
