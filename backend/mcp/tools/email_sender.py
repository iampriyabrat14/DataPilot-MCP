"""
DataPilot MCP — Email Sender Tool
Sends an HTML email via SMTP (smtplib) with summary text and optional embedded chart image.
"""

import base64
import logging
import re
import smtplib
import uuid
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from backend.config import (
    EMAIL_FROM,
    EMAIL_PROVIDER,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USER,
)

logger = logging.getLogger(__name__)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _validate_email(address: str) -> bool:
    return bool(_EMAIL_RE.match(address.strip()))


def send(
    recipient: str,
    subject: str,
    summary_html: str,
    chart_image_b64: str | None = None,
) -> dict[str, Any]:
    """
    Send an HTML email with an optional embedded chart image.

    Args:
        recipient: Destination email address.
        subject: Email subject line.
        summary_html: HTML body content (summary bullets).
        chart_image_b64: Optional base64-encoded PNG chart image.

    Returns:
        {
            "success": True,
            "message_id": str,
            "recipient": str,
        }
        or
        {
            "success": False,
            "error": str,
        }
    """
    if not _validate_email(recipient):
        return {"success": False, "error": f"Invalid recipient email address: {recipient}"}

    try:
        message_id = str(uuid.uuid4())

        if chart_image_b64:
            msg = _build_multipart_message(
                recipient=recipient,
                subject=subject,
                summary_html=summary_html,
                chart_image_b64=chart_image_b64,
                message_id=message_id,
            )
        else:
            msg = _build_simple_message(
                recipient=recipient,
                subject=subject,
                summary_html=summary_html,
                message_id=message_id,
            )

        _smtp_send(msg, recipient)

        logger.info("Email sent to %s (message_id=%s)", recipient, message_id)
        return {
            "success": True,
            "message_id": message_id,
            "recipient": recipient,
        }
    except Exception as exc:
        logger.exception("Failed to send email to %s: %s", recipient, exc)
        return {"success": False, "error": str(exc)}


def _build_simple_message(
    recipient: str,
    subject: str,
    summary_html: str,
    message_id: str,
) -> MIMEMultipart:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = recipient
    msg["Message-ID"] = f"<{message_id}@datapilot>"

    html_body = _wrap_html(summary_html, image_cid=None)
    msg.attach(MIMEText(html_body, "html"))
    return msg


def _build_multipart_message(
    recipient: str,
    subject: str,
    summary_html: str,
    chart_image_b64: str,
    message_id: str,
) -> MIMEMultipart:
    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = recipient
    msg["Message-ID"] = f"<{message_id}@datapilot>"

    image_cid = "chart_image"
    html_body = _wrap_html(summary_html, image_cid=image_cid)

    alt_part = MIMEMultipart("alternative")
    alt_part.attach(MIMEText(html_body, "html"))
    msg.attach(alt_part)

    # Attach chart image
    try:
        # Strip data URI prefix if present
        if "," in chart_image_b64:
            chart_image_b64 = chart_image_b64.split(",", 1)[1]
        img_data = base64.b64decode(chart_image_b64)
        img = MIMEImage(img_data, "png")
        img.add_header("Content-ID", f"<{image_cid}>")
        img.add_header("Content-Disposition", "inline", filename="chart.png")
        msg.attach(img)
    except Exception as e:
        logger.warning("Could not attach chart image: %s", e)

    return msg


def _wrap_html(summary_html: str, image_cid: str | None) -> str:
    chart_section = ""
    if image_cid:
        chart_section = f"""
        <div style="margin: 20px 0; text-align: center;">
            <img src="cid:{image_cid}" alt="Chart" style="max-width: 600px; border-radius: 8px;" />
        </div>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8" />
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                background-color: #f8fafc;
                color: #1e293b;
                margin: 0;
                padding: 0;
            }}
            .container {{
                max-width: 640px;
                margin: 32px auto;
                background: #ffffff;
                border-radius: 12px;
                overflow: hidden;
                box-shadow: 0 4px 24px rgba(0,0,0,0.08);
            }}
            .header {{
                background: linear-gradient(135deg, #7c3aed, #4f46e5);
                padding: 24px 32px;
            }}
            .header h1 {{
                color: #ffffff;
                margin: 0;
                font-size: 22px;
                font-weight: 700;
            }}
            .header p {{
                color: rgba(255,255,255,0.8);
                margin: 4px 0 0;
                font-size: 13px;
            }}
            .body {{
                padding: 32px;
            }}
            ul {{
                padding-left: 20px;
                line-height: 1.8;
            }}
            li {{
                margin-bottom: 8px;
                color: #334155;
            }}
            .footer {{
                background: #f1f5f9;
                padding: 16px 32px;
                text-align: center;
                font-size: 12px;
                color: #94a3b8;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>DataPilot — Business Summary</h1>
                <p>Generated by your AI data analyst</p>
            </div>
            <div class="body">
                {summary_html}
                {chart_section}
            </div>
            <div class="footer">
                Sent via DataPilot MCP &bull; AI-powered data insights
            </div>
        </div>
    </body>
    </html>
    """


def _smtp_send(msg: MIMEMultipart, recipient: str) -> None:
    """Send the message via SMTP with STARTTLS."""
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        if SMTP_USER and SMTP_PASSWORD:
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(EMAIL_FROM, [recipient], msg.as_string())
