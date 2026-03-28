"""
DataPilot MCP — Upload Route
POST /api/upload
Saves file to backend/data/uploads/, registers as DuckDB table, returns schema.
"""

import logging
import os

from flask import Blueprint, jsonify, request
from werkzeug.utils import secure_filename

from backend.config import MAX_UPLOAD_MB, UPLOADS_DIR
from backend.mcp.tools import file_reader

logger = logging.getLogger(__name__)

upload_bp = Blueprint("upload", __name__)

_ALLOWED_EXTENSIONS = {".csv", ".parquet"}
_MAX_BYTES = MAX_UPLOAD_MB * 1024 * 1024


@upload_bp.route("/api/upload", methods=["POST"])
def handle_upload():
    """
    Upload a CSV or Parquet file and register it as a DuckDB table.

    Multipart form fields:
        file: The file to upload.

    Response JSON (success):
        {
            "success": true,
            "table_name": str,
            "columns": [{"name": str, "type": str}, ...],
            "row_count": int,
            "filename": str
        }

    Response JSON (error):
        {"error": str}
    """
    if "file" not in request.files:
        return jsonify({"error": "No file provided. Send the file as multipart/form-data with field 'file'."}), 400

    uploaded_file = request.files["file"]
    if not uploaded_file.filename:
        return jsonify({"error": "Filename is empty."}), 400

    # Validate extension
    ext = os.path.splitext(uploaded_file.filename)[1].lower()
    if ext not in _ALLOWED_EXTENSIONS:
        return jsonify({
            "error": f"File type '{ext}' is not supported. Allowed types: {', '.join(_ALLOWED_EXTENSIONS)}"
        }), 400

    # Validate file size: read content and check length
    content = uploaded_file.read()
    if len(content) > _MAX_BYTES:
        return jsonify({
            "error": f"File size exceeds the {MAX_UPLOAD_MB} MB limit."
        }), 413

    # Save to uploads directory
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    safe_name = secure_filename(uploaded_file.filename)
    save_path = os.path.join(UPLOADS_DIR, safe_name)

    with open(save_path, "wb") as f:
        f.write(content)

    logger.info("File saved: %s (%d bytes)", save_path, len(content))

    # Register with DuckDB
    result = file_reader.read_file(save_path)

    if not result["success"]:
        # Clean up the saved file on registration failure
        try:
            os.remove(save_path)
        except OSError:
            pass
        return jsonify({"error": result["error"]}), 500

    return jsonify({
        "success": True,
        "table_name": result["table_name"],
        "columns": result["columns"],
        "row_count": result["row_count"],
        "filename": safe_name,
        "profile": result.get("profile", []),
    }), 200
