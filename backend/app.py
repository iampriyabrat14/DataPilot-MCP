"""
DataPilot MCP — Flask Application Entry Point
"""

import os
from flask import Flask, send_from_directory
from flask_cors import CORS

from backend.config import FLASK_PORT, FLASK_ENV, SECRET_KEY, FRONTEND_DIR, UPLOADS_DIR, EVAL_LOGS_DIR, ALLOWED_ORIGINS

# ------------------------------------------------------------------ #
# Ensure runtime directories exist
# ------------------------------------------------------------------ #
os.makedirs(UPLOADS_DIR, exist_ok=True)
os.makedirs(EVAL_LOGS_DIR, exist_ok=True)

# ------------------------------------------------------------------ #
# Create Flask app
# ------------------------------------------------------------------ #
app = Flask(__name__, static_folder=None)
app.secret_key = SECRET_KEY

# CORS: configurable via ALLOWED_ORIGINS env var
CORS(app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}})

# ------------------------------------------------------------------ #
# Register blueprints
# ------------------------------------------------------------------ #
from backend.routes.query import query_bp
from backend.routes.upload import upload_bp
from backend.routes.schema import schema_bp
from backend.routes.email_route import email_bp
from backend.routes.metrics_route import metrics_bp
from backend.routes.history import history_bp
from backend.routes.query_stream import query_stream_bp
from backend.routes.suggestions import suggestions_bp

app.register_blueprint(query_bp)
app.register_blueprint(upload_bp)
app.register_blueprint(schema_bp)
app.register_blueprint(email_bp)
app.register_blueprint(metrics_bp)
app.register_blueprint(history_bp)
app.register_blueprint(query_stream_bp)
app.register_blueprint(suggestions_bp)

# ------------------------------------------------------------------ #
# Serve frontend static files
# ------------------------------------------------------------------ #
@app.route("/")
def index():
    return send_from_directory(FRONTEND_DIR, "index.html")


@app.route("/css/<path:filename>")
def serve_css(filename):
    return send_from_directory(os.path.join(FRONTEND_DIR, "css"), filename)


@app.route("/js/<path:filename>")
def serve_js(filename):
    return send_from_directory(os.path.join(FRONTEND_DIR, "js"), filename)


@app.route("/<path:filename>")
def serve_static(filename):
    return send_from_directory(FRONTEND_DIR, filename)


# ------------------------------------------------------------------ #
# Error handlers
# ------------------------------------------------------------------ #
@app.errorhandler(404)
def not_found(e):
    from flask import jsonify
    return jsonify({"error": "Not found"}), 404


@app.errorhandler(500)
def internal_error(e):
    from flask import jsonify
    return jsonify({"error": "Internal server error"}), 500


# ------------------------------------------------------------------ #
# Health check (required by Render / Railway / Fly.io)
# ------------------------------------------------------------------ #
@app.route("/health")
def health():
    from flask import jsonify
    return jsonify({"status": "ok"}), 200


# ------------------------------------------------------------------ #
# Entry point — use run.py at project root instead
# ------------------------------------------------------------------ #
