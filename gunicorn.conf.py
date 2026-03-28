# Gunicorn configuration for DataPilot MCP
# Run with: gunicorn -c gunicorn.conf.py "backend.app:app"

import os

# ── Workers ──────────────────────────────────────────────────────────
# gevent worker is required for SSE (Server-Sent Events) streaming
worker_class = "gevent"
workers = int(os.getenv("WEB_CONCURRENCY", "2"))
worker_connections = 1000

# ── Network ──────────────────────────────────────────────────────────
bind = f"0.0.0.0:{os.getenv('FLASK_PORT', '5000')}"

# ── Timeouts ─────────────────────────────────────────────────────────
# SSE connections are long-lived — raise timeout to avoid premature kills
timeout = 120
keepalive = 5

# ── Logging ──────────────────────────────────────────────────────────
accesslog = "-"   # stdout
errorlog  = "-"   # stderr
loglevel  = os.getenv("LOG_LEVEL", "info")

# ── Lifecycle ────────────────────────────────────────────────────────
preload_app = True  # Load app before forking workers (saves memory)
