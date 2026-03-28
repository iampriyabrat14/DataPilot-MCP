"""
DataPilot MCP — entry point.
Run from project root:  python run.py
"""
import sys
import os

# Ensure project root is on the path so 'backend' is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.app import app
from backend.config import FLASK_PORT, FLASK_ENV

if __name__ == "__main__":
    debug = FLASK_ENV == "development"
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=debug)
