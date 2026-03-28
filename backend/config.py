"""
DataPilot MCP — Configuration Loader
Reads all environment variables with sensible defaults.
"""

import os
from dotenv import load_dotenv

# Load .env file from the project root (two levels up from this file)
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(_ROOT, ".env"))


# ------------------------------------------------------------------ #
# LLM
# ------------------------------------------------------------------ #
GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
GROQ_MODEL: str = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# ------------------------------------------------------------------ #
# Database
# ------------------------------------------------------------------ #
DB_BACKEND: str = os.getenv("DB_BACKEND", "duckdb").lower()
POSTGRES_URL: str = os.getenv("POSTGRES_URL", "")
MAX_UPLOAD_MB: int = int(os.getenv("MAX_UPLOAD_MB", "50"))

# ------------------------------------------------------------------ #
# Agent
# ------------------------------------------------------------------ #
SQL_MAX_RETRIES: int = int(os.getenv("SQL_MAX_RETRIES", "3"))
QUERY_HISTORY_LIMIT: int = int(os.getenv("QUERY_HISTORY_LIMIT", "10"))

# ------------------------------------------------------------------ #
# Email
# ------------------------------------------------------------------ #
EMAIL_PROVIDER: str = os.getenv("EMAIL_PROVIDER", "smtp").lower()
SMTP_HOST: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER: str = os.getenv("SMTP_USER", "")
SMTP_PASSWORD: str = os.getenv("SMTP_PASSWORD", "")
SENDGRID_API_KEY: str = os.getenv("SENDGRID_API_KEY", "")
EMAIL_FROM: str = os.getenv("EMAIL_FROM", "datapilot@yourdomain.com")

# ------------------------------------------------------------------ #
# Flask
# ------------------------------------------------------------------ #
FLASK_ENV: str = os.getenv("FLASK_ENV", "development")
FLASK_PORT: int = int(os.getenv("FLASK_PORT", "5000"))
SECRET_KEY: str = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")

# CORS — comma-separated list of allowed origins
# e.g. "https://datapilot.yourdomain.com,https://www.yourdomain.com"
_raw_origins: str = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:5000,http://localhost:3000,http://127.0.0.1:5000"
)
ALLOWED_ORIGINS: list[str] = [o.strip() for o in _raw_origins.split(",") if o.strip()]

IS_PRODUCTION: bool = FLASK_ENV == "production"

# ------------------------------------------------------------------ #
# Derived paths
# ------------------------------------------------------------------ #
BACKEND_DIR: str = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR: str = os.path.join(_ROOT, "frontend")
UPLOADS_DIR: str = os.path.join(BACKEND_DIR, "data", "uploads")
EVAL_LOGS_DIR: str = os.path.join(BACKEND_DIR, "evaluation", "logs")
