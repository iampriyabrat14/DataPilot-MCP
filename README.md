# DataPilot-MCP 🧠

> **Chat with your data.** MCP-based AI agent that queries SQL/CSV files, auto-generates charts, and delivers executive AI summaries — all in plain English.

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-000000?style=flat&logo=flask&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat&logo=duckdb&logoColor=black)
![Groq](https://img.shields.io/badge/Groq_LLM-F55036?style=flat&logoColor=white)
![MCP](https://img.shields.io/badge/MCP-Protocol-1C3C3C?style=flat&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

---

## What It Does

Ask questions about your data in plain English. DataPilot:

1. Converts your question into SQL using an LLM
2. Executes it against your CSV/SQL data via DuckDB
3. Auto-selects the right chart type and renders it
4. Generates an executive summary of the insights
5. Optionally emails the report via SMTP/SendGrid

---

## Architecture

```
User Query (Natural Language)
        │
        ▼
┌─────────────────────┐
│   MCP Agent Layer   │  ← Query understanding, tool selection
└──────────┬──────────┘
           │
    ┌──────┴──────┐
    ▼             ▼
SQL Tool      Chart Tool
(DuckDB)    (Chart.js)
    │             │
    └──────┬──────┘
           ▼
  ┌─────────────────┐
  │ Summary Agent   │  ← Groq Llama 3.3 70B
  └────────┬────────┘
           ▼
   Executive Report + Email
```

---

## Features

- **Natural Language → SQL** — Ask questions, get SQL-powered answers
- **Auto Chart Generation** — Bar, line, pie, scatter — chosen automatically
- **SSE Streaming** — Real-time response streaming via Server-Sent Events
- **Data Profiling** — Automatic schema detection, anomaly highlighting
- **Query Suggestions** — AI-generated follow-up questions
- **Email Reports** — Send summaries via SMTP or SendGrid
- **SQL Retry Logic** — Auto-corrects malformed SQL on failure
- **Evaluation Metrics** — Query success rate, latency tracking
- **Multi-format** — Supports CSV and Parquet files

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| LLM | Groq (Llama 3.3 70B) + OpenAI fallback |
| Protocol | Model Context Protocol (MCP) |
| Database | DuckDB (in-memory, SQL + CSV + Parquet) |
| Backend | Python, Flask, Flask-CORS |
| Frontend | HTML5, CSS3, Vanilla JS, Chart.js |
| Streaming | Server-Sent Events (SSE) |
| Deployment | Gunicorn + Gevent, Render.com ready |

---

## Quick Start

```bash
git clone https://github.com/iampriyabrat14/DataPilot-MCP.git
cd DataPilot-MCP
pip install -r requirements.txt
cp .env.example .env   # Add your GROQ_API_KEY
python app.py
```

Open `http://localhost:5000` and start asking questions about your data.

---

## Project Structure

```
DataPilot-MCP/
├── app.py                 # Flask entrypoint
├── backend/
│   ├── agents/            # MCP agent orchestration
│   ├── llm_clients/       # Groq + OpenAI clients
│   ├── db/                # DuckDB query engine
│   ├── tools/             # SQL, chart, email, file tools
│   ├── utils/             # Helpers, evaluation metrics
│   └── routes/            # API endpoints
├── frontend/              # HTML/CSS/JS single-page UI
├── requirements.txt
└── .env.example
```

---

## License

MIT © 2026 Priyabrat Dalbehera
