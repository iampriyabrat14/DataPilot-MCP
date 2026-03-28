# DataPilot MCP

A ChatGPT-style AI data analyst agent built on the **Model Context Protocol (MCP)**.
Ask questions in plain English — DataPilot writes the SQL, runs it, builds charts, summarizes insights, and emails the report.

---

## Demo

> **"Show me revenue trend for the last 7 days"**

```
User question
    → Query Rewriter     (expands dates, resolves aliases)
    → SQL Generator      (Groq LLM → DuckDB SQL)
    → SQL Executor       (with auto-retry on failure)
    → Chart Suggester    (picks best chart type)
    → Business Summarizer (CCO-style bullet points)
    → Email Sender       (optional)
```

---

## Features

| Feature | Description |
|---|---|
| Natural language → SQL | Ask anything in plain English; LLM converts it to DuckDB SQL |
| Live SSE streaming | Step-by-step progress card in chat (no full-page spinner) |
| Auto chart generation | Line, bar, pie, scatter — auto-selected from result shape |
| Business summary | 3–5 executive bullet points after every query |
| Email report | Send summary + chart to any email address |
| SQL retry loop | Auto-fixes invalid SQL up to 3 times by feeding errors back to LLM |
| Query rewriter | Expands relative dates, resolves column aliases, injects schema context |
| Data profiling | Instant column stats (null %, unique count, min/max, mean) on upload |
| Query suggestions | LLM generates 4 tailored questions after every file upload |
| Anomaly highlighting | Outlier cells highlighted red/blue (> 2 std dev) |
| Follow-up queries | "Filter to Q1 only" — agent references the previous SQL |
| Eval metrics panel | Live sidebar: latency, retry count, token usage, provider per query |
| CSV + PNG export | Download any result table or chart with one click |
| Dark / Light theme | Toggle persisted in localStorage |
| File support | CSV and Parquet via drag-and-drop or file picker |

---

## Tech Stack

```
Frontend   HTML5 · CSS3 · Vanilla JS · Chart.js
Backend    Python 3.11+ · Flask · Flask-CORS
LLM        Groq (llama-3.3-70b-versatile) → OpenAI fallback (gpt-4o-mini)
Database   DuckDB (in-memory, zero config)
Email      SMTP (smtplib) or SendGrid
Data I/O   DuckDB native CSV/Parquet reader · pandas · pyarrow
```

---

## Project Structure

```
DataPilot-MCP/
├── run.py                          # Entry point — run from project root
├── .env.example                    # All environment variables documented
├── requirements.txt
│
├── backend/
│   ├── app.py                      # Flask app + blueprint registration
│   ├── config.py                   # Env var loader with defaults
│   │
│   ├── llm/
│   │   ├── groq_client.py          # Groq SDK wrapper
│   │   ├── openai_client.py        # OpenAI SDK wrapper
│   │   └── router.py               # Groq → OpenAI fallback router
│   │
│   ├── data/
│   │   └── db.py                   # DuckDB singleton, query executor, schema reader
│   │
│   ├── mcp/
│   │   ├── tools/
│   │   │   ├── sql_executor.py     # MCP Tool: run SQL
│   │   │   ├── chart_generator.py  # MCP Tool: build Chart.js config
│   │   │   ├── file_reader.py      # MCP Tool: load CSV/Parquet + data profile
│   │   │   └── email_sender.py     # MCP Tool: send email via SMTP/SendGrid
│   │   └── context/
│   │       └── query_history.py    # Session query history (last 10 per session)
│   │
│   ├── agents/
│   │   ├── analyst_agent.py        # Full pipeline orchestrator
│   │   ├── query_rewriter.py       # Rewrite ambiguous questions
│   │   ├── sql_retry.py            # Generate SQL with retry loop
│   │   └── chart_suggester.py      # Auto-select chart type
│   │
│   ├── utils/
│   │   ├── summarizer.py           # CCO-style business summary generator
│   │   └── latency.py              # @track_latency decorator
│   │
│   ├── evaluation/
│   │   └── metrics.py              # Per-query metrics logger (memory + JSONL)
│   │
│   └── routes/
│       ├── query.py                # POST /api/query
│       ├── query_stream.py         # GET  /api/query/stream  (SSE)
│       ├── upload.py               # POST /api/upload
│       ├── schema.py               # GET  /api/schema
│       ├── suggestions.py          # POST /api/suggestions
│       ├── email_route.py          # POST /api/send-email
│       ├── metrics_route.py        # GET  /api/metrics
│       └── history.py              # DELETE /api/history
│
└── frontend/
    ├── index.html                  # Single-page chat UI
    ├── css/style.css               # Dark theme + all component styles
    └── js/
        ├── main.js                 # Chat logic, SSE, upload, schema, metrics
        ├── chart_render.js         # Chart.js render + capture helpers
        └── email.js                # Email modal logic
```

---

## Quick Start

### 1. Clone & install

```bash
git clone https://github.com/yourname/DataPilot-MCP.git
cd DataPilot-MCP
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
```

Open `.env` and set at minimum:

```env
GROQ_API_KEY=your_groq_key_here
GROQ_MODEL=llama-3.3-70b-versatile
```

Get a free Groq API key at [console.groq.com](https://console.groq.com).

### 3. Run

```bash
python run.py
```

Open [http://localhost:5000](http://localhost:5000)

---

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/query` | Run full analyst pipeline (non-streaming) |
| `GET` | `/api/query/stream` | Same pipeline as SSE stream |
| `POST` | `/api/upload` | Upload CSV or Parquet file |
| `GET` | `/api/schema` | List all tables and columns |
| `POST` | `/api/suggestions` | Get LLM-generated questions for a table |
| `POST` | `/api/send-email` | Send business summary by email |
| `GET` | `/api/metrics` | Evaluation metrics log |
| `DELETE` | `/api/history` | Clear session query history |

### POST /api/query

**Request**
```json
{
  "message": "Show top 10 products by revenue",
  "session_id": "uuid-here"
}
```

**Response**
```json
{
  "sql": "SELECT product, SUM(revenue) ...",
  "columns": ["product", "total_revenue"],
  "rows": [["Widget A", 124500], ...],
  "row_count": 10,
  "chart_type": "bar",
  "chart_config": { ... },
  "summary": "<ul><li>Widget A leads with $124k...</li></ul>",
  "provider": "groq",
  "attempts": 1,
  "total_latency_ms": 1842
}
```

---

## MCP Tools

DataPilot is structured around four MCP tools callable by the agent:

### `sql_executor`
Executes a SQL string against DuckDB. Returns columns, rows, row count, and latency. Returns a structured error on failure so the retry loop can fix the SQL.

### `chart_generator`
Takes query results and an optional chart type hint. Builds a complete Chart.js config with dark-theme palette. Auto-selects chart type based on result shape:

| Result shape | Chart type |
|---|---|
| 1 date column + 1 numeric | `line` |
| 1 category + 1 numeric | `bar` |
| Percentage / share data | `pie` |
| 2 numeric columns | `scatter` |
| Everything else | `table` |

### `file_reader`
Registers CSV or Parquet files as virtual DuckDB tables. Returns schema + full column profile (null %, unique count, min/max, mean/std dev).

### `email_sender`
Sends an HTML email with the business summary and an optional embedded chart image (base64 PNG). Supports SMTP and SendGrid.

---

## LLM Router

```
Request
  ├─ Groq  (llama-3.3-70b-versatile)
  │    └─ Success → return
  │    └─ Fail    → log warning
  └─ OpenAI (gpt-4o-mini)  [fallback]
       └─ Success → return
       └─ Fail    → RuntimeError
```

Both clients share an identical interface: `complete(messages, tools, temperature, max_tokens)`.
The router adds a `"provider"` key to every response so metrics can track which was used.

---

## SQL Retry Loop

```
Question + Schema
    → LLM generates SQL
    → Execute in DuckDB
         ├─ Success → continue
         └─ Error   → append (bad SQL + error) to prompt → retry
                           max 3 attempts
                           after 3 failures → RuntimeError
```

---

## Evaluation Metrics

After every query, the following is recorded and shown live in the sidebar:

| Metric | Description |
|---|---|
| `latency_ms` | Total end-to-end time |
| `llm_latency_ms` | Time in LLM calls only |
| `sql_latency_ms` | Time executing SQL |
| `retry_count` | SQL retries needed (0 = perfect) |
| `llm_provider_used` | `groq` or `openai` |
| `chart_type` | Auto-selected chart type |
| `token_count` | Prompt + completion tokens |
| `success` | Boolean |

Metrics are also persisted to `backend/evaluation/logs/metrics.jsonl`.

---

## Environment Variables

```env
# LLM
GROQ_API_KEY=
GROQ_MODEL=llama-3.3-70b-versatile
OPENAI_API_KEY=
OPENAI_MODEL=gpt-4o-mini

# Database
DB_BACKEND=duckdb
MAX_UPLOAD_MB=50

# Agent
SQL_MAX_RETRIES=3
QUERY_HISTORY_LIMIT=10

# Email
EMAIL_PROVIDER=smtp
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=
SMTP_PASSWORD=
SENDGRID_API_KEY=
EMAIL_FROM=datapilot@yourdomain.com

# Flask
FLASK_ENV=development
FLASK_PORT=5000
SECRET_KEY=change-me
```

---

## Security

- Uploaded files validated for type (CSV/Parquet) and size before processing
- SQL runs inside DuckDB sandbox — no filesystem access from queries
- Email recipient validated server-side before sending
- No secrets in code — all via `.env` (git-ignored)
- CORS restricted to `localhost` in development

---

## License

MIT
