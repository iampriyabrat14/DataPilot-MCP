"""
DataPilot MCP — Query Stream Route
GET /api/query/stream
Accepts message and session_id as query params.
Streams SSE progress events for each pipeline stage so the UI shows live feedback.
"""

import json
import logging
import time

from flask import Blueprint, Response, request, stream_with_context

from backend.agents import chart_suggester, query_rewriter, sql_retry
from backend.data.db import get_schema
from backend.evaluation import metrics
from backend.mcp.context import query_history
from backend.mcp.tools import chart_generator
from backend.utils import summarizer

logger = logging.getLogger(__name__)

query_stream_bp = Blueprint("query_stream", __name__)


def _sse(payload: dict) -> str:
    """Format a dict as an SSE data line."""
    return f"data: {json.dumps(payload)}\n\n"


@query_stream_bp.route("/api/query/stream", methods=["GET"])
def stream_query():
    """
    Stream query pipeline progress as Server-Sent Events.

    Query params:
        message: The natural language question.
        session_id: Client session UUID.

    SSE event shapes:
        {"stage": "rewriting",   "message": "..."}
        {"stage": "generating",  "message": "..."}
        {"stage": "executing",   "message": "..."}
        {"stage": "charting",    "message": "..."}
        {"stage": "summarizing", "message": "..."}
        {"stage": "done",        "data": { ...full response... }}
        {"stage": "error",       "message": "..."}
    """
    message = (request.args.get("message") or "").strip()
    session_id = (request.args.get("session_id") or "default").strip()

    if not message:
        def _err():
            yield _sse({"stage": "error", "message": "Field 'message' is required."})
        return Response(stream_with_context(_err()), mimetype="text/event-stream")

    def _generate():
        total_start = time.perf_counter()

        try:
            # -------------------------------------------------------- #
            # Stage 1: Rewriting
            # -------------------------------------------------------- #
            yield _sse({"stage": "rewriting", "message": "Understanding your question…"})
            try:
                rewritten = query_rewriter.rewrite(message, session_id)
            except Exception as exc:
                logger.warning("Rewriter error: %s. Using original.", exc)
                rewritten = message

            # -------------------------------------------------------- #
            # Stage 2: Generating SQL
            # -------------------------------------------------------- #
            yield _sse({"stage": "generating", "message": "Generating SQL query…"})
            try:
                gen_result = sql_retry.generate_and_run(rewritten, session_id)
            except RuntimeError as exc:
                total_ms = (time.perf_counter() - total_start) * 1000
                logger.error("SQL generation exhausted retries: %s", exc)
                yield _sse({"stage": "error", "message": str(exc)})
                return

            sql = gen_result["sql"]
            result = gen_result["result"]
            columns = result["columns"]
            rows = result["rows"]
            sql_latency_ms = result.get("latency_ms", 0.0)
            attempts = gen_result["attempts"]
            provider = gen_result.get("provider", "unknown")
            usage = gen_result.get("usage", {})

            # -------------------------------------------------------- #
            # Stage 3: Executing (already done inside sql_retry, but
            # we surface the stage message here for UX continuity)
            # -------------------------------------------------------- #
            yield _sse({"stage": "executing", "message": "Running query on database…"})

            # -------------------------------------------------------- #
            # Stage 4: Chart
            # -------------------------------------------------------- #
            yield _sse({"stage": "charting", "message": "Building chart…"})
            schema = get_schema()
            col_types = _get_col_types(columns, schema)
            suggested_type = chart_suggester.suggest(columns, rows, col_types)
            chart_result = chart_generator.generate(
                columns=columns,
                rows=rows,
                chart_type=suggested_type,
                col_types=col_types,
                title=rewritten[:80] if rewritten else message[:80],
            )
            chart_type = chart_result["chart_type"]
            chart_config = chart_result["config"]

            # -------------------------------------------------------- #
            # Stage 5: Summary
            # -------------------------------------------------------- #
            yield _sse({"stage": "summarizing", "message": "Writing executive summary…"})
            try:
                summary_html = summarizer.summarize(
                    question=rewritten,
                    columns=columns,
                    rows=rows,
                )
            except Exception as exc:
                logger.warning("Summarizer error: %s. Using fallback.", exc)
                summary_html = f"<ul><li>Query returned {len(rows)} rows.</li></ul>"

            # Save to history
            query_history.add(
                session_id=session_id,
                question=rewritten,
                sql=sql,
                summary=summary_html,
            )

            total_ms = (time.perf_counter() - total_start) * 1000

            # Log metrics
            metrics.log_metric(
                session_id=session_id,
                metric_dict={
                    "latency_ms": round(total_ms, 2),
                    "sql_latency_ms": sql_latency_ms,
                    "retry_count": max(0, attempts - 1),
                    "llm_provider_used": provider,
                    "chart_type": chart_type,
                    "token_count": usage.get("total_tokens", 0),
                    "success": True,
                    "question": message,
                },
            )

            # -------------------------------------------------------- #
            # Stage 6: Done — send full response payload
            # -------------------------------------------------------- #
            yield _sse({
                "stage": "done",
                "data": {
                    "question": message,
                    "rewritten_question": rewritten,
                    "sql": sql,
                    "columns": columns,
                    "rows": rows,
                    "row_count": len(rows),
                    "sql_latency_ms": sql_latency_ms,
                    "chart_type": chart_type,
                    "chart_config": chart_config,
                    "summary": summary_html,
                    "attempts": attempts,
                    "provider": provider,
                    "usage": usage,
                    "total_latency_ms": round(total_ms, 2),
                    "success": True,
                },
            })

        except Exception as exc:
            logger.exception("Unexpected error in query stream: %s", exc)
            yield _sse({"stage": "error", "message": f"Unexpected error: {exc}"})

    return Response(
        stream_with_context(_generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


def _get_col_types(columns: list[str], schema: dict) -> list[str]:
    """Look up column types from schema; falls back to 'varchar'."""
    name_to_type: dict[str, str] = {}
    for table_cols in schema.values():
        for col in table_cols:
            name_to_type[col["name"].lower()] = col["type"]
    return [name_to_type.get(c.lower(), "varchar") for c in columns]
