"""
DataPilot MCP — SQL Retry Agent
Generates SQL from a question with retry logic on execution failure.
Up to SQL_MAX_RETRIES attempts; each retry feeds the error back to the LLM.
"""

import logging
import re
from typing import Any

from backend.config import SQL_MAX_RETRIES
from backend.data.db import schema_as_text
from backend.llm import router
from backend.mcp.context import query_history
from backend.mcp.tools import sql_executor

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are an expert SQL analyst. Generate a single valid DuckDB SQL query.

Rules:
1. Return ONLY the raw SQL statement — no markdown, no code fences, no explanation.
2. Use only tables and columns that exist in the provided schema.
3. Use DuckDB syntax (e.g., CURRENT_DATE, DATE_TRUNC, INTERVAL syntax).
4. Always include a LIMIT clause (default LIMIT 500) unless the question asks for all rows.
5. Use aliases for clarity in multi-table queries.
6. Never use INSERT, UPDATE, DELETE, DROP, CREATE, or any DDL/DML statement.
7. If aggregating, always use GROUP BY for non-aggregated columns.
"""


def _extract_sql(text: str) -> str:
    """Extract raw SQL from LLM response, stripping markdown code fences if present."""
    text = text.strip()
    # Remove ```sql ... ``` or ``` ... ```
    fence_match = re.search(r"```(?:sql)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    if fence_match:
        return fence_match.group(1).strip()
    # Remove leading/trailing backticks
    text = re.sub(r"^`+|`+$", "", text).strip()
    return text


def generate_and_run(
    question: str,
    session_id: str = "",
) -> dict[str, Any]:
    """
    Generate SQL for the given question and execute it.
    Retries up to SQL_MAX_RETRIES times on failure, feeding errors back to the LLM.

    Args:
        question: The (possibly rewritten) user question.
        session_id: Session ID for history context.

    Returns:
        {
            "sql": str,
            "result": {columns, rows, row_count, latency_ms},
            "attempts": int,
        }

    Raises:
        RuntimeError: If all retry attempts are exhausted.
    """
    schema = schema_as_text()
    history_text = query_history.get_as_text(session_id)

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Schema:\n{schema}\n\n"
                f"Previous queries in this session:\n{history_text}\n\n"
                f"Question: {question}\n\n"
                f"SQL:"
            ),
        },
    ]

    last_error: str = ""
    last_sql: str = ""

    for attempt in range(1, SQL_MAX_RETRIES + 1):
        logger.info("SQL generation attempt %d/%d", attempt, SQL_MAX_RETRIES)

        # On retries, append error context
        if attempt > 1 and last_error and last_sql:
            messages.append(
                {
                    "role": "assistant",
                    "content": last_sql,
                }
            )
            messages.append(
                {
                    "role": "user",
                    "content": (
                        f"The following SQL failed:\n{last_sql}\n\n"
                        f"Error: {last_error}\n\n"
                        f"Please fix the SQL and return only the corrected query."
                    ),
                }
            )

        try:
            llm_response = router.complete(messages=messages, temperature=0.0, max_tokens=1024)
        except Exception as exc:
            raise RuntimeError(f"LLM call failed: {exc}") from exc

        raw_content = llm_response.get("content", "")
        sql = _extract_sql(raw_content)

        if not sql:
            last_error = "LLM returned an empty response."
            last_sql = ""
            logger.warning("Attempt %d: empty SQL from LLM.", attempt)
            continue

        last_sql = sql
        exec_result = sql_executor.run(sql)

        if exec_result["success"]:
            logger.info("SQL succeeded on attempt %d.", attempt)
            return {
                "sql": sql,
                "result": {
                    "columns": exec_result["columns"],
                    "rows": exec_result["rows"],
                    "row_count": exec_result["row_count"],
                    "latency_ms": exec_result["latency_ms"],
                },
                "attempts": attempt,
                "provider": llm_response.get("provider", "unknown"),
                "usage": llm_response.get("usage", {}),
            }

        last_error = exec_result["error"]
        logger.warning(
            "Attempt %d failed: %s | SQL: %s", attempt, last_error[:200], sql[:200]
        )

    # All retries exhausted
    raise RuntimeError(
        f"SQL generation failed after {SQL_MAX_RETRIES} attempts. "
        f"Last error: {last_error} | Last SQL: {last_sql}"
    )
