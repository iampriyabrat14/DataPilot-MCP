"""
DataPilot MCP — Business Summarizer
Calls the LLM to produce a CCO-style 3-5 bullet executive summary.
Returns an HTML string with <ul><li> bullets.
"""

import json
import logging
from typing import Any

from backend.llm import router

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are a Chief Commercial Officer writing a concise executive briefing.
Summarize the provided data query results in 3 to 5 bullet points.

Rules:
1. Each bullet must be specific — include actual numbers from the results.
2. Write in plain business English — no SQL, no jargon, no column names.
3. Tone: confident, direct, insight-driven.
   Examples: "Revenue grew 12% week-over-week, driven by the North region."
             "Top 3 products account for 68% of total units sold."
4. If results are empty or sparse, say so clearly in 1 bullet.
5. Return ONLY the bullet points as an HTML unordered list:
   <ul>
     <li>Bullet one</li>
     <li>Bullet two</li>
   </ul>
   No preamble, no closing remarks, no markdown.
"""

_MAX_ROWS_FOR_SUMMARY = 50  # Don't send all rows to the LLM; sample if large


def summarize(
    question: str,
    columns: list[str],
    rows: list[list],
) -> str:
    """
    Generate an HTML business summary for a query result.

    Args:
        question: The user question (or rewritten version).
        columns: Column names from the result.
        rows: Data rows.

    Returns:
        HTML string: <ul><li>...</li>...</ul>
    """
    if not rows:
        return "<ul><li>The query returned no results for this question.</li></ul>"

    # Truncate rows to avoid token overflow
    sample_rows = rows[:_MAX_ROWS_FOR_SUMMARY]
    has_more = len(rows) > _MAX_ROWS_FOR_SUMMARY

    # Format data as compact text
    result_text = _format_result(columns, sample_rows)
    if has_more:
        result_text += f"\n... (showing {_MAX_ROWS_FOR_SUMMARY} of {len(rows)} rows)"

    user_content = (
        f"Question: {question}\n\n"
        f"Query Results:\n{result_text}\n\n"
        f"Write the executive summary now:"
    )

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]

    try:
        response = router.complete(messages=messages, temperature=0.2, max_tokens=512)
        html = response["content"].strip()

        # Validate it contains a list; if not, wrap it
        if html and "<ul>" not in html.lower():
            lines = [line.strip() for line in html.split("\n") if line.strip() and line.strip() != "-"]
            bullets = "".join(f"<li>{line.lstrip('•-* ')}</li>" for line in lines if line)
            html = f"<ul>{bullets}</ul>" if bullets else f"<ul><li>{html}</li></ul>"

        return html if html else _fallback_summary(columns, rows)

    except Exception as exc:
        logger.warning("Summarizer LLM call failed: %s", exc)
        return _fallback_summary(columns, rows)


def _format_result(columns: list[str], rows: list[list]) -> str:
    """Format result as a pipe-delimited table string."""
    header = " | ".join(columns)
    separator = "-" * len(header)
    data_lines = [" | ".join(str(v) for v in row) for row in rows]
    return "\n".join([header, separator] + data_lines)


def _fallback_summary(columns: list[str], rows: list[list]) -> str:
    """Generate a minimal fallback summary when LLM is unavailable."""
    row_count = len(rows)
    col_count = len(columns)
    return (
        f"<ul>"
        f"<li>Query returned {row_count} row{'s' if row_count != 1 else ''} "
        f"across {col_count} column{'s' if col_count != 1 else ''}.</li>"
        f"<li>Columns: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}.</li>"
        f"</ul>"
    )
