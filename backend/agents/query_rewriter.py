"""
DataPilot MCP — Query Rewriter Agent
Uses the LLM router to rewrite ambiguous user questions before SQL generation.
Responsibilities:
  - Expand relative dates ("last 7 days" → explicit date predicates)
  - Resolve ambiguous column/table references using schema context
  - Handle follow-up modifications by incorporating the last executed SQL
  - Produce a clean, unambiguous question string
"""

import logging

from backend.data.db import schema_as_text
from backend.llm import router
from backend.mcp.context import query_history

logger = logging.getLogger(__name__)


def rewrite(question: str, session_id: str = "") -> str:
    """
    Rewrite a user question to be precise and schema-aware.
    Includes the last executed SQL from session history so follow-up
    questions like "now show only Q1" correctly modify the prior query.

    Args:
        question: The raw user question.
        session_id: Session ID used to fetch history context.

    Returns:
        A rewritten question string.
    """
    schema = schema_as_text()

    # Pull history for context
    history = query_history.get(session_id)
    history_text = ""
    if history:
        lines = []
        for i, entry in enumerate(history, 1):
            lines.append(f"Q{i}: {entry['question']} → {entry['sql']}")
        history_text = "\n".join(lines)

    # Provide the last SQL explicitly so follow-ups can reference it
    last_entry = history[-1] if history else None
    last_sql_context = (
        f"\nLast SQL executed:\n{last_entry['sql']}"
        if last_entry and last_entry.get("sql")
        else ""
    )

    system_prompt = f"""You are a query rewriter for a data analyst AI.
Your job is to rewrite the user's question to be precise and unambiguous.

Rules:
- Expand relative dates to absolute SQL date expressions
  (e.g. "last 7 days" → date >= CURRENT_DATE - INTERVAL 7 DAY)
- If the question says "filter", "narrow", "show only", "change to", "now show" — it likely
  refers to modifying the LAST query
- If it's a follow-up modification, incorporate the previous SQL logic into the rewritten question
- If a column name mentioned matches a schema column, use the exact column name
- Keep the rewritten question as a single sentence or short paragraph
- Do NOT generate SQL — only rewrite the question in clear English
- If the question is already unambiguous, return it unchanged
- Return ONLY the rewritten question text — no explanation, no preamble

Available tables and schema:
{schema}
{last_sql_context}

Conversation history (last {len(history)} queries):
{history_text if history_text else "No previous queries in this session."}"""

    user_content = (
        f"Original question: {question}\n\n"
        f"Rewritten question:"
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]

    try:
        response = router.complete(messages=messages, temperature=0.0, max_tokens=512)
        rewritten = response["content"].strip()
        if not rewritten:
            return question
        logger.debug("Query rewritten: '%s' → '%s'", question[:80], rewritten[:80])
        return rewritten
    except Exception as exc:
        logger.warning("Query rewriter failed (%s), using original question.", exc)
        return question
