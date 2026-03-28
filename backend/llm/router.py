"""
DataPilot MCP — LLM Router
Tries Groq first; falls back to OpenAI on any exception.
Logs which provider was used.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def complete(
    messages: list[dict],
    tools: list[dict] | None = None,
    temperature: float = 0.0,
    max_tokens: int = 4096,
) -> dict[str, Any]:
    """
    Route an LLM completion request through Groq → OpenAI fallback.

    Returns the standard response dict from whichever provider succeeded,
    with an extra "provider" key indicating which was used:
        {
            "content": str,
            "tool_calls": list,
            "usage": dict,
            "model": str,
            "provider": "groq" | "openai",
        }

    Raises RuntimeError if both providers fail.
    """
    # --- Try Groq ---
    groq_error_msg = None
    try:
        from backend.llm import groq_client
        result = groq_client.complete(
            messages=messages,
            tools=tools,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        result["provider"] = "groq"
        logger.info("LLM provider used: groq (model=%s)", result.get("model"))
        return result
    except Exception as groq_err:
        groq_error_msg = str(groq_err)
        logger.warning("Groq failed (%s), falling back to OpenAI.", groq_err)

    # --- Fallback to OpenAI ---
    try:
        from backend.llm import openai_client
        result = openai_client.complete(
            messages=messages,
            tools=tools,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        result["provider"] = "openai"
        logger.info("LLM provider used: openai (model=%s)", result.get("model"))
        return result
    except Exception as oai_err:
        logger.error("OpenAI also failed: %s", oai_err)
        raise RuntimeError(
            f"Both LLM providers failed. Groq error: {groq_error_msg}. OpenAI error: {oai_err}."
        ) from oai_err
