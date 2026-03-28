"""
DataPilot MCP — Groq API Client
Wraps the Groq SDK with a standard interface.
Returns: {"content": str, "tool_calls": list, "usage": dict}
"""

import logging
from typing import Any

from groq import Groq

from backend.config import GROQ_API_KEY, GROQ_MODEL

logger = logging.getLogger(__name__)

_client: Groq | None = None


def _get_client() -> Groq:
    global _client
    if _client is None:
        if not GROQ_API_KEY:
            raise ValueError("GROQ_API_KEY is not set in environment variables.")
        _client = Groq(api_key=GROQ_API_KEY)
    return _client


def complete(
    messages: list[dict],
    tools: list[dict] | None = None,
    model: str | None = None,
    temperature: float = 0.0,
    max_tokens: int = 4096,
) -> dict[str, Any]:
    """
    Call the Groq chat completion API.

    Args:
        messages: List of {"role": ..., "content": ...} dicts.
        tools: Optional list of tool definitions (OpenAI-compatible format).
        model: Override the default model.
        temperature: Sampling temperature (0 = deterministic).
        max_tokens: Maximum completion tokens.

    Returns:
        {
            "content": str,           # assistant text response
            "tool_calls": list,       # list of tool call dicts (may be empty)
            "usage": dict,            # token usage from the API
            "model": str,             # model that was used
        }
    """
    client = _get_client()
    chosen_model = model or GROQ_MODEL

    kwargs: dict[str, Any] = {
        "model": chosen_model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if tools:
        kwargs["tools"] = tools
        kwargs["tool_choice"] = "auto"

    logger.debug("Groq request: model=%s, messages=%d", chosen_model, len(messages))
    response = client.chat.completions.create(**kwargs)

    message = response.choices[0].message
    content: str = message.content or ""
    tool_calls: list = []

    if message.tool_calls:
        for tc in message.tool_calls:
            tool_calls.append(
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
            )

    usage = {}
    if response.usage:
        usage = {
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
            "total_tokens": response.usage.total_tokens,
        }

    logger.debug("Groq response: %d chars, %d tool_calls", len(content), len(tool_calls))

    return {
        "content": content,
        "tool_calls": tool_calls,
        "usage": usage,
        "model": chosen_model,
    }
