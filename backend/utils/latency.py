"""
DataPilot MCP — Latency Tracking Decorator
@track_latency injects latency_ms into the return dict of any function.
"""

import functools
import logging
import time
from typing import Any, Callable

logger = logging.getLogger(__name__)


def track_latency(func: Callable) -> Callable:
    """
    Decorator that measures wall-clock time for the wrapped function.
    Injects a `latency_ms` key into the returned dict.

    Usage:
        @track_latency
        def my_func() -> dict:
            ...
            return {"result": 42}
        # Returns: {"result": 42, "latency_ms": 12.34}

    Notes:
        - The wrapped function MUST return a dict.
        - If it returns something else, `latency_ms` is logged but not injected.
        - If an exception is raised, it propagates unchanged.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed_ms = (time.perf_counter() - start) * 1000

        if isinstance(result, dict):
            result["latency_ms"] = round(elapsed_ms, 2)
        else:
            logger.debug(
                "@track_latency: %s returned non-dict type %s (%.1f ms)",
                func.__name__,
                type(result).__name__,
                elapsed_ms,
            )
        return result

    return wrapper


class LatencyTimer:
    """
    Context manager for measuring latency of a code block.

    Usage:
        with LatencyTimer() as t:
            do_something()
        print(t.elapsed_ms)
    """

    def __init__(self):
        self._start: float = 0.0
        self.elapsed_ms: float = 0.0

    def __enter__(self) -> "LatencyTimer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args) -> None:
        self.elapsed_ms = (time.perf_counter() - self._start) * 1000
