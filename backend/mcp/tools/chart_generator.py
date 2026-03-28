"""
DataPilot MCP — Chart Generator Tool
Takes query result + optional chart_type hint and produces a Chart.js config JSON.

Auto-suggest logic:
  1 date col + 1 numeric col  → line
  1 category col + 1 numeric  → bar
  percentages (values ~100)   → pie
  2 numeric cols              → scatter
  else                        → table (no chart)
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Heuristic type buckets
_DATE_KEYWORDS = {"date", "time", "year", "month", "week", "day", "period", "quarter"}
_NUMERIC_TYPES = {"int", "float", "double", "decimal", "numeric", "real", "bigint", "smallint", "tinyint", "hugeint", "ubigint", "uinteger", "usmallint", "utinyint"}

# Chart.js colour palette
_PALETTE = [
    "rgba(124, 58, 237, 0.8)",   # purple
    "rgba(59, 130, 246, 0.8)",   # blue
    "rgba(16, 185, 129, 0.8)",   # green
    "rgba(245, 158, 11, 0.8)",   # amber
    "rgba(239, 68, 68, 0.8)",    # red
    "rgba(236, 72, 153, 0.8)",   # pink
    "rgba(14, 165, 233, 0.8)",   # sky
    "rgba(168, 85, 247, 0.8)",   # violet
]


def _is_date_col(col_name: str, col_type: str) -> bool:
    name_lower = col_name.lower()
    type_lower = col_type.lower()
    return (
        any(kw in name_lower for kw in _DATE_KEYWORDS)
        or "date" in type_lower
        or "time" in type_lower
        or "timestamp" in type_lower
    )


def _is_numeric_col(col_type: str) -> bool:
    t = col_type.lower()
    return any(nt in t for nt in _NUMERIC_TYPES)


def _looks_like_percentage(rows: list, col_idx: int) -> bool:
    """Return True if numeric column values look like percentages (0-100 range, sum ~100)."""
    try:
        values = [float(row[col_idx]) for row in rows if row[col_idx] is not None]
        if not values:
            return False
        total = sum(values)
        return 90 <= total <= 110 and all(0 <= v <= 100 for v in values)
    except (TypeError, ValueError):
        return False


def suggest_chart_type(
    columns: list[str],
    rows: list[list],
    col_types: list[str] | None = None,
) -> str:
    """
    Auto-suggest a chart type based on result shape.

    Args:
        columns: Column names.
        rows: Data rows.
        col_types: Optional list of DuckDB type strings matching columns.

    Returns:
        One of: "line", "bar", "pie", "scatter", "table"
    """
    if not columns or not rows:
        return "table"

    n_cols = len(columns)
    types = col_types or ["varchar"] * n_cols

    date_indices = [i for i, (c, t) in enumerate(zip(columns, types)) if _is_date_col(c, t)]
    numeric_indices = [i for i, t in enumerate(types) if _is_numeric_col(t)]
    category_indices = [i for i in range(n_cols) if i not in date_indices and i not in numeric_indices]

    # 1 date + 1 numeric → line
    if len(date_indices) == 1 and len(numeric_indices) >= 1:
        return "line"

    # percentages → pie
    if len(numeric_indices) == 1 and len(category_indices) == 1:
        if _looks_like_percentage(rows, numeric_indices[0]):
            return "pie"
        return "bar"

    # 1 category + 1 numeric → bar
    if len(category_indices) >= 1 and len(numeric_indices) >= 1 and n_cols <= 3:
        return "bar"

    # 2 numerics → scatter
    if len(numeric_indices) >= 2 and len(category_indices) == 0 and len(date_indices) == 0:
        return "scatter"

    return "table"


def generate(
    columns: list[str],
    rows: list[list],
    chart_type: str | None = None,
    col_types: list[str] | None = None,
    title: str = "",
) -> dict[str, Any]:
    """
    Generate a Chart.js config dict from query results.

    Args:
        columns: Column names from the query result.
        rows: Data rows.
        chart_type: Explicit chart type override, or None to auto-suggest.
        col_types: DuckDB column types (used for auto-suggestion).
        title: Optional chart title.

    Returns:
        {
            "chart_type": str,          # final chart type used
            "config": dict,             # Chart.js config object
        }
        or
        {
            "chart_type": "table",
            "config": None,
        }
    """
    if not columns or not rows:
        return {"chart_type": "table", "config": None}

    types = col_types or ["varchar"] * len(columns)
    resolved_type = chart_type or suggest_chart_type(columns, rows, types)

    if resolved_type == "table":
        return {"chart_type": "table", "config": None}

    config = _build_config(resolved_type, columns, rows, types, title)
    return {"chart_type": resolved_type, "config": config}


def _build_config(
    chart_type: str,
    columns: list[str],
    rows: list[list],
    types: list[str],
    title: str,
) -> dict[str, Any]:
    """Build a Chart.js configuration object."""

    date_indices = [i for i, (c, t) in enumerate(zip(columns, types)) if _is_date_col(c, t)]
    numeric_indices = [i for i, t in enumerate(types) if _is_numeric_col(t)]
    category_indices = [i for i in range(len(columns)) if i not in date_indices and i not in numeric_indices]

    # ---- LINE chart ------------------------------------------------
    if chart_type == "line":
        label_idx = date_indices[0] if date_indices else 0
        labels = [str(row[label_idx]) for row in rows]

        datasets = []
        for j, num_idx in enumerate(numeric_indices):
            datasets.append({
                "label": columns[num_idx],
                "data": [_safe_float(row[num_idx]) for row in rows],
                "borderColor": _PALETTE[j % len(_PALETTE)],
                "backgroundColor": _PALETTE[j % len(_PALETTE)].replace("0.8", "0.2"),
                "tension": 0.3,
                "fill": False,
                "pointRadius": 4,
            })

        return {
            "type": "line",
            "data": {"labels": labels, "datasets": datasets},
            "options": _common_options(title),
        }

    # ---- BAR chart -------------------------------------------------
    if chart_type == "bar":
        label_idx = category_indices[0] if category_indices else (date_indices[0] if date_indices else 0)
        labels = [str(row[label_idx]) for row in rows]

        datasets = []
        for j, num_idx in enumerate(numeric_indices):
            colors = [_PALETTE[k % len(_PALETTE)] for k in range(len(rows))]
            datasets.append({
                "label": columns[num_idx],
                "data": [_safe_float(row[num_idx]) for row in rows],
                "backgroundColor": colors if len(numeric_indices) == 1 else _PALETTE[j % len(_PALETTE)],
                "borderColor": colors if len(numeric_indices) == 1 else _PALETTE[j % len(_PALETTE)],
                "borderWidth": 1,
            })

        return {
            "type": "bar",
            "data": {"labels": labels, "datasets": datasets},
            "options": _common_options(title),
        }

    # ---- PIE chart -------------------------------------------------
    if chart_type == "pie":
        label_idx = category_indices[0] if category_indices else 0
        num_idx = numeric_indices[0] if numeric_indices else 1
        labels = [str(row[label_idx]) for row in rows]
        data = [_safe_float(row[num_idx]) for row in rows]
        colors = [_PALETTE[i % len(_PALETTE)] for i in range(len(rows))]

        return {
            "type": "pie",
            "data": {
                "labels": labels,
                "datasets": [{
                    "data": data,
                    "backgroundColor": colors,
                    "borderColor": "#1a1a2e",
                    "borderWidth": 2,
                }],
            },
            "options": {
                "responsive": True,
                "maintainAspectRatio": False,
                "plugins": {
                    "legend": {
                        "position": "right",
                        "labels": {"color": "#e2e8f0"},
                    },
                    "title": {
                        "display": bool(title),
                        "text": title,
                        "color": "#e2e8f0",
                    },
                },
            },
        }

    # ---- SCATTER chart ---------------------------------------------
    if chart_type == "scatter":
        x_idx = numeric_indices[0]
        y_idx = numeric_indices[1]
        scatter_data = [
            {"x": _safe_float(row[x_idx]), "y": _safe_float(row[y_idx])}
            for row in rows
        ]

        return {
            "type": "scatter",
            "data": {
                "datasets": [{
                    "label": f"{columns[y_idx]} vs {columns[x_idx]}",
                    "data": scatter_data,
                    "backgroundColor": _PALETTE[0],
                    "pointRadius": 5,
                }]
            },
            "options": {
                **_common_options(title),
                "scales": {
                    "x": {
                        "title": {"display": True, "text": columns[x_idx], "color": "#94a3b8"},
                        "grid": {"color": "rgba(255,255,255,0.05)"},
                        "ticks": {"color": "#94a3b8"},
                    },
                    "y": {
                        "title": {"display": True, "text": columns[y_idx], "color": "#94a3b8"},
                        "grid": {"color": "rgba(255,255,255,0.05)"},
                        "ticks": {"color": "#94a3b8"},
                    },
                },
            },
        }

    # Fallback
    return None


def _common_options(title: str) -> dict:
    return {
        "responsive": True,
        "maintainAspectRatio": False,
        "plugins": {
            "legend": {
                "labels": {"color": "#e2e8f0"},
            },
            "title": {
                "display": bool(title),
                "text": title,
                "color": "#e2e8f0",
            },
        },
        "scales": {
            "x": {
                "grid": {"color": "rgba(255,255,255,0.05)"},
                "ticks": {"color": "#94a3b8"},
            },
            "y": {
                "grid": {"color": "rgba(255,255,255,0.05)"},
                "ticks": {"color": "#94a3b8"},
            },
        },
    }


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
