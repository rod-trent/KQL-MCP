"""Result formatting utilities for KQL query output."""

from __future__ import annotations

import csv
import io
import json
from typing import Any, Literal

from kql_mcp.connections.adx import QueryResult


OutputFormat = Literal["table", "json", "csv", "markdown"]


def format_result(
    result: QueryResult,
    fmt: OutputFormat = "markdown",
    max_col_width: int = 80,
) -> str:
    """Format a QueryResult into the requested output format."""
    if result.is_empty:
        return "_No results returned._"

    if fmt == "json":
        return _format_json(result)
    elif fmt == "csv":
        return _format_csv(result)
    elif fmt == "table":
        return _format_table(result, max_col_width)
    else:  # markdown (default)
        return _format_markdown(result, max_col_width)


def _truncate(value: Any, max_width: int) -> str:
    s = str(value) if value is not None else ""
    s = s.replace("\n", " ").replace("\r", "")
    if len(s) > max_width:
        return s[: max_width - 3] + "..."
    return s


def _format_markdown(result: QueryResult, max_col_width: int) -> str:
    cols = result.columns
    rows = result.rows

    # Compute column widths
    widths = [len(c) for c in cols]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], min(len(str(val) if val is not None else ""), max_col_width))

    sep = "| " + " | ".join("-" * w for w in widths) + " |"
    header = "| " + " | ".join(c.ljust(widths[i]) for i, c in enumerate(cols)) + " |"

    lines = [header, sep]
    for row in rows:
        cells = [_truncate(val, max_col_width).ljust(widths[i]) for i, val in enumerate(row)]
        lines.append("| " + " | ".join(cells) + " |")

    lines.append(f"\n_{result.row_count} row(s) — {result.execution_time_ms}ms_")
    return "\n".join(lines)


def _format_table(result: QueryResult, max_col_width: int) -> str:
    try:
        from tabulate import tabulate
        rows = [[_truncate(v, max_col_width) for v in row] for row in result.rows]
        table = tabulate(rows, headers=result.columns, tablefmt="simple")
        return f"{table}\n\n{result.row_count} row(s) — {result.execution_time_ms}ms"
    except ImportError:
        return _format_markdown(result, max_col_width)


def _format_json(result: QueryResult) -> str:
    records = [dict(zip(result.columns, row)) for row in result.rows]
    return json.dumps(records, indent=2, default=str)


def _format_csv(result: QueryResult) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(result.columns)
    for row in result.rows:
        writer.writerow([str(v) if v is not None else "" for v in row])
    return buf.getvalue()


def summarize_schema(columns: list[dict[str, str]]) -> str:
    """Format a column schema list as a readable markdown table."""
    if not columns:
        return "_No columns found._"

    lines = ["| Column | Type | Description |", "| --- | --- | --- |"]
    for col in columns:
        name = col.get("name", "")
        ctype = col.get("type", "")
        desc = col.get("description", "")
        lines.append(f"| `{name}` | `{ctype}` | {desc} |")
    return "\n".join(lines)


def summarize_connections(connections: list[dict[str, str]]) -> str:
    """Format connection list as readable text."""
    if not connections:
        return "No connections configured. Add ADX_CLUSTERS or LOG_ANALYTICS_WORKSPACES to your .env file."

    lines = []
    for conn in connections:
        conn_type = conn.get("type", "unknown")
        name = conn.get("name", "unknown")
        if conn_type == "adx":
            lines.append(f"- **{name}** (ADX) → {conn.get('cluster_url', '')} / `{conn.get('default_database', '')}`")
        elif conn_type == "log_analytics":
            lines.append(f"- **{name}** (Log Analytics) → workspace `{conn.get('workspace_id', '')}`")
    return "\n".join(lines)
