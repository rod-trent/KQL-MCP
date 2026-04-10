"""
KQL MCP Server — The best MCP server for Kusto Query Language.

Supports:
  - Azure Data Explorer (ADX)
  - Azure Log Analytics
  - Microsoft Sentinel

Tools:
  - execute_query          Execute KQL against any configured connection
  - list_connections       List all configured connections
  - list_databases         List databases in an ADX cluster
  - list_tables            List tables in a database or workspace
  - get_table_schema       Get column schema for a table
  - get_sample_data        Get sample rows from a table
  - search_schema          Search for tables/columns by keyword
  - get_table_stats        Get row count and size statistics (ADX)
  - validate_query         Validate KQL syntax and get optimization tips
  - get_query_templates    Get battle-tested query templates by category or tag
  - search_templates       Search query templates by keyword
  - clear_schema_cache     Clear cached schema information

Resources:
  - kql://reference/operators          KQL tabular operator reference
  - kql://reference/functions/string   String function reference
  - kql://reference/functions/datetime DateTime function reference
  - kql://reference/functions/math     Math function reference
  - kql://reference/functions/dynamic  Dynamic/JSON function reference
  - kql://reference/functions/ip       IP address function reference
  - kql://reference/aggregations       Aggregation function reference
  - kql://reference/window-functions   Window function reference
  - kql://reference/series             Time series function reference
  - kql://reference/types              KQL data types
  - kql://reference/best-practices     KQL best practices
  - kql://templates/{category}         Query templates by category

Prompts:
  - write-kql                    Write a KQL query from a description
  - explain-kql                  Explain what a query does
  - optimize-kql                 Optimize a KQL query for performance
  - investigate-security-alert   Security investigation plan + queries
  - performance-investigation    Performance root cause queries
  - convert-sql-to-kql           Convert SQL to KQL
  - schema-explorer              Explore a table and get query suggestions
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

import mcp.types as types
from mcp.server import Server
from mcp.server.stdio import stdio_server

from kql_mcp.config import load_config
from kql_mcp.connections.registry import ConnectionRegistry
from kql_mcp.prompts.templates import KQL_PROMPTS, get_prompt_messages
from kql_mcp.resources.kql_reference import (
    KQL_REFERENCE,
    get_operator_help,
    get_function_help,
    search_reference,
)
from kql_mcp.resources.query_library import (
    QUERY_LIBRARY,
    get_templates_by_category,
    get_templates_by_tag,
    list_categories,
    search_templates,
)
from kql_mcp.tools.formatting import (
    format_result,
    summarize_schema,
    summarize_connections,
    OutputFormat,
)
from kql_mcp.tools.validation import validate_kql

logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
logger = logging.getLogger(__name__)


def create_server() -> Server:
    config = load_config()
    registry = ConnectionRegistry(config)

    app = Server("kql-mcp")

    # ─────────────────────────────────────────────────────────────────────────
    # TOOLS
    # ─────────────────────────────────────────────────────────────────────────

    @app.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="execute_query",
                description=(
                    "Execute a KQL query against a configured Azure Data Explorer cluster "
                    "or Log Analytics workspace. Returns formatted results."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The KQL query to execute.",
                        },
                        "connection": {
                            "type": "string",
                            "description": (
                                "Name of the connection to use (from list_connections). "
                                "Uses the first configured connection if not specified."
                            ),
                        },
                        "database": {
                            "type": "string",
                            "description": "Database name (ADX only). Uses the connection's default if not specified.",
                        },
                        "timespan": {
                            "type": "string",
                            "description": (
                                "Time range for Log Analytics queries (ISO 8601 duration, e.g. 'PT1H', 'P7D'). "
                                "Defaults to P1D (last 24 hours)."
                            ),
                        },
                        "max_rows": {
                            "type": "integer",
                            "description": "Maximum rows to return. Defaults to the server's configured limit.",
                        },
                        "format": {
                            "type": "string",
                            "enum": ["markdown", "json", "csv", "table"],
                            "description": "Output format. Defaults to 'markdown'.",
                        },
                    },
                    "required": ["query"],
                },
            ),
            types.Tool(
                name="list_connections",
                description="List all configured KQL connections (ADX clusters and Log Analytics workspaces).",
                inputSchema={"type": "object", "properties": {}},
            ),
            types.Tool(
                name="list_databases",
                description="List all databases in an Azure Data Explorer cluster.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "connection": {
                            "type": "string",
                            "description": "ADX connection name. Uses the first ADX connection if not specified.",
                        }
                    },
                },
            ),
            types.Tool(
                name="list_tables",
                description="List all tables in an ADX database or Log Analytics workspace.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "connection": {
                            "type": "string",
                            "description": "Connection name.",
                        },
                        "database": {
                            "type": "string",
                            "description": "Database name (ADX only).",
                        },
                    },
                },
            ),
            types.Tool(
                name="get_table_schema",
                description="Get the column schema (name, type, description) for a specific table.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table": {
                            "type": "string",
                            "description": "Table name.",
                        },
                        "connection": {
                            "type": "string",
                            "description": "Connection name.",
                        },
                        "database": {
                            "type": "string",
                            "description": "Database name (ADX only).",
                        },
                    },
                    "required": ["table"],
                },
            ),
            types.Tool(
                name="get_sample_data",
                description="Get sample rows from a table to understand its structure and data.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table": {
                            "type": "string",
                            "description": "Table name.",
                        },
                        "rows": {
                            "type": "integer",
                            "description": "Number of sample rows to return (default: 5, max: 50).",
                            "default": 5,
                        },
                        "connection": {
                            "type": "string",
                            "description": "Connection name.",
                        },
                        "database": {
                            "type": "string",
                            "description": "Database name (ADX only).",
                        },
                    },
                    "required": ["table"],
                },
            ),
            types.Tool(
                name="search_schema",
                description=(
                    "Search for tables and columns matching a keyword across the schema. "
                    "Useful for discovering where a particular concept (e.g., 'IP', 'user', 'error') is stored."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "keyword": {
                            "type": "string",
                            "description": "Search keyword.",
                        },
                        "connection": {
                            "type": "string",
                            "description": "Connection name.",
                        },
                        "database": {
                            "type": "string",
                            "description": "Database name (ADX only).",
                        },
                        "search_columns": {
                            "type": "boolean",
                            "description": "Also search column names (default: true). Set to false for faster results.",
                            "default": True,
                        },
                    },
                    "required": ["keyword"],
                },
            ),
            types.Tool(
                name="get_table_stats",
                description="Get row count and storage statistics for an ADX table.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table": {
                            "type": "string",
                            "description": "Table name.",
                        },
                        "connection": {
                            "type": "string",
                            "description": "ADX connection name.",
                        },
                        "database": {
                            "type": "string",
                            "description": "Database name.",
                        },
                    },
                    "required": ["table"],
                },
            ),
            types.Tool(
                name="validate_query",
                description=(
                    "Validate a KQL query for syntax errors, performance issues, and best practice violations. "
                    "Returns errors, warnings, suggestions, and a cost estimate — without actually executing the query."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The KQL query to validate.",
                        }
                    },
                    "required": ["query"],
                },
            ),
            types.Tool(
                name="get_query_templates",
                description=(
                    "Get battle-tested KQL query templates by category or tag. "
                    "Categories: security, performance, operations, kusto_adx, time_series. "
                    "Tags: sentinel, authentication, anomaly-detection, brute-force, cpu, memory, etc."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "category": {
                            "type": "string",
                            "description": "Template category (security, performance, operations, kusto_adx, time_series).",
                        },
                        "tag": {
                            "type": "string",
                            "description": "Filter by tag (e.g., 'sentinel', 'authentication', 'anomaly-detection').",
                        },
                    },
                },
            ),
            types.Tool(
                name="search_templates",
                description="Search KQL query templates by keyword (searches name, description, tags, and query text).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "keyword": {
                            "type": "string",
                            "description": "Search keyword.",
                        }
                    },
                    "required": ["keyword"],
                },
            ),
            types.Tool(
                name="kql_reference_search",
                description=(
                    "Search the KQL language reference for operators, functions, and syntax. "
                    "Use this to look up how a specific operator or function works."
                ),
                inputSchema={
                    "type": "object",
                    "properties": {
                        "keyword": {
                            "type": "string",
                            "description": "Operator or function name, or a keyword to search.",
                        }
                    },
                    "required": ["keyword"],
                },
            ),
            types.Tool(
                name="clear_schema_cache",
                description="Clear cached schema information so the next schema request fetches fresh data.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "connection": {
                            "type": "string",
                            "description": "Clear cache for a specific connection only. Clears all if not specified.",
                        }
                    },
                },
            ),
        ]

    @app.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
        try:
            result = await _dispatch_tool(name, arguments, registry, config)
            return [types.TextContent(type="text", text=result)]
        except Exception as e:
            return [types.TextContent(type="text", text=f"**Error:** {e}")]

    # ─────────────────────────────────────────────────────────────────────────
    # RESOURCES
    # ─────────────────────────────────────────────────────────────────────────

    @app.list_resources()
    async def list_resources() -> list[types.Resource]:
        resources = [
            types.Resource(
                uri="kql://reference/operators",
                name="KQL Tabular Operators",
                description="Complete reference for KQL tabular operators (where, summarize, join, project, etc.)",
                mimeType="text/markdown",
            ),
            types.Resource(
                uri="kql://reference/functions/string",
                name="KQL String Functions",
                description="String manipulation functions: contains, has, split, extract, parse, etc.",
                mimeType="text/markdown",
            ),
            types.Resource(
                uri="kql://reference/functions/datetime",
                name="KQL DateTime Functions",
                description="DateTime functions: ago, bin, startofday, datetime_diff, format_datetime, etc.",
                mimeType="text/markdown",
            ),
            types.Resource(
                uri="kql://reference/functions/math",
                name="KQL Math & Conditional Functions",
                description="Math functions: abs, round, sqrt, iff, coalesce, isnull, etc.",
                mimeType="text/markdown",
            ),
            types.Resource(
                uri="kql://reference/functions/dynamic",
                name="KQL Dynamic/JSON Functions",
                description="Functions for working with dynamic values, arrays, and property bags.",
                mimeType="text/markdown",
            ),
            types.Resource(
                uri="kql://reference/functions/ip",
                name="KQL IP Address Functions",
                description="IP address parsing and matching: ipv4_is_match, ipv4_is_private, ipv4_is_in_range, etc.",
                mimeType="text/markdown",
            ),
            types.Resource(
                uri="kql://reference/aggregations",
                name="KQL Aggregation Functions",
                description="Aggregation functions for use in summarize: count, dcount, avg, percentile, make_list, etc.",
                mimeType="text/markdown",
            ),
            types.Resource(
                uri="kql://reference/window-functions",
                name="KQL Window Functions",
                description="Window functions: row_number, prev, next, row_cumsum, etc.",
                mimeType="text/markdown",
            ),
            types.Resource(
                uri="kql://reference/series",
                name="KQL Time Series Functions",
                description="Time series analysis: series_decompose_anomalies, series_fit_line, make-series, etc.",
                mimeType="text/markdown",
            ),
            types.Resource(
                uri="kql://reference/types",
                name="KQL Data Types",
                description="KQL data types: bool, int, long, real, string, datetime, timespan, dynamic, guid.",
                mimeType="text/markdown",
            ),
            types.Resource(
                uri="kql://reference/best-practices",
                name="KQL Best Practices",
                description="Performance, readability, and security best practices for KQL.",
                mimeType="text/markdown",
            ),
        ]

        # Add template resources
        for category in list_categories():
            templates = get_templates_by_category(category)
            resources.append(
                types.Resource(
                    uri=f"kql://templates/{category}",
                    name=f"KQL Templates: {category.replace('_', ' ').title()}",
                    description=f"{len(templates)} ready-to-use KQL queries for {category.replace('_', ' ')} scenarios.",
                    mimeType="text/markdown",
                )
            )

        return resources

    @app.read_resource()
    async def read_resource(uri: str) -> str:
        uri_str = str(uri)

        if uri_str == "kql://reference/operators":
            return _render_operators_reference()
        elif uri_str.startswith("kql://reference/functions/"):
            category = uri_str.split("/")[-1]
            return _render_functions_reference(category)
        elif uri_str == "kql://reference/aggregations":
            return _render_aggregations_reference()
        elif uri_str == "kql://reference/window-functions":
            return _render_window_functions_reference()
        elif uri_str == "kql://reference/series":
            return _render_series_reference()
        elif uri_str == "kql://reference/types":
            return _render_types_reference()
        elif uri_str == "kql://reference/best-practices":
            return _render_best_practices()
        elif uri_str.startswith("kql://templates/"):
            category = uri_str.split("/")[-1]
            return _render_templates(category)
        else:
            raise ValueError(f"Unknown resource URI: {uri_str}")

    # ─────────────────────────────────────────────────────────────────────────
    # PROMPTS
    # ─────────────────────────────────────────────────────────────────────────

    @app.list_prompts()
    async def list_prompts() -> list[types.Prompt]:
        prompts = []
        for prompt_def in KQL_PROMPTS.values():
            args = [
                types.PromptArgument(
                    name=arg["name"],
                    description=arg["description"],
                    required=arg.get("required", False),
                )
                for arg in prompt_def.get("arguments", [])
            ]
            prompts.append(
                types.Prompt(
                    name=prompt_def["name"],
                    description=prompt_def["description"],
                    arguments=args,
                )
            )
        return prompts

    @app.get_prompt()
    async def get_prompt(name: str, arguments: dict[str, str] | None) -> types.GetPromptResult:
        args = arguments or {}
        messages = get_prompt_messages(name, args)
        return types.GetPromptResult(
            description=KQL_PROMPTS[name]["description"],
            messages=messages,
        )

    return app


# ─────────────────────────────────────────────────────────────────────────────
# Tool dispatcher
# ─────────────────────────────────────────────────────────────────────────────

async def _dispatch_tool(
    name: str,
    args: dict[str, Any],
    registry: ConnectionRegistry,
    config: Any,
) -> str:
    if name == "execute_query":
        return await _tool_execute_query(args, registry, config)
    elif name == "list_connections":
        return _tool_list_connections(registry)
    elif name == "list_databases":
        return await _tool_list_databases(args, registry)
    elif name == "list_tables":
        return await _tool_list_tables(args, registry)
    elif name == "get_table_schema":
        return await _tool_get_table_schema(args, registry)
    elif name == "get_sample_data":
        return await _tool_get_sample_data(args, registry, config)
    elif name == "search_schema":
        return await _tool_search_schema(args, registry)
    elif name == "get_table_stats":
        return await _tool_get_table_stats(args, registry)
    elif name == "validate_query":
        return _tool_validate_query(args)
    elif name == "get_query_templates":
        return _tool_get_query_templates(args)
    elif name == "search_templates":
        return _tool_search_templates(args)
    elif name == "kql_reference_search":
        return _tool_kql_reference_search(args)
    elif name == "clear_schema_cache":
        return _tool_clear_schema_cache(args, registry)
    else:
        raise ValueError(f"Unknown tool: {name}")


def _resolve_connection(args: dict, registry: ConnectionRegistry):
    """Resolve connection name to (connection, type) tuple."""
    conn_name = args.get("connection")
    if conn_name:
        # Try ADX first, then Log Analytics
        try:
            return registry.get_adx(conn_name), "adx"
        except ValueError:
            pass
        try:
            return registry.get_log_analytics(conn_name), "la"
        except ValueError:
            raise ValueError(
                f"Connection '{conn_name}' not found. "
                f"Available connections: {[c['name'] for c in registry.list_all_connections()]}"
            )
    else:
        # Auto-detect: prefer ADX if available, else Log Analytics
        adx = registry.get_default_adx()
        if adx:
            return adx, "adx"
        la = registry.get_default_log_analytics()
        if la:
            return la, "la"
        raise ValueError(
            "No connections configured. Set ADX_CLUSTERS or LOG_ANALYTICS_WORKSPACES in your .env file."
        )


async def _tool_execute_query(args: dict, registry: ConnectionRegistry, config: Any) -> str:
    query = args["query"]
    fmt: OutputFormat = args.get("format", config.default_output_format)
    conn, conn_type = _resolve_connection(args, registry)

    # Validate first
    validation = validate_kql(query)
    warnings_block = ""
    if validation.warnings:
        warnings = "\n".join(f"- {w}" for w in validation.warnings)
        warnings_block = f"\n> **Query warnings:**\n{warnings}\n\n"

    if not validation.is_valid:
        errors = "\n".join(f"- {e}" for e in validation.errors)
        return f"**Query validation failed:**\n{errors}\n\nPlease fix the query before executing."

    if conn_type == "adx":
        result = await conn.execute_query(
            query,
            database=args.get("database"),
            max_rows=args.get("max_rows"),
        )
    else:
        result = await conn.execute_query(
            query,
            timespan=args.get("timespan"),
            max_rows=args.get("max_rows"),
        )

    result_warnings = "\n".join(f"- {w}" for w in result.warnings) if result.warnings else ""
    if result_warnings:
        result_warnings = f"\n> **Result warnings:**\n{result_warnings}\n\n"

    formatted = format_result(result, fmt=fmt)
    return f"{warnings_block}{result_warnings}{formatted}"


def _tool_list_connections(registry: ConnectionRegistry) -> str:
    connections = registry.list_all_connections()
    if not connections:
        return (
            "No connections configured.\n\n"
            "Add to your `.env` file:\n"
            "```\n"
            'ADX_CLUSTERS=\'[{"name": "my-cluster", "cluster_url": "https://...", "database": "mydb"}]\'\n'
            'LOG_ANALYTICS_WORKSPACES=\'[{"name": "sentinel", "workspace_id": "..."}]\'\n'
            "```"
        )
    return f"## Configured Connections\n\n{summarize_connections(connections)}"


async def _tool_list_databases(args: dict, registry: ConnectionRegistry) -> str:
    conn_name = args.get("connection")
    if conn_name:
        conn = registry.get_adx(conn_name)
    else:
        conn = registry.get_default_adx()
        if not conn:
            return "No ADX connections configured."

    databases = await conn.list_databases()
    if not databases:
        return "No databases found."

    lines = [f"## Databases in `{conn.name}` ({conn.cluster_url})\n"]
    for db in sorted(databases):
        lines.append(f"- `{db}`")
    return "\n".join(lines)


async def _tool_list_tables(args: dict, registry: ConnectionRegistry) -> str:
    conn, conn_type = _resolve_connection(args, registry)

    if conn_type == "adx":
        tables = await conn.list_tables(database=args.get("database"))
        db_label = args.get("database") or conn.default_database
        header = f"## Tables in `{conn.name}` / `{db_label}`\n"
    else:
        tables = await conn.list_tables()
        header = f"## Tables in workspace `{conn.name}` (`{conn.workspace_id}`)\n"

    if not tables:
        return "No tables found."

    lines = [header, f"_{len(tables)} table(s)_\n"]
    for t in sorted(tables, key=lambda x: x["name"]):
        desc = f" — {t['description']}" if t.get("description") else ""
        lines.append(f"- `{t['name']}`{desc}")
    return "\n".join(lines)


async def _tool_get_table_schema(args: dict, registry: ConnectionRegistry) -> str:
    table = args["table"]
    conn, conn_type = _resolve_connection(args, registry)

    if conn_type == "adx":
        columns = await conn.get_table_schema(table, database=args.get("database"))
    else:
        columns = await conn.get_table_schema(table)

    if not columns:
        return f"No schema found for table `{table}`. It may not exist or you may not have access."

    schema_md = summarize_schema(columns)
    return f"## Schema: `{table}`\n\n{schema_md}\n\n_{len(columns)} column(s)_"


async def _tool_get_sample_data(args: dict, registry: ConnectionRegistry, config: Any) -> str:
    table = args["table"]
    rows = min(int(args.get("rows", 5)), 50)
    conn, conn_type = _resolve_connection(args, registry)

    if conn_type == "adx":
        result = await conn.get_sample_data(table, database=args.get("database"), rows=rows)
    else:
        result = await conn.get_sample_data(table, rows=rows)

    formatted = format_result(result, fmt="markdown")
    return f"## Sample Data: `{table}`\n\n{formatted}"


async def _tool_search_schema(args: dict, registry: ConnectionRegistry) -> str:
    keyword = args["keyword"]
    search_columns = args.get("search_columns", True)
    conn, conn_type = _resolve_connection(args, registry)

    if conn_type == "adx":
        results = await conn.search_schema(keyword, database=args.get("database"), search_columns=search_columns)
    else:
        results = await conn.search_schema(keyword, search_columns=search_columns)

    lines = [f"## Schema search: `{keyword}`\n"]

    if results["tables"]:
        lines.append(f"**Matching tables ({len(results['tables'])}):**")
        for t in results["tables"]:
            lines.append(f"- `{t}`")
    else:
        lines.append("_No matching tables found._")

    if search_columns:
        lines.append("")
        if results["columns"]:
            lines.append(f"**Matching columns ({len(results['columns'])}):**")
            for c in results["columns"]:
                lines.append(f"- `{c}`")
        else:
            lines.append("_No matching columns found._")

    return "\n".join(lines)


async def _tool_get_table_stats(args: dict, registry: ConnectionRegistry) -> str:
    table = args["table"]
    conn_name = args.get("connection")
    if conn_name:
        conn = registry.get_adx(conn_name)
    else:
        conn = registry.get_default_adx()
        if not conn:
            return "get_table_stats is only available for ADX connections."

    stats = await conn.get_table_statistics(table, database=args.get("database"))
    if not stats:
        return f"No statistics found for table `{table}`."

    rows = stats.get("row_count", "unknown")
    compressed = stats.get("compressed_size_bytes", 0) / (1024**3)
    original = stats.get("original_size_bytes", 0) / (1024**3)
    ratio = (original / compressed) if compressed > 0 else 0

    return (
        f"## Statistics: `{table}`\n\n"
        f"| Metric | Value |\n"
        f"| --- | --- |\n"
        f"| Row count | {rows:,} |\n"
        f"| Compressed size | {compressed:.2f} GB |\n"
        f"| Original size | {original:.2f} GB |\n"
        f"| Compression ratio | {ratio:.1f}x |\n"
    )


def _tool_validate_query(args: dict) -> str:
    query = args["query"]
    result = validate_kql(query)
    return result.to_markdown()


def _tool_get_query_templates(args: dict) -> str:
    category = args.get("category")
    tag = args.get("tag")

    if category:
        templates = get_templates_by_category(category)
        if not templates:
            cats = ", ".join(f"`{c}`" for c in list_categories())
            return f"Category `{category}` not found. Available: {cats}"
        title = f"## KQL Templates: {category.replace('_', ' ').title()}"
    elif tag:
        templates = get_templates_by_tag(tag)
        title = f"## KQL Templates tagged `{tag}`"
    else:
        # List all categories
        lines = ["## KQL Query Template Library\n"]
        for cat in list_categories():
            cat_templates = get_templates_by_category(cat)
            lines.append(f"### {cat.replace('_', ' ').title()} ({len(cat_templates)} templates)")
            for t in cat_templates:
                tags = ", ".join(f"`{tag}`" for tag in t.get("tags", []))
                lines.append(f"- **{t['name']}** — {t['description']} [{tags}]")
            lines.append("")
        return "\n".join(lines)

    if not templates:
        return f"No templates found for the specified criteria."

    lines = [f"{title}\n"]
    for t in templates:
        tags = ", ".join(f"`{tag}`" for tag in t.get("tags", []))
        lines.append(f"### {t['name']}")
        lines.append(f"_{t['description']}_ [{tags}]\n")
        lines.append(f"```kql\n{t['query'].strip()}\n```\n")

    return "\n".join(lines)


def _tool_search_templates(args: dict) -> str:
    keyword = args["keyword"]
    results = search_templates(keyword)
    if not results:
        return f"No templates found matching `{keyword}`."

    lines = [f"## Templates matching `{keyword}`\n"]
    for t in results:
        tags = ", ".join(f"`{tag}`" for tag in t.get("tags", []))
        cat = t.get("category", "")
        lines.append(f"### {t['name']} _(category: {cat})_")
        lines.append(f"_{t['description']}_ [{tags}]\n")
        lines.append(f"```kql\n{t['query'].strip()}\n```\n")
    return "\n".join(lines)


def _tool_kql_reference_search(args: dict) -> str:
    keyword = args["keyword"]

    # Try exact operator match first
    ops = KQL_REFERENCE.get("tabular_operators", {}).get("operators", {})
    if keyword.lower() in ops:
        return get_operator_help(keyword.lower())

    # Try exact function match
    func_help = get_function_help(keyword)
    if "not found" not in func_help:
        return func_help

    # General search
    results = search_reference(keyword)
    if not results:
        return f"No KQL reference entries found for `{keyword}`."

    lines = [f"## KQL Reference: `{keyword}`\n"]
    lines.extend(results[:20])  # Limit results
    if len(results) > 20:
        lines.append(f"\n_...and {len(results) - 20} more results._")
    return "\n".join(lines)


def _tool_clear_schema_cache(args: dict, registry: ConnectionRegistry) -> str:
    conn_name = args.get("connection")
    if conn_name:
        try:
            conn, _ = _resolve_connection({"connection": conn_name}, registry)
            conn.clear_cache()
            return f"Schema cache cleared for connection `{conn_name}`."
        except ValueError as e:
            return str(e)
    else:
        registry.clear_all_caches()
        return "Schema cache cleared for all connections."


# ─────────────────────────────────────────────────────────────────────────────
# Resource renderers
# ─────────────────────────────────────────────────────────────────────────────

def _render_operators_reference() -> str:
    ops = KQL_REFERENCE["tabular_operators"]
    lines = [f"# {ops['title']}\n\n{ops['description']}\n"]
    for op_name, op in ops["operators"].items():
        lines.append(f"## `{op_name}`")
        lines.append(f"\n{op['description']}")
        lines.append(f"\n**Syntax:** `{op['syntax']}`")
        if op.get("examples"):
            lines.append("\n**Examples:**")
            for ex in op["examples"]:
                lines.append(f"```kql\n{ex}\n```")
        if op.get("tips"):
            lines.append("\n**Tips:**")
            for tip in op["tips"]:
                lines.append(f"- {tip}")
        lines.append("")
    return "\n".join(lines)


def _render_functions_reference(category: str) -> str:
    cats = KQL_REFERENCE["scalar_functions"]["categories"]
    if category not in cats:
        return f"Unknown function category: {category}. Available: {', '.join(cats.keys())}"
    cat = cats[category]
    lines = [f"# {cat['title']}\n"]
    for fname, fdesc in cat["functions"].items():
        lines.append(f"- **`{fname}`** — {fdesc}")
    return "\n".join(lines)


def _render_aggregations_reference() -> str:
    aggs = KQL_REFERENCE["aggregation_functions"]
    lines = [f"# {aggs['title']}\n\n{aggs['description']}\n"]
    for fname, fdesc in aggs["functions"].items():
        lines.append(f"- **`{fname}`** — {fdesc}")
    return "\n".join(lines)


def _render_window_functions_reference() -> str:
    wf = KQL_REFERENCE["window_functions"]
    lines = [f"# {wf['title']}\n\n{wf['description']}\n"]
    for fname, fdesc in wf["functions"].items():
        lines.append(f"- **`{fname}`** — {fdesc}")
    if wf.get("notes"):
        lines.append(f"\n> **Note:** {wf['notes']}")
    return "\n".join(lines)


def _render_series_reference() -> str:
    sf = KQL_REFERENCE["series_functions"]
    lines = [f"# {sf['title']}\n\n{sf['description']}\n"]
    for fname, fdesc in sf["functions"].items():
        lines.append(f"- **`{fname}`** — {fdesc}")
    return "\n".join(lines)


def _render_types_reference() -> str:
    dt = KQL_REFERENCE["data_types"]
    lines = [f"# {dt['title']}\n\n## Scalar Types\n"]
    for tname, tdesc in dt["types"].items():
        lines.append(f"- **`{tname}`** — {tdesc}")
    lines.append("\n## Timespan Literals\n")
    lines.append(dt["timespan_literals"]["description"] + ":\n")
    for lit, meaning in dt["timespan_literals"]["examples"].items():
        lines.append(f"- `{lit}` = {meaning}")
    return "\n".join(lines)


def _render_best_practices() -> str:
    bp = KQL_REFERENCE["best_practices"]
    lines = [f"# {bp['title']}\n\n## Performance\n"]
    for tip in bp["performance"]:
        lines.append(f"- {tip}")
    lines.append("\n## Readability\n")
    for tip in bp["readability"]:
        lines.append(f"- {tip}")
    lines.append("\n## Security\n")
    for tip in bp["security"]:
        lines.append(f"- {tip}")
    return "\n".join(lines)


def _render_templates(category: str) -> str:
    templates = get_templates_by_category(category)
    if not templates:
        return f"No templates found for category `{category}`."

    lines = [f"# KQL Templates: {category.replace('_', ' ').title()}\n"]
    for t in templates:
        tags = ", ".join(f"`{tag}`" for tag in t.get("tags", []))
        lines.append(f"## {t['name']}")
        lines.append(f"\n_{t['description']}_ [{tags}]\n")
        lines.append(f"```kql\n{t['query'].strip()}\n```\n")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    import asyncio
    server = create_server()
    asyncio.run(stdio_server(server))


if __name__ == "__main__":
    main()
