"""Prompt templates for common KQL assistance scenarios."""

from __future__ import annotations

from mcp.types import PromptMessage, TextContent


KQL_PROMPTS: dict[str, dict] = {
    "write-kql": {
        "name": "write-kql",
        "description": "Write a KQL query from a natural language description",
        "arguments": [
            {"name": "description", "description": "What the query should do", "required": True},
            {"name": "table", "description": "The primary table to query (optional)", "required": False},
            {"name": "time_range", "description": "Time range (e.g., '1h', '7d', '30m')", "required": False},
        ],
    },
    "explain-kql": {
        "name": "explain-kql",
        "description": "Explain what a KQL query does in plain English",
        "arguments": [
            {"name": "query", "description": "The KQL query to explain", "required": True},
        ],
    },
    "optimize-kql": {
        "name": "optimize-kql",
        "description": "Analyze a KQL query for performance issues and suggest optimizations",
        "arguments": [
            {"name": "query", "description": "The KQL query to optimize", "required": True},
            {"name": "context", "description": "Additional context (table size, expected results, etc.)", "required": False},
        ],
    },
    "investigate-security-alert": {
        "name": "investigate-security-alert",
        "description": "Generate an investigation plan and KQL queries for a security alert",
        "arguments": [
            {"name": "alert_type", "description": "Type of alert (e.g., 'failed logins', 'malware', 'data exfiltration')", "required": True},
            {"name": "entity", "description": "The affected entity (user, IP, computer, etc.)", "required": False},
            {"name": "time_range", "description": "Time range to investigate (e.g., '24h', '7d')", "required": False},
        ],
    },
    "performance-investigation": {
        "name": "performance-investigation",
        "description": "Generate KQL queries to investigate a performance problem",
        "arguments": [
            {"name": "symptom", "description": "The performance symptom (e.g., 'high CPU', 'slow API', 'memory pressure')", "required": True},
            {"name": "resource", "description": "The resource name (computer, app, service)", "required": False},
            {"name": "time_range", "description": "Time range to analyze", "required": False},
        ],
    },
    "convert-sql-to-kql": {
        "name": "convert-sql-to-kql",
        "description": "Convert a SQL query to equivalent KQL",
        "arguments": [
            {"name": "sql", "description": "The SQL query to convert", "required": True},
            {"name": "target_table", "description": "The KQL table name (if different from SQL table)", "required": False},
        ],
    },
    "schema-explorer": {
        "name": "schema-explorer",
        "description": "Explore a table's schema and suggest useful queries",
        "arguments": [
            {"name": "table_name", "description": "The table to explore", "required": True},
            {"name": "goal", "description": "What you're trying to accomplish with this table", "required": False},
        ],
    },
}


def build_write_kql_messages(description: str, table: str | None = None, time_range: str | None = None) -> list[PromptMessage]:
    time_hint = f"Time range: last {time_range}." if time_range else "Use an appropriate time range (e.g., ago(1h) or ago(24h))."
    table_hint = f"Primary table: `{table}`." if table else "Choose the most appropriate table based on the description."

    return [
        PromptMessage(
            role="user",
            content=TextContent(
                type="text",
                text=f"""Write a KQL (Kusto Query Language) query for the following requirement:

**Goal:** {description}

**Constraints:**
- {table_hint}
- {time_hint}
- Filter early to minimize data scanned.
- Use `| project` to select only relevant columns.
- Add `| sort by TimeGenerated desc` or appropriate ordering.
- Include comments (`//`) to explain non-obvious steps.
- Follow KQL best practices (has > contains, dcount > count distinct, etc.).

Return the KQL query in a code block, followed by a brief explanation of what each step does.""",
            ),
        )
    ]


def build_explain_kql_messages(query: str) -> list[PromptMessage]:
    return [
        PromptMessage(
            role="user",
            content=TextContent(
                type="text",
                text=f"""Explain the following KQL query in plain English:

```kql
{query}
```

Please:
1. Summarize what the query does in 1-2 sentences.
2. Walk through each pipe operator step by step.
3. Explain any complex expressions, functions, or patterns.
4. Describe what the output columns represent.
5. Note any potential performance concerns or improvements.""",
            ),
        )
    ]


def build_optimize_kql_messages(query: str, context: str | None = None) -> list[PromptMessage]:
    ctx = f"\n\n**Additional context:** {context}" if context else ""
    return [
        PromptMessage(
            role="user",
            content=TextContent(
                type="text",
                text=f"""Analyze and optimize the following KQL query:{ctx}

```kql
{query}
```

Please:
1. Identify any performance issues (full table scans, expensive joins, missing filters, etc.).
2. Check for correctness issues (wrong operators, null handling, type mismatches).
3. Suggest specific optimizations with explanations.
4. Provide the optimized query in a code block.
5. Rate the original query cost as Low / Medium / High and explain why.""",
            ),
        )
    ]


def build_investigate_security_alert_messages(
    alert_type: str,
    entity: str | None = None,
    time_range: str | None = None,
) -> list[PromptMessage]:
    entity_str = f"Affected entity: **{entity}**" if entity else "Entity not specified."
    time_str = f"Investigate the last **{time_range}**." if time_range else "Use an appropriate time window (start with 24h, expand to 7d if needed)."

    return [
        PromptMessage(
            role="user",
            content=TextContent(
                type="text",
                text=f"""Generate a security investigation plan and KQL queries for the following alert:

**Alert type:** {alert_type}
**{entity_str}**
**{time_str}**

Please provide:
1. **Triage queries** — Quick queries to confirm whether the alert is real.
2. **Scope queries** — Determine how widespread the activity is (affected users, computers, IPs).
3. **Timeline query** — Reconstruct the sequence of events.
4. **Correlation queries** — Look for related activity (lateral movement, persistence, exfiltration).
5. **Hunting queries** — Broader queries to find similar patterns not yet alerted on.

For each query, specify:
- What table it targets (SecurityEvent, SigninLogs, AzureActivity, etc.)
- What question it answers
- The KQL query in a code block""",
            ),
        )
    ]


def build_performance_investigation_messages(
    symptom: str,
    resource: str | None = None,
    time_range: str | None = None,
) -> list[PromptMessage]:
    resource_str = f"Resource: **{resource}**" if resource else ""
    time_str = f"Time range: last **{time_range}**." if time_range else "Use last 1 hour for immediate issues, last 7 days for trend analysis."

    return [
        PromptMessage(
            role="user",
            content=TextContent(
                type="text",
                text=f"""Generate KQL queries to investigate the following performance problem:

**Symptom:** {symptom}
{resource_str}
**{time_str}**

Please provide queries that:
1. **Confirm the problem** — Verify the symptom with data.
2. **Find the root cause** — Drill into metrics, errors, or resource saturation.
3. **Identify the blast radius** — What else is affected?
4. **Show the timeline** — When did it start? Is it getting worse?
5. **Suggest remediation hints** — Based on the data patterns, what should be investigated next?

Use tables like Perf, requests, dependencies, exceptions, customMetrics, AzureMetrics as appropriate.
For each query, include a brief description of what it measures.""",
            ),
        )
    ]


def build_convert_sql_messages(sql: str, target_table: str | None = None) -> list[PromptMessage]:
    table_hint = f"\n\nThe KQL table name is `{target_table}`." if target_table else ""
    return [
        PromptMessage(
            role="user",
            content=TextContent(
                type="text",
                text=f"""Convert the following SQL query to equivalent KQL (Kusto Query Language):{table_hint}

```sql
{sql}
```

KQL equivalents:
- `SELECT col1, col2` → `| project col1, col2`
- `WHERE condition` → `| where condition`
- `GROUP BY col` → `| summarize ... by col`
- `ORDER BY col DESC` → `| sort by col desc`
- `JOIN table ON key` → `| join kind=inner (table) on key`
- `HAVING count > 10` → filter after `summarize` with `| where`
- `DISTINCT col` → `| distinct col`
- `TOP 10` → `| take 10` or `| top 10 by col`
- `COUNT(*)` → `count()`
- `IS NULL` → `isnull(col)`
- `LIKE '%pattern%'` → `contains 'pattern'` or `matches regex`

Provide:
1. The equivalent KQL query in a code block.
2. Notes on any constructs that don't translate directly.
3. Any KQL improvements over the original SQL (e.g., using `has` instead of `contains` for better performance).""",
            ),
        )
    ]


def build_schema_explorer_messages(table_name: str, goal: str | None = None) -> list[PromptMessage]:
    goal_str = f"\n\n**Goal:** {goal}" if goal else ""
    return [
        PromptMessage(
            role="user",
            content=TextContent(
                type="text",
                text=f"""I want to explore the `{table_name}` table in KQL.{goal_str}

Please help me by:
1. Describing what this table typically contains (if you know it).
2. Suggesting 3-5 useful KQL queries for this table based on common use cases.
3. Explaining the most important columns to filter and project.
4. Noting any common pitfalls or gotchas with this table (e.g., multi-value columns, nested JSON, high cardinality fields).

If a goal was specified, prioritize queries relevant to that goal.""",
            ),
        )
    ]


def get_prompt_messages(prompt_name: str, arguments: dict[str, str]) -> list[PromptMessage]:
    """Build prompt messages for the given prompt name and arguments."""
    builders = {
        "write-kql": lambda a: build_write_kql_messages(
            a["description"], a.get("table"), a.get("time_range")
        ),
        "explain-kql": lambda a: build_explain_kql_messages(a["query"]),
        "optimize-kql": lambda a: build_optimize_kql_messages(a["query"], a.get("context")),
        "investigate-security-alert": lambda a: build_investigate_security_alert_messages(
            a["alert_type"], a.get("entity"), a.get("time_range")
        ),
        "performance-investigation": lambda a: build_performance_investigation_messages(
            a["symptom"], a.get("resource"), a.get("time_range")
        ),
        "convert-sql-to-kql": lambda a: build_convert_sql_messages(a["sql"], a.get("target_table")),
        "schema-explorer": lambda a: build_schema_explorer_messages(a["table_name"], a.get("goal")),
    }

    builder = builders.get(prompt_name)
    if not builder:
        raise ValueError(f"Unknown prompt: {prompt_name}. Available: {list(builders.keys())}")

    return builder(arguments)
