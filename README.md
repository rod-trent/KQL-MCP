# KQL MCP Server

The best MCP server for **KQL (Kusto Query Language)** — supporting Azure Data Explorer, Log Analytics, and Microsoft Sentinel.

## Features

### Tools
| Tool | Description |
|------|-------------|
| `execute_query` | Execute KQL against ADX or Log Analytics |
| `list_connections` | List configured connections |
| `list_databases` | List databases in an ADX cluster |
| `list_tables` | List tables in a database or workspace |
| `get_table_schema` | Get column names, types, and descriptions |
| `get_sample_data` | Get sample rows to understand a table |
| `search_schema` | Find tables/columns by keyword |
| `get_table_stats` | Row count and storage size (ADX) |
| `validate_query` | Validate KQL syntax and get optimization tips |
| `get_query_templates` | Browse battle-tested query templates |
| `search_templates` | Search templates by keyword |
| `kql_reference_search` | Look up any KQL operator or function |
| `clear_schema_cache` | Refresh cached schema data |

### Resources (KQL Reference)
- Tabular operators: `where`, `summarize`, `join`, `project`, `extend`, `parse`, `mv-expand`, `make-series`, etc.
- Scalar functions: string, datetime, math, dynamic/JSON, IP address
- Aggregation functions: `count`, `dcount`, `avg`, `percentile`, `make_list`, `arg_max`, etc.
- Window functions: `prev`, `next`, `row_number`, `row_cumsum`
- Time series functions: `series_decompose_anomalies`, `series_fit_line`, `series_decompose_forecast`
- Data types and timespan literals
- Best practices for performance, readability, and security

### Query Templates
- **Security**: Failed logins, impossible travel, suspicious PowerShell, Azure resource deletions, network anomalies
- **Performance**: CPU/memory/disk metrics, slow HTTP requests, dependency failures, exception rates
- **Operations**: Heartbeat health checks, VM events, ingestion volume, alert rule firings
- **ADX**: Query statistics, ingestion failures, extent stats
- **Time Series**: Anomaly detection, forecasting, event rate spikes

### Prompts
- `write-kql` — Write a KQL query from a natural language description
- `explain-kql` — Explain what a query does in plain English
- `optimize-kql` — Analyze and optimize a query for performance
- `investigate-security-alert` — Security investigation plan + queries
- `performance-investigation` — Performance root cause queries
- `convert-sql-to-kql` — Convert SQL to KQL
- `schema-explorer` — Explore a table and get query suggestions

## Installation

```bash
pip install -e .
```

## Configuration

Copy `.env.example` to `.env` and configure your connections:

```env
# Azure Data Explorer
ADX_CLUSTERS='[{"name": "my-cluster", "cluster_url": "https://mycluster.eastus.kusto.windows.net", "database": "mydb"}]'

# Log Analytics / Sentinel
LOG_ANALYTICS_WORKSPACES='[{"name": "sentinel", "workspace_id": "your-workspace-id"}]'

# Authentication (cli = az login, managed_identity, service_principal, interactive)
AZURE_AUTH_METHOD=cli
```

## Claude Desktop Configuration

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "kql": {
      "command": "kql-mcp",
      "env": {
        "ADX_CLUSTERS": "[{\"name\": \"my-cluster\", \"cluster_url\": \"https://...\", \"database\": \"mydb\"}]",
        "LOG_ANALYTICS_WORKSPACES": "[{\"name\": \"sentinel\", \"workspace_id\": \"...\"}]",
        "AZURE_AUTH_METHOD": "cli"
      }
    }
  }
}
```

Or using a `.env` file in a specific directory:

```json
{
  "mcpServers": {
    "kql": {
      "command": "kql-mcp",
      "cwd": "/path/to/your/kql-mcp-config"
    }
  }
}
```

## Claude Code Configuration

```bash
claude mcp add kql -- kql-mcp
```

## Authentication

The server supports multiple Azure authentication methods:

| Method | Use case |
|--------|----------|
| `cli` | Local development — uses `az login` |
| `managed_identity` | Azure-hosted workloads |
| `service_principal` | CI/CD pipelines, automated workflows |
| `interactive` | Browser-based interactive login |

## Requirements

- Python 3.11+
- Azure CLI logged in (`az login`) for `cli` auth mode
- Access to Azure Data Explorer cluster or Log Analytics workspace
