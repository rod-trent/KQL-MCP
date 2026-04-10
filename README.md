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
pip install git+https://github.com/rod-trent/KQL-MCP.git
```

Or clone and install locally:

```bash
git clone https://github.com/rod-trent/KQL-MCP.git
cd KQL-MCP
pip install -e .
```

## Configuration

Copy `.env.example` to `.env` and fill in your connection details:

```env
# Azure Data Explorer
ADX_CLUSTERS='[{"name": "my-cluster", "cluster_url": "https://mycluster.eastus.kusto.windows.net", "database": "mydb"}]'

# Log Analytics / Sentinel
LOG_ANALYTICS_WORKSPACES='[{"name": "sentinel", "workspace_id": "your-workspace-id"}]'

# Authentication (cli = az login, managed_identity, service_principal, interactive)
AZURE_AUTH_METHOD=cli
```

## Authentication

The server supports multiple Azure authentication methods:

| Method | Use case |
|--------|----------|
| `cli` | Local development — uses `az login` |
| `managed_identity` | Azure-hosted workloads (VMs, Container Apps, etc.) |
| `service_principal` | CI/CD pipelines, automated workflows |
| `interactive` | Browser-based interactive login |

For `cli` auth, log in first:

```bash
az login
```

---

## Using with AI Assistants

The KQL MCP server works with any AI assistant or IDE that supports the [Model Context Protocol (MCP)](https://modelcontextprotocol.io). Choose your platform below.

---

### Claude Desktop

**Config file location:**
- Windows: `%APPDATA%\Claude\claude_desktop_config.json`
- macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "kql": {
      "command": "kql-mcp",
      "env": {
        "ADX_CLUSTERS": "[{\"name\": \"my-cluster\", \"cluster_url\": \"https://mycluster.eastus.kusto.windows.net\", \"database\": \"mydb\"}]",
        "LOG_ANALYTICS_WORKSPACES": "[{\"name\": \"sentinel\", \"workspace_id\": \"your-workspace-id\"}]",
        "AZURE_AUTH_METHOD": "cli"
      }
    }
  }
}
```

Alternatively, point to a directory containing your `.env` file:

```json
{
  "mcpServers": {
    "kql": {
      "command": "kql-mcp",
      "cwd": "C:\\path\\to\\KQL-MCP"
    }
  }
}
```

Restart Claude Desktop after saving. You should see the KQL tools available in a new conversation.

---

### Claude Code (CLI)

```bash
claude mcp add kql -- kql-mcp
```

To pass connection config directly:

```bash
claude mcp add kql \
  -e ADX_CLUSTERS='[{"name":"prod","cluster_url":"https://mycluster.eastus.kusto.windows.net","database":"mydb"}]' \
  -e AZURE_AUTH_METHOD=cli \
  -- kql-mcp
```

Verify the server is registered:

```bash
claude mcp list
```

---

### ChatGPT (OpenAI)

OpenAI supports MCP servers in the [ChatGPT desktop app](https://openai.com/chatgpt/download/) (macOS and Windows).

**Config file location:**
- Windows: `%APPDATA%\ChatGPT\claude_desktop_config.json`
- macOS: `~/Library/Application Support/ChatGPT/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "kql": {
      "command": "kql-mcp",
      "env": {
        "ADX_CLUSTERS": "[{\"name\": \"my-cluster\", \"cluster_url\": \"https://mycluster.eastus.kusto.windows.net\", \"database\": \"mydb\"}]",
        "LOG_ANALYTICS_WORKSPACES": "[{\"name\": \"sentinel\", \"workspace_id\": \"your-workspace-id\"}]",
        "AZURE_AUTH_METHOD": "cli"
      }
    }
  }
}
```

Restart ChatGPT after saving. MCP tools appear automatically when you start a new conversation.

> **Note:** MCP support in ChatGPT desktop requires the latest version of the app. Check [OpenAI's documentation](https://platform.openai.com/docs) for the most current setup instructions.

---

### Cursor

Open **Settings → Cursor Settings → MCP** and add a new server, or edit `~/.cursor/mcp.json` directly:

```json
{
  "mcpServers": {
    "kql": {
      "command": "kql-mcp",
      "env": {
        "ADX_CLUSTERS": "[{\"name\": \"my-cluster\", \"cluster_url\": \"https://mycluster.eastus.kusto.windows.net\", \"database\": \"mydb\"}]",
        "LOG_ANALYTICS_WORKSPACES": "[{\"name\": \"sentinel\", \"workspace_id\": \"your-workspace-id\"}]",
        "AZURE_AUTH_METHOD": "cli"
      }
    }
  }
}
```

Reload Cursor after saving. The KQL tools will be available to Cursor's AI in Agent mode.

---

### Windsurf (Codeium)

Edit `~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "kql": {
      "command": "kql-mcp",
      "env": {
        "ADX_CLUSTERS": "[{\"name\": \"my-cluster\", \"cluster_url\": \"https://mycluster.eastus.kusto.windows.net\", \"database\": \"mydb\"}]",
        "LOG_ANALYTICS_WORKSPACES": "[{\"name\": \"sentinel\", \"workspace_id\": \"your-workspace-id\"}]",
        "AZURE_AUTH_METHOD": "cli"
      }
    }
  }
}
```

Restart Windsurf after saving. MCP tools are available in Cascade (Windsurf's AI agent).

---

### VS Code (GitHub Copilot)

Add to your VS Code `settings.json` (open via **Ctrl+Shift+P → Preferences: Open User Settings (JSON)**):

```json
{
  "mcp": {
    "servers": {
      "kql": {
        "type": "stdio",
        "command": "kql-mcp",
        "env": {
          "ADX_CLUSTERS": "[{\"name\": \"my-cluster\", \"cluster_url\": \"https://mycluster.eastus.kusto.windows.net\", \"database\": \"mydb\"}]",
          "LOG_ANALYTICS_WORKSPACES": "[{\"name\": \"sentinel\", \"workspace_id\": \"your-workspace-id\"}]",
          "AZURE_AUTH_METHOD": "cli"
        }
      }
    }
  }
}
```

Or add a workspace-scoped `.vscode/mcp.json` file to share the config with your team:

```json
{
  "servers": {
    "kql": {
      "type": "stdio",
      "command": "kql-mcp",
      "env": {
        "ADX_CLUSTERS": "[{\"name\": \"my-cluster\", \"cluster_url\": \"https://mycluster.eastus.kusto.windows.net\", \"database\": \"mydb\"}]",
        "AZURE_AUTH_METHOD": "cli"
      }
    }
  }
}
```

The KQL tools are then available in GitHub Copilot Chat when using **Agent mode** (`@agent`).

---

### Any Other MCP-Compatible Client

The server speaks standard [MCP over stdio](https://modelcontextprotocol.io/docs/concepts/transports). Any client that supports `stdio` MCP servers can use it with this generic config shape:

```json
{
  "mcpServers": {
    "kql": {
      "command": "kql-mcp",
      "env": {
        "ADX_CLUSTERS": "[{\"name\": \"<alias>\", \"cluster_url\": \"https://<cluster>.<region>.kusto.windows.net\", \"database\": \"<database>\"}]",
        "LOG_ANALYTICS_WORKSPACES": "[{\"name\": \"<alias>\", \"workspace_id\": \"<workspace-id>\"}]",
        "AZURE_AUTH_METHOD": "cli"
      }
    }
  }
}
```

**Key values:**

| Key | Description |
|-----|-------------|
| `command` | `kql-mcp` (the installed CLI entry point) |
| `ADX_CLUSTERS` | JSON array of ADX cluster connections |
| `LOG_ANALYTICS_WORKSPACES` | JSON array of Log Analytics workspace connections |
| `AZURE_AUTH_METHOD` | `cli`, `managed_identity`, `service_principal`, or `interactive` |

Refer to your AI client's MCP documentation for the exact config file location and format.

---

## Requirements

- Python 3.11+
- Azure CLI (`az login`) for `cli` auth mode, or appropriate credentials for other auth methods
- Access to an Azure Data Explorer cluster or Log Analytics / Sentinel workspace
