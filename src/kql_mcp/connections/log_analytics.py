"""Azure Log Analytics / Sentinel connection management."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from azure.identity import (
    AzureCliCredential,
    ClientSecretCredential,
    InteractiveBrowserCredential,
    ManagedIdentityCredential,
)
from azure.monitor.query import LogsQueryClient, LogsQueryStatus

from kql_mcp.config import LogAnalyticsWorkspaceConfig, ServerConfig
from kql_mcp.connections.adx import QueryResult


class LogAnalyticsConnection:
    """Manages a connection to an Azure Log Analytics workspace."""

    def __init__(self, config: LogAnalyticsWorkspaceConfig, server_config: ServerConfig) -> None:
        self.config = config
        self.server_config = server_config
        self._client: LogsQueryClient | None = None
        self._schema_cache: dict[str, Any] = {}
        self._schema_cache_ts: dict[str, float] = {}

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def workspace_id(self) -> str:
        return self.config.workspace_id

    def _get_credential(self) -> Any:
        auth = self.server_config.azure_auth_method
        if auth == "cli":
            return AzureCliCredential()
        elif auth == "managed_identity":
            return ManagedIdentityCredential()
        elif auth == "service_principal":
            return ClientSecretCredential(
                tenant_id=self.server_config.azure_tenant_id,
                client_id=self.server_config.azure_client_id,
                client_secret=self.server_config.azure_client_secret,
            )
        elif auth == "interactive":
            return InteractiveBrowserCredential()
        return AzureCliCredential()

    def _get_client(self) -> LogsQueryClient:
        if self._client is None:
            credential = self._get_credential()
            self._client = LogsQueryClient(credential)
        return self._client

    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._schema_cache_ts:
            return False
        ttl = self.server_config.schema_cache_ttl_minutes * 60
        return time.time() - self._schema_cache_ts[key] < ttl

    async def execute_query(
        self,
        query: str,
        timespan: str | None = None,
        max_rows: int | None = None,
    ) -> QueryResult:
        """Execute a KQL query against Log Analytics."""
        from datetime import timedelta

        limit = max_rows or self.server_config.max_query_rows

        # Inject row limit if not present
        if "| take" not in query.lower() and "| limit" not in query.lower():
            query = f"{query}\n| take {limit}"

        client = self._get_client()

        # Default to last 24 hours if no timespan specified
        ts = timespan or timedelta(days=1)

        start = time.monotonic()
        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.query_workspace(
                    workspace_id=self.config.workspace_id,
                    query=query,
                    timespan=ts,
                ),
            )
        except Exception as e:
            raise RuntimeError(f"Log Analytics query failed: {e}") from e

        elapsed_ms = int((time.monotonic() - start) * 1000)

        if response.status == LogsQueryStatus.FAILURE:
            raise RuntimeError(f"Query failed: {response.partial_error}")

        table = response.tables[0] if response.tables else None
        if table is None:
            return QueryResult(columns=[], rows=[], row_count=0, execution_time_ms=elapsed_ms)

        columns = [col.name for col in table.columns]
        rows = [list(row) for row in table.rows]

        warnings = []
        if response.status == LogsQueryStatus.PARTIAL:
            warnings.append("Query returned partial results due to timeout or data limit.")

        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            execution_time_ms=elapsed_ms,
            warnings=warnings,
        )

    async def list_tables(self) -> list[dict[str, str]]:
        """List all tables available in the workspace."""
        cache_key = "tables"
        if self._is_cache_valid(cache_key):
            return self._schema_cache[cache_key]

        # Use the search.* and union to discover tables in Log Analytics
        result = await self.execute_query(
            "search * | distinct $table | project TableName=$table | sort by TableName asc",
            timespan="P1D",
        )
        tables = [{"name": row[0], "description": ""} for row in result.rows]

        self._schema_cache[cache_key] = tables
        self._schema_cache_ts[cache_key] = time.time()
        return tables

    async def get_table_schema(self, table: str) -> list[dict[str, str]]:
        """Get column schema for a Log Analytics table."""
        cache_key = f"schema:{table}"
        if self._is_cache_valid(cache_key):
            return self._schema_cache[cache_key]

        result = await self.execute_query(
            f"{table} | getschema | project ColumnName, DataType, ColumnType",
            timespan="P1D",
            max_rows=500,
        )

        columns = [
            {"name": row[0], "type": row[2] or row[1], "description": ""}
            for row in result.rows
        ]

        self._schema_cache[cache_key] = columns
        self._schema_cache_ts[cache_key] = time.time()
        return columns

    async def get_sample_data(self, table: str, rows: int = 5) -> QueryResult:
        """Get sample rows from a table."""
        return await self.execute_query(f"{table} | take {rows}", max_rows=rows)

    async def search_schema(
        self,
        keyword: str,
        search_columns: bool = True,
    ) -> dict[str, list[str]]:
        """Search for tables and columns matching a keyword."""
        tables = await self.list_tables()
        keyword_lower = keyword.lower()

        matching_tables = [t["name"] for t in tables if keyword_lower in t["name"].lower()]
        matching_columns: list[str] = []

        if search_columns:
            for table in tables[:50]:  # Limit to avoid too many schema calls
                try:
                    columns = await self.get_table_schema(table["name"])
                    for col in columns:
                        if keyword_lower in col["name"].lower():
                            matching_columns.append(f"{table['name']}.{col['name']} ({col['type']})")
                except Exception:
                    pass

        return {"tables": matching_tables, "columns": matching_columns}

    def clear_cache(self) -> None:
        """Clear all cached schema data."""
        self._schema_cache.clear()
        self._schema_cache_ts.clear()
