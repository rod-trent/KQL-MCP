"""Azure Data Explorer connection management."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

from kql_mcp.config import AdxClusterConfig, ServerConfig


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    execution_time_ms: int
    warnings: list[str] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return self.row_count == 0


class AdxConnection:
    """Manages a connection to an Azure Data Explorer cluster."""

    def __init__(self, config: AdxClusterConfig, server_config: ServerConfig) -> None:
        self.config = config
        self.server_config = server_config
        self._client: KustoClient | None = None
        self._schema_cache: dict[str, Any] = {}
        self._schema_cache_ts: dict[str, float] = {}

    @property
    def name(self) -> str:
        return self.config.name

    @property
    def cluster_url(self) -> str:
        return self.config.cluster_url

    @property
    def default_database(self) -> str:
        return self.config.database

    def _build_kcsb(self) -> KustoConnectionStringBuilder:
        url = self.config.cluster_url
        auth = self.server_config.azure_auth_method

        if auth == "cli":
            return KustoConnectionStringBuilder.with_az_cli_authentication(url)
        elif auth == "managed_identity":
            return KustoConnectionStringBuilder.with_managed_service_identity(url)
        elif auth == "service_principal":
            if not all([
                self.server_config.azure_tenant_id,
                self.server_config.azure_client_id,
                self.server_config.azure_client_secret,
            ]):
                raise ValueError("service_principal auth requires AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET")
            return KustoConnectionStringBuilder.with_aad_application_key_authentication(
                url,
                self.server_config.azure_client_id,
                self.server_config.azure_client_secret,
                self.server_config.azure_tenant_id,
            )
        elif auth == "interactive":
            return KustoConnectionStringBuilder.with_interactive_login(url)
        else:
            return KustoConnectionStringBuilder.with_az_cli_authentication(url)

    def _get_client(self) -> KustoClient:
        if self._client is None:
            kcsb = self._build_kcsb()
            self._client = KustoClient(kcsb)
        return self._client

    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._schema_cache_ts:
            return False
        ttl = self.server_config.schema_cache_ttl_minutes * 60
        return time.time() - self._schema_cache_ts[key] < ttl

    async def execute_query(
        self,
        query: str,
        database: str | None = None,
        max_rows: int | None = None,
    ) -> QueryResult:
        """Execute a KQL query and return structured results."""
        db = database or self.config.database
        limit = max_rows or self.server_config.max_query_rows

        # Inject row limit if not present
        if "| take" not in query.lower() and "| limit" not in query.lower():
            query = f"{query}\n| take {limit}"

        client = self._get_client()
        start = time.monotonic()

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None, lambda: client.execute(db, query)
            )
        except KustoServiceError as e:
            raise RuntimeError(f"KQL execution failed: {e}") from e

        elapsed_ms = int((time.monotonic() - start) * 1000)

        primary_table = response.primary_results[0]
        columns = [col.column_name for col in primary_table.columns]
        rows = [[row[col] for col in columns] for row in primary_table]

        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            execution_time_ms=elapsed_ms,
        )

    async def list_databases(self) -> list[str]:
        """List all databases in the cluster."""
        cache_key = "databases"
        if self._is_cache_valid(cache_key):
            return self._schema_cache[cache_key]

        result = await self.execute_query(".show databases | project DatabaseName", database="NetDefaultDB")
        databases = [row[0] for row in result.rows]

        self._schema_cache[cache_key] = databases
        self._schema_cache_ts[cache_key] = time.time()
        return databases

    async def list_tables(self, database: str | None = None) -> list[dict[str, str]]:
        """List all tables in a database with their docstrings."""
        db = database or self.config.database
        cache_key = f"tables:{db}"
        if self._is_cache_valid(cache_key):
            return self._schema_cache[cache_key]

        result = await self.execute_query(
            ".show tables | project TableName, DocString",
            database=db,
        )
        tables = [{"name": row[0], "description": row[1] or ""} for row in result.rows]

        self._schema_cache[cache_key] = tables
        self._schema_cache_ts[cache_key] = time.time()
        return tables

    async def get_table_schema(self, table: str, database: str | None = None) -> list[dict[str, str]]:
        """Get column schema for a table."""
        db = database or self.config.database
        cache_key = f"schema:{db}:{table}"
        if self._is_cache_valid(cache_key):
            return self._schema_cache[cache_key]

        result = await self.execute_query(
            f".show table {table} schema as json | project Schema",
            database=db,
        )

        import json
        if result.is_empty:
            return []

        schema_json = json.loads(result.rows[0][0])
        columns = [
            {"name": col["Name"], "type": col["CslType"], "description": col.get("DocString", "")}
            for col in schema_json.get("OrderedColumns", [])
        ]

        self._schema_cache[cache_key] = columns
        self._schema_cache_ts[cache_key] = time.time()
        return columns

    async def get_sample_data(self, table: str, database: str | None = None, rows: int = 5) -> QueryResult:
        """Get sample rows from a table."""
        db = database or self.config.database
        return await self.execute_query(f"{table} | take {rows}", database=db, max_rows=rows)

    async def search_schema(
        self,
        keyword: str,
        database: str | None = None,
        search_columns: bool = True,
    ) -> dict[str, list[str]]:
        """Search for tables and columns matching a keyword."""
        db = database or self.config.database
        tables = await self.list_tables(db)
        keyword_lower = keyword.lower()

        matching_tables = [t["name"] for t in tables if keyword_lower in t["name"].lower()]
        matching_columns: list[str] = []

        if search_columns:
            for table in tables:
                try:
                    columns = await self.get_table_schema(table["name"], db)
                    for col in columns:
                        if keyword_lower in col["name"].lower():
                            matching_columns.append(f"{table['name']}.{col['name']} ({col['type']})")
                except Exception:
                    pass

        return {"tables": matching_tables, "columns": matching_columns}

    async def get_table_statistics(self, table: str, database: str | None = None) -> dict[str, Any]:
        """Get row count and size statistics for a table."""
        db = database or self.config.database
        result = await self.execute_query(
            f".show table {table} details | project RowCount, TotalExtentSize, TotalOriginalSize",
            database=db,
        )
        if result.is_empty:
            return {}
        row = result.rows[0]
        return {
            "row_count": row[0],
            "compressed_size_bytes": row[1],
            "original_size_bytes": row[2],
        }

    def clear_cache(self) -> None:
        """Clear all cached schema data."""
        self._schema_cache.clear()
        self._schema_cache_ts.clear()
