"""Configuration management for the KQL MCP server."""

from __future__ import annotations

import json
import os
from typing import Literal

from dotenv import load_dotenv
from pydantic import BaseModel, Field

load_dotenv()


class AdxClusterConfig(BaseModel):
    name: str
    cluster_url: str
    database: str = "default"


class LogAnalyticsWorkspaceConfig(BaseModel):
    name: str
    workspace_id: str
    subscription_id: str | None = None


class ServerConfig(BaseModel):
    # Connections
    adx_clusters: list[AdxClusterConfig] = Field(default_factory=list)
    log_analytics_workspaces: list[LogAnalyticsWorkspaceConfig] = Field(default_factory=list)

    # Auth
    azure_auth_method: Literal["cli", "managed_identity", "service_principal", "interactive"] = "cli"
    azure_tenant_id: str | None = None
    azure_client_id: str | None = None
    azure_client_secret: str | None = None

    # Query settings
    max_query_rows: int = 10_000
    query_timeout_seconds: int = 120
    cost_warning_threshold_seconds: int = 30

    # Cache
    schema_cache_ttl_minutes: int = 30

    # Output
    default_output_format: Literal["table", "json", "csv", "markdown"] = "markdown"


def load_config() -> ServerConfig:
    adx_clusters_raw = os.getenv("ADX_CLUSTERS", "[]")
    la_workspaces_raw = os.getenv("LOG_ANALYTICS_WORKSPACES", "[]")

    try:
        adx_clusters = [AdxClusterConfig(**c) for c in json.loads(adx_clusters_raw)]
    except (json.JSONDecodeError, TypeError, ValueError):
        adx_clusters = []

    try:
        la_workspaces = [LogAnalyticsWorkspaceConfig(**w) for w in json.loads(la_workspaces_raw)]
    except (json.JSONDecodeError, TypeError, ValueError):
        la_workspaces = []

    return ServerConfig(
        adx_clusters=adx_clusters,
        log_analytics_workspaces=la_workspaces,
        azure_auth_method=os.getenv("AZURE_AUTH_METHOD", "cli"),  # type: ignore[arg-type]
        azure_tenant_id=os.getenv("AZURE_TENANT_ID"),
        azure_client_id=os.getenv("AZURE_CLIENT_ID"),
        azure_client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        max_query_rows=int(os.getenv("MAX_QUERY_ROWS", "10000")),
        query_timeout_seconds=int(os.getenv("QUERY_TIMEOUT_SECONDS", "120")),
        cost_warning_threshold_seconds=int(os.getenv("COST_WARNING_THRESHOLD_SECONDS", "30")),
        schema_cache_ttl_minutes=int(os.getenv("SCHEMA_CACHE_TTL_MINUTES", "30")),
        default_output_format=os.getenv("DEFAULT_OUTPUT_FORMAT", "markdown"),  # type: ignore[arg-type]
    )
