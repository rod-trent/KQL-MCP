"""Registry for managing multiple KQL connections."""

from __future__ import annotations

from kql_mcp.config import ServerConfig
from kql_mcp.connections.adx import AdxConnection
from kql_mcp.connections.log_analytics import LogAnalyticsConnection


class ConnectionRegistry:
    """Central registry for all configured KQL connections."""

    def __init__(self, config: ServerConfig) -> None:
        self.config = config
        self._adx: dict[str, AdxConnection] = {}
        self._la: dict[str, LogAnalyticsConnection] = {}
        self._initialize()

    def _initialize(self) -> None:
        for cluster_config in self.config.adx_clusters:
            self._adx[cluster_config.name] = AdxConnection(cluster_config, self.config)

        for workspace_config in self.config.log_analytics_workspaces:
            self._la[workspace_config.name] = LogAnalyticsConnection(workspace_config, self.config)

    # ── ADX ──────────────────────────────────────────────────────────────────

    def get_adx(self, name: str) -> AdxConnection:
        if name not in self._adx:
            raise ValueError(
                f"ADX connection '{name}' not found. Available: {list(self._adx.keys())}"
            )
        return self._adx[name]

    def list_adx_connections(self) -> list[dict[str, str]]:
        return [
            {
                "name": conn.name,
                "cluster_url": conn.cluster_url,
                "default_database": conn.default_database,
                "type": "adx",
            }
            for conn in self._adx.values()
        ]

    def get_default_adx(self) -> AdxConnection | None:
        if not self._adx:
            return None
        return next(iter(self._adx.values()))

    # ── Log Analytics ─────────────────────────────────────────────────────────

    def get_log_analytics(self, name: str) -> LogAnalyticsConnection:
        if name not in self._la:
            raise ValueError(
                f"Log Analytics connection '{name}' not found. Available: {list(self._la.keys())}"
            )
        return self._la[name]

    def list_log_analytics_connections(self) -> list[dict[str, str]]:
        return [
            {
                "name": conn.name,
                "workspace_id": conn.workspace_id,
                "type": "log_analytics",
            }
            for conn in self._la.values()
        ]

    def get_default_log_analytics(self) -> LogAnalyticsConnection | None:
        if not self._la:
            return None
        return next(iter(self._la.values()))

    # ── Combined ─────────────────────────────────────────────────────────────

    def list_all_connections(self) -> list[dict[str, str]]:
        return self.list_adx_connections() + self.list_log_analytics_connections()

    @property
    def has_connections(self) -> bool:
        return bool(self._adx or self._la)

    def clear_all_caches(self) -> None:
        for conn in self._adx.values():
            conn.clear_cache()
        for conn in self._la.values():
            conn.clear_cache()
