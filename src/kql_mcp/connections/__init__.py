"""Connection management for ADX and Log Analytics."""

from .adx import AdxConnection
from .log_analytics import LogAnalyticsConnection
from .registry import ConnectionRegistry

__all__ = ["AdxConnection", "LogAnalyticsConnection", "ConnectionRegistry"]
