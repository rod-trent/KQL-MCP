"""KQL MCP tools — executable functions exposed to the AI."""

from .formatting import format_result
from .validation import validate_kql, estimate_query_cost

__all__ = ["format_result", "validate_kql", "estimate_query_cost"]
