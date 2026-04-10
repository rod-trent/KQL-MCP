"""KQL query validation, syntax checking, and cost estimation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[str]
    warnings: list[str]
    suggestions: list[str]
    estimated_cost: str  # "low", "medium", "high", "unknown"

    def to_markdown(self) -> str:
        lines = []
        if self.is_valid:
            lines.append("**Status:** Valid KQL")
        else:
            lines.append("**Status:** Invalid KQL")

        lines.append(f"**Estimated cost:** {self.estimated_cost.upper()}")

        if self.errors:
            lines.append("\n**Errors:**")
            for e in self.errors:
                lines.append(f"- {e}")

        if self.warnings:
            lines.append("\n**Warnings:**")
            for w in self.warnings:
                lines.append(f"- {w}")

        if self.suggestions:
            lines.append("\n**Suggestions:**")
            for s in self.suggestions:
                lines.append(f"- {s}")

        return "\n".join(lines)


# Patterns that indicate potentially expensive queries
_EXPENSIVE_PATTERNS = [
    (r"\bsearch\b\s+\*", "Avoid `search *` — it scans all tables. Specify a table name instead."),
    (r"\bunion\b\s+\*", "Avoid `union *` — it unions all tables. Specify tables explicitly."),
    (r"\bfull\b.*\bjoin\b", "`fullouter` joins can be expensive. Consider using `inner` or `leftouter` join."),
    (r"\bcross\b.*\bjoin\b", "`crossjoin` produces a cartesian product — extremely expensive on large tables."),
    (r"^(?!.*\|\s*where\b)(?!.*\|\s*filter\b)", "Query has no `where` filter — may scan entire table."),
    (r"\bago\s*\(\s*\d+[dD]\s*\)", "Querying more than 1 day of data — ensure time filters are appropriate."),
    (r"\bago\s*\(\s*[3-9]\d+[dD]\s*\)", "Querying 30+ days of data — this could be very expensive."),
]

# Common syntax mistakes — each pattern matches SQL-style usage NOT preceded by a pipe (|)
# In KQL, operators always follow `|`. Without a pipe, it's likely SQL-style syntax.
_SYNTAX_CHECKS = [
    # SELECT without a leading pipe — SQL style
    (r"(?m)^(?!\s*\|)\s*SELECT\b", "KQL uses `| project` instead of SQL `SELECT`.", True),
    # FROM tableName at the start (not preceded by | or a function call)
    (r"(?m)^(?!\s*\|)\s*FROM\s+\w", "KQL starts with a table name, not `FROM tableName`.", True),
    # WHERE clause on its own line (not `| where`)
    (r"(?m)^(?!\s*\|)\s*WHERE\b", "Use `| where` (with a leading pipe) instead of a bare SQL `WHERE`.", True),
    # GROUP BY — not valid in KQL (use `| summarize ... by`)
    (r"(?i)\bGROUP\s+BY\b", "KQL uses `| summarize ... by` instead of `GROUP BY`.", True),
    # ORDER BY without a leading pipe — SQL style (KQL uses `| sort by` or `| order by`)
    (r"(?m)^(?!\s*\|)\s*ORDER\s+BY\b", "Use `| sort by` or `| order by` (with a leading pipe) instead of SQL `ORDER BY`.", True),
    # COUNT(*) — SQL style; KQL uses count() without *
    (r"\bCOUNT\(\s*\*\s*\)", "KQL uses `count()` (without `*`) inside `summarize`.", False),
    # == null comparisons
    (r"==\s*null\b", "Use `isnull()` or `isempty()` instead of `== null` in KQL.", False),
    (r"!=\s*null\b", "Use `isnotnull()` or `isnotempty()` instead of `!= null` in KQL.", False),
    # LIKE — SQL pattern matching
    (r"(?m)^(?!\s*\|).*\bLIKE\b|\|\s*where\b.*\bLIKE\b", "KQL uses `contains`, `startswith`, `endswith`, or `matches regex` instead of `LIKE`.", True),
]

# Good practices
_GOOD_PATTERNS = [
    (r"\|\s*where\b.*\bTimeGenerated\b", "Good: filtering on `TimeGenerated` enables time partitioning."),
    (r"\|\s*where\b.*\btimestamp\b", "Good: filtering on `timestamp` enables time partitioning."),
    (r"\|\s*project\b", "Good: using `| project` to select only needed columns reduces data movement."),
    (r"\|\s*take\b|\|\s*limit\b", "Good: using `| take` / `| limit` to cap result size."),
    (r"\blet\b\s+\w+\s*=", "Good: using `let` for variable assignment improves readability."),
]


def validate_kql(query: str) -> ValidationResult:
    """
    Validate a KQL query for syntax issues, performance problems, and best practices.
    Returns a ValidationResult with errors, warnings, and suggestions.
    """
    errors: list[str] = []
    warnings: list[str] = []
    suggestions: list[str] = []

    # Check for SQL-style mistakes
    for pattern, message, is_error in _SYNTAX_CHECKS:
        if re.search(pattern, query, re.IGNORECASE):
            if is_error:
                errors.append(message)
            else:
                warnings.append(message)

    # Check for expensive patterns
    for pattern, message in _EXPENSIVE_PATTERNS:
        if re.search(pattern, query, re.IGNORECASE | re.MULTILINE):
            warnings.append(message)

    # Check for good practices
    for pattern, message in _GOOD_PATTERNS:
        if re.search(pattern, query, re.IGNORECASE):
            suggestions.append(message)

    # Basic structure checks
    stripped = query.strip()
    if not stripped:
        errors.append("Query is empty.")

    if stripped.startswith("|"):
        errors.append("Query cannot start with `|` — it must begin with a table name or `let` statement.")

    # Estimate cost
    cost = _estimate_cost(query)

    return ValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        suggestions=suggestions,
        estimated_cost=cost,
    )


def _estimate_cost(query: str) -> str:
    """Heuristic cost estimation: low / medium / high / unknown."""
    q = query.lower()

    # High cost signals
    if any(p in q for p in ["search *", "union *", "crossjoin", "ago(30d", "ago(60d", "ago(90d"]):
        return "high"

    # Has time filter and table projection — low cost
    has_time_filter = bool(re.search(r"timegenerated|timestamp|ago\(", q))
    has_project = bool(re.search(r"\|\s*project\b", q))
    has_where = bool(re.search(r"\|\s*where\b", q))
    has_take = bool(re.search(r"\|\s*take\b|\|\s*limit\b", q))

    if has_time_filter and has_where and (has_project or has_take):
        return "low"

    if has_time_filter or has_where:
        return "medium"

    return "high"


def estimate_query_cost(query: str) -> dict[str, Any]:
    """Return a structured cost estimate for a query."""
    result = validate_kql(query)
    return {
        "estimated_cost": result.estimated_cost,
        "warnings": result.warnings,
        "has_time_filter": bool(re.search(r"timegenerated|timestamp|ago\(", query, re.IGNORECASE)),
        "has_row_limit": bool(re.search(r"\|\s*take\b|\|\s*limit\b", query, re.IGNORECASE)),
        "has_column_projection": bool(re.search(r"\|\s*project\b", query, re.IGNORECASE)),
    }
