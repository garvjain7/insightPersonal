"""
core/sql_validator.py
=====================
AST-based SQL safety validator using sqlglot.

Enforces a strict read-only policy:
  - Only SELECT statements are permitted
  - No subquery mutations (INSERT INTO SELECT, CREATE TABLE AS SELECT)
  - No system/catalog table access
  - No EXECUTE / CALL statements
  - Statement must be parseable (malformed SQL is rejected)

This runs on every generated SQL query before it reaches DuckDB.
It is the security boundary between the LLM and the data.
"""

from __future__ import annotations

import logging

import sqlglot
import sqlglot.expressions as exp

logger = logging.getLogger("chatbot.sql_validator")


# ── Blocked node types ────────────────────────────────────────────────────────
# Any of these appearing anywhere in the AST will reject the query.

_BLOCKED_TYPES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Create,
    exp.Alter,
    exp.TruncateTable,
    exp.Command,        # EXECUTE, CALL, etc.
    exp.Transaction,
    exp.Commit,
    exp.Rollback,
)

# Blocked table/schema names (system tables, information schema)
_BLOCKED_TABLE_PATTERNS = {
    "information_schema",
    "pg_catalog",
    "sqlite_master",
    "sqlite_sequence",
    "sys",
}


# ── Public API ────────────────────────────────────────────────────────────────

class ValidationError(Exception):
    """Raised when a SQL query fails safety validation."""


def validate(sql: str) -> str:
    """
    Validate and normalise a SQL query.

    Returns the normalised SQL string if safe.
    Raises ValidationError with a human-readable message if not.
    """
    if not sql or not sql.strip():
        raise ValidationError("Empty SQL query")

    # Parse with sqlglot — raises ParseError if malformed
    try:
        statements = sqlglot.parse(sql, dialect="duckdb")
    except sqlglot.errors.ParseError as exc:
        raise ValidationError(f"SQL parse error: {exc}") from exc

    if not statements:
        raise ValidationError("No valid SQL statement found")

    if len(statements) > 1:
        raise ValidationError(
            "Multiple statements are not allowed. Send one query at a time."
        )

    statement = statements[0]

    # Must be a SELECT at the top level
    if not isinstance(statement, exp.Select):
        raise ValidationError(
            f"Only SELECT queries are allowed. Got: {type(statement).__name__}"
        )

    # Walk the full AST and check for blocked node types
    for node in statement.walk():
        if isinstance(node, _BLOCKED_TYPES):
            raise ValidationError(
                f"Blocked SQL operation detected: {type(node).__name__}"
            )

    # Check for blocked table references
    for table in statement.find_all(exp.Table):
        table_name = (table.name or "").lower()
        db_name    = (table.db   or "").lower()
        if table_name in _BLOCKED_TABLE_PATTERNS or db_name in _BLOCKED_TABLE_PATTERNS:
            raise ValidationError(
                f"Access to system table '{table_name}' is not allowed"
            )

    # Normalise back to SQL string (consistent formatting)
    normalised = statement.sql(dialect="duckdb", pretty=False)
    logger.debug(f"SQL validated: {normalised[:120]!r}")
    return normalised