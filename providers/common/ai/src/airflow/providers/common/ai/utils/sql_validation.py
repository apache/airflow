# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
SQL safety validation for LLM-generated queries.

Uses an allowlist approach: only explicitly permitted statement types pass.
This is safer than a denylist because new/unexpected statement types
(INSERT, UPDATE, MERGE, TRUNCATE, COPY, etc.) are blocked by default.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.dialects import Dialects
from sqlglot.errors import ErrorLevel

# Dialect names sqlglot recognizes. Used to drop unknown dialect names so a bad
# value never breaks parsing (sqlglot raises on an unknown dialect).
_KNOWN_SQLGLOT_DIALECTS: frozenset[str] = frozenset(d.value for d in Dialects)

# SQLAlchemy ``dialect_name`` → sqlglot dialect mapping for names that differ.
_SQLALCHEMY_TO_SQLGLOT_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "mssql": "tsql",
}


def resolve_sqlglot_dialect(dialect_name: str | None) -> str | None:
    """
    Normalize a SQLAlchemy dialect name to a sqlglot dialect.

    Returns ``None`` (dialect-agnostic parsing) for empty, non-string, or
    unknown inputs, so a bad dialect value never breaks SQL validation.

    :param dialect_name: A SQLAlchemy ``dialect_name`` (e.g. ``"postgresql"``).
    :return: The matching sqlglot dialect (e.g. ``"postgres"``), or ``None``.
    """
    if not isinstance(dialect_name, str) or not dialect_name:
        return None
    mapped = _SQLALCHEMY_TO_SQLGLOT_DIALECT.get(dialect_name, dialect_name)
    return mapped if mapped in _KNOWN_SQLGLOT_DIALECTS else None


# Allowlist: only these top-level statement types pass validation by default.
# - Select: plain queries and CTE-wrapped queries (WITH ... AS ... SELECT is parsed
#   as Select with a `with` clause property — still a Select node at the top level)
# - Union/Intersect/Except: set operations on SELECT results
DEFAULT_ALLOWED_TYPES: tuple[type[exp.Expr], ...] = (
    exp.Select,
    exp.Union,
    exp.Intersect,
    exp.Except,
)

# Read-only metadata statements that introspect the schema without touching data:
# - Describe: DESCRIBE / DESC <table> (and EXPLAIN on some dialects)
# - Show: SHOW TABLES / SHOW COLUMNS / SHOW DATABASES, etc.
# Opt-in via ``allow_read_only_metadata=True``. SHOW only parses to ``exp.Show``
# when a dialect that supports it is passed (e.g. snowflake, mysql); without a
# dialect sqlglot falls back to ``exp.Command``, which stays blocked.
READ_ONLY_METADATA_TYPES: tuple[type[exp.Expr], ...] = (
    exp.Describe,
    exp.Show,
)

# Denylist: expression types that mutate data or schema when found anywhere in the AST.
# This catches data-modifying CTEs (e.g. WITH del AS (DELETE …) SELECT …),
# SELECT INTO, DDL or DML wrapped behind DESCRIBE/EXPLAIN (e.g. DESCRIBE DROP TABLE …),
# and other constructs that bypass top-level type checks.
# Note: exp.Command is sqlglot's fallback for any syntax it doesn't recognize.
# Including it makes the denylist fail-closed (safer), but may block legitimate
# vendor-specific SQL that sqlglot can't parse. Callers who need such syntax can
# provide custom allowed_types to bypass the deep scan entirely.
_DATA_MODIFYING_NODES: tuple[type[exp.Expr], ...] = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Into,
    exp.Command,
    # DDL — newly reachable through the DESCRIBE/SHOW allowlist, so deny it here too.
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
)


class SQLSafetyError(Exception):
    """Generated SQL failed safety validation."""


def validate_sql(
    sql: str,
    *,
    allowed_types: tuple[type[exp.Expr], ...] | None = None,
    dialect: str | None = None,
    allow_multiple_statements: bool = False,
    allow_read_only_metadata: bool = False,
) -> list[exp.Expr]:
    """
    Parse SQL and verify all statements are in the allowed types list.

    By default, only a single SELECT-family statement is allowed. Multi-statement
    SQL (separated by semicolons) is rejected unless ``allow_multiple_statements=True``,
    because multi-statement inputs can hide dangerous operations after a benign SELECT.

    Returns parsed statements on success, raises :class:`SQLSafetyError` on violation.

    :param sql: SQL string to validate.
    :param allowed_types: Tuple of sqlglot expression types to permit.
        Defaults to ``(Select, Union, Intersect, Except)``. When supplied, the
        caller takes full control of the allow-list and ``allow_read_only_metadata``
        is ignored.
    :param dialect: SQL dialect for parsing (``postgres``, ``mysql``, etc.).
    :param allow_multiple_statements: Whether to allow multiple semicolon-separated
        statements. Default ``False``.
    :param allow_read_only_metadata: Also permit read-only metadata statements
        (``DESCRIBE``/``SHOW``) on top of the default read-only allow-list. Ignored
        when ``allowed_types`` is supplied. Note ``SHOW`` only parses to a metadata
        statement when a ``dialect`` that supports it is given. Default ``False``.
    :return: List of parsed sqlglot Expression objects.
    :raises SQLSafetyError: If the SQL is empty, contains disallowed statement types,
        or has multiple statements when not permitted.
    """
    if not sql or not sql.strip():
        raise SQLSafetyError("Empty SQL input.")

    # A caller-supplied ``allowed_types`` is an explicit opt-out of the curated
    # read-only defaults (and the data-modifying deep scan). Otherwise we use the
    # read-only defaults, optionally widened with metadata statements, and keep
    # the deep scan on.
    if allowed_types is None:
        types: tuple[type[exp.Expr], ...] = DEFAULT_ALLOWED_TYPES
        if allow_read_only_metadata:
            types = types + READ_ONLY_METADATA_TYPES
        run_data_modifying_scan = True
    else:
        types = allowed_types
        run_data_modifying_scan = types == DEFAULT_ALLOWED_TYPES

    try:
        statements = sqlglot.parse(sql, dialect=dialect, error_level=ErrorLevel.RAISE)
    except sqlglot.errors.ParseError as e:
        raise SQLSafetyError(f"SQL parse error: {e}") from e

    # sqlglot.parse can return [None] for empty input
    parsed = [s for s in statements if s is not None]
    if not parsed:
        raise SQLSafetyError("Empty SQL input.")

    if not allow_multiple_statements and len(parsed) > 1:
        raise SQLSafetyError(
            f"Multiple statements detected ({len(parsed)}). Only single statements are allowed by default."
        )

    for stmt in parsed:
        if not isinstance(stmt, types):
            allowed_names = ", ".join(t.__name__ for t in types)
            raise SQLSafetyError(
                f"Statement type '{type(stmt).__name__}' is not allowed. Allowed types: {allowed_names}"
            )

    # Deep scan: reject data-modifying nodes hidden inside otherwise-allowed statements
    # (e.g. data-modifying CTEs, SELECT INTO, EXPLAIN <write>). Runs for the curated
    # read-only allow-list — callers who provide custom allowed_types have explicitly
    # opted into non-read-only operations.
    if run_data_modifying_scan:
        _check_for_data_modifying_nodes(parsed)

    return parsed


def _check_for_data_modifying_nodes(statements: list[exp.Expr]) -> None:
    """
    Walk the full AST of each statement and reject data-modifying expressions.

    This catches bypass vectors like:
    - Data-modifying CTEs: ``WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d``
    - SELECT INTO: ``SELECT * INTO new_table FROM t``
    - INSERT/UPDATE/DELETE hidden inside subqueries or CTEs

    :raises SQLSafetyError: If any data-modifying node is found in the AST.
    """
    for stmt in statements:
        for node in stmt.walk():
            if isinstance(node, _DATA_MODIFYING_NODES):
                raise SQLSafetyError(
                    f"Data-modifying operation '{type(node).__name__}' found inside statement. "
                    f"Only pure read operations are allowed in read-only mode."
                )
