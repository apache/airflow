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
from sqlglot.errors import ErrorLevel

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

# Denylist: expression types that mutate data or schema when found anywhere in the AST.
# This catches data-modifying CTEs (e.g. WITH del AS (DELETE …) SELECT …),
# SELECT INTO, and other constructs that bypass top-level type checks.
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
)


class SQLSafetyError(Exception):
    """Generated SQL failed safety validation."""


def validate_sql(
    sql: str,
    *,
    allowed_types: tuple[type[exp.Expr], ...] | None = None,
    dialect: str | None = None,
    allow_multiple_statements: bool = False,
) -> list[exp.Expr]:
    """
    Parse SQL and verify all statements are in the allowed types list.

    By default, only a single SELECT-family statement is allowed. Multi-statement
    SQL (separated by semicolons) is rejected unless ``allow_multiple_statements=True``,
    because multi-statement inputs can hide dangerous operations after a benign SELECT.

    Returns parsed statements on success, raises :class:`SQLSafetyError` on violation.

    :param sql: SQL string to validate.
    :param allowed_types: Tuple of sqlglot expression types to permit.
        Defaults to ``(Select, Union, Intersect, Except)``.
    :param dialect: SQL dialect for parsing (``postgres``, ``mysql``, etc.).
    :param allow_multiple_statements: Whether to allow multiple semicolon-separated
        statements. Default ``False``.
    :return: List of parsed sqlglot Expression objects.
    :raises SQLSafetyError: If the SQL is empty, contains disallowed statement types,
        or has multiple statements when not permitted.
    """
    if not sql or not sql.strip():
        raise SQLSafetyError("Empty SQL input.")

    types = allowed_types or DEFAULT_ALLOWED_TYPES

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
    # (e.g. data-modifying CTEs, SELECT INTO). Only applies when using the default
    # read-only allowlist — callers who provide custom allowed_types have explicitly
    # opted into non-read-only operations.
    if types is DEFAULT_ALLOWED_TYPES:
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
