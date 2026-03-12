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
DEFAULT_ALLOWED_TYPES: tuple[type[exp.Expression], ...] = (
    exp.Select,
    exp.Union,
    exp.Intersect,
    exp.Except,
)


class SQLSafetyError(Exception):
    """Generated SQL failed safety validation."""


def validate_sql(
    sql: str,
    *,
    allowed_types: tuple[type[exp.Expression], ...] | None = None,
    dialect: str | None = None,
    allow_multiple_statements: bool = False,
) -> list[exp.Expression]:
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
    parsed: list[exp.Expression] = [s for s in statements if s is not None]
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

    return parsed
