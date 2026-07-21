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
from __future__ import annotations

import logging

log = logging.getLogger(__name__)

try:
    import sqlglot  # this is an optional dependency
    from sqlglot import exp
    from sqlglot.errors import ParseError, TokenError

    _SQLGLOT_AVAILABLE = True
except ImportError:  # pragma: no cover
    _SQLGLOT_AVAILABLE = False

# Postgres only matches the current scope of the deferrable SQLExecuteQueryOperator
_DIALECT = "postgres"

if _SQLGLOT_AVAILABLE:
    _WRITE_TYPES = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Alter, exp.Drop)
    for name in ("TruncateTable", "Rename"):
        cls = getattr(exp, name, None)
        if cls is not None:
            _WRITE_TYPES += (cls,)
else:
    _WRITE_TYPES = ()


def _statement_write_node(stmt: exp.Expression):
    """Return the first node that proves this statement writes, or None."""
    if isinstance(stmt, _WRITE_TYPES):
        return stmt
    return next(stmt.find_all(*_WRITE_TYPES), None)


def scan_for_writes(sql: str | list[str]) -> tuple[bool, str]:
    """Pre-defer check for write statements``."""
    if not _SQLGLOT_AVAILABLE:
        return False, "sqlglot not installed; read-only transaction will enforce"

    if isinstance(sql, (list, tuple)):
        sql_text = ";\n".join(str(s) for s in sql)
    else:
        sql_text = str(sql)

    try:
        statements = sqlglot.parse(sql_text, read=_DIALECT)
    except (ParseError, TokenError) as exc:
        log.info(
            "read-only guard: could not parse SQL (%s); deferring judgement to the read-only transaction",
            exc.__class__.__name__,
        )
        return False, "unparseable; read-only transaction will enforce"

    for idx, stmt in enumerate(statements):
        if stmt is None:  # empty fragment from a trailing ';'
            continue
        node = _statement_write_node(stmt)
        if node is not None:
            kind = type(node).__name__.upper()
            return True, (
                f"statement #{idx + 1} is a proven write ({kind}): {stmt.sql(dialect=_DIALECT)[:120]!r}"
            )

    return False, (
        "no write detected by parse (writes inside functions/procedures/dynamic "
        "SQL are invisible here and are enforced by the read-only transaction)"
    )
