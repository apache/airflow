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
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp

log = logging.getLogger(__name__)

_JINJA_MARKERS = ("{{", "{%")


@dataclass
class TableRef:
    """Represents a parsed table reference with optional schema and database qualifiers."""

    table: str
    schema: str | None = None
    database: str | None = None


def parse_sql_tables(
    sql: str | list[str],
    dialect: str | None = None,
) -> tuple[list[TableRef], list[TableRef]]:
    """
    Parse SQL and return ``(source_tables, target_tables)``.

    Source tables are those read by FROM/JOIN clauses.
    Target tables are those written by INSERT INTO, CREATE TABLE AS, or MERGE INTO.
    Returns empty lists when SQL cannot be parsed instead of raising.
    """
    statements = [sql] if isinstance(sql, str) else sql
    sources: list[TableRef] = []
    targets: list[TableRef] = []

    for idx, stmt in enumerate(statements):
        if not isinstance(stmt, str) or not stmt.strip():
            continue
        if any(marker in stmt for marker in _JINJA_MARKERS):
            log.debug(
                "SQL statement %d contains unrendered Jinja templates; skipping lineage extraction.", idx
            )
            continue
        try:
            for parsed in sqlglot.parse(stmt, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN):
                if parsed is None:
                    continue
                if not isinstance(parsed, exp.Expression):
                    continue
                stmt_sources, stmt_targets = _extract_tables(parsed)
                sources.extend(stmt_sources)
                targets.extend(stmt_targets)
        except Exception:
            log.debug("Failed to parse SQL statement %d", idx, exc_info=True)

    return _dedup(sources), _dedup(targets)


def _extract_tables(parsed: exp.Expression) -> tuple[list[TableRef], list[TableRef]]:
    cte_names: set[str] = set()
    with_node = parsed.find(exp.With)
    if with_node:
        for cte in with_node.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(cte.alias.lower())

    target_node_id: int | None = None
    if isinstance(parsed, (exp.Insert, exp.Create, exp.Merge)):
        write_target = _get_write_target(parsed)
        if write_target is not None:
            target_node_id = id(write_target)

    sources: list[TableRef] = []
    targets: list[TableRef] = []

    for table in parsed.find_all(exp.Table):
        if table.name.lower() in cte_names:
            continue
        ref = TableRef(
            table=table.name,
            schema=table.db or None,
            database=table.catalog or None,
        )
        if id(table) == target_node_id:
            targets.append(ref)
        else:
            sources.append(ref)

    return sources, targets


def _get_write_target(node: exp.Expression) -> exp.Table | None:
    if isinstance(node, (exp.Insert, exp.Merge)):
        candidate = node.this
    elif isinstance(node, exp.Create):
        candidate = node.this
    else:
        return None

    # INSERT INTO target(col1, col2) ... is represented as Schema(Table(...), ...)
    if isinstance(candidate, exp.Schema):
        candidate = candidate.this

    return candidate if isinstance(candidate, exp.Table) else None


def _dedup(refs: list[TableRef]) -> list[TableRef]:
    seen: set[tuple[str | None, str | None, str]] = set()
    result: list[TableRef] = []
    for ref in refs:
        key = (ref.database, ref.schema, ref.table)
        if key not in seen:
            seen.add(key)
            result.append(ref)
    return result
