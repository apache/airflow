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
import re
from collections.abc import Sequence

from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection

from airflow.configuration import conf
from airflow.settings import get_engine

log = logging.getLogger(__name__)


def _parse_index_spec(spec: str) -> tuple[str, list[str]]:
    """
    Parse spec format.

    - "table(col1, col2, ...)"

    Whitespace around tokens is ignored.
    """
    s = spec.strip()
    # table(col1,col2)
    m = re.match(r"^(?P<table>(?:[^().:]+))\s*\(\s*(?P<cols>[^()]+)\s*\)\s*$", s)
    if m:
        table_ref = m.group("table").strip()
        cols_part = m.group("cols").strip()
        cols = [c.strip() for c in cols_part.split(",") if c.strip()]
    else:
        raise ValueError(f"Invalid index spec '{spec}'. Expected 'table(col1,...)'.")
    if not table_ref or not cols:
        raise ValueError(f"Invalid index spec '{spec}'. Table name and at least one column are required.")
    table = table_ref
    return table, cols


def _build_index_name(table: str, cols: Sequence[str]) -> str:
    """Build a deterministic index name based on table and columns."""
    base = f"idx_{table}_{'_'.join(cols)}"
    sanitized = re.sub(r"[^a-zA-Z0-9_]+", "_", base)
    if len(sanitized) > 63:
        # Simple truncation with tail hash to avoid collisions
        import hashlib

        digest = hashlib.sha1(sanitized.encode("utf-8")).hexdigest()[:8]
        sanitized = f"{sanitized[:54]}_{digest}"
    return sanitized


def _index_exists(conn: Connection, table: str, index_name: str) -> bool:
    insp = inspect(conn)
    existing = insp.get_indexes(table)
    return any(idx.get("name") == index_name for idx in existing or [])


def _create_index_postgres(conn: Connection, table: str, cols: Sequence[str], name: str) -> None:
    qualified_table = table
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    # This statement must run outside of a transaction block.
    ddl = f'CREATE INDEX CONCURRENTLY "{name}" ON {qualified_table} ({cols_sql})'
    # Commit to allow changing the isolation level
    conn.commit()
    conn = conn.execution_options(isolation_level="AUTOCOMMIT")
    conn.execute(text(ddl))


def _create_index_mysql(conn: Connection, table: str, cols: Sequence[str], name: str) -> None:
    qualified_table = f"`{table}`"
    cols_sql = ", ".join(f"`{c}`" for c in cols)

    # Prefer ONLINE DDL when possible. MySQL supports ALTER TABLE ... ADD INDEX with algorithm/lock hints.
    # Try non-blocking first; if it fails, fallback to plain create index.
    try:
        conn.execute(
            text(
                f"ALTER TABLE {qualified_table} ADD INDEX `{name}` ({cols_sql}) ALGORITHM=INPLACE, LOCK=NONE"
            )
        )
    except Exception as err:
        log.debug("Online index creation failed for %s: %s. Falling back to CREATE INDEX.", name, err)
        conn.execute(text(f"CREATE INDEX `{name}` ON {qualified_table} ({cols_sql})"))


def _create_index_sqlite(conn: Connection, table: str, cols: Sequence[str], name: str) -> None:
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    conn.execute(text(f'CREATE INDEX "{name}" ON "{table}" ({cols_sql})'))


def _create_index(conn: Connection, table: str, cols: Sequence[str], name: str) -> None:
    dialect = conn.dialect.name
    if dialect == "postgresql":
        _create_index_postgres(conn, table, cols, name)
    elif dialect in {"mysql", "mariadb"}:
        _create_index_mysql(conn, table, cols, name)
    elif dialect == "sqlite":
        _create_index_sqlite(conn, table, cols, name)
    else:
        raise NotImplementedError(f"Index creation not implemented for dialect '{dialect}'.")


def init_metadata_indexes() -> list[tuple[str, bool, str | None]]:
    """
    Create configured metadata indexes if they do not already exist.

    Returns a list of (index_name, created, error) tuples for logging/diagnostics.
    """
    engine = get_engine()
    results: list[tuple[str, bool, str | None]] = []
    specs = conf.getlist("database", "metadata_indexes", fallback=None, delimiter="|")

    if not specs:
        return results

    with engine.connect() as conn:
        for spec in specs:
            try:
                table, cols = _parse_index_spec(spec)
            except ValueError as e:
                log.error("Skipping invalid index spec '%s': %s", spec, e)
                results.append((spec, False, str(e)))
                continue

            name = _build_index_name(table, cols)
            try:
                if _index_exists(conn, table, name):
                    log.debug("Index %s already exists on %s", name, table)
                    results.append((name, False, None))
                    continue
                _create_index(conn, table, cols, name)
                log.info("Created index %s on %s(%s)", name, table, ", ".join(cols))
                results.append((name, True, None))
            except Exception as e:
                log.warning("Failed to create index %s from spec '%s': %s", name, spec, e)
                results.append((name, False, str(e)))
    return results
