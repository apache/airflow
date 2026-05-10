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
from abc import ABC, abstractmethod
from typing import Any

from airflow.providers.informatica.lineage.sql_parser import TableRef, parse_sql_tables

log = logging.getLogger(__name__)

try:
    from airflow.providers.common.sql.operators.sql import BaseSQLOperator as _BaseSQLOperator

    _HAS_BASE_SQL_OPERATOR = True
except ImportError:
    _BaseSQLOperator = None  # type: ignore[assignment, misc]
    _HAS_BASE_SQL_OPERATOR = False

# Operator attribute names scanned in order to locate a connection ID.
# conn_id_field (BaseSQLOperator) is tried first; this list is the fallback.
_CONN_ID_ATTRS: tuple[str, ...] = (
    "conn_id",
    "source_conn_id",
    "mysql_conn_id",
    "postgres_conn_id",
    "mssql_conn_id",
    "oracle_conn_id",
    "sqlite_conn_id",
    "snowflake_conn_id",
    "databricks_conn_id",
    "exasol_conn_id",
    "hiveserver2_conn_id",
)

# Keyword fragments found in a conn_id string mapped to sqlglot dialect names.
_CONN_TYPE_TO_DIALECT: dict[str, str] = {
    "postgres": "postgres",
    "redshift": "redshift",
    "mysql": "mysql",
    "mssql": "tsql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "databricks": "databricks",
    "sqlite": "sqlite",
    "oracle": "oracle",
    "trino": "trino",
    "presto": "presto",
    "hive": "hive",
    "spark": "spark",
}

# Operator attribute names checked as explicit write-target table when SQL
# parsing yields no targets (e.g. GenericTransfer, HiveToMySqlOperator).
_TARGET_TABLE_ATTRS: tuple[str, ...] = (
    "destination_table",
    "mysql_table",
    "hive_table",
    "target_table",
)


class BaseLineageResolver(ABC):
    """Base class for operator lineage resolvers."""

    @abstractmethod
    def resolve(self, task: Any) -> tuple[list[TableRef], list[TableRef]] | None:
        """Return ``(source_refs, target_refs)`` or ``None`` if the resolver does not apply."""


class SQLLineageResolver(BaseLineageResolver):
    """
    Resolves lineage for any operator that exposes a ``sql`` attribute.

    Detection is tiered:

    - Tier 1: operators inheriting from ``BaseSQLOperator`` — ``conn_id_field``
      points to the right connection attribute.
    - Tier 2: operators with a ``sql`` attribute but no ``BaseSQLOperator``
      base (e.g. ``GenericTransfer``, ``BaseSQLToGCSOperator``) — dialect is
      inferred from the first recognizable connection ID string found.

    Returns ``None`` when there is no SQL, when Jinja templates are detected,
    or when parsing produces no table references.
    """

    def resolve(self, task: Any) -> tuple[list[TableRef], list[TableRef]] | None:
        sql = getattr(task, "sql", None)
        if not sql:
            return None
        dialect = _infer_dialect(task)
        default_database: str | None = getattr(task, "database", None)
        sources, targets = parse_sql_tables(sql, dialect=dialect)
        if not targets:
            for attr in _TARGET_TABLE_ATTRS:
                table_name = getattr(task, attr, None)
                if table_name and isinstance(table_name, str):
                    targets.append(TableRef(table=table_name))
                    break

        if not sources and not targets:
            return None

        # Fill in default_database for refs that have none set
        if default_database:
            sources = [TableRef(t.table, t.schema, t.database or default_database) for t in sources]
            targets = [TableRef(t.table, t.schema, t.database or default_database) for t in targets]

        return sources, targets


def _infer_dialect(task: Any) -> str | None:
    conn_id_field = getattr(task, "conn_id_field", None)
    if conn_id_field:
        conn_id = getattr(task, conn_id_field, None)
        if conn_id and isinstance(conn_id, str):
            result = _dialect_from_conn_id_str(conn_id)
            if result:
                return result

    for attr in _CONN_ID_ATTRS:
        conn_id = getattr(task, attr, None)
        if conn_id and isinstance(conn_id, str):
            result = _dialect_from_conn_id_str(conn_id)
            if result:
                return result

    return None


def _dialect_from_conn_id_str(conn_id: str) -> str | None:
    conn_id_lower = conn_id.lower()
    for keyword, dialect in _CONN_TYPE_TO_DIALECT.items():
        if keyword in conn_id_lower:
            return dialect
    return None


_SQL_RESOLVER = SQLLineageResolver()


def get_resolver(task: Any) -> BaseLineageResolver | None:
    """Return a resolver for *task*, or ``None`` when no resolver applies."""
    if getattr(task, "sql", None):
        return _SQL_RESOLVER
    return None
