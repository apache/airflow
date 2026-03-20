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
Shared database hook and schema introspection utilities.

These helpers are used by both :class:`~airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator`
and :class:`~airflow.providers.common.ai.operators.llm_data_quality.LLMDataQualityOperator` to
avoid code duplication while keeping both operators decoupled from each other.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from airflow.providers.common.sql.config import DataSourceConfig
    from airflow.providers.common.sql.hooks.sql import DbApiHook

log = logging.getLogger(__name__)

# SQLAlchemy dialect_name → sqlglot dialect mapping for names that differ.
SQLALCHEMY_TO_SQLGLOT_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "mssql": "tsql",
}


def get_db_hook(db_conn_id: str) -> DbApiHook:
    """
    Return a :class:`~airflow.providers.common.sql.hooks.sql.DbApiHook` for *db_conn_id*.

    :param db_conn_id: Airflow connection ID that resolves to a ``DbApiHook``.
    :raises ValueError: If the connection does not resolve to a ``DbApiHook``.
    """
    # Lazy load to avoid hard dependency on common.sql
    from airflow.providers.common.sql.hooks.sql import DbApiHook

    connection = BaseHook.get_connection(db_conn_id)
    hook = connection.get_hook()
    if not isinstance(hook, DbApiHook):
        raise ValueError(
            f"Connection {db_conn_id!r} does not provide a DbApiHook. Got {type(hook).__name__}."
        )
    return hook


def resolve_dialect(db_hook: DbApiHook | None, explicit_dialect: str | None) -> str | None:
    """
    Resolve the SQL dialect from an explicit parameter or a database hook.

    Normalises SQLAlchemy dialect names to sqlglot equivalents
    (e.g. ``postgresql`` → ``postgres``).

    :param db_hook: Database hook to read ``dialect_name`` from when *explicit_dialect* is absent.
    :param explicit_dialect: Caller-supplied dialect string; takes priority over the hook.
    :return: Resolved dialect string, or ``None`` when neither source provides one.
    """
    raw = explicit_dialect
    if not raw and db_hook and hasattr(db_hook, "dialect_name"):
        candidate = db_hook.dialect_name
        raw = candidate if isinstance(candidate, str) else None
    if raw:
        return SQLALCHEMY_TO_SQLGLOT_DIALECT.get(raw, raw)
    return None


def build_schema_context(
    *,
    db_hook: DbApiHook | None,
    table_names: list[str] | None,
    schema_context: str | None,
    datasource_config: DataSourceConfig | None,
) -> str:
    """
    Return a schema description string suitable for inclusion in an LLM prompt.

    Resolution order:
    1. *schema_context* — returned as-is when provided (manual override).
    2. DB introspection via *db_hook* + *table_names*.
    3. Object-storage introspection via *datasource_config*.
    4. Empty string when none of the above are available.

    :param db_hook: Hook used for relational-database schema introspection.
    :param table_names: Table names to introspect via *db_hook*.
    :param schema_context: Manual schema description; bypasses introspection when set.
    :param datasource_config: DataFusion datasource config for object-storage schema.
    :raises ValueError: If *table_names* are provided but none yield schema information.
    :raises ValueError: If *datasource_config* is ``None`` and no DB tables are available.
    """
    if schema_context:
        return schema_context

    if (db_hook and table_names) or datasource_config:
        return _introspect_schemas(
            db_hook=db_hook,
            table_names=table_names,
            datasource_config=datasource_config,
        )

    return ""


def _introspect_schemas(
    *,
    db_hook: DbApiHook | None,
    table_names: list[str] | None,
    datasource_config: DataSourceConfig | None,
) -> str:
    """Build schema context by introspecting tables and/or object-storage sources."""
    parts: list[str] = []
    table_to_columns: dict[str, list[dict[str, str]]] = {}

    if table_names and db_hook is None:
        raise ValueError("table_names requires db_conn_id so table schema can be introspected.")

    if db_hook and table_names:
        bulk_schemas = _try_to_get_bulk_table_schemas(db_hook=db_hook, table_names=table_names)
        if bulk_schemas is not None:
            table_to_columns = bulk_schemas

        for table in table_names:
            columns = table_to_columns.get(table)
            if columns is None:
                columns = db_hook.get_table_schema(table)

            if not columns:
                log.warning("Table %r returned no columns — it may not exist.", table)
                continue

            col_info = ", ".join(f"{c['name']} {c['type']}" for c in columns)
            parts.append(f"Table: {table}\nColumns: {col_info}")

    if not parts and table_names:
        raise ValueError(
            f"None of the requested tables ({table_names}) returned schema information. "
            "Check that the table names are correct and the database connection has access."
        )

    if datasource_config:
        object_storage_schema = _introspect_object_storage_schema(datasource_config)
        parts.append(f"Table: {datasource_config.table_name}\nColumns: {object_storage_schema}")

    return "\n\n".join(parts)


def _try_to_get_bulk_table_schemas(
    *,
    db_hook: DbApiHook,
    table_names: list[str],
) -> dict[str, list[dict[str, str]]] | None:
    """Try a single-call schema fetch when the hook exposes a bulk API."""
    get_table_schemas = getattr(db_hook, "get_table_schemas", None)
    if not callable(get_table_schemas):
        return None

    try:
        raw_result = get_table_schemas(table_names)
    except TypeError:
        return None

    if not isinstance(raw_result, dict):
        log.warning(
            "Ignoring get_table_schemas() result from %s because it is not a dict.",
            type(db_hook).__name__,
        )
        return None

    result: dict[str, list[dict[str, str]]] = {}
    for table in table_names:
        columns = raw_result.get(table)
        if isinstance(columns, list):
            result[table] = columns

    return result


def _introspect_object_storage_schema(datasource_config: DataSourceConfig) -> str:
    """Use DataFusion Engine to get the schema of an object-storage source."""
    try:
        # Lazy load for optional feature degradation — datafusion extra may not be installed
        from airflow.providers.common.sql.datafusion.engine import DataFusionEngine
    except ImportError as e:
        from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

        raise AirflowOptionalProviderFeatureException(e)

    engine = DataFusionEngine()
    engine.register_datasource(datasource_config)
    return engine.get_schema(datasource_config.table_name)
