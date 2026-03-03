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
"""Operator for cross-system schema drift detection powered by LLM reasoning."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

try:
    from airflow.providers.common.sql.datafusion.engine import DataFusionEngine
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)


from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from airflow.providers.common.sql.config import DataSourceConfig
    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.sdk import Context


class SchemaMismatch(BaseModel):
    """A single schema mismatch between data sources."""

    source: str = Field(description="Source table")
    target: str = Field(description="Target table")
    column: str = Field(description="Column name where the mismatch was detected")
    source_type: str = Field(description="Data type in the source system")
    target_type: str = Field(description="Data type in the target system")
    severity: str = Field(description="One of: critical, warning, info")
    description: str = Field(description="Human-readable description of the mismatch")
    suggested_action: str = Field(description="Recommended action to resolve the mismatch")
    migration_query: str = Field("Provide migration query")


class SchemaCompareResult(BaseModel):
    """Structured output from schema comparison."""

    compatible: bool = Field(description="Whether the schemas are compatible for data loading")
    mismatches: list[SchemaMismatch] = Field(default_factory=list)
    summary: str = Field(description="High-level summary of the comparison")


class LLMSchemaCompareOperator(LLMOperator):
    """
    Compare schemas across different database systems and detect drift using LLM reasoning.

    The LLM handles complex cross-system type mapping that simple equality checks
    miss (e.g., ``varchar(255)`` vs ``string``, ``timestamp`` vs ``timestamptz``).

    Accepts data sources via two patterns:

    1. **data_sources** — a list of
       :class:`~airflow.providers.common.sql.config.DataSourceConfig` for each
       system. If the connection resolves to a
       :class:`~airflow.providers.common.sql.hooks.sql.DbApiHook`, schema is
       introspected via SQLAlchemy; otherwise DataFusion is used.
    2. **db_conn_ids + table_names** — shorthand for comparing the same table
       across multiple database connections (all must resolve to ``DbApiHook``).

    :param prompt: Instructions for the LLM on what to compare and flag.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"openai:gpt-5"``).
    :param system_prompt: Additional instructions appended to the built-in
        schema comparison prompt.
    :param agent_params: Extra keyword arguments for the pydantic-ai ``Agent``.
    :param data_sources: List of DataSourceConfig objects, one per system.
    :param db_conn_ids: Connection IDs for databases to compare (used with
        ``table_names``).
    :param table_names: Tables to introspect from each ``db_conn_id``.
    :param context_strategy: ``"basic"`` for column names and types only;
        ``"full"`` to include primary keys, foreign keys, and indexes.
        Default ``"full"``.
    :param reasoning_mode: Strongly recommended — cross-system type mapping
    benefits from step-by-step analysis.
    """

    template_fields: Sequence[str] = (
        *LLMOperator.template_fields,
        "db_conn_ids",
        "table_names",
    )

    def __init__(
        self,
        *,
        data_sources: list[DataSourceConfig] | None = None,
        db_conn_ids: list[str] | None = None,
        table_names: list[str] | None = None,
        context_strategy: str = "full",
        reasoning_mode: bool = True,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("output_type", None)
        super().__init__(**kwargs)
        self.data_sources = data_sources or []
        self.db_conn_ids = db_conn_ids or []
        self.table_names = table_names or []
        self.context_strategy = context_strategy
        self.reasoning_mode = reasoning_mode

        if not self.data_sources and not self.db_conn_ids:
            raise ValueError("Provide at least one of 'data_sources' or 'db_conn_ids'.")

        if self.db_conn_ids and not self.table_names:
            raise ValueError("'table_names' is required when using 'db_conn_ids'.")

    @staticmethod
    def _get_db_hook(conn_id: str) -> DbApiHook:
        """Resolve a connection ID to a DbApiHook."""
        from airflow.providers.common.sql.hooks.sql import DbApiHook

        connection = BaseHook.get_connection(conn_id)
        hook = connection.get_hook()
        if not isinstance(hook, DbApiHook):
            raise ValueError(
                f"Connection {conn_id!r} does not provide a DbApiHook. Got {type(hook).__name__}."
            )
        return hook

    @staticmethod
    def _is_dbapi_connection(conn_id: str) -> bool:
        """Check whether a connection resolves to a DbApiHook."""
        from airflow.providers.common.sql.hooks.sql import DbApiHook

        try:
            connection = BaseHook.get_connection(conn_id)
            hook = connection.get_hook()
            return isinstance(hook, DbApiHook)
        except Exception:
            return False

    @cached_property
    def _db_hooks(self) -> dict[str, DbApiHook]:
        """Cache DbApiHook instances keyed by connection ID."""
        hooks: dict[str, DbApiHook] = {}
        for conn_id in self.db_conn_ids:
            hooks[conn_id] = self._get_db_hook(conn_id)
        return hooks

    def _introspect_db_schema(self, hook: DbApiHook, table_name: str) -> str:
        """Introspect schema from a database connection via DbApiHook."""
        columns = hook.get_table_schema(table_name)
        if not columns:
            self.log.warning("Table %r returned no columns — it may not exist.", table_name)
            return ""

        col_info = ", ".join(f"{c['name']} {c['type']}" for c in columns)
        parts = [f"Columns: {col_info}"]

        if self.context_strategy == "full":
            try:
                pks = hook.dialect.get_primary_keys(table_name)
                if pks:
                    parts.append(f"Primary Key: {', '.join(pks)}")
            except Exception:
                self.log.debug("Could not retrieve PK for %r", table_name)

            try:
                fks = hook.inspector.get_foreign_keys(table_name)
                for fk in fks:
                    cols = ", ".join(fk.get("constrained_columns", []))
                    ref = fk.get("referred_table", "?")
                    ref_cols = ", ".join(fk.get("referred_columns", []))
                    parts.append(f"Foreign Key: ({cols}) -> {ref}({ref_cols})")
            except Exception:
                self.log.debug("Could not retrieve FKs for %r", table_name)

            try:
                indexes = hook.inspector.get_indexes(table_name)
                for idx in indexes:
                    column_names = [c for c in idx.get("column_names", []) if c is not None]
                    idx_cols = ", ".join(c for c in column_names)
                    unique = " UNIQUE" if idx.get("unique") else ""
                    parts.append(f"Index{unique}: {idx.get('name', '?')} ({idx_cols})")
            except Exception:
                self.log.debug("Could not retrieve indexes for %r", table_name)

        return "\n".join(parts)

    def _introspect_datasource_schema(self, ds_config: DataSourceConfig) -> str:
        """Introspect schema from a DataSourceConfig, choosing DbApiHook or DataFusion."""
        if self._is_dbapi_connection(ds_config.conn_id):
            hook = self._get_db_hook(ds_config.conn_id)
            dialect_name = getattr(hook, "dialect_name", "unknown")
            schema_text = self._introspect_db_schema(hook, ds_config.table_name)
            return (
                f"Source: {ds_config.conn_id} ({dialect_name})\nTable: {ds_config.table_name}\n{schema_text}"
            )

        engine = DataFusionEngine()
        engine.register_datasource(ds_config)
        schema_text = engine.get_schema(ds_config.table_name)

        return f"Source: {ds_config.conn_id} \nFormat: ({ds_config.format})\nTable: {ds_config.table_name}\nColumns: {schema_text}"

    def _build_schema_context(self) -> str:
        """Collect schemas from all configured sources each clearly."""
        sections: list[str] = []

        for conn_id in self.db_conn_ids:
            hook = self._get_db_hook(conn_id)
            dialect_name = getattr(hook, "dialect_name", "unknown")
            for table in self.table_names:
                schema_text = self._introspect_db_schema(hook, table)
                if schema_text:
                    sections.append(f"Source: {conn_id} ({dialect_name})\nTable: {table}\n{schema_text}")

        for ds_config in self.data_sources:
            sections.append(self._introspect_datasource_schema(ds_config))

        if not sections:
            raise ValueError(
                "No schema information could be retrieved from any of the configured sources. "
                "Check that connection IDs, table names, and data source configs are correct."
            )

        return "\n\n".join(sections)

    def _build_system_prompt(self, schema_context: str) -> str:
        """Construct the system prompt for cross-system schema comparison."""
        prompt = ""
        if self.reasoning_mode:
            prompt = prompt + (
                "Consider cross-system type equivalences:\n"
                "- varchar(n) / text / string / TEXT may be compatible\n"
                "- int / integer / int4 / INT32 are equivalent\n"
                "- bigint / int8 / int64 / BIGINT are equivalent\n"
                "- timestamp / timestamptz / TIMESTAMP_NTZ / datetime may differ in timezone handling\n"
                "- numeric(p,s) / decimal(p,s) / NUMBER — check precision and scale\n"
                "- boolean / bool / BOOLEAN / tinyint(1) — check semantic equivalence\n\n"
                "Severity levels:\n"
                "- critical: Will cause data loading failures or data loss "
                "(e.g., column missing in target, incompatible types)\n"
                "- warning: May cause data quality issues "
                "(e.g., precision loss, timezone mismatch)\n"
                "- info: Cosmetic differences that won't affect data loading "
                "(e.g., varchar length differences within safe range)\n\n"
            )

        prompt = prompt + (
            "You are a database schema comparison expert. "
            "You understand type systems across PostgreSQL, MySQL, Snowflake, BigQuery, "
            "Redshift, S3 Parquet/CSV, Iceberg, and other data systems.\n\n"
            "Analyze the schemas from the following data sources and identify mismatches "
            "that could break data loading, cause data loss, or produce unexpected behavior.\n\n"
            f"{prompt}"
            f"Schemas to compare:\n\n{schema_context}\n"
        )
        if self.system_prompt:
            prompt += f"\nAdditional instructions:\n{self.system_prompt}\n"
        return prompt

    def execute(self, context: Context) -> dict[str, Any]:
        schema_context = self._build_schema_context()

        self.log.info("Schema comparison context:\n%s", schema_context)

        full_system_prompt = self._build_system_prompt(schema_context)

        agent = self.llm_hook.create_agent(
            output_type=SchemaCompareResult,
            instructions=full_system_prompt,
            **self.agent_params,
        )
        self.log.info("Running LLM schema comparison...")
        result = agent.run_sync(self.prompt)
        self.log.info("LLM schema comparison complete.")
        return result.output.model_dump()
