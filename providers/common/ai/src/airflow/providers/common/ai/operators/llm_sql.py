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
"""Operator for generating SQL queries from natural language using LLMs."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

try:
    from airflow.providers.common.ai.utils.sql_validation import (
        DEFAULT_ALLOWED_TYPES,
        validate_sql as _validate_sql,
    )
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from sqlglot import exp

    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.sdk import Context

# SQLAlchemy dialect_name → sqlglot dialect mapping for names that differ.
_SQLALCHEMY_TO_SQLGLOT_DIALECT: dict[str, str] = {
    "postgresql": "postgres",
    "mssql": "tsql",
}


class LLMSQLQueryOperator(LLMOperator):
    """
    Generate SQL queries from natural language using an LLM.

    Inherits from :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`
    for LLM access and optionally uses a
    :class:`~airflow.providers.common.sql.hooks.sql.DbApiHook`
    for schema introspection. The operator generates SQL but does not execute it —
    the generated SQL is returned as XCom and can be passed to
    ``SQLExecuteQueryOperator`` or used in downstream tasks.

    When ``system_prompt`` is provided, it is appended to the built-in SQL safety
    instructions — use it for domain-specific guidance (e.g. "prefer CTEs over
    subqueries", "always use LEFT JOINs").

    :param prompt: Natural language description of the desired query.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"openai:gpt-4o"``).
        Overrides the model stored in the connection's extra field.
    :param system_prompt: Additional instructions appended to the built-in SQL
        safety prompt. Use for domain-specific guidance.
    :param agent_params: Additional keyword arguments passed to the pydantic-ai
        ``Agent`` constructor (e.g. ``retries``, ``model_settings``).
    :param db_conn_id: Connection ID for database schema introspection.
        The connection must resolve to a ``DbApiHook``.
    :param table_names: Tables to include in the LLM's schema context.
        Used with ``db_conn_id`` for automatic introspection.
    :param schema_context: Manual schema context string. When provided,
        this is used instead of ``db_conn_id`` introspection.
    :param validate_sql: Whether to validate generated SQL via AST parsing.
        Default ``True`` (safe by default).
    :param allowed_sql_types: SQL statement types to allow.
        Default: ``(Select, Union, Intersect, Except)``.
    :param dialect: SQL dialect for parsing (``postgres``, ``mysql``, etc.).
        Auto-detected from the database hook if not set.
    """

    template_fields: Sequence[str] = (
        *LLMOperator.template_fields,
        "db_conn_id",
        "table_names",
        "schema_context",
    )

    def __init__(
        self,
        *,
        db_conn_id: str | None = None,
        table_names: list[str] | None = None,
        schema_context: str | None = None,
        validate_sql: bool = True,
        allowed_sql_types: tuple[type[exp.Expression], ...] = DEFAULT_ALLOWED_TYPES,
        dialect: str | None = None,
        **kwargs: Any,
    ) -> None:
        kwargs.pop("output_type", None)  # SQL operator always returns str
        super().__init__(**kwargs)
        self.db_conn_id = db_conn_id
        self.table_names = table_names
        self.schema_context = schema_context
        self.validate_sql = validate_sql
        self.allowed_sql_types = allowed_sql_types
        self.dialect = dialect

    @cached_property
    def db_hook(self) -> DbApiHook | None:
        """Return DbApiHook for the configured database connection, or None."""
        if not self.db_conn_id:
            return None
        from airflow.providers.common.sql.hooks.sql import DbApiHook

        connection = BaseHook.get_connection(self.db_conn_id)
        hook = connection.get_hook()
        if not isinstance(hook, DbApiHook):
            raise ValueError(
                f"Connection {self.db_conn_id!r} does not provide a DbApiHook. Got {type(hook).__name__}."
            )
        return hook

    def execute(self, context: Context) -> str:
        schema_info = self._get_schema_context()
        full_system_prompt = self._build_system_prompt(schema_info)

        agent = self.llm_hook.create_agent(
            output_type=str, instructions=full_system_prompt, **self.agent_params
        )
        result = agent.run_sync(self.prompt)
        sql = self._strip_llm_output(result.output)

        if self.validate_sql:
            _validate_sql(sql, allowed_types=self.allowed_sql_types, dialect=self._resolved_dialect)

        self.log.info("Generated SQL:\n%s", sql)
        return sql

    @staticmethod
    def _strip_llm_output(raw: str) -> str:
        """Strip whitespace and markdown code fences from LLM output."""
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove opening fence (```sql, ```, etc.) and closing fence
            if len(lines) >= 2:
                end = -1 if lines[-1].strip().startswith("```") else len(lines)
                text = "\n".join(lines[1:end]).strip()
        return text

    def _get_schema_context(self) -> str:
        """Return schema context from manual override or database introspection."""
        if self.schema_context:
            return self.schema_context
        if self.db_hook and self.table_names:
            return self._introspect_schemas()
        return ""

    def _introspect_schemas(self) -> str:
        """Build schema context by introspecting tables via the database hook."""
        parts: list[str] = []
        for table in self.table_names or []:
            columns = self.db_hook.get_table_schema(table)  # type: ignore[union-attr]
            if not columns:
                self.log.warning("Table %r returned no columns — it may not exist.", table)
                continue
            col_info = ", ".join(f"{c['name']} {c['type']}" for c in columns)
            parts.append(f"Table: {table}\nColumns: {col_info}")
        if not parts and self.table_names:
            raise ValueError(
                f"None of the requested tables ({self.table_names}) returned schema information. "
                "Check that the table names are correct and the database connection has access."
            )
        return "\n\n".join(parts)

    def _build_system_prompt(self, schema_info: str) -> str:
        """Construct the system prompt for the LLM."""
        dialect_label = self._resolved_dialect or "SQL"
        prompt = (
            f"You are a {dialect_label} expert. "
            "Generate a single SQL query based on the user's request.\n"
            "Return ONLY the SQL query, no explanation or markdown.\n"
        )
        if schema_info:
            prompt += f"\nAvailable schema:\n{schema_info}\n"
        prompt += (
            "\nRules:\n"
            "- Generate only SELECT queries (including CTEs, JOINs, subqueries, UNION)\n"
            "- Never generate data modification statements "
            "(INSERT, UPDATE, DELETE, DROP, etc.)\n"
            "- Use proper syntax for the specified dialect\n"
        )
        if self.system_prompt:
            prompt += f"\nAdditional instructions:\n{self.system_prompt}\n"
        return prompt

    @cached_property
    def _resolved_dialect(self) -> str | None:
        """
        Resolve the SQL dialect from explicit parameter or database hook.

        Normalizes SQLAlchemy dialect names to sqlglot equivalents
        (e.g. ``postgresql`` → ``postgres``).
        """
        raw = self.dialect
        if not raw and self.db_hook and hasattr(self.db_hook, "dialect_name"):
            raw = self.db_hook.dialect_name
        if raw:
            return _SQLALCHEMY_TO_SQLGLOT_DIALECT.get(raw, raw)
        return None
