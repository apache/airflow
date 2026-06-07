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
"""Curated SQL toolset wrapping DbApiHook for agentic database workflows."""

from __future__ import annotations

import json
from contextlib import suppress
from typing import TYPE_CHECKING, Any

try:
    from airflow.providers.common.ai.utils.sql_validation import (
        resolve_sqlglot_dialect,
        validate_sql as _validate_sql,
    )
    from airflow.providers.common.sql.hooks.sql import DbApiHook
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from pydantic_core import SchemaValidator, core_schema

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from pydantic_ai._run_context import RunContext

_PASSTHROUGH_VALIDATOR = SchemaValidator(core_schema.any_schema())

# JSON Schemas for the four SQL tools.
_LIST_TABLES_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {},
}

_GET_SCHEMA_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "table_name": {"type": "string", "description": "Name of the table to inspect."},
    },
    "required": ["table_name"],
}

_QUERY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "sql": {"type": "string", "description": "SQL query to execute."},
    },
    "required": ["sql"],
}

_CHECK_QUERY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "sql": {"type": "string", "description": "SQL query to validate."},
    },
    "required": ["sql"],
}


class SQLToolset(AbstractToolset[Any]):
    """
    Curated toolset that gives an LLM agent safe access to a SQL database.

    Provides four tools — ``list_tables``, ``get_schema``, ``query``, and
    ``check_query`` — inspired by LangChain's ``SQLDatabaseToolkit`` pattern.

    Uses a :class:`~airflow.providers.common.sql.hooks.sql.DbApiHook` resolved
    lazily from the given ``db_conn_id``.

    When a tool fails, the database's error message is returned to the agent as a
    retry (:class:`pydantic_ai.ModelRetry`) so the model can correct its SQL within
    the run instead of failing the task. ``pydantic-ai`` bounds this by the tool's
    ``max_retries``, so an unrecoverable error -- a bad connection or an auth
    failure -- exhausts the retries and fails the task for Airflow to retry. The
    toolset does not inspect the error type or message.

    :param db_conn_id: Airflow connection ID for the database.
    :param allowed_tables: Restrict which tables the agent can discover via
        ``list_tables`` and ``get_schema``. ``None`` (default) exposes all tables
        in ``schema``. Entries may be schema-qualified (``"SCHEMA.TABLE"``) to span
        multiple schemas in one database -- common on warehouses such as Snowflake.
        ``list_tables`` then introspects each referenced schema and returns the
        matching tables fully qualified, and ``get_schema`` routes to the table's
        own schema. Unqualified entries use ``schema``. Matching is
        case-insensitive, since databases reflect identifiers in their own case.

        .. note::
            ``allowed_tables`` controls metadata visibility only. It does **not**
            parse or validate table references in SQL queries. An LLM can still
            query tables outside this list if it guesses the name. For query-level
            restrictions, use database-level permissions (e.g. a read-only role
            with grants limited to specific tables).

    :param schema: Default schema/namespace for table listing and introspection,
        used for unqualified ``allowed_tables`` entries and unqualified
        ``get_schema`` calls. Schema-qualified ``allowed_tables`` entries override
        it per table.
    :param allow_writes: Allow data-modifying SQL (INSERT, UPDATE, DELETE, etc.).
        Default ``False`` — only SELECT-family statements are permitted.
    :param max_rows: Maximum number of rows returned from the ``query`` tool.
        Default ``50``.
    """

    def __init__(
        self,
        db_conn_id: str,
        *,
        allowed_tables: list[str] | None = None,
        schema: str | None = None,
        allow_writes: bool = False,
        max_rows: int = 50,
    ) -> None:
        self._db_conn_id = db_conn_id
        self._allowed_tables: frozenset[str] | None = frozenset(allowed_tables) if allowed_tables else None
        # Case-folded view for membership tests: databases reflect identifiers in
        # their own case (Snowflake stores unquoted names uppercase but reflects
        # them lowercased), so a byte-exact match against the user's entries would
        # silently miss. allowed_tables is a visibility hint, not access control,
        # so case-insensitive matching is safe.
        self._allowed_tables_ci: frozenset[str] | None = (
            frozenset(t.casefold() for t in self._allowed_tables)
            if self._allowed_tables is not None
            else None
        )
        self._schema = schema
        self._allow_writes = allow_writes
        self._max_rows = max_rows
        self._hook: DbApiHook | None = None

        # Derive which schemas to introspect from schema-qualified allowed_tables.
        # Qualified entries ("SCHEMA.TABLE") are listed under their own schema and
        # returned fully qualified; unqualified entries (and allow-all) use the
        # default ``schema``.
        self._qualified_schemas: frozenset[str] = frozenset()
        self._include_default_schema: bool = True
        if self._allowed_tables is not None:
            qualified_schemas: set[str] = set()
            include_default = False
            for entry in self._allowed_tables:
                entry_schema, sep, _ = entry.rpartition(".")
                if sep:
                    qualified_schemas.add(entry_schema)
                else:
                    include_default = True
            self._qualified_schemas = frozenset(qualified_schemas)
            self._include_default_schema = include_default

    def _is_table_allowed(self, name: str) -> bool:
        """Case-insensitive membership test against ``allowed_tables`` (allow-all when unset)."""
        return self._allowed_tables_ci is None or name.casefold() in self._allowed_tables_ci

    @property
    def id(self) -> str:
        return f"sql-{self._db_conn_id}"

    # ------------------------------------------------------------------
    # Lazy hook resolution
    # ------------------------------------------------------------------

    def _get_db_hook(self) -> DbApiHook:
        if self._hook is None:
            connection = BaseHook.get_connection(self._db_conn_id)
            hook = connection.get_hook()
            if not isinstance(hook, DbApiHook):
                raise ValueError(
                    f"Connection {self._db_conn_id!r} does not provide a DbApiHook. "
                    f"Got {type(hook).__name__}."
                )
            self._hook = hook
        return self._hook

    # ------------------------------------------------------------------
    # AbstractToolset interface
    # ------------------------------------------------------------------

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools: dict[str, ToolsetTool[Any]] = {}

        for name, description, schema in (
            ("list_tables", "List available table names in the database.", _LIST_TABLES_SCHEMA),
            ("get_schema", "Get column names and types for a table.", _GET_SCHEMA_SCHEMA),
            ("query", "Execute a SQL query and return rows as JSON.", _QUERY_SCHEMA),
            ("check_query", "Validate SQL syntax without executing it.", _CHECK_QUERY_SCHEMA),
        ):
            # sequential=True because all tools use a shared DbApiHook with
            # synchronous I/O — they must not run concurrently.
            tool_def = ToolDefinition(
                name=name,
                description=description,
                parameters_json_schema=schema,
                sequential=True,
            )
            tools[name] = ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=1,
                args_validator=_PASSTHROUGH_VALIDATOR,
            )
        return tools

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        if name not in ("list_tables", "get_schema", "query", "check_query"):
            raise ValueError(f"Unknown tool: {name!r}")
        try:
            if name == "list_tables":
                return self._list_tables()
            if name == "get_schema":
                return self._get_schema(tool_args["table_name"])
            if name == "query":
                return self._query(tool_args["sql"])
            return self._check_query(tool_args["sql"])
        except Exception as e:
            # Hand the database's own error back to the agent as a retry so it can
            # read the message and fix its SQL within the run. pydantic-ai bounds
            # this by the tool's max_retries, so an unrecoverable error (a bad
            # connection, an auth failure) exhausts the budget and fails the task
            # for Airflow to retry, rather than being silently worked around.
            raise ModelRetry(
                f"The {name} tool failed: {e}\n"
                "Use the list_tables and get_schema tools to inspect the database, "
                "then fix the query and try again."
            ) from e

    # ------------------------------------------------------------------
    # Tool implementations
    # ------------------------------------------------------------------

    def _split_table_identifier(self, table_name: str) -> tuple[str | None, str]:
        """Split ``"SCHEMA.TABLE"`` into ``(schema, table)``; unqualified uses the default schema."""
        schema, sep, table = table_name.rpartition(".")
        if not sep:
            return self._schema, table_name
        return schema, table

    def _list_tables(self) -> str:
        hook = self._get_db_hook()
        tables: list[str] = []
        # Dedupe by (schema, table) so a table reachable both qualified and via the
        # default schema (e.g. "public.users" and "users" with schema="public") is
        # listed once. Case-folded because databases reflect identifiers in their case.
        seen: set[tuple[str | None, str]] = set()

        def add(schema: str | None, name: str, display: str) -> None:
            key = (schema.casefold() if schema else None, name.casefold())
            if self._is_table_allowed(display) and key not in seen:
                seen.add(key)
                tables.append(display)

        # Schemas referenced by qualified allowed_tables entries: introspect each
        # and return matching tables fully qualified so they round-trip to get_schema.
        for schema in sorted(self._qualified_schemas):
            for name in hook.inspector.get_table_names(schema=schema):
                add(schema, name, f"{schema}.{name}")

        # Default schema: used for allow-all and unqualified allowed_tables entries.
        # Names stay bare to preserve the single-schema behaviour.
        if self._include_default_schema:
            for name in hook.inspector.get_table_names(schema=self._schema):
                add(self._schema, name, name)

        return json.dumps(tables)

    def _get_schema(self, table_name: str) -> str:
        if not self._is_table_allowed(table_name):
            return json.dumps({"error": f"Table {table_name!r} is not in the allowed tables list."})
        hook = self._get_db_hook()
        schema, table = self._split_table_identifier(table_name)
        columns = hook.get_table_schema(table, schema=schema)
        return json.dumps(columns)

    def _dialect_for_validation(self) -> str | None:
        """Resolve the hook's sqlglot dialect so DESCRIBE/SHOW validate correctly."""
        hook = self._get_db_hook()
        return resolve_sqlglot_dialect(getattr(hook, "dialect_name", None))

    def _query(self, sql: str) -> str:
        hook = self._get_db_hook()
        if not self._allow_writes:
            # allow_read_only_metadata lets agents inspect schemas with DESCRIBE/SHOW
            # (a common first move) instead of hard-failing; the deep scan still
            # rejects any data-modifying statement, including EXPLAIN <write>.
            _validate_sql(
                sql,
                dialect=self._dialect_for_validation(),
                allow_read_only_metadata=True,
            )

        rows = hook.get_records(sql)
        # Fetch column names from cursor description.
        col_names: list[str] | None = None
        if hook.last_description:
            col_names = [desc[0] for desc in hook.last_description]

        result: list[dict[str, Any]] | list[list[Any]]
        if rows and col_names:
            result = [dict(zip(col_names, row)) for row in rows[: self._max_rows]]
        else:
            result = [list(row) for row in (rows or [])[: self._max_rows]]

        truncated = len(rows or []) > self._max_rows
        output: dict[str, Any] = {"rows": result, "count": len(rows or [])}
        if truncated:
            output["truncated"] = True
            output["max_rows"] = self._max_rows
        return json.dumps(output, default=str)

    def _check_query(self, sql: str) -> str:
        # Resolve the dialect best-effort: if the connection can't be reached we
        # still syntax-check dialect-agnostically rather than reporting invalid.
        dialect: str | None = None
        with suppress(Exception):
            dialect = self._dialect_for_validation()
        try:
            _validate_sql(sql, dialect=dialect, allow_read_only_metadata=True)
            return json.dumps({"valid": True})
        except Exception as e:
            return json.dumps({"valid": False, "error": str(e)})
