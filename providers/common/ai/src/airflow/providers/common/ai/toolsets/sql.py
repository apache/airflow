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
from typing import TYPE_CHECKING, Any

try:
    from airflow.providers.common.ai.utils.sql_validation import validate_sql as _validate_sql
    from airflow.providers.common.sql.hooks.sql import DbApiHook
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

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

    :param db_conn_id: Airflow connection ID for the database.
    :param allowed_tables: Restrict which tables the agent can discover via
        ``list_tables`` and ``get_schema``. ``None`` (default) exposes all tables.

        .. note::
            ``allowed_tables`` controls metadata visibility only. It does **not**
            parse or validate table references in SQL queries. An LLM can still
            query tables outside this list if it guesses the name. For query-level
            restrictions, use database-level permissions (e.g. a read-only role
            with grants limited to specific tables).

    :param schema: Database schema/namespace for table listing and introspection.
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
        self._schema = schema
        self._allow_writes = allow_writes
        self._max_rows = max_rows
        self._hook: DbApiHook | None = None

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
        if name == "list_tables":
            return self._list_tables()
        if name == "get_schema":
            return self._get_schema(tool_args["table_name"])
        if name == "query":
            return self._query(tool_args["sql"])
        if name == "check_query":
            return self._check_query(tool_args["sql"])
        raise ValueError(f"Unknown tool: {name!r}")

    # ------------------------------------------------------------------
    # Tool implementations
    # ------------------------------------------------------------------

    def _list_tables(self) -> str:
        hook = self._get_db_hook()
        tables: list[str] = hook.inspector.get_table_names(schema=self._schema)
        if self._allowed_tables is not None:
            tables = [t for t in tables if t in self._allowed_tables]
        return json.dumps(tables)

    def _get_schema(self, table_name: str) -> str:
        if self._allowed_tables is not None and table_name not in self._allowed_tables:
            return json.dumps({"error": f"Table {table_name!r} is not in the allowed tables list."})
        hook = self._get_db_hook()
        columns = hook.get_table_schema(table_name, schema=self._schema)
        return json.dumps(columns)

    def _query(self, sql: str) -> str:
        if not self._allow_writes:
            _validate_sql(sql)

        hook = self._get_db_hook()
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
        try:
            _validate_sql(sql)
            return json.dumps({"valid": True})
        except Exception as e:
            return json.dumps({"valid": False, "error": str(e)})
