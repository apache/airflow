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
        SQLSafetyError,
        collect_table_references,
        parse_sql as _parse_sql,
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

from airflow.providers.common.ai.utils.tool_definition import return_schema_kwargs
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
    :param allowed_tables: Restrict the agent to a fixed set of tables. ``None``
        (default) exposes every table in ``schema``. Entries may be schema-qualified
        (``"SCHEMA.TABLE"``) to span multiple schemas in one database -- common on
        warehouses such as Snowflake. ``list_tables`` introspects each referenced
        schema and returns the matching tables fully qualified, and ``get_schema``
        routes to the table's own schema. Unqualified entries use ``schema``.
        Matching is case-insensitive, since databases reflect identifiers in their
        own case.

        When set, the list is enforced on the ``query`` and ``check_query`` tools as
        well as on discovery: every table a query reaches -- through subqueries, CTEs,
        JOINs, set operations, ``DESCRIBE``, catalog views such as
        ``information_schema``, or DML -- must be on the list, resolved with its
        database/catalog, or the query is rejected before it runs. CTE references are
        excluded by lexical scope (a same-named CTE in another scope never hides a real
        table). Constructs the list cannot describe are rejected outright while it is
        active: table-valued functions (``dblink``), ``TABLE('name')`` row sources, the
        ``TABLE <name>`` shorthand, ``SHOW``, dynamic SQL, and **inline comments**
        (where parser-vs-engine differences such as MySQL ``/*! ... */`` executable
        comments hide).

        .. note::
            This is an application-level guardrail, enforced by parsing the SQL with
            sqlglot. It is strong defense-in-depth but not a substitute for database
            permissions: it cannot police data reached through a function whose
            argument is itself SQL or a path -- ``pg_read_file('...')`` (a file) or
            ``query_to_xml('SELECT ... FROM other_table', ...)`` and ``dblink`` in
            scalar position (a table, read through a string the parser cannot inspect)
            -- and any query the engine parses differently from sqlglot is a residual
            gap. For a hard guarantee, also point ``db_conn_id`` at a least-privilege
            role whose ``SELECT`` grants are limited to the same tables.

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
        self._schema = schema
        self._allow_writes = allow_writes
        self._max_rows = max_rows
        self._hook: DbApiHook | None = None

        # Canonical ``(catalog, schema, table)`` view of allowed_tables for membership
        # tests, plus the schemas to introspect. Built once: every reference -- a
        # discovery hit, a get_schema arg, or a table parsed out of a query -- is
        # normalised to the same shape and matched against this set.
        #
        # Identifiers are case-folded: databases reflect them in their own case
        # (Snowflake stores unquoted names uppercase but reflects them lowercased), so
        # a byte-exact match against the user's entries would silently miss. Unqualified
        # entries resolve to the default ``schema`` (``None`` when unset) so that
        # ``"orders"`` and ``"<schema>.orders"`` denote the same table. Allow-list
        # entries carry no catalog, so any catalog-qualified reference
        # (``otherdb.public.orders``) has a non-null catalog in its key and cannot match
        # -- that closes cross-database access the single-connection allow-list can't
        # describe.
        self._allowed_canonical: frozenset[tuple[str | None, str | None, str]] | None = None
        # Qualified entries ("SCHEMA.TABLE") are listed under their own schema and
        # returned fully qualified; unqualified entries (and allow-all) use the
        # default ``schema``.
        self._qualified_schemas: frozenset[str] = frozenset()
        self._include_default_schema: bool = True
        if self._allowed_tables is not None:
            canonical: set[tuple[str | None, str | None, str]] = set()
            qualified_schemas: set[str] = set()
            include_default = False
            for entry in self._allowed_tables:
                entry_schema, sep, table = entry.rpartition(".")
                if sep:
                    qualified_schemas.add(entry_schema)
                    canonical.add(self._canonical_ref("", entry_schema, table))
                else:
                    include_default = True
                    canonical.add(self._canonical_ref("", self._schema, entry))
            self._allowed_canonical = frozenset(canonical)
            self._qualified_schemas = frozenset(qualified_schemas)
            self._include_default_schema = include_default

    @staticmethod
    def _canonical_ref(
        catalog: str | None, schema: str | None, table: str
    ) -> tuple[str | None, str | None, str]:
        """Normalise a ``(catalog, schema, table)`` reference to its case-folded comparison key."""
        return (
            catalog.casefold() if catalog else None,
            schema.casefold() if schema else None,
            table.casefold(),
        )

    def _is_ref_allowed(self, catalog: str | None, schema: str | None, table: str) -> bool:
        """Membership test for a resolved ``(catalog, schema, table)`` reference (allow-all when unset)."""
        if self._allowed_canonical is None:
            return True
        return self._canonical_ref(catalog, schema, table) in self._allowed_canonical

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
            # return_schema is "string": every tool returns a JSON-encoded string
            # (json.dumps), so code mode renders `-> str` instead of `-> Any`.
            tool_def = ToolDefinition(
                name=name,
                description=description,
                parameters_json_schema=schema,
                sequential=True,
                **return_schema_kwargs({"type": "string"}),
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
        seen: set[tuple[str | None, str | None, str]] = set()

        def add(schema: str | None, name: str, display: str) -> None:
            key = self._canonical_ref("", schema, name)
            if self._is_ref_allowed("", schema, name) and key not in seen:
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
        schema, table = self._split_table_identifier(table_name)
        if not self._is_ref_allowed("", schema, table):
            return json.dumps({"error": f"Table {table_name!r} is not in the allowed tables list."})
        hook = self._get_db_hook()
        columns = hook.get_table_schema(table, schema=schema)
        return json.dumps(columns)

    def _dialect_for_validation(self) -> str | None:
        """Resolve the hook's sqlglot dialect so DESCRIBE/SHOW validate correctly."""
        hook = self._get_db_hook()
        return resolve_sqlglot_dialect(getattr(hook, "dialect_name", None))

    def _query(self, sql: str) -> str:
        hook = self._get_db_hook()
        dialect = self._dialect_for_validation()
        statements: list[Any] | None = None
        if not self._allow_writes:
            # allow_read_only_metadata lets agents inspect schemas with DESCRIBE/SHOW
            # (a common first move) instead of hard-failing; the deep scan still
            # rejects any data-modifying statement, including EXPLAIN <write>.
            statements = _validate_sql(sql, dialect=dialect, allow_read_only_metadata=True)
        elif self._allowed_canonical is not None:
            # Writes are allowed but tables are restricted: parse anyway so the
            # allow-list still governs which tables a write may touch.
            statements = _parse_sql(sql, dialect=dialect)
        if statements is not None:
            self._enforce_allowed_tables(statements)

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
            statements = _validate_sql(sql, dialect=dialect, allow_read_only_metadata=True)
            self._enforce_allowed_tables(statements)
            return json.dumps({"valid": True})
        except Exception as e:
            return json.dumps({"valid": False, "error": str(e)})

    def _enforce_allowed_tables(self, statements: list[Any]) -> None:
        """
        Reject a parsed query that reaches any table outside ``allowed_tables``.

        No-op when ``allowed_tables`` is unset (allow-all). Otherwise every table the
        query references (resolved scope-correctly, including catalog) must be on the
        list, and any construct the list cannot describe -- a table-valued function,
        ``SHOW``, dynamic SQL, an inline comment, or the ``TABLE <name>`` shorthand --
        is refused. Raises :class:`SQLSafetyError` -- ``call_tool`` turns it into a
        ``ModelRetry`` so the agent can re-target an allowed table, while
        ``check_query`` reports it invalid.
        """
        if self._allowed_canonical is None:
            return
        scan = collect_table_references(statements)
        if scan.unverifiable_sources:
            raise SQLSafetyError(
                f"Query uses a data source that cannot be checked against allowed_tables: "
                f"{'; '.join(scan.unverifiable_sources)}. Query the allowed tables directly: "
                f"use list_tables to see them."
            )
        disallowed = [
            ".".join(part for part in (catalog, schema, table) if part)
            for catalog, schema, table in scan.tables
            if not self._is_ref_allowed(catalog, schema or self._schema, table)
        ]
        if disallowed:
            raise SQLSafetyError(
                f"Query references tables that are not in the allowed tables list: "
                f"{', '.join(sorted(set(disallowed)))}. Use list_tables to see the allowed tables."
            )
