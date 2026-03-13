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
"""Curated SQL toolset wrapping DataFusionEngine for agentic object-store workflows."""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any

try:
    from airflow.providers.common.ai.utils.sql_validation import SQLSafetyError, validate_sql as _validate_sql
    from airflow.providers.common.sql.datafusion.engine import DataFusionEngine
    from airflow.providers.common.sql.datafusion.exceptions import QueryExecutionException
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from pydantic_core import SchemaValidator, core_schema

if TYPE_CHECKING:
    from pydantic_ai._run_context import RunContext

    from airflow.providers.common.sql.config import DataSourceConfig

log = logging.getLogger(__name__)

_PASSTHROUGH_VALIDATOR = SchemaValidator(core_schema.any_schema())

# JSON Schemas for the three DataFusion tools.
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

# DataFusion python bindings don't expose any native exception types, it uses rust exceptions.
# So we have to rely on error message parsing with regex.
_RETRYABLE_IDENTIFIER = r"""(?:['"][^'"]+['"]|\w+)"""
_RETRYABLE_QUERY_ERROR_PATTERNS = (
    re.compile(rf"""column\s+{_RETRYABLE_IDENTIFIER}\s+not\s+found""", re.IGNORECASE),
    re.compile(rf"""table\s+{_RETRYABLE_IDENTIFIER}\s+not\s+found""", re.IGNORECASE),
)


class DataFusionToolset(AbstractToolset[Any]):
    """
    Curated toolset that gives an LLM agent SQL access to object-storage data via Apache DataFusion.

    Provides three tools — ``list_tables``, ``get_schema``, and ``query`` —
    backed by
    :class:`~airflow.providers.common.sql.datafusion.engine.DataFusionEngine`.

    Each :class:`~airflow.providers.common.sql.config.DataSourceConfig` entry
    registers a table backed by Parquet, CSV, Avro, or Iceberg data on S3 or
    local storage. Multiple configs can be registered so that SQL queries can
    join across tables.

    Requires the ``datafusion`` extra of ``apache-airflow-providers-common-sql``.

    :param datasource_configs: One or more DataFusion data-source configurations.
    :param allow_writes: Allow data-modifying SQL (CREATE TABLE, CREATE VIEW,
        INSERT INTO, etc.). Default ``False`` — only SELECT-family statements
        are permitted.
    :param max_rows: Maximum number of rows returned from the ``query`` tool.
        Default ``50``.
    """

    def __init__(
        self,
        datasource_configs: list[DataSourceConfig],
        *,
        allow_writes: bool = False,
        max_rows: int = 50,
    ) -> None:
        if not datasource_configs:
            raise ValueError("datasource_configs must contain at least one DataSourceConfig")
        self._datasource_configs = datasource_configs
        self._allow_writes = allow_writes
        self._max_rows = max_rows
        self._engine: DataFusionEngine | None = None

    @property
    def id(self) -> str:
        suffix = "_".join(config.table_name.replace("-", "_") for config in self._datasource_configs)
        return f"sql_datafusion_{suffix}"

    def _get_engine(self) -> DataFusionEngine:
        """Lazily create and configure a DataFusionEngine from *datasource_configs*."""
        if self._engine is None:
            engine = DataFusionEngine()
            for config in self._datasource_configs:
                engine.register_datasource(config)
            self._engine = engine
        return self._engine

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools: dict[str, ToolsetTool[Any]] = {}

        for name, description, schema in (
            ("list_tables", "List available table names.", _LIST_TABLES_SCHEMA),
            ("get_schema", "Get column names and types for a table.", _GET_SCHEMA_SCHEMA),
            ("query", "Execute a SQL query and return rows as JSON.", _QUERY_SCHEMA),
        ):
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
        raise ValueError(f"Unknown tool: {name!r}")

    def _list_tables(self) -> str:
        try:
            engine = self._get_engine()
            tables: list[str] = list(engine.session_context.catalog().schema().table_names())
            return json.dumps(tables)
        except Exception as ex:
            log.warning("list_tables failed: %s", ex)
            return json.dumps({"error": str(ex)})

    def _get_schema(self, table_name: str) -> str:
        engine = self._get_engine()
        # session_context lookup is required here instead of engine.registered_tables,
        # because registered_tables only tracks tables registered via datasource config.
        # When allow_writes is enabled, the agent may create temporary in-memory tables
        # that would not be captured there.
        if not engine.session_context.table_exist(table_name):
            return json.dumps({"error": f"Table {table_name!r} is not available"})
        # Intentionally using session_context instead of engine.get_schema() —
        # the latter returns a pre-formatted string intended for other operators,
        # not a JSON-compatible format.
        # TODO: refactor engine.get_schema() to return JSON and update this accordingly
        table = engine.session_context.table(table_name)
        columns = [{"name": f.name, "type": str(f.type)} for f in table.schema()]
        return json.dumps(columns)

    def _query(self, sql: str) -> str:
        try:
            if not self._allow_writes:
                _validate_sql(sql)

            engine = self._get_engine()
            pydict = engine.execute_query(sql)
            col_names = list(pydict.keys())
            num_rows = len(next(iter(pydict.values()), []))

            result: list[dict[str, Any]] = [
                {col: pydict[col][i] for col in col_names} for i in range(min(num_rows, self._max_rows))
            ]

            truncated = num_rows > self._max_rows
            output: dict[str, Any] = {"rows": result, "count": num_rows}
            if truncated:
                output["truncated"] = True
                output["max_rows"] = self._max_rows
            return json.dumps(output, default=str)
        except SQLSafetyError as ex:
            log.warning("query failed SQL safety validation: %s", ex)
            raise
        except QueryExecutionException as ex:
            if self._is_retryable_query_error(ex):
                raise ModelRetry(
                    f"error: {ex!s}, Use get_schema and list_tables tools for more details."
                ) from ex
            return json.dumps({"error": str(ex), "query": sql})

    @staticmethod
    def _is_retryable_query_error(error: QueryExecutionException) -> bool:
        message = str(error)
        return any(pattern.search(message) for pattern in _RETRYABLE_QUERY_ERROR_PATTERNS)
