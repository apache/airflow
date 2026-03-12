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

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai._run_context import RunContext
from pydantic_ai.toolsets.abstract import ToolsetTool

from airflow.providers.common.ai.toolsets.datafusion import DataFusionToolset
from airflow.providers.common.ai.utils.sql_validation import SQLSafetyError
from airflow.providers.common.sql.config import DataSourceConfig


def _make_mock_datasource_config(table_name: str = "sales_data"):
    """Create a mock DataSourceConfig."""

    mock = MagicMock(spec=DataSourceConfig)
    mock.table_name = table_name
    return mock


def _make_mock_engine(
    registered_tables: dict[str, str] | None = None,
    schema_fields: list[tuple[str, str]] | None = None,
    query_result: dict[str, list] | None = None,
):
    """Create a mock DataFusionEngine with sensible defaults."""
    mock = MagicMock()
    tables = registered_tables or {"sales_data": "s3://bucket/sales/"}
    mock.registered_tables = tables
    mock.session_context.catalog().schema().table_names.return_value = list(tables.keys())
    mock.session_context.table_exist.side_effect = lambda name: name in tables

    fields = schema_fields or [("id", "Int64"), ("amount", "Float64")]
    arrow_fields = []
    for name, ftype in fields:
        field = MagicMock()
        field.name = name
        field.type = ftype
        arrow_fields.append(field)
    for tname in tables:
        mock.session_context.table(tname).schema.return_value = arrow_fields

    mock.execute_query.return_value = (
        query_result
        if query_result is not None
        else {
            "id": [1, 2],
            "amount": [10.5, 20.0],
        }
    )
    return mock


class TestDataFusionToolsetInit:
    def test_id_includes_table_names(self):
        cfg_a = _make_mock_datasource_config("alpha")
        cfg_b = _make_mock_datasource_config("beta")
        ts = DataFusionToolset([cfg_b, cfg_a])
        assert ts.id == "sql_datafusion_beta_alpha"

    def test_single_table_id(self):
        cfg = _make_mock_datasource_config("orders")
        ts = DataFusionToolset([cfg])
        assert ts.id == "sql_datafusion_orders"

    def test_requires_datasource_configs(self):
        with pytest.raises(ValueError, match="datasource_configs must contain at least one DataSourceConfig"):
            DataFusionToolset([])


class TestDataFusionToolsetGetTools:
    def test_returns_three_tools(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock(spec=RunContext)))
        assert set(tools.keys()) == {"list_tables", "get_schema", "query"}

    def test_tool_definitions_have_descriptions(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock(spec=RunContext)))
        for tool in tools.values():
            assert tool.tool_def.description


class TestDataFusionToolsetListTables:
    def test_returns_registered_tables(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        ts._engine = _make_mock_engine(
            registered_tables={"sales": "s3://bucket/sales/", "orders": "s3://bucket/orders/"}
        )

        result = asyncio.run(
            ts.call_tool("list_tables", {}, ctx=MagicMock(spec=RunContext), tool=MagicMock(spec=ToolsetTool))
        )
        tables = json.loads(result)
        assert set(tables) == {"sales", "orders"}


class TestDataFusionToolsetGetSchema:
    def test_returns_column_info(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        ts._engine = _make_mock_engine(
            schema_fields=[("id", "Int64"), ("amount", "Float64"), ("name", "Utf8")]
        )

        result = asyncio.run(
            ts.call_tool(
                "get_schema",
                {"table_name": "sales_data"},
                ctx=MagicMock(spec=RunContext),
                tool=MagicMock(spec=ToolsetTool),
            )
        )
        columns = json.loads(result)
        assert columns == [
            {"name": "id", "type": "Int64"},
            {"name": "amount", "type": "Float64"},
            {"name": "name", "type": "Utf8"},
        ]


class TestDataFusionToolsetQuery:
    def test_returns_rows_as_json(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        ts._engine = _make_mock_engine(query_result={"id": [1, 2], "amount": [10.5, 20.0]})

        result = asyncio.run(
            ts.call_tool(
                "query",
                {"sql": "SELECT id, amount FROM sales_data"},
                ctx=MagicMock(spec=RunContext),
                tool=MagicMock(spec=ToolsetTool),
            )
        )
        data = json.loads(result)
        assert data["rows"] == [{"id": 1, "amount": 10.5}, {"id": 2, "amount": 20.0}]
        assert data["count"] == 2

    def test_truncates_at_max_rows(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg], max_rows=1)
        ts._engine = _make_mock_engine(query_result={"id": [1, 2, 3], "name": ["a", "b", "c"]})

        result = asyncio.run(
            ts.call_tool(
                "query",
                {"sql": "SELECT id, name FROM sales_data"},
                ctx=MagicMock(spec=RunContext),
                tool=MagicMock(spec=ToolsetTool),
            )
        )
        data = json.loads(result)
        assert len(data["rows"]) == 1
        assert data["truncated"] is True
        assert data["count"] == 3

    def test_handles_empty_result(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        ts._engine = _make_mock_engine(query_result={})

        result = asyncio.run(
            ts.call_tool(
                "query",
                {"sql": "SELECT * FROM sales_data WHERE 1=0"},
                ctx=MagicMock(spec=RunContext),
                tool=MagicMock(spec=ToolsetTool),
            )
        )
        data = json.loads(result)
        assert data["rows"] == []
        assert data["count"] == 0

    def test_blocks_create_table_by_default(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        ts._engine = _make_mock_engine()

        with pytest.raises(SQLSafetyError, match="Statement type 'Create' is not allowed"):
            asyncio.run(
                ts.call_tool(
                    "query",
                    {"sql": "CREATE TABLE new_table (id INT, name TEXT)"},
                    ctx=MagicMock(spec=RunContext),
                    tool=MagicMock(spec=ToolsetTool),
                )
            )

    def test_allows_create_table_when_writes_enabled(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg], allow_writes=True)
        ts._engine = _make_mock_engine(query_result={})

        result = asyncio.run(
            ts.call_tool(
                "query",
                {"sql": "CREATE TABLE new_table (id INT, name TEXT)"},
                ctx=MagicMock(spec=RunContext),
                tool=MagicMock(spec=ToolsetTool),
            )
        )
        data = json.loads(result)
        assert "error" not in data

    def test_unknown_tool_raises(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])

        with pytest.raises(ValueError, match="Unknown tool"):
            asyncio.run(
                ts.call_tool("bad_tool", {}, ctx=MagicMock(spec=RunContext), tool=MagicMock(spec=ToolsetTool))
            )


class TestDataFusionToolsetGetSchemaErrors:
    def test_unknown_table_returns_error_json(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        ts._engine = _make_mock_engine(registered_tables={"sales_data": "s3://bucket/sales/"})

        result = asyncio.run(
            ts.call_tool(
                "get_schema",
                {"table_name": "nonexistent"},
                ctx=MagicMock(spec=RunContext),
                tool=MagicMock(spec=ToolsetTool),
            )
        )
        data = json.loads(result)
        assert "error" in data
        assert "nonexistent" in data["error"]

    def test_missing_table_name_arg_raises_key_error(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        ts._engine = _make_mock_engine()

        with pytest.raises(KeyError):
            asyncio.run(
                ts.call_tool(
                    "get_schema", {}, ctx=MagicMock(spec=RunContext), tool=MagicMock(spec=ToolsetTool)
                )
            )


class TestDataFusionToolsetQueryErrors:
    def test_query_execution_exception_returns_error_json(self):
        from airflow.providers.common.sql.datafusion.exceptions import QueryExecutionException

        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        engine = _make_mock_engine()
        engine.execute_query.side_effect = QueryExecutionException("execution failed: column x not found")
        ts._engine = engine

        result = asyncio.run(
            ts.call_tool(
                "query",
                {"sql": "SELECT x FROM t"},
                ctx=MagicMock(spec=RunContext),
                tool=MagicMock(spec=ToolsetTool),
            )
        )
        data = json.loads(result)
        assert "column x not found" in data["error"]
        assert data["query"] == "SELECT x FROM t"

    def test_unexpected_exception_propagates(self):
        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        engine = _make_mock_engine()
        engine.execute_query.side_effect = TypeError("unexpected error")
        ts._engine = engine

        with pytest.raises(TypeError, match="unexpected error"):
            asyncio.run(
                ts.call_tool(
                    "query",
                    {"sql": "SELECT 1"},
                    ctx=MagicMock(spec=RunContext),
                    tool=MagicMock(spec=ToolsetTool),
                )
            )


class TestDataFusionToolsetEngineResolution:
    @patch("airflow.providers.common.ai.toolsets.datafusion.DataFusionEngine", autospec=True)
    def test_lazy_creates_engine(self, MockEngine):
        mock_engine_instance = MagicMock()
        MockEngine.return_value = mock_engine_instance

        cfg = _make_mock_datasource_config("my_table")
        ts = DataFusionToolset([cfg])
        engine = ts._get_engine()

        assert engine is mock_engine_instance
        MockEngine.assert_called_once()
        mock_engine_instance.register_datasource.assert_called_once_with(cfg)

    @patch("airflow.providers.common.ai.toolsets.datafusion.DataFusionEngine", autospec=True)
    def test_registers_multiple_datasources(self, MockEngine):
        mock_engine_instance = MagicMock()
        MockEngine.return_value = mock_engine_instance

        cfg_a = _make_mock_datasource_config("table_a")
        cfg_b = _make_mock_datasource_config("table_b")
        ts = DataFusionToolset([cfg_a, cfg_b])
        ts._get_engine()

        assert mock_engine_instance.register_datasource.call_count == 2

    @patch("airflow.providers.common.ai.toolsets.datafusion.DataFusionEngine", autospec=True)
    def test_caches_engine_after_first_creation(self, MockEngine):
        MockEngine.return_value = MagicMock()

        cfg = _make_mock_datasource_config()
        ts = DataFusionToolset([cfg])
        ts._get_engine()
        ts._get_engine()

        MockEngine.assert_called_once()
