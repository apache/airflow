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
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.ai.utils.sql_validation import SQLSafetyError


def _make_mock_db_hook(
    table_names: list[str] | None = None,
    table_schema: list[dict[str, str]] | None = None,
    records: list[tuple] | None = None,
    last_description: list[tuple] | None = None,
):
    """Create a mock DbApiHook with sensible defaults."""
    from airflow.providers.common.sql.hooks.sql import DbApiHook

    mock = MagicMock(spec=DbApiHook)
    mock.inspector = MagicMock()
    mock.inspector.get_table_names.return_value = table_names or ["users", "orders"]
    mock.get_table_schema.return_value = table_schema or [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "VARCHAR"},
    ]
    mock.get_records.return_value = records or [(1, "Alice"), (2, "Bob")]
    type(mock).last_description = PropertyMock(return_value=last_description or [("id",), ("name",)])
    return mock


def _make_mock_datasource_config(table_name: str = "sales_data"):
    """Create a mock DataSourceConfig."""
    from airflow.providers.common.sql.config import DataSourceConfig

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
    mock.registered_tables = registered_tables or {"sales_data": "s3://bucket/sales/"}

    fields = []
    for name, ftype in schema_fields or [("id", "Int64"), ("amount", "Float64")]:
        field = MagicMock()
        field.name = name
        field.type = ftype
        fields.append(field)
    mock.session_context.table.return_value.schema.return_value = fields

    mock.execute_query.return_value = (
        query_result
        if query_result is not None
        else {
            "id": [1, 2],
            "amount": [10.5, 20.0],
        }
    )
    return mock


class TestSQLToolsetInit:
    def test_id_includes_conn_id(self):
        ts = SQLToolset("my_pg")
        assert ts.id == "sql-my_pg"


class TestSQLToolsetGetTools:
    def test_returns_four_tools(self):
        ts = SQLToolset("pg_default")
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert set(tools.keys()) == {"list_tables", "get_schema", "query", "check_query"}

    def test_tool_definitions_have_descriptions(self):
        ts = SQLToolset("pg_default")
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        for tool in tools.values():
            assert tool.tool_def.description


class TestSQLToolsetListTables:
    def test_returns_all_tables(self):
        ts = SQLToolset("pg_default")
        mock_hook = _make_mock_db_hook(table_names=["users", "orders", "products"])
        ts._hook = mock_hook

        result = asyncio.run(ts.call_tool("list_tables", {}, ctx=MagicMock(), tool=MagicMock()))
        tables = json.loads(result)
        assert tables == ["users", "orders", "products"]

    def test_filters_by_allowed_tables(self):
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        mock_hook = _make_mock_db_hook(table_names=["users", "orders", "products"])
        ts._hook = mock_hook

        result = asyncio.run(ts.call_tool("list_tables", {}, ctx=MagicMock(), tool=MagicMock()))
        tables = json.loads(result)
        assert tables == ["orders"]


class TestSQLToolsetGetSchema:
    def test_returns_column_info(self):
        ts = SQLToolset("pg_default")
        mock_hook = _make_mock_db_hook()
        ts._hook = mock_hook

        result = asyncio.run(
            ts.call_tool("get_schema", {"table_name": "users"}, ctx=MagicMock(), tool=MagicMock())
        )
        columns = json.loads(result)
        assert columns == [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "VARCHAR"}]
        mock_hook.get_table_schema.assert_called_once_with("users", schema=None)

    def test_blocks_table_not_in_allowed_list(self):
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()

        result = asyncio.run(
            ts.call_tool("get_schema", {"table_name": "secrets"}, ctx=MagicMock(), tool=MagicMock())
        )
        data = json.loads(result)
        assert "error" in data
        assert "secrets" in data["error"]


class TestSQLToolsetQuery:
    def test_returns_rows_as_json(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(
            records=[(1, "Alice"), (2, "Bob")],
            last_description=[("id",), ("name",)],
        )

        result = asyncio.run(
            ts.call_tool("query", {"sql": "SELECT id, name FROM users"}, ctx=MagicMock(), tool=MagicMock())
        )
        data = json.loads(result)
        assert data["rows"] == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        assert data["count"] == 2

    def test_truncates_at_max_rows(self):
        ts = SQLToolset("pg_default", max_rows=1)
        ts._hook = _make_mock_db_hook(
            records=[(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            last_description=[("id",), ("name",)],
        )

        result = asyncio.run(
            ts.call_tool("query", {"sql": "SELECT id, name FROM users"}, ctx=MagicMock(), tool=MagicMock())
        )
        data = json.loads(result)
        assert len(data["rows"]) == 1
        assert data["truncated"] is True
        assert data["count"] == 3

    def test_blocks_unsafe_sql_by_default(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        with pytest.raises(SQLSafetyError, match="not allowed"):
            asyncio.run(ts.call_tool("query", {"sql": "DROP TABLE users"}, ctx=MagicMock(), tool=MagicMock()))

    def test_allows_writes_when_enabled(self):
        ts = SQLToolset("pg_default", allow_writes=True)
        ts._hook = _make_mock_db_hook(
            records=[(1,)],
            last_description=[("count",)],
        )

        # Should not raise even with INSERT
        result = asyncio.run(
            ts.call_tool(
                "query", {"sql": "INSERT INTO users VALUES (3, 'Eve')"}, ctx=MagicMock(), tool=MagicMock()
            )
        )
        # The mock doesn't actually execute, just returns mocked records
        data = json.loads(result)
        assert "rows" in data


class TestSQLToolsetCheckQuery:
    def test_valid_select(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        result = asyncio.run(
            ts.call_tool("check_query", {"sql": "SELECT 1"}, ctx=MagicMock(), tool=MagicMock())
        )
        data = json.loads(result)
        assert data["valid"] is True

    def test_invalid_sql(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        result = asyncio.run(
            ts.call_tool("check_query", {"sql": "DROP TABLE users"}, ctx=MagicMock(), tool=MagicMock())
        )
        data = json.loads(result)
        assert data["valid"] is False
        assert "error" in data


class TestSQLToolsetHookResolution:
    @patch("airflow.providers.common.ai.toolsets.sql.BaseHook", autospec=True)
    def test_lazy_resolves_db_hook(self, mock_base_hook):
        from airflow.providers.common.sql.hooks.sql import DbApiHook

        mock_hook = MagicMock(spec=DbApiHook)
        mock_conn = MagicMock(spec=["get_hook"])
        mock_conn.get_hook.return_value = mock_hook
        mock_base_hook.get_connection.return_value = mock_conn

        ts = SQLToolset("pg_default")
        hook = ts._get_db_hook()

        assert hook is mock_hook
        mock_base_hook.get_connection.assert_called_once_with("pg_default")

    @patch("airflow.providers.common.ai.toolsets.sql.BaseHook", autospec=True)
    def test_raises_for_non_dbapi_hook(self, mock_base_hook):
        mock_conn = MagicMock(spec=["get_hook"])
        mock_conn.get_hook.return_value = MagicMock()  # Not a DbApiHook
        mock_base_hook.get_connection.return_value = mock_conn

        ts = SQLToolset("bad_conn")

        with pytest.raises(ValueError, match="does not provide a DbApiHook"):
            ts._get_db_hook()

    @patch("airflow.providers.common.ai.toolsets.sql.BaseHook", autospec=True)
    def test_caches_hook_after_first_resolution(self, mock_base_hook):
        from airflow.providers.common.sql.hooks.sql import DbApiHook

        mock_hook = MagicMock(spec=DbApiHook)
        mock_conn = MagicMock(spec=["get_hook"])
        mock_conn.get_hook.return_value = mock_hook
        mock_base_hook.get_connection.return_value = mock_conn

        ts = SQLToolset("pg_default")
        ts._get_db_hook()
        ts._get_db_hook()

        # Only called once because result is cached.
        mock_base_hook.get_connection.assert_called_once()


class TestSQLToolsetInitValidation:
    def test_rejects_both_db_conn_id_and_datasource_configs(self):
        config = _make_mock_datasource_config()
        with pytest.raises(ValueError, match="Specify either 'db_conn_id' or 'datasource_configs', not both"):
            SQLToolset("pg_default", datasource_configs=[config])

    def test_rejects_neither_db_conn_id_nor_datasource_configs(self):
        with pytest.raises(ValueError, match="Either 'db_conn_id' or 'datasource_configs' must be provided."):
            SQLToolset()

    def test_uses_datafusion_flag(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config])
        assert ts._uses_datafusion is True

        ts_db = SQLToolset("pg_default")
        assert ts_db._uses_datafusion is False


class TestSQLToolsetDataFusionInit:
    def test_id_includes_table_names(self):
        cfg_a = _make_mock_datasource_config("s3_table_a")
        cfg_b = _make_mock_datasource_config("s3_table_b")
        ts = SQLToolset(datasource_configs=[cfg_b, cfg_a])
        assert ts.id == "sql-datafusion"

    def test_get_tools_returns_four_tools(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config])
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert set(tools.keys()) == {"list_tables", "get_schema", "query", "check_query"}


class TestSQLToolsetDataFusionListTables:
    def test_returns_registered_tables(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config])
        ts._engine = _make_mock_engine(
            registered_tables={"sales": "s3://bucket/sales/", "orders": "s3://bucket/orders/"}
        )

        result = asyncio.run(ts.call_tool("list_tables", {}, ctx=MagicMock(), tool=MagicMock()))
        tables = json.loads(result)
        assert set(tables) == {"sales", "orders"}

    def test_filters_by_allowed_tables(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config], allowed_tables=["sales"])
        ts._engine = _make_mock_engine(
            registered_tables={"sales": "s3://bucket/sales/", "orders": "s3://bucket/orders/"}
        )

        result = asyncio.run(ts.call_tool("list_tables", {}, ctx=MagicMock(), tool=MagicMock()))
        tables = json.loads(result)
        assert tables == ["sales"]


class TestSQLToolsetDataFusionGetSchema:
    def test_returns_column_info(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config])
        ts._engine = _make_mock_engine(
            schema_fields=[("id", "Int64"), ("amount", "Float64"), ("name", "Utf8")]
        )

        result = asyncio.run(
            ts.call_tool("get_schema", {"table_name": "sales_data"}, ctx=MagicMock(), tool=MagicMock())
        )
        columns = json.loads(result)
        assert columns == [
            {"name": "id", "type": "Int64"},
            {"name": "amount", "type": "Float64"},
            {"name": "name", "type": "Utf8"},
        ]

    def test_blocks_table_not_in_allowed_list(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config], allowed_tables=["sales"])
        ts._engine = _make_mock_engine()

        result = asyncio.run(
            ts.call_tool("get_schema", {"table_name": "secrets"}, ctx=MagicMock(), tool=MagicMock())
        )
        data = json.loads(result)
        assert "error" in data
        assert "secrets" in data["error"]


class TestSQLToolsetDataFusionQuery:
    def test_returns_rows_as_json(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config])
        ts._engine = _make_mock_engine(query_result={"id": [1, 2], "amount": [10.5, 20.0]})

        result = asyncio.run(
            ts.call_tool(
                "query", {"sql": "SELECT id, amount FROM sales_data"}, ctx=MagicMock(), tool=MagicMock()
            )
        )
        data = json.loads(result)
        assert data["rows"] == [{"id": 1, "amount": 10.5}, {"id": 2, "amount": 20.0}]
        assert data["count"] == 2

    def test_truncates_at_max_rows(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config], max_rows=1)
        ts._engine = _make_mock_engine(query_result={"id": [1, 2, 3], "name": ["a", "b", "c"]})

        result = asyncio.run(
            ts.call_tool(
                "query", {"sql": "SELECT id, name FROM sales_data"}, ctx=MagicMock(), tool=MagicMock()
            )
        )
        data = json.loads(result)
        assert len(data["rows"]) == 1
        assert data["truncated"] is True
        assert data["count"] == 3

    def test_handles_empty_result(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config])
        ts._engine = _make_mock_engine(query_result={})

        result = asyncio.run(
            ts.call_tool(
                "query", {"sql": "SELECT * FROM sales_data WHERE 1=0"}, ctx=MagicMock(), tool=MagicMock()
            )
        )
        data = json.loads(result)
        assert data["rows"] == []
        assert data["count"] == 0

    def test_blocks_unsafe_sql_by_default(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config])
        ts._engine = _make_mock_engine()

        with pytest.raises(SQLSafetyError, match="not allowed"):
            asyncio.run(
                ts.call_tool("query", {"sql": "DROP TABLE sales_data"}, ctx=MagicMock(), tool=MagicMock())
            )

    def test_allows_writes_when_enabled(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config], allow_writes=True)
        ts._engine = _make_mock_engine(query_result={"count": [1]})

        result = asyncio.run(
            ts.call_tool(
                "query",
                {"sql": "INSERT INTO sales_data VALUES (3, 30.0)"},
                ctx=MagicMock(),
                tool=MagicMock(),
            )
        )
        data = json.loads(result)
        assert "rows" in data


class TestSQLToolsetDataFusionCheckQuery:
    def test_valid_select(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config])

        result = asyncio.run(
            ts.call_tool("check_query", {"sql": "SELECT 1"}, ctx=MagicMock(), tool=MagicMock())
        )
        data = json.loads(result)
        assert data["valid"] is True

    def test_invalid_sql(self):
        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config])

        result = asyncio.run(
            ts.call_tool("check_query", {"sql": "DROP TABLE sales_data"}, ctx=MagicMock(), tool=MagicMock())
        )
        data = json.loads(result)
        assert data["valid"] is False
        assert "error" in data


class TestSQLToolsetDataFusionEngineResolution:
    @patch("airflow.providers.common.sql.datafusion.engine.DataFusionEngine", autospec=True)
    def test_lazy_creates_engine(self, MockEngine):
        mock_engine_instance = MagicMock()
        MockEngine.return_value = mock_engine_instance

        config = _make_mock_datasource_config("my_table")
        ts = SQLToolset(datasource_configs=[config])
        engine = ts._get_engine()

        assert engine is mock_engine_instance
        MockEngine.assert_called_once()
        mock_engine_instance.register_datasource.assert_called_once_with(config)

    @patch("airflow.providers.common.sql.datafusion.engine.DataFusionEngine", autospec=True)
    def test_registers_multiple_datasources(self, MockEngine):
        mock_engine_instance = MagicMock()
        MockEngine.return_value = mock_engine_instance

        cfg_a = _make_mock_datasource_config("table_a")
        cfg_b = _make_mock_datasource_config("table_b")
        ts = SQLToolset(datasource_configs=[cfg_a, cfg_b])
        ts._get_engine()

        assert mock_engine_instance.register_datasource.call_count == 2

    @patch("airflow.providers.common.sql.datafusion.engine.DataFusionEngine", autospec=True)
    def test_caches_engine_after_first_creation(self, MockEngine):
        MockEngine.return_value = MagicMock()

        config = _make_mock_datasource_config()
        ts = SQLToolset(datasource_configs=[config])
        ts._get_engine()
        ts._get_engine()

        MockEngine.assert_called_once()
