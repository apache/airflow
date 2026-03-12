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
import importlib.util
import json
import sqlite3
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from pydantic_ai.exceptions import ModelRetry

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

    def test_raises_model_retry_when_query_fails_with_retryable_error(self):
        """When the query fails with a retryable error, raise ModelRetry so the model retries."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.conn_type = "sqlite"
        ts._hook.get_records.side_effect = sqlite3.OperationalError("no such column: nonexistent")

        with pytest.raises(ModelRetry) as exc_info:
            asyncio.run(
                ts.call_tool(
                    "query",
                    {"sql": "SELECT id, nonexistent FROM users"},
                    ctx=MagicMock(),
                    tool=MagicMock(),
                )
            )
        assert "nonexistent" in exc_info.value.message
        assert "get_schema" in exc_info.value.message
        assert "list_tables" in exc_info.value.message

    def test_model_retry_message_includes_schema_hint(self):
        """ModelRetry message tells the model to use get_schema and list_tables for more details."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.conn_type = "sqlite"
        ts._hook.get_records.side_effect = sqlite3.OperationalError("no such table: missing_table")

        with pytest.raises(ModelRetry) as exc_info:
            asyncio.run(
                ts.call_tool("query", {"sql": "SELECT foo FROM x"}, ctx=MagicMock(), tool=MagicMock())
            )
        assert "get_schema" in exc_info.value.message
        assert "list_tables" in exc_info.value.message

    def test_non_retryable_error_is_propagated(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.conn_type = "sqlite"
        ts._hook.get_records.side_effect = sqlite3.OperationalError("database is locked")

        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            asyncio.run(ts.call_tool("query", {"sql": "SELECT 1"}, ctx=MagicMock(), tool=MagicMock()))

    def test_error_propagates_when_hook_conn_type_not_supported(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.conn_type = "mysql"
        ts._hook.get_records.side_effect = RuntimeError("unexpected db error")

        with pytest.raises(RuntimeError, match="unexpected db error"):
            asyncio.run(ts.call_tool("query", {"sql": "SELECT 1"}, ctx=MagicMock(), tool=MagicMock()))

    def test_error_propagates_when_hook_has_no_conn_type(self):
        ts = SQLToolset("pg_default")
        mock_hook = MagicMock(spec=["get_records", "last_description"])
        mock_hook.get_records.side_effect = RuntimeError("hook error")
        type(mock_hook).last_description = PropertyMock(return_value=[])
        ts._hook = mock_hook

        with pytest.raises(RuntimeError, match="hook error"):
            asyncio.run(ts.call_tool("query", {"sql": "SELECT 1"}, ctx=MagicMock(), tool=MagicMock()))

    @pytest.mark.skipif(
        importlib.util.find_spec("psycopg2") is None,
        reason="psycopg2 is not available for lowest dependency tests",
    )
    def test_sqlalchemy_programming_error_with_psycopg2_undefined_column_orig_raises_model_retry_for_postgres(
        self,
    ):
        from psycopg2 import errors as psycopg2_errors
        from sqlalchemy.exc import ProgrammingError

        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.conn_type = "postgres"
        ts._hook.get_records.side_effect = ProgrammingError(
            statement="SELECT id, missing FROM users",
            params=None,
            orig=psycopg2_errors.UndefinedColumn('column "missing" does not exist'),
        )

        with (
            patch(
                "airflow.providers.common.ai.toolsets.sql._POSTGRES_RETRYABLE_EXCEPTIONS",
                (psycopg2_errors.UndefinedColumn,),
            ),
            patch(
                "airflow.providers.common.ai.toolsets.sql._SQLALCHEMY_RETRYABLE_EXCEPTIONS",
                (ProgrammingError,),
            ),
            pytest.raises(ModelRetry),
        ):
            asyncio.run(
                ts.call_tool(
                    "query",
                    {"sql": "SELECT id, missing FROM users"},
                    ctx=MagicMock(),
                    tool=MagicMock(),
                )
            )

    @pytest.mark.skipif(
        importlib.util.find_spec("psycopg2") is None,
        reason="psycopg2 is not available for lowest dependency tests",
    )
    def test_sqlalchemy_programming_error_with_psycopg2_insufficient_privilege_orig_is_not_retried_for_postgres(
        self,
    ):
        from psycopg2 import errors as psycopg2_errors
        from sqlalchemy.exc import ProgrammingError

        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.conn_type = "postgres"
        ts._hook.get_records.side_effect = ProgrammingError(
            statement="SELECT id FROM users",
            params=None,
            orig=psycopg2_errors.InsufficientPrivilege("permission denied for table users"),
        )

        with (
            patch(
                "airflow.providers.common.ai.toolsets.sql._POSTGRES_RETRYABLE_EXCEPTIONS",
                (psycopg2_errors.UndefinedColumn, psycopg2_errors.UndefinedTable),
            ),
            patch(
                "airflow.providers.common.ai.toolsets.sql._SQLALCHEMY_RETRYABLE_EXCEPTIONS",
                (ProgrammingError,),
            ),
            pytest.raises(ProgrammingError),
        ):
            asyncio.run(
                ts.call_tool(
                    "query",
                    {"sql": "SELECT id FROM users"},
                    ctx=MagicMock(),
                    tool=MagicMock(),
                )
            )


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
