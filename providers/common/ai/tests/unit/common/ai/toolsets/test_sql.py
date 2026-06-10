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
from pydantic_ai.exceptions import ModelRetry

from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.sql.hooks.sql import DbApiHook


def _make_mock_db_hook(
    table_names: list[str] | None = None,
    table_schema: list[dict[str, str]] | None = None,
    records: list[tuple] | None = None,
    last_description: list[tuple] | None = None,
):
    """Create a mock DbApiHook with sensible defaults."""
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

    def test_introspection_error_raises_model_retry(self):
        """A failure while listing tables is returned to the agent as a retry."""
        ts = SQLToolset("pg_default")
        mock_hook = _make_mock_db_hook()
        mock_hook.inspector.get_table_names.side_effect = Exception("could not connect to server")
        ts._hook = mock_hook

        with pytest.raises(ModelRetry) as exc_info:
            asyncio.run(ts.call_tool("list_tables", {}, ctx=MagicMock(), tool=MagicMock()))
        assert "could not connect to server" in exc_info.value.message


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

    def test_introspection_error_raises_model_retry(self):
        """A failure while reading a table's schema is returned to the agent as a retry."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.get_table_schema.side_effect = Exception('relation "users" does not exist')

        with pytest.raises(ModelRetry) as exc_info:
            asyncio.run(
                ts.call_tool("get_schema", {"table_name": "users"}, ctx=MagicMock(), tool=MagicMock())
            )
        assert "does not exist" in exc_info.value.message


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

    def test_unsafe_sql_raises_model_retry(self):
        """An unsafe statement is surfaced to the agent as a retry so it can switch to a SELECT."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        with pytest.raises(ModelRetry) as exc_info:
            asyncio.run(ts.call_tool("query", {"sql": "DROP TABLE users"}, ctx=MagicMock(), tool=MagicMock()))
        assert "not allowed" in exc_info.value.message

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

    @pytest.mark.parametrize(
        "error",
        [
            Exception("001003 (42000): SQL compilation error: unexpected 'rows'"),
            RuntimeError("type mismatch"),
            ConnectionError("could not connect to server"),
        ],
    )
    def test_query_error_is_returned_to_agent_as_model_retry(self, error):
        """Any error from the query, whatever its type, is handed back to the agent as a retry with
        the database's own message. The toolset never inspects the error type or text; pydantic-ai's
        max_retries bounds the loop, so an unrecoverable error still fails the task."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.get_records.side_effect = error

        with pytest.raises(ModelRetry) as exc_info:
            asyncio.run(
                ts.call_tool("query", {"sql": "SELECT foo FROM bar"}, ctx=MagicMock(), tool=MagicMock())
            )
        message = exc_info.value.message
        assert str(error) in message
        assert "list_tables" in message
        assert "get_schema" in message


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
        mock_hook = MagicMock(spec=DbApiHook)
        mock_conn = MagicMock(spec=["get_hook"])
        mock_conn.get_hook.return_value = mock_hook
        mock_base_hook.get_connection.return_value = mock_conn

        ts = SQLToolset("pg_default")
        ts._get_db_hook()
        ts._get_db_hook()

        # Only called once because result is cached.
        mock_base_hook.get_connection.assert_called_once()


class TestSQLToolsetMultiSchema:
    """Schema-qualified allowed_tables span multiple schemas in one database."""

    @staticmethod
    def _schema_aware_hook(tables_by_schema: dict[str | None, list[str]]):
        hook = MagicMock(spec=DbApiHook)
        hook.inspector = MagicMock()
        hook.inspector.get_table_names.side_effect = lambda schema=None: tables_by_schema.get(schema, [])
        hook.get_table_schema.return_value = [{"name": "id", "type": "INTEGER"}]
        return hook

    def test_list_tables_spans_multiple_schemas(self):
        ts = SQLToolset(
            "sf",
            allowed_tables=["MODEL_ASTRO.DEPLOYMENT_IMAGE_DETAILS", "MODEL_CRM.SF_ASTRO_ORGS"],
        )
        ts._hook = self._schema_aware_hook(
            {
                "MODEL_ASTRO": ["DEPLOYMENT_IMAGE_DETAILS", "OTHER_TABLE"],
                "MODEL_CRM": ["SF_ASTRO_ORGS"],
            }
        )

        result = json.loads(asyncio.run(ts.call_tool("list_tables", {}, ctx=MagicMock(), tool=MagicMock())))
        assert result == ["MODEL_ASTRO.DEPLOYMENT_IMAGE_DETAILS", "MODEL_CRM.SF_ASTRO_ORGS"]

    def test_list_tables_never_introspects_none_schema_when_all_qualified(self):
        """Regression for the 'SHOW TABLES IN SCHEMA "DB"."None"' failure."""
        ts = SQLToolset("sf", allowed_tables=["MODEL_ASTRO.X", "MODEL_CRM.Y"])
        ts._hook = self._schema_aware_hook({"MODEL_ASTRO": ["X"], "MODEL_CRM": ["Y"]})

        asyncio.run(ts.call_tool("list_tables", {}, ctx=MagicMock(), tool=MagicMock()))

        called_schemas = {c.kwargs.get("schema") for c in ts._hook.inspector.get_table_names.call_args_list}
        assert called_schemas == {"MODEL_ASTRO", "MODEL_CRM"}
        assert None not in called_schemas

    def test_list_tables_mixed_qualified_and_default(self):
        ts = SQLToolset("pg", allowed_tables=["users", "MODEL_ASTRO.X"], schema="public")
        ts._hook = self._schema_aware_hook({"public": ["users", "orders"], "MODEL_ASTRO": ["X", "Z"]})

        result = json.loads(asyncio.run(ts.call_tool("list_tables", {}, ctx=MagicMock(), tool=MagicMock())))
        # Qualified schemas listed first (sorted), then the default schema.
        assert result == ["MODEL_ASTRO.X", "users"]

    def test_get_schema_routes_to_qualified_schema(self):
        ts = SQLToolset("sf", allowed_tables=["MODEL_ASTRO.DEPLOYMENT_IMAGE_DETAILS"])
        ts._hook = self._schema_aware_hook({"MODEL_ASTRO": ["DEPLOYMENT_IMAGE_DETAILS"]})

        result = json.loads(
            asyncio.run(
                ts.call_tool(
                    "get_schema",
                    {"table_name": "MODEL_ASTRO.DEPLOYMENT_IMAGE_DETAILS"},
                    ctx=MagicMock(),
                    tool=MagicMock(),
                )
            )
        )
        assert result == [{"name": "id", "type": "INTEGER"}]
        ts._hook.get_table_schema.assert_called_once_with("DEPLOYMENT_IMAGE_DETAILS", schema="MODEL_ASTRO")

    def test_get_schema_blocks_table_outside_allowed_schema(self):
        ts = SQLToolset("sf", allowed_tables=["MODEL_ASTRO.X"])
        ts._hook = self._schema_aware_hook({"MODEL_ASTRO": ["X"]})

        result = json.loads(
            asyncio.run(
                ts.call_tool(
                    "get_schema", {"table_name": "SECRETS.PASSWORDS"}, ctx=MagicMock(), tool=MagicMock()
                )
            )
        )
        assert "error" in result
        ts._hook.get_table_schema.assert_not_called()

    def test_get_schema_unqualified_uses_default_schema(self):
        ts = SQLToolset("pg", schema="public")
        ts._hook = self._schema_aware_hook({"public": ["users"]})

        asyncio.run(ts.call_tool("get_schema", {"table_name": "users"}, ctx=MagicMock(), tool=MagicMock()))
        ts._hook.get_table_schema.assert_called_once_with("users", schema="public")

    def test_list_tables_matches_case_insensitively(self):
        """Snowflake reflects unquoted names lowercased; uppercase allowed_tables still match."""
        ts = SQLToolset(
            "sf",
            allowed_tables=["MODEL_ASTRO.DEPLOYMENT_IMAGE_DETAILS", "MODEL_CRM.SF_ASTRO_ORGS"],
        )
        ts._hook = self._schema_aware_hook(
            {
                "MODEL_ASTRO": ["deployment_image_details", "other"],
                "MODEL_CRM": ["sf_astro_orgs"],
            }
        )

        result = json.loads(asyncio.run(ts.call_tool("list_tables", {}, ctx=MagicMock(), tool=MagicMock())))
        assert result == ["MODEL_ASTRO.deployment_image_details", "MODEL_CRM.sf_astro_orgs"]

    def test_get_schema_matches_case_insensitively(self):
        ts = SQLToolset("sf", allowed_tables=["MODEL_ASTRO.DEPLOYMENT_IMAGE_DETAILS"])
        ts._hook = self._schema_aware_hook({"MODEL_ASTRO": ["deployment_image_details"]})

        result = json.loads(
            asyncio.run(
                ts.call_tool(
                    "get_schema",
                    {"table_name": "MODEL_ASTRO.deployment_image_details"},
                    ctx=MagicMock(),
                    tool=MagicMock(),
                )
            )
        )
        assert "error" not in result
        ts._hook.get_table_schema.assert_called_once_with("deployment_image_details", schema="MODEL_ASTRO")

    def test_list_tables_deduplicates_same_table(self):
        """A table listed both qualified and unqualified appears once."""
        ts = SQLToolset("pg", allowed_tables=["public.users", "users"], schema="public")
        ts._hook = self._schema_aware_hook({"public": ["users"]})

        result = json.loads(asyncio.run(ts.call_tool("list_tables", {}, ctx=MagicMock(), tool=MagicMock())))
        assert result == ["public.users"]


class TestSQLToolsetMetadataStatements:
    """Read-only metadata statements (DESCRIBE/SHOW) flow through the query tool."""

    def test_describe_allowed_through_query(self):
        """DESCRIBE is read-only metadata and should not be rejected as unsafe."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(
            records=[("id", "INTEGER"), ("name", "VARCHAR")],
            last_description=[("column_name",), ("data_type",)],
        )

        result = asyncio.run(
            ts.call_tool("query", {"sql": "DESCRIBE TABLE users"}, ctx=MagicMock(), tool=MagicMock())
        )
        data = json.loads(result)
        assert "rows" in data
        ts._hook.get_records.assert_called_once_with("DESCRIBE TABLE users")

    def test_show_allowed_with_snowflake_dialect(self):
        """SHOW parses to a metadata statement once the hook's dialect is passed through."""
        ts = SQLToolset("sf_default")
        ts._hook = _make_mock_db_hook(records=[("USERS",)], last_description=[("name",)])
        ts._hook.dialect_name = "snowflake"

        result = asyncio.run(ts.call_tool("query", {"sql": "SHOW TABLES"}, ctx=MagicMock(), tool=MagicMock()))
        data = json.loads(result)
        assert "rows" in data
        ts._hook.get_records.assert_called_once_with("SHOW TABLES")

    @pytest.mark.parametrize(
        "sql",
        # SHOW falls back to Command on Postgres (no SHOW support); DELETE is a write.
        ["SHOW TABLES", "DELETE FROM users"],
        ids=["show_without_dialect_support", "write"],
    )
    def test_query_blocks_disallowed_statements(self, sql):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = "postgresql"

        # The statement is rejected before execution and surfaced to the agent as a
        # retry; get_records is never reached, so the guardrail still holds.
        with pytest.raises(ModelRetry) as exc_info:
            asyncio.run(ts.call_tool("query", {"sql": sql}, ctx=MagicMock(), tool=MagicMock()))
        assert "not allowed" in exc_info.value.message
        ts._hook.get_records.assert_not_called()

    def test_check_query_accepts_describe(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        result = asyncio.run(
            ts.call_tool("check_query", {"sql": "DESCRIBE TABLE users"}, ctx=MagicMock(), tool=MagicMock())
        )
        assert json.loads(result)["valid"] is True

    def test_check_query_handles_unresolvable_connection(self):
        """check_query stays usable (dialect-agnostic) when the connection can't be resolved."""
        ts = SQLToolset("missing_conn")
        with patch.object(ts, "_get_db_hook", side_effect=RuntimeError("no such connection")):
            result = asyncio.run(
                ts.call_tool("check_query", {"sql": "SELECT 1"}, ctx=MagicMock(), tool=MagicMock())
            )
        assert json.loads(result)["valid"] is True
