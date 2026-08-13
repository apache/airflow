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
from pydantic_ai._run_context import RunContext
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.toolsets.abstract import ToolsetTool
from pydantic_core import ValidationError
from sqlalchemy.engine.reflection import Inspector

from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.ai.utils.tool_definition import _SUPPORTS_RETURN_SCHEMA
from airflow.providers.common.sql.hooks.sql import DbApiHook


class _FakeCursor:
    """
    Minimal DBAPI cursor over canned records.

    The toolset fetches through ``DbApiHook.run``'s handler protocol rather than
    ``get_records``, so the mock hook drives the real handler over this instead of
    returning a canned list -- that keeps ``fetchmany``'s bounded-fetch behaviour
    under test rather than mocked away.
    """

    def __init__(self, records: list[tuple], description: list[tuple] | None, rowcount: int) -> None:
        self.description = description
        self.rowcount = rowcount
        self._remaining = list(records)

    def fetchmany(self, size: int) -> list[tuple]:
        chunk, self._remaining = self._remaining[:size], self._remaining[size:]
        return chunk


def _make_mock_db_hook(
    table_names: list[str] | None = None,
    table_schema: list[dict[str, str]] | None = None,
    records: list[tuple] | None = None,
    last_description: list[tuple] | None = None,
    rowcount: int = -1,
):
    """Create a mock DbApiHook with sensible defaults."""
    mock = MagicMock(spec=DbApiHook)
    mock.inspector = MagicMock(spec=Inspector)
    mock.inspector.get_table_names.return_value = table_names or ["users", "orders"]
    mock.get_table_schema.return_value = table_schema or [
        {"name": "id", "type": "INTEGER"},
        {"name": "name", "type": "VARCHAR"},
    ]
    rows = [(1, "Alice"), (2, "Bob")] if records is None else records
    description = last_description or [("id",), ("name",)]
    # -1 is what a driver reports when it has no row count for a SELECT (SQLite);
    # get_row_count normalises that to None, so total_rows is absent by default.
    mock.strip_sql_string.side_effect = DbApiHook.strip_sql_string
    mock.run.side_effect = lambda sql, handler, **kwargs: handler(_FakeCursor(rows, description, rowcount))
    type(mock).last_description = PropertyMock(return_value=description)
    return mock


def _assert_executed(hook, sql: str) -> None:
    """Assert the toolset ran exactly *sql* (the handler is an implementation detail)."""
    assert hook.run.call_count == 1
    assert hook.run.call_args.args[0] == sql


def _query(ts: SQLToolset, sql: str) -> dict:
    """Run the ``query`` tool and decode its JSON result."""
    result = asyncio.run(
        ts.call_tool("query", {"sql": sql}, ctx=MagicMock(spec=RunContext), tool=MagicMock(spec=ToolsetTool))
    )
    return json.loads(result)


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

    @pytest.mark.parametrize(
        ("name", "valid_args"),
        [
            ("get_schema", {"table_name": "users"}),
            ("query", {"sql": "SELECT 1"}),
            ("check_query", {"sql": "SELECT 1"}),
        ],
    )
    def test_args_validator_enforces_required_keys(self, name, valid_args):
        ts = SQLToolset("pg_default")
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        validator = tools[name].args_validator

        assert validator.validate_python(valid_args) == valid_args
        with pytest.raises(ValidationError):
            validator.validate_python({})

    @pytest.mark.skipif(
        not _SUPPORTS_RETURN_SCHEMA, reason="pydantic-ai too old for ToolDefinition.return_schema"
    )
    def test_tools_declare_string_return_schema(self):
        # Every tool returns a JSON-encoded string, so code mode should see `-> str`.
        ts = SQLToolset("pg_default")
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        for tool in tools.values():
            assert tool.tool_def.return_schema == {"type": "string"}


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
    def test_returns_rows_columnar(self):
        """Column names are named once, not repeated per row -- on a wide table the
        repeated names, not the values, dominate the payload."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(
            records=[(1, "Alice"), (2, "Bob")],
            last_description=[("id",), ("name",)],
        )

        data = _query(ts, "SELECT id, name FROM users")
        assert data["columns"] == ["id", "name"]
        assert data["rows"] == [[1, "Alice"], [2, "Bob"]]
        assert data["row_count"] == 2
        assert "truncated" not in data

    def test_duplicate_column_names_are_preserved(self):
        """A dict per row silently dropped one of two same-named columns; positional
        rows keep both."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, 2)], last_description=[("id",), ("id",)])

        data = _query(ts, "SELECT o.id, c.id FROM orders o JOIN customers c ON o.id = c.id")
        assert data["columns"] == ["id", "id"]
        assert data["rows"] == [[1, 2]]

    def test_truncates_at_max_rows(self):
        ts = SQLToolset("pg_default", max_rows=1)
        ts._hook = _make_mock_db_hook(
            records=[(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            last_description=[("id",), ("name",)],
        )

        data = _query(ts, "SELECT id, name FROM users")
        assert data["rows"] == [[1, "Alice"]]
        assert data["row_count"] == 1
        assert data["truncated"] is True
        assert data["truncated_by"] == "max_rows"

    def test_fetches_only_one_row_beyond_max_rows(self):
        """The rows past the cap are never pulled out of the cursor. Fetching them all
        and then slicing pays the full transfer cost for data the agent never sees."""
        cursor = _FakeCursor([(i, "x") for i in range(1000)], [("id",), ("name",)], -1)
        ts = SQLToolset("pg_default", max_rows=2)
        ts._hook = _make_mock_db_hook()
        ts._hook.run.side_effect = lambda sql, handler, **kwargs: handler(cursor)

        _query(ts, "SELECT id, name FROM users")
        # 1000 rows matched; 3 left the cursor (max_rows plus the one that proves
        # there are more), so 997 were never transferred.
        assert len(cursor._remaining) == 997

    def test_reports_total_rows_when_the_driver_provides_one(self):
        ts = SQLToolset("pg_default", max_rows=1)
        ts._hook = _make_mock_db_hook(
            records=[(1, "Alice"), (2, "Bob")],
            last_description=[("id",), ("name",)],
            rowcount=4_200_000,
        )

        assert _query(ts, "SELECT id, name FROM users")["total_rows"] == 4_200_000

    def test_omits_total_rows_when_the_driver_counts_rows_fetched_so_far(self):
        """python-oracledb documents rowcount for SELECT as rows fetched *so far*, so
        after a capped fetch it equals the cap, not the query total. Reporting it would
        tell an agent it saw 50 of 51 rows when the table holds millions."""
        ts = SQLToolset("pg_default", max_rows=50)
        ts._hook = _make_mock_db_hook(
            records=[(i, f"name_{i}") for i in range(500)],
            last_description=[("id",), ("name",)],
            # Fetching max_rows + 1 leaves an incremental driver reporting exactly 51.
            rowcount=51,
        )

        data = _query(ts, "SELECT id, name FROM big")
        assert data["row_count"] == 50
        assert data["truncated"] is True
        assert "total_rows" not in data

    def test_omits_total_rows_when_the_driver_has_none(self):
        """SQLite and several warehouse drivers report -1 for a SELECT. Absent beats
        inventing a count the agent would treat as authoritative."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1, "Alice")], rowcount=-1)

        assert "total_rows" not in _query(ts, "SELECT id, name FROM users")

    def test_wide_rows_are_capped_by_byte_budget_before_max_rows(self):
        """max_rows says nothing about size. A handful of very wide rows must still be
        bounded, or the result dominates the context for the rest of the run."""
        ts = SQLToolset("pg_default", max_rows=50, max_result_bytes=16_384)
        ts._hook = _make_mock_db_hook(
            records=[tuple(f"value_{i}" for i in range(200))] * 50,
            last_description=[(f"col_{i}",) for i in range(200)],
        )

        data = _query(ts, "SELECT * FROM wide")
        # 50 rows are under max_rows, so only the byte budget can have stopped this.
        assert 0 < data["row_count"] < 50
        assert len(json.dumps(data, separators=(",", ":"))) <= 16_384 + 512  # budget + envelope
        assert data["truncated"] is True
        assert data["truncated_by"] == "max_result_bytes"

    def test_row_too_wide_to_fit_tells_the_agent_to_narrow_the_projection(self):
        ts = SQLToolset("pg_default", max_result_bytes=200)
        ts._hook = _make_mock_db_hook(records=[("x" * 500,)], last_description=[("blob",)])

        data = _query(ts, "SELECT blob FROM docs")
        assert data["rows"] == []
        assert data["truncated_by"] == "max_result_bytes"
        assert "max_result_bytes" in data["hint"]

    def test_column_names_alone_over_budget_report_the_shape_instead(self):
        """When even the header does not fit, returning it would spend the whole budget
        on column names and leave no room for data."""
        ts = SQLToolset("pg_default", max_result_bytes=256)
        ts._hook = _make_mock_db_hook(
            records=[tuple(range(3000))],
            last_description=[(f"column_name_{i}",) for i in range(3000)],
        )

        data = _query(ts, "SELECT * FROM very_wide")
        assert "columns" not in data
        assert data["column_count"] == 3000
        assert data["rows"] == []
        assert "3000 columns" in data["hint"]

    def test_no_rows_matched(self):
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[], last_description=[("id",), ("name",)])

        data = _query(ts, "SELECT id, name FROM users WHERE 1=0")
        assert data == {"columns": ["id", "name"], "rows": [], "row_count": 0}

    def test_statement_returning_no_result_set(self):
        """A DBAPI cursor reports description=None for DDL and for DML without
        RETURNING; that is not an error, just no rows."""
        ts = SQLToolset("pg_default", allow_writes=True)
        ts._hook = _make_mock_db_hook()
        ts._hook.run.side_effect = lambda sql, handler, **kwargs: handler(_FakeCursor([], None, -1))

        assert _query(ts, "INSERT INTO users VALUES (3, 'Eve')")["row_count"] == 0

    def test_non_dbapi_cursor_falls_back_to_a_full_fetch(self):
        """ExasolHook hands its handler a pyexasol statement, which has no
        ``description``. Bounding the fetch needs driver-specific knowledge, so those
        fall back to fetching everything -- as the toolset did before -- rather than
        failing the query. The payload is still bounded."""

        class _NonDbapiStatement:
            def __init__(self, records):
                self._records = records

            def fetchall(self):
                return self._records

        ts = SQLToolset("pg_default", max_rows=2)
        ts._hook = _make_mock_db_hook()
        ts._hook.run.side_effect = lambda sql, handler, **kwargs: handler(
            _NonDbapiStatement([(i, f"name_{i}") for i in range(10)])
        )

        data = _query(ts, "SELECT id, name FROM users")
        assert data["rows"] == [[0, "name_0"], [1, "name_1"]]
        assert data["truncated_by"] == "max_rows"
        # A full fetch left nothing behind, so the count is exact and worth reporting.
        assert data["total_rows"] == 10

    def test_cursor_that_can_neither_describe_nor_fetch_is_an_error(self):
        """Returning an empty result would read to the agent as 'the table is empty'."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()
        ts._hook.run.side_effect = lambda sql, handler, **kwargs: handler(object())

        with pytest.raises(ModelRetry, match="DBAPI 2.0"):
            _query(ts, "SELECT id FROM users")

    def test_trailing_semicolon_is_stripped(self):
        """get_records did this for the hooks that override it -- Trino rejects a
        trailing semicolon -- so fetching through run() has to keep doing it."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook()

        _query(ts, "SELECT id FROM users;")
        _assert_executed(ts._hook, "SELECT id FROM users")

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
        ts._hook.run.side_effect = error

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
        _assert_executed(ts._hook, "DESCRIBE TABLE users")

    def test_show_allowed_with_snowflake_dialect(self):
        """SHOW parses to a metadata statement once the hook's dialect is passed through."""
        ts = SQLToolset("sf_default")
        ts._hook = _make_mock_db_hook(records=[("USERS",)], last_description=[("name",)])
        ts._hook.dialect_name = "snowflake"

        result = asyncio.run(ts.call_tool("query", {"sql": "SHOW TABLES"}, ctx=MagicMock(), tool=MagicMock()))
        data = json.loads(result)
        assert "rows" in data
        _assert_executed(ts._hook, "SHOW TABLES")

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
        ts._hook.run.assert_not_called()

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


def _run_query(ts: SQLToolset, sql: str):
    return asyncio.run(ts.call_tool("query", {"sql": sql}, ctx=MagicMock(), tool=MagicMock()))


def _run_check(ts: SQLToolset, sql: str):
    return json.loads(
        asyncio.run(ts.call_tool("check_query", {"sql": sql}, ctx=MagicMock(), tool=MagicMock()))
    )


class TestSQLToolsetAllowedTablesQueryEnforcement:
    """``allowed_tables`` is enforced on the query/check_query tools, not just on discovery."""

    def test_query_allows_table_on_the_list(self):
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook(records=[(1,)], last_description=[("id",)])

        result = _run_query(ts, "SELECT id FROM orders")

        assert "rows" in json.loads(result)
        _assert_executed(ts._hook, "SELECT id FROM orders")

    def test_query_blocks_table_off_the_list(self):
        """The headline escape: querying a table that is not on the allow-list is refused."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, "SELECT * FROM secret_salaries")

        assert "not in the allowed tables list" in exc_info.value.message
        assert "secret_salaries" in exc_info.value.message
        ts._hook.run.assert_not_called()

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM (SELECT * FROM secret_salaries) x",
            "WITH s AS (SELECT * FROM secret_salaries) SELECT * FROM s",
            "SELECT * FROM orders JOIN secret_salaries ON orders.id = secret_salaries.id",
            "SELECT * FROM orders UNION SELECT * FROM secret_salaries",
            "SELECT * FROM secret_salaries WHERE id IN (SELECT id FROM orders)",
        ],
        ids=["subquery", "cte_body", "join", "union", "where_subquery"],
    )
    def test_query_blocks_disallowed_table_reached_indirectly(self, sql):
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, sql)

        assert "secret_salaries" in exc_info.value.message
        ts._hook.run.assert_not_called()

    def test_query_blocks_catalog_enumeration(self):
        """information_schema/pg_catalog are ordinary tables, so the allow-list blocks them too."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, "SELECT table_name FROM information_schema.tables")

        assert "information_schema.tables" in exc_info.value.message
        ts._hook.run.assert_not_called()

    def test_query_allows_cte_reference_not_mistaken_for_table(self):
        """A CTE whose name is not on the list is fine as long as its body stays allowed."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook(records=[(1,)], last_description=[("id",)])

        result = _run_query(ts, "WITH ranked AS (SELECT * FROM orders) SELECT * FROM ranked")

        assert "rows" in json.loads(result)

    def test_query_blocks_table_valued_function(self):
        """dblink reaches data through a path the list can't describe, so it is refused."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = "postgresql"

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, "SELECT * FROM dblink('host=evil', 'SELECT 1') AS t(x int)")

        assert "cannot be checked against allowed_tables" in exc_info.value.message
        ts._hook.run.assert_not_called()

    @pytest.mark.parametrize(
        "sql",
        [
            # File read (needs a privileged role) and table exfiltration (needs only a
            # read role) -- the two headline classes. The full function family is covered
            # at the unit layer (test_sql_validation.py); here we prove the wiring.
            "SELECT pg_read_file('/etc/passwd')",
            "SELECT query_to_xml('SELECT * FROM secret_salaries', true, false, '')",
        ],
        ids=["pg_read_file", "query_to_xml"],
    )
    def test_query_blocks_data_reaching_functions(self, sql):
        """A function reaching a file/other table/program carries no table node, so it
        would otherwise slip past the allow-list. It must be refused before execution,
        surfaced as a ModelRetry, with the query never handed to the database."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = "postgresql"

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, sql)

        assert "cannot be checked against allowed_tables" in exc_info.value.message
        ts._hook.run.assert_not_called()

    def test_query_blocks_copy_from_program_under_allow_writes(self):
        """COPY ... FROM PROGRAM is OS command execution. Even with allow_writes=True (only
        the table scan runs there), the allow-list must refuse it -- its allow-listed target
        table does not make the program channel safe."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"], allow_writes=True)
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = "postgresql"

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, "COPY orders FROM PROGRAM 'id > /tmp/x'")

        assert "cannot be checked against allowed_tables" in exc_info.value.message
        ts._hook.run.assert_not_called()

    def test_check_query_reports_data_reaching_function_invalid(self):
        """check_query surfaces the same rejection so the agent learns before executing."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = "postgresql"

        result = _run_check(ts, "SELECT pg_read_file('/etc/passwd')")

        assert result["valid"] is False
        assert "cannot be checked against allowed_tables" in result["error"]

    def test_query_allows_typed_builtins_over_allowed_table(self):
        """sqlglot-recognized builtins over an allowed table pass without allowed_functions."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook(records=[(2,)], last_description=[("n",)])
        ts._hook.dialect_name = "postgresql"

        result = _run_query(ts, "SELECT count(*), lower(name) FROM orders")

        assert "rows" in json.loads(result)
        _assert_executed(ts._hook, "SELECT count(*), lower(name) FROM orders")

    def test_query_blocks_unrecognized_function_by_default(self):
        """Fail-closed: a legit-but-unrecognized builtin (json_build_object) is refused
        until the operator permits it, so an unknown function can never slip through."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = "postgresql"

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, "SELECT json_build_object('id', id) FROM orders")

        assert "cannot be checked against allowed_tables" in exc_info.value.message
        ts._hook.run.assert_not_called()

    def test_query_allows_function_named_in_allowed_functions(self):
        """allowed_functions is the opt-in escape hatch for a safe unrecognized function."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"], allowed_functions=["json_build_object"])
        ts._hook = _make_mock_db_hook(records=[(1,)], last_description=[("obj",)])
        ts._hook.dialect_name = "postgresql"

        result = _run_query(ts, "SELECT json_build_object('id', id) FROM orders")

        assert "rows" in json.loads(result)
        _assert_executed(ts._hook, "SELECT json_build_object('id', id) FROM orders")

    def test_allowed_functions_does_not_permit_a_dangerous_sibling(self):
        """Permitting one function does not open the door to another unlisted one."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"], allowed_functions=["json_build_object"])
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = "postgresql"

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, "SELECT json_build_object('x', pg_read_file('/etc/passwd')) FROM orders")

        assert "cannot be checked against allowed_tables" in exc_info.value.message
        ts._hook.run.assert_not_called()

    def test_query_blocks_show_when_allowlist_active(self):
        ts = SQLToolset("sf_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = "snowflake"

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, "SHOW TABLES")

        assert "cannot be checked against allowed_tables" in exc_info.value.message
        ts._hook.run.assert_not_called()

    def test_query_blocks_describe_of_disallowed_table(self):
        ts = SQLToolset("sf_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = "snowflake"

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, "DESCRIBE TABLE secret_salaries")

        assert "secret_salaries" in exc_info.value.message
        ts._hook.run.assert_not_called()

    def test_query_allows_describe_of_allowed_table(self):
        ts = SQLToolset("sf_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook(records=[("id", "INT")], last_description=[("name",), ("type",)])
        ts._hook.dialect_name = "snowflake"

        result = _run_query(ts, "DESCRIBE TABLE orders")

        assert "rows" in json.loads(result)
        _assert_executed(ts._hook, "DESCRIBE TABLE orders")

    def test_query_allows_schema_qualified_table_on_list(self):
        ts = SQLToolset("sf", allowed_tables=["MODEL_CRM.SF_ASTRO_ORGS"])
        ts._hook = _make_mock_db_hook(records=[(1,)], last_description=[("id",)])
        ts._hook.dialect_name = "snowflake"

        result = _run_query(ts, "SELECT * FROM MODEL_CRM.SF_ASTRO_ORGS")

        assert "rows" in json.loads(result)

    def test_query_unqualified_resolves_to_default_schema(self):
        """``public.orders`` and ``orders`` denote the same table when schema='public'."""
        ts = SQLToolset("pg", allowed_tables=["orders"], schema="public")
        ts._hook = _make_mock_db_hook(records=[(1,)], last_description=[("id",)])

        # Qualifying with the default schema must still match the bare allow-list entry.
        result = _run_query(ts, "SELECT * FROM public.orders")
        assert "rows" in json.loads(result)

    def test_no_allowlist_leaves_queries_unrestricted(self):
        """Without allowed_tables the query tool behaves exactly as before (allow-all)."""
        ts = SQLToolset("pg_default")
        ts._hook = _make_mock_db_hook(records=[(1,)], last_description=[("id",)])

        result = _run_query(ts, "SELECT * FROM anything_at_all")

        assert "rows" in json.loads(result)
        _assert_executed(ts._hook, "SELECT * FROM anything_at_all")

    def test_check_query_reports_disallowed_table_as_invalid(self):
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()

        data = _run_check(ts, "SELECT * FROM secret_salaries")

        assert data["valid"] is False
        assert "secret_salaries" in data["error"]

    def test_check_query_valid_for_allowed_table(self):
        ts = SQLToolset("pg_default", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook()

        assert _run_check(ts, "SELECT * FROM orders")["valid"] is True

    def test_writes_still_bounded_by_allowed_tables(self):
        """allow_writes widens the statement types, but the allow-list still scopes the target."""
        ts = SQLToolset("pg_default", allowed_tables=["orders"], allow_writes=True)
        ts._hook = _make_mock_db_hook(records=[], last_description=None)

        # An allowed target is written.
        _run_query(ts, "INSERT INTO orders (id) VALUES (1)")
        _assert_executed(ts._hook, "INSERT INTO orders (id) VALUES (1)")

        # A disallowed target is refused before execution.
        ts._hook.run.reset_mock()
        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, "INSERT INTO secret_salaries (id) VALUES (1)")
        assert "secret_salaries" in exc_info.value.message
        ts._hook.run.assert_not_called()

    def test_writes_reject_dynamic_sql_the_parser_cannot_inspect(self):
        """allow_writes skips the read-only validator, so the allow-list must still
        refuse dynamic SQL (EXEC/EXECUTE) whose table access is opaque."""
        ts = SQLToolset("mssql_default", allowed_tables=["orders"], allow_writes=True)
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = "mssql"

        with pytest.raises(ModelRetry) as exc_info:
            _run_query(ts, "EXEC sp_who")

        assert "cannot be checked against allowed_tables" in exc_info.value.message
        ts._hook.run.assert_not_called()


class TestSQLToolsetAllowedTablesBypassRegressions:
    """Regression tests for bypasses found by adversarial red-teaming of the allow-list."""

    @pytest.mark.parametrize(
        ("sql", "dialect", "allow_writes"),
        [
            # CTE scope: a same-named CTE in an inner/sibling scope must not hide the real table.
            (
                "SELECT * FROM secret_salaries WHERE id IN "
                "(WITH secret_salaries AS (SELECT 1 id) SELECT id FROM secret_salaries)",
                "postgresql",
                False,
            ),
            # Non-recursive CTE is not in scope within its own body.
            (
                "WITH secret_salaries AS (SELECT * FROM secret_salaries) SELECT * FROM secret_salaries",
                "postgresql",
                False,
            ),
            # A CTE may only reference earlier siblings; a later-defined name is the real table.
            (
                "WITH a AS (SELECT * FROM secret_salaries), secret_salaries AS (SELECT 1 id) SELECT * FROM a",
                "postgresql",
                False,
            ),
            # Cross-database / catalog qualifier the schema.table allow-list cannot describe.
            ("SELECT * FROM secretdb.public.orders", "snowflake", False),
            ("SELECT * FROM secret_salaries..orders", "mssql", False),
            # MySQL executable comments execute on the engine but sqlglot treats them as inert.
            ("SELECT * FROM orders/*!UNION SELECT * FROM secret_salaries*/", "mysql", False),
            ("SELECT id FROM orders /*!50000 UNION SELECT id FROM secret_salaries */", "mysql", False),
            # TABLE <name> shorthand (mis-parsed) and TABLE('name') row source (string-named).
            ("TABLE secret_salaries UNION SELECT * FROM orders", "postgresql", False),
            ("SELECT * FROM TABLE('secret_salaries')", "snowflake", False),
            # Write-mode CTE shadowing the DML target.
            ("WITH secret_salaries AS (SELECT 1) DELETE FROM secret_salaries", "postgresql", True),
            # Quoted identifier is case-distinct on the engine but case-folds into the list.
            ('SELECT * FROM "Orders"', "postgresql", False),
            # A DML source CTE whose body reads an off-list table is still caught.
            (
                "WITH src AS (SELECT * FROM secret_salaries) INSERT INTO orders SELECT * FROM src",
                "postgresql",
                True,
            ),
        ],
        ids=[
            "cte_inner_shadow",
            "cte_self_body",
            "cte_forward_ref",
            "catalog_cross_db",
            "mssql_empty_middle",
            "mysql_exec_comment",
            "mysql_versioned_comment",
            "table_shorthand",
            "table_row_source",
            "write_cte_target",
            "quoted_case_distinct",
            "dml_cte_body_reads_offlist",
        ],
    )
    def test_known_bypasses_are_rejected(self, sql, dialect, allow_writes):
        ts = SQLToolset("c", allowed_tables=["orders"], allow_writes=allow_writes)
        ts._hook = _make_mock_db_hook()
        ts._hook.dialect_name = dialect

        with pytest.raises(ModelRetry):
            _run_query(ts, sql)
        ts._hook.run.assert_not_called()

    def test_legit_cte_over_allowed_table_still_runs(self):
        """The scope-aware fix must not false-reject a genuine CTE over an allowed table."""
        ts = SQLToolset("c", allowed_tables=["orders"])
        ts._hook = _make_mock_db_hook(records=[(1,)], last_description=[("id",)])

        result = _run_query(ts, "WITH ranked AS (SELECT * FROM orders) SELECT * FROM ranked")

        assert "rows" in json.loads(result)
        assert ts._hook.run.call_count == 1

    def test_dml_with_cte_source_over_allowed_table_runs(self):
        """A CTE used as a DML source must not be mistaken for a disallowed base table."""
        ts = SQLToolset("c", allowed_tables=["orders"], allow_writes=True)
        ts._hook = _make_mock_db_hook(records=[], last_description=None)

        sql = "WITH src AS (SELECT * FROM orders) INSERT INTO orders SELECT * FROM src"
        _run_query(ts, sql)

        _assert_executed(ts._hook, sql)
