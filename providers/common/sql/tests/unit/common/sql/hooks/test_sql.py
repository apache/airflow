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

#
from __future__ import annotations

import inspect
import logging
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pandas as pd
import polars as pl
import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.common.sql.dialects.dialect import Dialect
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.hooks.sql import DbApiHook, resolve_dialects

from tests_common.test_utils.common_sql import mock_db_hook
from tests_common.test_utils.providers import get_provider_min_airflow_version

TASK_ID = "sql-operator"
HOST = "host"
DEFAULT_CONN_ID = "sqlite_default"
PASSWORD = "password"


class DBApiHookForTests(DbApiHook):
    conn_name_attr = "conn_id"
    get_conn = MagicMock(name="conn")


@pytest.fixture(autouse=True)
def create_connection(create_connection_without_db):
    create_connection_without_db(
        Connection(
            conn_id=DEFAULT_CONN_ID,
            conn_type="sqlite",
            host=HOST,
            login=None,
            password=PASSWORD,
            extra=None,
        )
    )


def get_cursor_descriptions(fields: list[str]) -> list[tuple[str]]:
    return [(field,) for field in fields]


def _make_async_cursor(descriptions_per_call, fetchall_results):
    """Cursor mock whose description advances on each async execute call."""
    cur = MagicMock()
    cur.rowcount = 2
    cur.description = None
    call_idx = 0

    async def _execute(*args, **kwargs):
        nonlocal call_idx
        cur.description = descriptions_per_call[call_idx]
        call_idx += 1

    cur.execute = _execute
    cur.fetchall.side_effect = fetchall_results
    return cur


@pytest.mark.db_test
@pytest.mark.parametrize(
    (
        "return_last",
        "split_statements",
        "sql",
        "cursor_calls",
        "cursor_descriptions",
        "cursor_results",
        "hook_descriptions",
        "hook_results",
    ),
    [
        pytest.param(
            True,
            False,
            "select * from test.test",
            ["select * from test.test"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[1, 2], [11, 12]],
            id="The return_last set and no split statements set on single query in string",
        ),
        pytest.param(
            False,
            False,
            "select * from test.test;",
            ["select * from test.test;"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[1, 2], [11, 12]],
            id="The return_last not set and no split statements set on single query in string",
        ),
        pytest.param(
            True,
            True,
            "select * from test.test;",
            ["select * from test.test;"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[1, 2], [11, 12]],
            id="The return_last set and split statements set on single query in string",
        ),
        pytest.param(
            False,
            True,
            "select * from test.test;",
            ["select * from test.test;"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[[1, 2], [11, 12]]],
            id="The return_last not set and split statements set on single query in string",
        ),
        pytest.param(
            True,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test;", "select * from test.test2;"],
            [["id", "value"], ["id2", "value2"]],
            ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
            [[("id2",), ("value2",)]],
            [[3, 4], [13, 14]],
            id="The return_last set and split statements set on multiple queries in string",
        ),  # Failing
        pytest.param(
            False,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test;", "select * from test.test2;"],
            [["id", "value"], ["id2", "value2"]],
            ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [[[1, 2], [11, 12]], [[3, 4], [13, 14]]],
            id="The return_last not set and split statements set on multiple queries in string",
        ),
        pytest.param(
            True,
            True,
            ["select * from test.test;"],
            ["select * from test.test"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[[1, 2], [11, 12]]],
            id="The return_last set on single query in list",
        ),
        pytest.param(
            False,
            True,
            ["select * from test.test;"],
            ["select * from test.test"],
            [["id", "value"]],
            ([[1, 2], [11, 12]],),
            [[("id",), ("value",)]],
            [[[1, 2], [11, 12]]],
            id="The return_last not set on single query in list",
        ),
        pytest.param(
            True,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test", "select * from test.test2"],
            [["id", "value"], ["id2", "value2"]],
            ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
            [[("id2",), ("value2",)]],
            [[3, 4], [13, 14]],
            id="The return_last set on multiple queries in list",
        ),
        pytest.param(
            False,
            True,
            "select * from test.test;select * from test.test2;",
            ["select * from test.test", "select * from test.test2"],
            [["id", "value"], ["id2", "value2"]],
            ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
            [[("id",), ("value",)], [("id2",), ("value2",)]],
            [[[1, 2], [11, 12]], [[3, 4], [13, 14]]],
            id="The return_last not set on multiple queries not set",
        ),
    ],
)
def test_query(
    return_last,
    split_statements,
    sql,
    cursor_calls,
    cursor_descriptions,
    cursor_results,
    hook_descriptions,
    hook_results,
):
    modified_descriptions = [
        get_cursor_descriptions(cursor_description) for cursor_description in cursor_descriptions
    ]
    dbapi_hook = DBApiHookForTests()
    dbapi_hook.get_conn.return_value.cursor.return_value.rowcount = 2
    dbapi_hook.get_conn.return_value.cursor.return_value._description_index = 0

    def mock_execute(*args, **kwargs):
        # the run method accesses description property directly, and we need to modify it after
        # every execute, to make sure that different descriptions are returned. I could not find easier
        # method with mocking
        dbapi_hook.get_conn.return_value.cursor.return_value.description = modified_descriptions[
            dbapi_hook.get_conn.return_value.cursor.return_value._description_index
        ]
        dbapi_hook.get_conn.return_value.cursor.return_value._description_index += 1

    dbapi_hook.get_conn.return_value.cursor.return_value.execute = mock_execute
    dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = cursor_results
    results = dbapi_hook.run(
        sql=sql, handler=fetch_all_handler, return_last=return_last, split_statements=split_statements
    )

    assert dbapi_hook.descriptions == hook_descriptions
    assert dbapi_hook.last_description == hook_descriptions[-1]
    assert results == hook_results

    dbapi_hook.get_conn.return_value.cursor.return_value.close.assert_called()


class TestDbApiHook:
    def setup_method(self, **kwargs):
        logging.root.disabled = True

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "empty_statement",
        [
            pytest.param([], id="Empty list"),
            pytest.param("", id="Empty string"),
            pytest.param("\n", id="Only EOL"),
        ],
    )
    def test_no_query(self, empty_statement):
        dbapi_hook = mock_db_hook(DbApiHook)
        with pytest.raises(ValueError, match="List of SQL statements is empty"):
            dbapi_hook.run(sql=empty_statement)

    @pytest.mark.db_test
    def test_placeholder_config_from_extra(self):
        dbapi_hook = mock_db_hook(DbApiHook, conn_params={"extra": {"placeholder": "?"}})
        assert dbapi_hook.placeholder == "?"

    @pytest.mark.db_test
    def test_placeholder_config_from_extra_when_not_in_default_sql_placeholders(self, caplog):
        with caplog.at_level(logging.WARNING, logger="airflow.providers.common.sql.hooks.test_sql"):
            dbapi_hook = mock_db_hook(DbApiHook, conn_params={"extra": {"placeholder": "!"}})
            assert dbapi_hook.placeholder == "%s"
            assert (
                "Placeholder '!' defined in Connection 'default_conn_id' is not listed in 'DEFAULT_SQL_PLACEHOLDERS' "
                f"and got ignored. Falling back to the default placeholder '{DbApiHook._placeholder}'."
                in caplog.text
            )

    @pytest.mark.db_test
    def test_placeholder_multiple_times_and_make_sure_connection_is_only_invoked_once(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        for _ in range(10):
            assert dbapi_hook.placeholder == "%s"
        assert dbapi_hook.connection_invocations == 1

    @pytest.mark.db_test
    def test_escape_column_names(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        assert not dbapi_hook.escape_column_names

    @pytest.mark.db_test
    def test_dialect_name(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        assert dbapi_hook.dialect_name == "default"

    @pytest.mark.db_test
    def test_dialect(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        assert isinstance(dbapi_hook.dialect, Dialect)

    @pytest.mark.db_test
    def test_when_provider_min_airflow_version_is_3_0_or_higher_remove_obsolete_code(self):
        """
        Once this test starts failing due to the fact that the minimum Airflow version is now 3.0.0 or higher
        for this provider, you should remove the obsolete code in the get_dialects method of the DbApiHook
        and remove this test.  This test was added to make sure to not forget to remove the fallback code
        for backward compatibility with Airflow 2.8.x which isn't need anymore once this provider depends on
        Airflow 3.0.0 or higher.
        """
        min_airflow_version = get_provider_min_airflow_version("apache-airflow-providers-common-sql")

        # Check if the current Airflow version is 3.0.0 or higher
        if min_airflow_version[0] >= 3:
            method_source = inspect.getsource(resolve_dialects)
            raise AirflowProviderDeprecationWarning(
                f"Check TODO's to remove obsolete code in resolve_dialects method:\n\r\n\r\t\t\t{method_source}"
            )

    @pytest.mark.db_test
    def test_uri(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        assert dbapi_hook.get_uri() == "//login:password@host:1234/schema"

    @pytest.mark.db_test
    def test_uri_with_schema(self):
        dbapi_hook = mock_db_hook(DbApiHook, conn_params={"schema": "other_schema"})
        assert dbapi_hook.get_uri() == "//login:password@host:1234/other_schema"

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("df_type", "expected_type"),
        [
            ("test_default_df_type", pd.DataFrame),
            ("pandas", pd.DataFrame),
            ("polars", pl.DataFrame),
        ],
    )
    def test_get_df_with_df_type(db, df_type, expected_type):
        dbapi_hook = mock_db_hook(DbApiHook)
        if df_type == "test_default_df_type":
            df = dbapi_hook.get_df("SQL")
            assert isinstance(df, pd.DataFrame)
        else:
            df = dbapi_hook.get_df("SQL", df_type=df_type)
            assert isinstance(df, expected_type)

    @pytest.mark.db_test
    def test_build_conn_kwargs_basic_mapping(self):
        hook = mock_db_hook(DbApiHook)
        db = Connection(
            conn_id="c",
            conn_type="test",
            host="dbhost",
            login="user",
            password="secret",
            schema="mydb",
            port=5432,
        )
        result = hook._build_conn_kwargs_from_airflow_connection(db)
        assert result["host"] == "dbhost"
        assert result["port"] == 5432
        assert result["username"] == "user"
        assert result["password"] == "secret"
        assert result["schema"] == "mydb"
        assert result["raw_connection"] is db

    @pytest.mark.db_test
    def test_build_conn_kwargs_none_values_become_empty(self):
        hook = mock_db_hook(DbApiHook)
        db = Connection(conn_id="c", conn_type="test")
        result = hook._build_conn_kwargs_from_airflow_connection(db)
        assert result["host"] == ""
        assert result["username"] == ""
        assert result["schema"] == ""
        assert result["port"] is None

    @pytest.mark.db_test
    def test_build_conn_kwargs_uses_extra_database(self):
        hook = mock_db_hook(DbApiHook)
        db = Connection(conn_id="c", conn_type="test", extra='{"database": "myschema"}')
        assert hook._build_conn_kwargs_from_airflow_connection(db)["database"] == "myschema"

    @pytest.mark.db_test
    def test_build_conn_kwargs_falls_back_to_dbname(self):
        hook = mock_db_hook(DbApiHook)
        db = Connection(conn_id="c", conn_type="test", extra='{"dbname": "altdb"}')
        assert hook._build_conn_kwargs_from_airflow_connection(db)["database"] == "altdb"

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @patch("airflow.providers.common.sql.hooks.sql.get_async_connection", new_callable=AsyncMock)
    async def test_aget_conn_success(self, mock_get_async_connection):
        hook = mock_db_hook(DbApiHook)
        mock_get_async_connection.return_value = Connection(conn_id="c", conn_type="test")
        mock_db_conn = MagicMock()
        hook.connector = AsyncMock()
        hook.connector.connect.return_value = mock_db_conn
        assert await hook.aget_conn() is mock_db_conn
        hook.connector.connect.assert_awaited_once()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @patch("airflow.providers.common.sql.hooks.sql.get_async_connection", new_callable=AsyncMock)
    async def test_aget_conn_raises_without_connector(self, mock_get_async_connection):
        hook = mock_db_hook(DbApiHook)
        mock_get_async_connection.return_value = Connection(conn_id="c", conn_type="test")
        hook.connector = None
        with pytest.raises(RuntimeError, match="didn't have `self.connector` set"):
            await hook.aget_conn()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @patch("airflow.providers.common.sql.hooks.sql.get_async_connection", new_callable=AsyncMock)
    async def test_aget_conn_caches_airflow_connection(self, mock_get_async_connection):
        hook = mock_db_hook(DbApiHook)
        mock_get_async_connection.return_value = Connection(conn_id="c", conn_type="test")
        hook.connector = AsyncMock()
        hook.connector.connect.return_value = MagicMock()
        await hook.aget_conn()
        await hook.aget_conn()
        mock_get_async_connection.assert_awaited_once()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_call_awaits_coroutine_function(self):
        hook = mock_db_hook(DbApiHook)

        async def fn(x):
            return x * 3

        assert await hook._call(fn, 4) == 12

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_call_runs_sync_function(self):
        hook = mock_db_hook(DbApiHook)

        def fn(x):
            return x + 1

        assert await hook._call(fn, 9) == 10

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_acreate_connection_yields_conn(self):
        hook = mock_db_hook(DbApiHook)
        mock_conn = MagicMock()
        mock_conn.close = MagicMock()
        with patch.object(hook, "aget_conn", AsyncMock(return_value=mock_conn)):
            async with hook._acreate_autocommit_connection() as conn:
                assert conn is mock_conn

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_acreate_connection_calls_close(self):
        hook = mock_db_hook(DbApiHook)
        mock_conn = MagicMock()
        mock_conn.close = MagicMock()
        del mock_conn.aclose
        with patch.object(hook, "aget_conn", AsyncMock(return_value=mock_conn)):
            async with hook._acreate_autocommit_connection():
                pass
        mock_conn.close.assert_called_once()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_acreate_connection_prefers_aclose(self):
        hook = mock_db_hook(DbApiHook)
        mock_conn = MagicMock()
        mock_conn.aclose = AsyncMock()
        mock_conn.close = MagicMock()
        with patch.object(hook, "aget_conn", AsyncMock(return_value=mock_conn)):
            async with hook._acreate_autocommit_connection():
                pass
        mock_conn.aclose.assert_awaited_once()
        mock_conn.close.assert_not_called()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_acreate_connection_sets_sync_autocommit(self):
        hook = mock_db_hook(DbApiHook)
        hook.supports_autocommit = True
        mock_conn = MagicMock()
        mock_conn.close = MagicMock()
        del mock_conn.set_autocommit
        with patch.object(hook, "aget_conn", AsyncMock(return_value=mock_conn)):
            async with hook._acreate_autocommit_connection(autocommit=True):
                pass
        assert mock_conn.autocommit is True

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_acreate_connection_sets_async_autocommit(self):
        hook = mock_db_hook(DbApiHook)
        hook.supports_autocommit = True
        mock_conn = MagicMock()
        mock_conn.close = MagicMock()
        mock_conn.set_autocommit = AsyncMock()
        with patch.object(hook, "aget_conn", AsyncMock(return_value=mock_conn)):
            async with hook._acreate_autocommit_connection(autocommit=True):
                pass
        mock_conn.set_autocommit.assert_awaited_once_with(True)

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_aget_cursor_raises_without_cursor_method(self):
        hook = mock_db_hook(DbApiHook)
        with pytest.raises(TypeError, match="has no cursor\\(\\) method"):
            async with hook._aget_cursor(MagicMock(spec=[])):
                pass

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_aget_cursor_uses_async_context_manager(self):
        hook = mock_db_hook(DbApiHook)
        mock_cur = MagicMock()
        cursor_cm = AsyncMock()
        cursor_cm.__aenter__ = AsyncMock(return_value=mock_cur)
        cursor_cm.__aexit__ = AsyncMock(return_value=False)
        conn = MagicMock()
        conn.cursor.return_value = cursor_cm
        async with hook._aget_cursor(conn) as cur:
            assert cur is mock_cur

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_aget_cursor_awaits_cursor_then_uses_async_cm(self):
        hook = mock_db_hook(DbApiHook)
        mock_cur = MagicMock()
        cursor_cm = AsyncMock()
        cursor_cm.__aenter__ = AsyncMock(return_value=mock_cur)
        cursor_cm.__aexit__ = AsyncMock(return_value=False)

        async def async_cursor():
            return cursor_cm

        conn = MagicMock()
        conn.cursor = async_cursor
        async with hook._aget_cursor(conn) as cur:
            assert cur is mock_cur

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_aget_cursor_yields_cursor_with_async_execute(self):
        hook = mock_db_hook(DbApiHook)
        mock_cur = MagicMock()
        mock_cur.execute = AsyncMock()
        mock_cur.close = MagicMock()
        del mock_cur.__aenter__
        del mock_cur.__aexit__
        del mock_cur.aclose
        conn = MagicMock()
        conn.cursor.return_value = mock_cur
        async with hook._aget_cursor(conn) as cur:
            assert cur is mock_cur
        mock_cur.close.assert_called_once()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_aget_cursor_raises_for_unsupported_type(self):
        hook = mock_db_hook(DbApiHook)
        mock_cur = MagicMock()
        mock_cur.execute = MagicMock()
        del mock_cur.__aenter__
        del mock_cur.__aexit__
        conn = MagicMock()
        conn.cursor.return_value = mock_cur
        with pytest.raises(RuntimeError, match="Unsupported cursor type"):
            async with hook._aget_cursor(conn) as _:
                pass

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_arun_command_no_params(self):
        hook = mock_db_hook(DbApiHook)
        cur = AsyncMock()
        cur.rowcount = 0
        await hook._arun_command(cur, "SELECT 1", None)
        cur.execute.assert_awaited_once_with("SELECT 1")

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_arun_command_with_params(self):
        hook = mock_db_hook(DbApiHook)
        cur = AsyncMock()
        cur.rowcount = 3
        await hook._arun_command(cur, "SELECT * FROM t WHERE id=%s", (42,))
        cur.execute.assert_awaited_once_with("SELECT * FROM t WHERE id=%s", (42,))

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_arun_command_logs_rowcount(self):
        hook = mock_db_hook(DbApiHook)
        cur = AsyncMock()
        cur.rowcount = 5
        mock_log = MagicMock()
        with patch.object(hook, "_log", mock_log):
            await hook._arun_command(cur, "DELETE FROM t", None)
        mock_log.info.assert_any_call("Rows affected: %s", 5)

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_arun_command_skips_rowcount_when_negative(self):
        hook = mock_db_hook(DbApiHook)
        cur = AsyncMock()
        cur.rowcount = -1
        mock_log = MagicMock()
        with patch.object(hook, "_log", mock_log):
            await hook._arun_command(cur, "SELECT 1", None)
        assert not any(call.args[0] == "Rows affected: %s" for call in mock_log.info.call_args_list)

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @pytest.mark.parametrize("empty_sql", [[], "", "\n"])
    async def test_arun_empty_sql_raises(self, empty_sql):
        hook = mock_db_hook(DbApiHook)
        mock_conn = MagicMock()
        mock_conn.autocommit = False

        @asynccontextmanager
        async def _conn_cm(autocommit=False):
            yield mock_conn

        @asynccontextmanager
        async def _cursor_cm(conn):
            yield AsyncMock()

        with (
            patch.object(hook, "_acreate_autocommit_connection", _conn_cm),
            patch.object(hook, "_aget_cursor", _cursor_cm),
        ):
            with pytest.raises(ValueError, match="List of SQL statements is empty"):
                await hook.arun(sql=empty_sql)

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_arun_no_handler_returns_none(self):
        hook = mock_db_hook(DbApiHook)
        cur = AsyncMock()
        cur.rowcount = 0
        mock_conn = MagicMock()
        mock_conn.autocommit = False
        mock_conn.commit = MagicMock()

        @asynccontextmanager
        async def _conn_cm(autocommit=False):
            yield mock_conn

        @asynccontextmanager
        async def _cursor_cm(conn):
            yield cur

        with (
            patch.object(hook, "_acreate_autocommit_connection", _conn_cm),
            patch.object(hook, "_aget_cursor", _cursor_cm),
        ):
            assert await hook.arun(sql="INSERT INTO t VALUES (1)") is None

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        (
            "return_last",
            "split_statements",
            "sql",
            "cursor_descriptions",
            "cursor_results",
            "hook_descriptions",
            "hook_results",
        ),
        [
            pytest.param(
                True,
                False,
                "select * from test.test",
                [["id", "value"]],
                ([[1, 2], [11, 12]],),
                [[("id",), ("value",)]],
                [[1, 2], [11, 12]],
                id="The return_last set and no split statements set on single query in string",
            ),
            pytest.param(
                False,
                False,
                "select * from test.test;",
                [["id", "value"]],
                ([[1, 2], [11, 12]],),
                [[("id",), ("value",)]],
                [[1, 2], [11, 12]],
                id="The return_last not set and no split statements set on single query in string",
            ),
            pytest.param(
                True,
                True,
                "select * from test.test;",
                [["id", "value"]],
                ([[1, 2], [11, 12]],),
                [[("id",), ("value",)]],
                [[1, 2], [11, 12]],
                id="The return_last set and split statements set on single query in string",
            ),
            pytest.param(
                False,
                True,
                "select * from test.test;",
                [["id", "value"]],
                ([[1, 2], [11, 12]],),
                [[("id",), ("value",)]],
                [[[1, 2], [11, 12]]],
                id="The return_last not set and split statements set on single query in string",
            ),
            pytest.param(
                True,
                True,
                "select * from test.test;select * from test.test2;",
                [["id", "value"], ["id2", "value2"]],
                ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
                [[("id2",), ("value2",)]],
                [[3, 4], [13, 14]],
                id="The return_last set and split statements set on multiple queries in string",
            ),
            pytest.param(
                False,
                True,
                "select * from test.test;select * from test.test2;",
                [["id", "value"], ["id2", "value2"]],
                ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
                [[("id",), ("value",)], [("id2",), ("value2",)]],
                [[[1, 2], [11, 12]], [[3, 4], [13, 14]]],
                id="The return_last not set and split statements set on multiple queries in string",
            ),
            pytest.param(
                True,
                True,
                ["select * from test.test;"],
                [["id", "value"]],
                ([[1, 2], [11, 12]],),
                [[("id",), ("value",)]],
                [[[1, 2], [11, 12]]],
                id="The return_last set on single query in list",
            ),
            pytest.param(
                False,
                True,
                ["select * from test.test;"],
                [["id", "value"]],
                ([[1, 2], [11, 12]],),
                [[("id",), ("value",)]],
                [[[1, 2], [11, 12]]],
                id="The return_last not set on single query in list",
            ),
            pytest.param(
                True,
                True,
                "select * from test.test;select * from test.test2;",
                [["id", "value"], ["id2", "value2"]],
                ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
                [[("id2",), ("value2",)]],
                [[3, 4], [13, 14]],
                id="The return_last set on multiple queries in list",
            ),
            pytest.param(
                False,
                True,
                "select * from test.test;select * from test.test2;",
                [["id", "value"], ["id2", "value2"]],
                ([[1, 2], [11, 12]], [[3, 4], [13, 14]]),
                [[("id",), ("value",)], [("id2",), ("value2",)]],
                [[[1, 2], [11, 12]], [[3, 4], [13, 14]]],
                id="The return_last not set on multiple queries not set",
            ),
        ],
    )
    async def test_arun_query(
        self,
        return_last,
        split_statements,
        sql,
        cursor_descriptions,
        cursor_results,
        hook_descriptions,
        hook_results,
    ):
        modified = [get_cursor_descriptions(d) for d in cursor_descriptions]
        cur = _make_async_cursor(modified, cursor_results)
        mock_conn = MagicMock()
        mock_conn.autocommit = False
        mock_conn.commit = MagicMock()
        hook = mock_db_hook(DbApiHook)

        @asynccontextmanager
        async def _conn_cm(autocommit=False):
            yield mock_conn

        @asynccontextmanager
        async def _cursor_cm(conn):
            yield cur

        with (
            patch.object(hook, "_acreate_autocommit_connection", _conn_cm),
            patch.object(hook, "_aget_cursor", _cursor_cm),
        ):
            results = await hook.arun(
                sql=sql,
                handler=fetch_all_handler,
                return_last=return_last,
                split_statements=split_statements,
            )

        assert hook.descriptions == hook_descriptions
        assert hook.last_description == hook_descriptions[-1]
        assert results == hook_results

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_arun_commits_on_success(self):
        hook = mock_db_hook(DbApiHook)
        cur = AsyncMock()
        cur.rowcount = 0
        mock_conn = MagicMock()
        mock_conn.autocommit = False
        mock_conn.commit = MagicMock()

        @asynccontextmanager
        async def _conn_cm(autocommit=False):
            yield mock_conn

        @asynccontextmanager
        async def _cursor_cm(conn):
            yield cur

        with (
            patch.object(hook, "_acreate_autocommit_connection", _conn_cm),
            patch.object(hook, "_aget_cursor", _cursor_cm),
        ):
            await hook.arun(sql="INSERT INTO t VALUES (1)")

        mock_conn.commit.assert_called_once()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_arun_rollback_on_exception(self):
        hook = mock_db_hook(DbApiHook)
        cur = AsyncMock()
        cur.execute.side_effect = RuntimeError("DB error")
        mock_conn = MagicMock()
        mock_conn.autocommit = False
        mock_conn.rollback = MagicMock()

        @asynccontextmanager
        async def _conn_cm(autocommit=False):
            yield mock_conn

        @asynccontextmanager
        async def _cursor_cm(conn):
            yield cur

        with (
            patch.object(hook, "_acreate_autocommit_connection", _conn_cm),
            patch.object(hook, "_aget_cursor", _cursor_cm),
        ):
            with pytest.raises(RuntimeError, match="DB error"):
                await hook.arun(sql="SELECT fail")

        mock_conn.rollback.assert_called_once()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_arun_awaits_async_handler(self):
        hook = mock_db_hook(DbApiHook)
        rows = [(1, "a"), (2, "b")]
        cur = AsyncMock()
        cur.rowcount = 2
        cur.description = [("id",), ("name",)]
        mock_conn = MagicMock()
        mock_conn.autocommit = False
        mock_conn.commit = MagicMock()

        async def async_handler(cursor):
            return rows

        @asynccontextmanager
        async def _conn_cm(autocommit=False):
            yield mock_conn

        @asynccontextmanager
        async def _cursor_cm(conn):
            yield cur

        with (
            patch.object(hook, "_acreate_autocommit_connection", _conn_cm),
            patch.object(hook, "_aget_cursor", _cursor_cm),
        ):
            result = await hook.arun(sql="SELECT 1", handler=async_handler)

        assert result == rows


class TestDbApiHookGetSqlalchemyEngine:
    """SQLAlchemy resolves a bare ``postgresql`` scheme to the psycopg2 DB-API by default, which
    may not be installed now that it's an optional extra of apache-airflow-providers-postgres."""

    @pytest.mark.db_test
    @patch("airflow.providers.common.sql.hooks.sql._is_sqlalchemy_2", return_value=True)
    @patch("airflow.providers.common.sql.hooks.sql.find_spec")
    @patch("airflow.providers.common.sql.hooks.sql.create_engine")
    def test_falls_back_to_psycopg_when_psycopg2_import_fails(
        self, mock_create_engine, mock_find_spec, mock_is_sqlalchemy_2, caplog
    ):
        mock_find_spec.return_value = object()
        mock_create_engine.side_effect = [ModuleNotFoundError("No module named 'psycopg2'"), MagicMock()]
        dbapi_hook = mock_db_hook(DbApiHook, conn_params={"conn_type": "postgresql"})

        dbapi_hook.get_sqlalchemy_engine()

        assert mock_create_engine.call_count == 2
        retried_url = mock_create_engine.call_args.kwargs["url"]
        assert retried_url.drivername == "postgresql+psycopg"
        assert (
            "SQLAlchemy could not load a DB-API driver for the bare 'postgresql://' URL; "
            "retrying with psycopg (v3) ('postgresql+psycopg')."
        ) in caplog.text

    @pytest.mark.db_test
    @patch("airflow.providers.common.sql.hooks.sql._is_sqlalchemy_2", return_value=False)
    @patch("airflow.providers.common.sql.hooks.sql.find_spec")
    @patch("airflow.providers.common.sql.hooks.sql.create_engine")
    def test_falls_back_to_psycopg2_when_sqlalchemy_below_2(
        self, mock_create_engine, mock_find_spec, mock_is_sqlalchemy_2, caplog
    ):
        # SQLAlchemy 1.4 (shipped with Airflow 2.11) has no native "postgresql+psycopg" dialect,
        # so even when psycopg (v3) is importable the retry must use psycopg2, not psycopg.
        mock_find_spec.return_value = object()
        mock_create_engine.side_effect = [ModuleNotFoundError("No module named 'psycopg2'"), MagicMock()]
        dbapi_hook = mock_db_hook(DbApiHook, conn_params={"conn_type": "postgresql"})

        dbapi_hook.get_sqlalchemy_engine()

        assert mock_create_engine.call_count == 2
        retried_url = mock_create_engine.call_args.kwargs["url"]
        assert retried_url.drivername == "postgresql+psycopg2"
        assert (
            "SQLAlchemy could not load a DB-API driver for the bare 'postgresql://' URL; "
            "retrying with 'postgresql+psycopg2'."
        ) in caplog.text

    @pytest.mark.db_test
    @patch("airflow.providers.common.sql.hooks.sql.find_spec")
    @patch("airflow.providers.common.sql.hooks.sql.create_engine")
    def test_falls_back_to_psycopg2_when_psycopg_unavailable(
        self, mock_create_engine, mock_find_spec, caplog
    ):
        mock_find_spec.side_effect = lambda name: None if name == "psycopg" else object()
        mock_create_engine.side_effect = [ModuleNotFoundError("No module named 'psycopg2'"), MagicMock()]
        dbapi_hook = mock_db_hook(DbApiHook, conn_params={"conn_type": "postgresql"})

        dbapi_hook.get_sqlalchemy_engine()

        assert mock_create_engine.call_count == 2
        retried_url = mock_create_engine.call_args.kwargs["url"]
        assert retried_url.drivername == "postgresql+psycopg2"
        assert (
            "SQLAlchemy could not load a DB-API driver for the bare 'postgresql://' URL; "
            "retrying with 'postgresql+psycopg2'."
        ) in caplog.text

    @pytest.mark.db_test
    @patch("airflow.providers.common.sql.hooks.sql.find_spec", return_value=None)
    @patch("airflow.providers.common.sql.hooks.sql.create_engine")
    def test_reraises_when_no_postgres_driver_is_available(self, mock_create_engine, mock_find_spec):
        original_error = ModuleNotFoundError("No module named 'psycopg2'")
        mock_create_engine.side_effect = original_error
        dbapi_hook = mock_db_hook(DbApiHook, conn_params={"conn_type": "postgresql"})

        with pytest.raises(ModuleNotFoundError, match="psycopg2"):
            dbapi_hook.get_sqlalchemy_engine()
        mock_create_engine.assert_called_once()

    @pytest.mark.db_test
    @patch("airflow.providers.common.sql.hooks.sql.create_engine")
    def test_does_not_retry_for_non_postgres_schemes(self, mock_create_engine):
        original_error = ModuleNotFoundError("No module named 'some_other_driver'")
        mock_create_engine.side_effect = original_error
        dbapi_hook = mock_db_hook(DbApiHook, conn_params={"conn_type": "mysql"})

        with pytest.raises(ModuleNotFoundError, match="some_other_driver"):
            dbapi_hook.get_sqlalchemy_engine()
        mock_create_engine.assert_called_once()


class TestDbApiHookGetTableSchema:
    @pytest.mark.db_test
    def test_get_table_schema(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        mock_inspector = MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": True},
            {"name": "name", "type": "VARCHAR(255)", "nullable": False},
        ]
        with patch.object(
            type(dbapi_hook), "inspector", new_callable=PropertyMock, return_value=mock_inspector
        ):
            result = dbapi_hook.get_table_schema("users")

        assert result == [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR(255)"},
        ]
        mock_inspector.get_columns.assert_called_once_with("users", schema=None)

    @pytest.mark.db_test
    def test_get_table_schema_with_schema(self):
        dbapi_hook = mock_db_hook(DbApiHook)
        mock_inspector = MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "col1", "type": "TEXT"},
        ]
        with patch.object(
            type(dbapi_hook), "inspector", new_callable=PropertyMock, return_value=mock_inspector
        ):
            dbapi_hook.get_table_schema("my_table", schema="my_schema")

        mock_inspector.get_columns.assert_called_once_with("my_table", schema="my_schema")


def test_inspector_is_cached():
    """inspector should return the same object on repeated access (not create N engines)."""
    hook = DBApiHookForTests(conn_id=DEFAULT_CONN_ID)
    mock_engine = MagicMock()
    with patch.object(hook, "get_sqlalchemy_engine", return_value=mock_engine) as mock_get_engine:
        inspector1 = hook.inspector
        inspector2 = hook.inspector
        assert inspector1 is inspector2
        mock_get_engine.assert_called_once()
