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
import sqlite3
from contextlib import closing
from unittest.mock import MagicMock, PropertyMock, patch

import pandas as pd
import polars as pl
import pytest
from sqlalchemy import event

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowTaskTimeout
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


index = 0


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


def test_inspector_closes_its_dbapi_connections_after_each_inspection(tmp_path):
    """A cached inspector must not hold a pooled DBAPI connection open for the hook's lifetime."""
    db_path = tmp_path / "inspect.db"
    with closing(sqlite3.connect(db_path)) as conn:
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    hook = DBApiHookForTests(conn_id=DEFAULT_CONN_ID)
    opened, closed = [], []
    real_get_sqlalchemy_engine = hook.get_sqlalchemy_engine

    def get_sqlalchemy_engine_with_listeners(engine_kwargs=None):
        engine = real_get_sqlalchemy_engine(engine_kwargs)
        event.listen(engine, "connect", lambda dbapi_conn, _record: opened.append(dbapi_conn))
        event.listen(engine, "close", lambda dbapi_conn, _record: closed.append(dbapi_conn))
        return engine

    with (
        patch.object(hook, "get_uri", autospec=True, return_value=f"sqlite:///{db_path}"),
        patch.object(
            hook, "get_sqlalchemy_engine", autospec=True, side_effect=get_sqlalchemy_engine_with_listeners
        ),
    ):
        columns = hook.get_table_schema("users")
        table_names = hook.inspector.get_table_names()

    assert columns == [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "TEXT"}]
    assert table_names == ["users"]
    assert opened
    assert closed == opened


CURSOR_SPEC = ["execute", "fetchall", "close", "description", "rowcount"]


class TestDbApiHookCancelQuery:
    @staticmethod
    def _hook_with_cursor(cursor: MagicMock) -> DbApiHook:
        hook = mock_db_hook(DbApiHook)
        hook.get_conn().cursor.return_value = cursor
        return hook

    @staticmethod
    def _cancel_during(hook: DbApiHook, cursor_method: MagicMock) -> list[bool]:
        """Record what ``cancel_query`` returns when it is called from inside ``cursor_method``."""
        outcome: list[bool] = []

        def cancel(*args, **kwargs):
            outcome.append(hook.cancel_query())
            return []

        cursor_method.side_effect = cancel
        return outcome

    def test_nothing_to_cancel_when_no_statement_is_running(self, caplog):
        cursor = MagicMock(spec=[*CURSOR_SPEC, "cancel"])
        hook = self._hook_with_cursor(cursor)

        with caplog.at_level(logging.WARNING):
            assert hook.cancel_query() is False

        assert "No running SQL statement is tracked by" in caplog.text
        cursor.cancel.assert_not_called()

    def test_cancel_through_cursor(self):
        cursor = MagicMock(spec=[*CURSOR_SPEC, "cancel", "connection"])
        hook = self._hook_with_cursor(cursor)
        outcome = self._cancel_during(hook, cursor.execute)

        hook.run("select pg_sleep(600)")

        assert outcome == [True]
        cursor.cancel.assert_called_once_with()
        cursor.connection.cancel.assert_not_called()
        assert hook._running_cursor is None

    def test_cancel_falls_back_to_connection(self):
        cursor = MagicMock(spec=[*CURSOR_SPEC, "connection"])
        hook = self._hook_with_cursor(cursor)
        outcome = self._cancel_during(hook, cursor.execute)

        hook.run("select pg_sleep(600)")

        assert outcome == [True]
        cursor.connection.cancel.assert_called_once_with()
        assert hook._running_cursor is None

    def test_cancel_not_supported_by_driver(self, caplog):
        cursor = MagicMock(spec=[*CURSOR_SPEC, "connection"])
        cursor.connection = MagicMock(spec=["commit", "close"])
        hook = self._hook_with_cursor(cursor)
        outcome = self._cancel_during(hook, cursor.execute)

        with caplog.at_level(logging.WARNING):
            hook.run("select pg_sleep(600)")

        assert outcome == [False]
        assert "does not support cancelling a running SQL statement" in caplog.text

    def test_cancel_while_handler_fetches_results(self):
        cursor = MagicMock(spec=[*CURSOR_SPEC, "cancel"])
        hook = self._hook_with_cursor(cursor)
        outcome = self._cancel_during(hook, cursor.fetchall)

        hook.run("select * from big_table", handler=fetch_all_handler)

        assert outcome == [True]
        cursor.cancel.assert_called_once_with()
        assert hook._running_cursor is None

    def test_cancel_when_run_command_is_called_directly(self):
        # Hooks with their own ``run`` (e.g. ``DatabricksSqlHook``) only call ``_run_command``
        cursor = MagicMock(spec=[*CURSOR_SPEC, "cancel"])
        hook = self._hook_with_cursor(cursor)
        outcome = self._cancel_during(hook, cursor.execute)

        hook._run_command(cursor, "select pg_sleep(600)", None)

        assert outcome == [True]
        assert hook._running_cursor is None

    def test_running_cursor_is_cleared_when_statement_fails(self):
        cursor = MagicMock(spec=[*CURSOR_SPEC, "cancel"])
        cursor.execute.side_effect = RuntimeError("boom")
        hook = self._hook_with_cursor(cursor)

        with pytest.raises(RuntimeError, match="boom"):
            hook.run("select 1")

        cursor.cancel.assert_not_called()
        assert hook._running_cursor is None

    @pytest.mark.parametrize("interrupted_method", ["execute", "fetchall"])
    def test_statement_is_cancelled_when_interrupted_by_timeout(self, interrupted_method):
        cursor = MagicMock(spec=[*CURSOR_SPEC, "cancel"])
        getattr(cursor, interrupted_method).side_effect = AirflowTaskTimeout()
        hook = self._hook_with_cursor(cursor)

        with pytest.raises(AirflowTaskTimeout):
            hook.run("select pg_sleep(600)", handler=fetch_all_handler)

        cursor.cancel.assert_called_once_with()
        assert hook._running_cursor is None

    def test_statement_is_not_cancelled_on_timeout_when_disabled(self):
        cursor = MagicMock(spec=[*CURSOR_SPEC, "cancel"])
        cursor.execute.side_effect = AirflowTaskTimeout()
        hook = self._hook_with_cursor(cursor)
        hook._cancel_on_interrupt = False

        with pytest.raises(AirflowTaskTimeout):
            hook.run("select pg_sleep(600)")

        cursor.cancel.assert_not_called()
        assert hook._running_cursor is None

    def test_timeout_is_raised_when_cancelling_fails(self, caplog):
        cursor = MagicMock(spec=[*CURSOR_SPEC, "cancel"])
        cursor.execute.side_effect = AirflowTaskTimeout()
        cursor.cancel.side_effect = RuntimeError("connection is closed")
        hook = self._hook_with_cursor(cursor)

        with caplog.at_level(logging.ERROR), pytest.raises(AirflowTaskTimeout):
            hook.run("select pg_sleep(600)")

        assert "Failed to cancel the running SQL statement" in caplog.text
        assert hook._running_cursor is None
