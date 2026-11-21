#
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

import os
import signal
import sqlite3
import sys
import threading
import time
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowTaskTimeout
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.sqlite.hooks import sqlite as sqlite_hook_module
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


def mock_connection(host=None, extra=None, uri=None):
    """Create a mock connection object without triggering SQLAlchemy ORM initialization."""
    conn = MagicMock(spec=Connection)
    conn.host = host
    conn.extra = extra
    conn.get_uri.return_value = uri if uri is not None else (host or "")
    return conn


class TestSqliteHookConn:
    def setup_method(self):
        class UnitTestSqliteHook(SqliteHook):
            conn_name_attr = "test_conn_id"

        self.db_hook = UnitTestSqliteHook()

    @pytest.mark.parametrize(
        ("connection", "uri"),
        [
            (mock_connection(host="host", uri="sqlite:///host"), "file:/host"),
            (
                mock_connection(host="host", extra='{"mode":"ro"}', uri="sqlite:///host?mode=ro"),
                "file:/host?mode=ro",
            ),
            (mock_connection(host=":memory:", uri="sqlite:///:memory:"), "file:/:memory:"),
            (mock_connection(uri="sqlite:///"), "file:/"),
            (
                mock_connection(uri="sqlite:///relative/path/to/db?mode=ro"),
                "file:/relative/path/to/db?mode=ro",
            ),
            (
                mock_connection(uri="sqlite:////absolute/path/to/db?mode=ro"),
                "file://absolute/path/to/db?mode=ro",
            ),
            (mock_connection(uri="sqlite://?mode=ro"), "sqlite:/?mode=ro"),
        ],
    )
    @patch("airflow.providers.sqlite.hooks.sqlite.sqlite3.connect")
    def test_get_conn(self, mock_connect, connection, uri):
        self.db_hook.get_connection = mock.Mock(return_value=connection)
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(uri, uri=True)

    @patch("airflow.providers.sqlite.hooks.sqlite.sqlite3.connect")
    def test_get_conn_non_default_id(self, mock_connect):
        self.db_hook.get_connection = mock.Mock(
            return_value=mock_connection(host="host", uri="sqlite:///host")
        )
        self.db_hook.test_conn_id = "non_default"
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with("file:/host", uri=True)
        self.db_hook.get_connection.assert_called_once_with("non_default")


class TestSqliteHook:
    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestSqliteHook(SqliteHook):
            conn_name_attr = "test_conn_id"
            log = mock.MagicMock()

            def get_conn(self):
                return conn

        self.db_hook = UnitTestSqliteHook()
        self.db_hook.get_connection = mock.Mock(return_value=Connection(conn_type="sqlite"))

    def test_get_first_record(self):
        statement = "SQL"
        result_sets = [("row1",), ("row2",)]
        self.cur.fetchone.return_value = result_sets[0]

        assert result_sets[0] == self.db_hook.get_first(statement)
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_records(self):
        statement = "SQL"
        result_sets = [("row1",), ("row2",)]
        self.cur.fetchall.return_value = result_sets

        assert result_sets == self.db_hook.get_records(statement)
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_df_pandas(self):
        statement = "SQL"
        column = "col"
        result_sets = [("row1",), ("row2",)]
        self.cur.description = [(column,)]
        self.cur.fetchall.return_value = result_sets
        df = self.db_hook.get_df(statement, df_type="pandas")

        assert column == df.columns[0]

        assert result_sets[0][0] == df.values.tolist()[0][0]
        assert result_sets[1][0] == df.values.tolist()[1][0]

        self.cur.execute.assert_called_once_with(statement)

    def test_get_df_polars(self):
        statement = "SQL"
        column = "col"
        result_sets = [("row1",), ("row2",)]
        mock_execute = mock.MagicMock()
        mock_execute.description = [(column, None, None, None, None, None, None)]
        mock_execute.fetchall.return_value = result_sets
        self.cur.execute.return_value = mock_execute
        df = self.db_hook.get_df(statement, df_type="polars")

        self.cur.execute.assert_called_once_with(statement)
        mock_execute.fetchall.assert_called_once_with()
        assert column == df.columns[0]
        assert result_sets[0][0] == df.row(0)[0]
        assert result_sets[1][0] == df.row(1)[0]

    def test_run_log(self):
        statement = "SQL"
        self.db_hook.run(statement)
        assert self.db_hook.log.info.call_count == 2

    @pytest.mark.db_test
    def test_generate_insert_sql_replace_false(self):
        expected_sql = "INSERT INTO Customer (first_name, last_name) VALUES (?,?)"
        rows = ("James", "1")
        target_fields = ["first_name", "last_name"]
        sql = self.db_hook._generate_insert_sql(
            table="Customer", values=rows, target_fields=target_fields, replace=False
        )

        assert sql == expected_sql

    @pytest.mark.db_test
    def test_generate_insert_sql_replace_true(self):
        expected_sql = "REPLACE INTO Customer (first_name, last_name) VALUES (?,?)"
        rows = ("James", "1")
        target_fields = ["first_name", "last_name"]
        sql = self.db_hook._generate_insert_sql(
            table="Customer", values=rows, target_fields=target_fields, replace=True
        )

        assert sql == expected_sql

    @mock.patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    def test_run_hook_lineage(self, mock_send_lineage):
        sql = "SELECT 1"
        self.db_hook.run(sql)

        mock_send_lineage.assert_called_once()
        call_kw = mock_send_lineage.call_args.kwargs
        assert call_kw["context"] is self.db_hook
        assert call_kw["sql"] == sql
        assert call_kw["sql_parameters"] is None
        assert call_kw["cur"] is self.cur

    @mock.patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    def test_insert_rows_hook_lineage(self, mock_send_lineage):
        table = "table"
        rows = [("hello",), ("world",)]
        self.db_hook.insert_rows(table, rows)

        mock_send_lineage.assert_called()
        call_kw = mock_send_lineage.call_args.kwargs
        assert call_kw["context"] is self.db_hook
        assert call_kw["sql"] == "INSERT INTO table  VALUES (?)"
        assert call_kw["row_count"] == 2

    @mock.patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df")
    def test_get_df_hook_lineage(self, mock_get_pandas_df, mock_send_lineage):
        sql = "SELECT 1"
        self.db_hook.get_df(sql, df_type="pandas")

        mock_send_lineage.assert_called_once()
        call_kw = mock_send_lineage.call_args.kwargs
        assert call_kw["context"] is self.db_hook
        assert call_kw["sql"] == sql
        assert call_kw["sql_parameters"] is None

    @mock.patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df_by_chunks")
    def test_get_df_by_chunks_hook_lineage(self, mock_get_pandas_df_by_chunks, mock_send_lineage):
        sql = "SELECT 1"
        parameters = ("x",)
        self.db_hook.get_df_by_chunks(sql, parameters=parameters, chunksize=1)

        mock_send_lineage.assert_called_once()
        call_kw = mock_send_lineage.call_args.kwargs
        assert call_kw["context"] is self.db_hook
        assert call_kw["sql"] == sql
        assert call_kw["sql_parameters"] == parameters

    @pytest.mark.db_test
    def test_sqlalchemy_engine(self):
        """Test that the sqlalchemy engine is initialized"""
        conn_id = "sqlite_default"
        hook = SqliteHook(sqlite_conn_id=conn_id)
        engine = hook.get_sqlalchemy_engine()
        assert isinstance(engine, sqlalchemy.engine.Engine)
        assert engine.name == "sqlite"
        # Assert filepath of the sqliate DB is correct
        assert engine.url.database == hook.get_connection(conn_id).host


class TestSqliteHookInterruptRunningStatement:
    """A long statement is interrupted by on_kill and by execution_timeout instead of running to completion."""

    # Counts to 300 million, which takes well over 10 s; an interrupted statement stops after about 0.5 s
    LONG_STATEMENT = (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 300000000) "
        "SELECT count(*) FROM c"
    )

    @pytest.fixture
    def hook(self):
        class InMemorySqliteHook(SqliteHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                conn = sqlite3.connect(":memory:")

                def boom():
                    raise ValueError("boom")

                conn.create_function("boom", 0, boom)
                return conn

        return InMemorySqliteHook()

    @pytest.fixture
    def signal_after(self):
        """
        Run a signal handler 0.5 s into the test, like the SIGTERM (on_kill) and SIGALRM (execution_timeout)
        handlers of a task. SIGUSR1 is used so that the timers of pytest-timeouts are left alone.
        """
        previous = signal.getsignal(signal.SIGUSR1)
        timers = []

        def install(handler):
            signal.signal(signal.SIGUSR1, handler)
            timers.append(threading.Timer(0.5, os.kill, (os.getpid(), signal.SIGUSR1)))
            timers[-1].start()

        yield install
        for timer in timers:
            timer.cancel()
        signal.signal(signal.SIGUSR1, previous)

    def test_statement_is_interrupted_when_the_execution_timeout_elapses(self, hook, signal_after):
        unraisablehook = sys.unraisablehook

        def raise_timeout(signum, frame):
            raise AirflowTaskTimeout()

        signal_after(raise_timeout)
        start = time.monotonic()
        with pytest.raises(AirflowTaskTimeout):
            hook.run(self.LONG_STATEMENT)

        assert time.monotonic() - start < 5
        assert sys.unraisablehook is unraisablehook

    def test_statement_is_interrupted_by_cancel_query_from_a_signal_handler(self, hook, signal_after):
        def cancel(signum, frame):
            # What SQLExecuteQueryOperator.on_kill does when the task receives SIGTERM
            assert hook.cancel_query() is True

        signal_after(cancel)
        start = time.monotonic()
        with pytest.raises(sqlite3.OperationalError, match="interrupted"):
            hook.run(self.LONG_STATEMENT)

        assert time.monotonic() - start < 5

    def test_other_callback_errors_are_passed_to_the_previous_unraisablehook(self, hook, mocker):
        # Only the progress handler's own interruptions are taken over, and they raise in place of the
        # OperationalError; an error raised by a user-defined function still fails the statement as usual
        previous = mocker.patch.object(sys, "unraisablehook")

        with pytest.raises(sqlite3.OperationalError, match="user-defined function raised exception"):
            hook.run("SELECT boom()")

        assert sys.unraisablehook is previous
        (unraisable,) = previous.call_args.args
        assert isinstance(unraisable.exc_value, ValueError)

    def test_result_is_returned_when_not_interrupted(self, hook):
        assert hook.run("SELECT 1 + 1", handler=fetch_all_handler) == [(2,)]

    @staticmethod
    def mocked_hook():
        cursor = mock.MagicMock(
            spec=["execute", "fetchall", "close", "description", "rowcount", "connection"]
        )
        cursor.connection = mock.MagicMock(spec=sqlite3.Connection)
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor

        class UnitTestSqliteHook(SqliteHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        return UnitTestSqliteHook(), cursor.connection

    def test_progress_handler_is_installed_only_while_the_statement_runs(self):
        hook, sqlite_conn = self.mocked_hook()

        hook.run(["select 1", "select 2"])

        assert (
            sqlite_conn.set_progress_handler.call_args_list
            == [
                mock.call(sqlite_hook_module._run_signal_handlers, SqliteHook.progress_handler_interval),
                mock.call(None, 0),
            ]
            * 2
        )

    def test_progress_handler_is_not_installed_when_cancel_on_interrupt_is_disabled(self):
        hook, sqlite_conn = self.mocked_hook()
        hook.cancel_on_interrupt = False

        hook.run("select 1")

        sqlite_conn.set_progress_handler.assert_not_called()

    def test_progress_handler_is_not_installed_outside_the_main_thread(self):
        hook, sqlite_conn = self.mocked_hook()

        thread = threading.Thread(target=hook.run, args=("select 1",))
        thread.start()
        thread.join()

        sqlite_conn.set_progress_handler.assert_not_called()

    def test_cancel_query_without_a_running_statement(self):
        hook, sqlite_conn = self.mocked_hook()

        assert hook.cancel_query() is False
        sqlite_conn.interrupt.assert_not_called()
