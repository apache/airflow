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

from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy

from airflow.models import Connection
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
