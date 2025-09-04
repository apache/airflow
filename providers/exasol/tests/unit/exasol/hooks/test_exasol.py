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

import json
from unittest import mock

import pytest

from airflow import models
from airflow.providers.exasol.hooks.exasol import ExasolHook


class TestExasolHookConn:
    def setup_method(self):
        self.connection = models.Connection(
            login="login",
            password="password",
            host="host",
            port=1234,
            schema="schema",
        )

        self.db_hook = ExasolHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch("airflow.providers.exasol.hooks.exasol.pyexasol")
    def test_get_conn(self, mock_pyexasol):
        self.db_hook.get_conn()
        mock_connect = mock_pyexasol.connect
        mock_connect.assert_called_once()
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["dsn"] == "host:1234"
        assert kwargs["schema"] == "schema"

    @mock.patch("airflow.providers.exasol.hooks.exasol.pyexasol")
    def test_get_conn_extra_args(self, mock_pyexasol):
        self.connection.extra = json.dumps({"encryption": True})
        self.db_hook.get_conn()
        mock_connect = mock_pyexasol.connect
        mock_connect.assert_called_once()
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["encryption"] is True


class TestExasolHookSqlalchemy:
    def get_connection(self, extra: dict | None = None) -> models.Connection:
        return models.Connection(
            login="login",
            password="password",
            host="host",
            port=1234,
            schema="schema",
            extra=extra,
        )

    @pytest.mark.parametrize(
        "init_scheme, extra_scheme, expected_result, expect_error",
        [
            (None, None, "exa+websocket", False),
            ("exa+pyodbc", None, "exa+pyodbc", False),
            (None, "exa+turbodbc", "exa+turbodbc", False),
            ("exa+invalid", None, None, True),
            (None, "exa+invalid", None, True),
        ],
        ids=[
            "default",
            "from_init_arg",
            "from_extra",
            "invalid_from_init_arg",
            "invalid_from_extra",
        ],
    )
    def test_sqlalchemy_scheme_property(self, init_scheme, extra_scheme, expected_result, expect_error):
        hook = ExasolHook(sqlalchemy_scheme=init_scheme) if init_scheme else ExasolHook()
        connection = self.get_connection(extra={"sqlalchemy_scheme": extra_scheme} if extra_scheme else None)
        hook.get_connection = mock.Mock(return_value=connection)

        if not expect_error:
            assert hook.sqlalchemy_scheme == expected_result
        else:
            with pytest.raises(ValueError):
                _ = hook.sqlalchemy_scheme

    @pytest.mark.parametrize(
        "hook_scheme, extra, expected_url",
        [
            (None, {}, "exa+websocket://login:password@host:1234/schema"),
            (
                None,
                {"CONNECTIONLCALL": "en_US.UTF-8", "driver": "EXAODBC"},
                "exa+websocket://login:password@host:1234/schema?CONNECTIONLCALL=en_US.UTF-8&driver=EXAODBC",
            ),
            (
                None,
                {"sqlalchemy_scheme": "exa+turbodbc", "CONNECTIONLCALL": "en_US.UTF-8", "driver": "EXAODBC"},
                "exa+turbodbc://login:password@host:1234/schema?CONNECTIONLCALL=en_US.UTF-8&driver=EXAODBC",
            ),
            (
                "exa+pyodbc",
                {
                    "sqlalchemy_scheme": "exa+turbodbc",  # should be overridden
                    "CONNECTIONLCALL": "en_US.UTF-8",
                    "driver": "EXAODBC",
                },
                "exa+pyodbc://login:password@host:1234/schema?CONNECTIONLCALL=en_US.UTF-8&driver=EXAODBC",
            ),
        ],
        ids=[
            "default",
            "default_with_extra",
            "scheme_from_extra_turbodbc",
            "scheme_from_hook",
        ],
    )
    def test_sqlalchemy_url_property(self, hook_scheme, extra, expected_url):
        hook = ExasolHook(sqlalchemy_scheme=hook_scheme) if hook_scheme else ExasolHook()
        hook.get_connection = mock.Mock(return_value=self.get_connection(extra=extra))
        assert hook.sqlalchemy_url.render_as_string(hide_password=False) == expected_url

    def test_get_uri(self):
        hook = ExasolHook()
        connection = self.get_connection(extra={"CONNECTIONLCALL": "en_US.UTF-8", "driver": "EXAODBC"})
        hook.get_connection = mock.Mock(return_value=connection)
        assert (
            hook.get_uri()
            == "exa+websocket://login:password@host:1234/schema?CONNECTIONLCALL=en_US.UTF-8&driver=EXAODBC"
        )


class TestExasolHook:
    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=lambda: 0)
        self.conn = mock.MagicMock()
        self.conn.execute.return_value = self.cur
        conn = self.conn

        class SubExasolHook(ExasolHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = SubExasolHook()

    def test_set_autocommit(self):
        autocommit = True
        self.db_hook.set_autocommit(self.conn, autocommit)

        self.conn.set_autocommit.assert_called_once_with(autocommit)

    def test_get_autocommit(self):
        setattr(self.conn, "autocommit", True)
        setattr(self.conn, "attr", {"autocommit": False})
        assert not self.db_hook.get_autocommit(self.conn)

    def test_run_without_autocommit(self):
        sql = "SQL"
        setattr(self.conn, "attr", {"autocommit": False})

        # Default autocommit setting should be False.
        # Testing default autocommit value as well as run() behavior.
        self.db_hook.run(sql, autocommit=False)
        self.conn.set_autocommit.assert_called_once_with(False)
        self.conn.execute.assert_called_once_with(sql, None)
        self.conn.commit.assert_called_once()

    def test_run_with_autocommit(self):
        sql = "SQL"
        self.db_hook.run(sql, autocommit=True)
        self.conn.set_autocommit.assert_called_once_with(True)
        self.conn.execute.assert_called_once_with(sql, None)
        self.conn.commit.assert_not_called()

    def test_run_with_parameters(self):
        sql = "SQL"
        parameters = ("param1", "param2")
        self.db_hook.run(sql, autocommit=True, parameters=parameters)
        self.conn.set_autocommit.assert_called_once_with(True)
        self.conn.execute.assert_called_once_with(sql, parameters)
        self.conn.commit.assert_not_called()

    def test_run_multi_queries(self):
        sql = ["SQL1", "SQL2"]
        self.db_hook.run(sql, autocommit=True)
        self.conn.set_autocommit.assert_called_once_with(True)
        for i, item in enumerate(self.conn.execute.call_args_list):
            args, kwargs = item
            assert len(args) == 2
            assert args[0] == sql[i]
            assert kwargs == {}
        self.conn.execute.assert_called_with(sql[1], None)
        self.conn.commit.assert_not_called()

    def test_run_no_queries(self):
        with pytest.raises(ValueError) as err:
            self.db_hook.run(sql=[])
        assert err.value.args[0] == "List of SQL statements is empty"

    def test_no_result_set(self):
        """Queries like DROP and SELECT are of type rowCount (not resultSet),
        which raises an error in pyexasol if trying to iterate over them"""
        self.cur.result_type = mock.Mock()
        self.cur.result_type.return_value = "rowCount"

        sql = "SQL"
        self.db_hook.run(sql)

    def test_bulk_load(self):
        with pytest.raises(NotImplementedError):
            self.db_hook.bulk_load("table", "/tmp/file")

    def test_bulk_dump(self):
        with pytest.raises(NotImplementedError):
            self.db_hook.bulk_dump("table", "/tmp/file")

    def test_serialize_cell(self):
        assert self.db_hook._serialize_cell("foo", None) == "foo"

    def test_export_to_file(self):
        file_name = "file_name"
        query_or_table = "query_or_table"
        query_params = {"query_params": "1"}
        export_params = {"export_params": "2"}

        self.db_hook.export_to_file(
            filename=file_name,
            query_or_table=query_or_table,
            query_params=query_params,
            export_params=export_params,
        )

        self.conn.export_to_file.assert_called_once_with(
            dst=file_name,
            query_or_table=query_or_table,
            query_params=query_params,
            export_params=export_params,
        )

    def test_get_df_pandas(self):
        statement = "SQL"
        column = "col"
        result_sets = [("row1",), ("row2",)]
        mock_df = mock.MagicMock()
        mock_df.columns = [column]
        mock_df.values.tolist.return_value = result_sets
        self.conn.export_to_pandas.return_value = mock_df

        df = self.db_hook.get_df(statement, df_type="pandas")

        assert column == df.columns[0]

        assert result_sets[0][0] == df.values.tolist()[0][0]
        assert result_sets[1][0] == df.values.tolist()[1][0]

    def test_get_df_polars(self):
        with pytest.raises(NotImplementedError):
            self.db_hook.get_df("SQL", df_type="polars")
