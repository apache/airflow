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


class TestExasolHook:
    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0)
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
        assert "foo" == self.db_hook._serialize_cell("foo", None)

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
