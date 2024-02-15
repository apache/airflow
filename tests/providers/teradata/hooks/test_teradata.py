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
from datetime import datetime
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.teradata.hooks.teradata import TeradataHook


class TestTeradataHook:
    def setup_method(self):
        self.connection = Connection(
            conn_type="teradata",
            login="login",
            password="password",
            host="host",
            schema="schema",
        )
        self.db_hook = TeradataHook(teradata_conn_id="teradata_conn_id", database="test_db")
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestTeradataHook(TeradataHook):
            conn_name_attr = "teradata_conn_id"

            def get_conn(self):
                return conn

        self.test_db_hook = UnitTestTeradataHook()

    @mock.patch("teradatasql.connect")
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["host"] == "host"
        assert kwargs["database"] == "schema"
        assert kwargs["dbs_port"] == "1025"
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"

    @mock.patch("teradatasql.connect")
    def test_get_tmode_conn(self, mock_connect):
        tmode_name = {"tmode": "tera"}
        self.connection.extra = json.dumps(tmode_name)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["host"] == "host"
        assert kwargs["database"] == "schema"
        assert kwargs["dbs_port"] == "1025"
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["tmode"] == "tera"

    @mock.patch("teradatasql.connect")
    def test_get_sslmode_conn(self, mock_connect):
        tmode_name = {"sslmode": "require"}
        self.connection.extra = json.dumps(tmode_name)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["host"] == "host"
        assert kwargs["database"] == "schema"
        assert kwargs["dbs_port"] == "1025"
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["sslmode"] == "require"

    @mock.patch("teradatasql.connect")
    def test_get_sslverifyca_conn(self, mock_connect):
        extravalues = {"sslmode": "verify-ca", "sslca": "/tmp/cert"}
        self.connection.extra = json.dumps(extravalues)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["host"] == "host"
        assert kwargs["database"] == "schema"
        assert kwargs["dbs_port"] == "1025"
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["sslmode"] == "verify-ca"
        assert kwargs["sslca"] == "/tmp/cert"

    @mock.patch("teradatasql.connect")
    def test_get_sslverifyfull_conn(self, mock_connect):
        extravalues = {"sslmode": "verify-full", "sslca": "/tmp/cert"}
        self.connection.extra = json.dumps(extravalues)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["host"] == "host"
        assert kwargs["database"] == "schema"
        assert kwargs["dbs_port"] == "1025"
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["sslmode"] == "verify-full"
        assert kwargs["sslca"] == "/tmp/cert"

    @mock.patch("teradatasql.connect")
    def test_get_sslcrc_conn(self, mock_connect):
        extravalues = {"sslcrc": "sslcrc"}
        self.connection.extra = json.dumps(extravalues)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["host"] == "host"
        assert kwargs["database"] == "schema"
        assert kwargs["dbs_port"] == "1025"
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["sslcrc"] == "sslcrc"

    @mock.patch("teradatasql.connect")
    def test_get_sslprotocol_conn(self, mock_connect):
        extravalues = {"sslprotocol": "protocol"}
        self.connection.extra = json.dumps(extravalues)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["host"] == "host"
        assert kwargs["database"] == "schema"
        assert kwargs["dbs_port"] == "1025"
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["sslprotocol"] == "protocol"

    @mock.patch("teradatasql.connect")
    def test_get_sslcipher_conn(self, mock_connect):
        extravalues = {"sslcipher": "cipher"}
        self.connection.extra = json.dumps(extravalues)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["host"] == "host"
        assert kwargs["database"] == "schema"
        assert kwargs["dbs_port"] == "1025"
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["sslcipher"] == "cipher"

    @mock.patch("sqlalchemy.create_engine")
    def test_get_sqlalchemy_conn(self, mock_connect):
        self.db_hook.get_sqlalchemy_engine()
        assert mock_connect.call_count == 1
        args = mock_connect.call_args.args
        assert len(args) == 1
        expected_link = (
            f"teradatasql://{self.connection.login}:{self.connection.password}@{self.connection.host}"
        )
        assert expected_link == args[0]

    def test_get_uri(self):
        ret_uri = self.db_hook.get_uri()
        expected_uri = f"teradata://{self.connection.login}:{self.connection.password}@{self.connection.host}/{self.connection.schema}"
        assert expected_uri == ret_uri

    def test_get_records(self):
        sql = "SQL"
        self.test_db_hook.get_records(sql)
        self.cur.execute.assert_called_once_with(sql)
        assert self.conn.commit.called

    def test_run_without_parameters(self):
        sql = "SQL"
        self.test_db_hook.run(sql)
        self.cur.execute.assert_called_once_with(sql)
        assert self.conn.commit.called

    def test_run_with_parameters(self):
        sql = "SQL"
        param = ("p1", "p2")
        self.test_db_hook.run(sql, parameters=param)
        self.cur.execute.assert_called_once_with(sql, param)
        assert self.conn.commit.called

    def test_insert_rows(self):
        rows = [
            (
                "'test_string",
                None,
                datetime(2023, 8, 15),
                1,
                3.14,
                "str",
            )
        ]
        target_fields = [
            "basestring",
            "none",
            "datetime",
            "int",
            "float",
            "str",
        ]
        self.test_db_hook.insert_rows("table", rows, target_fields)
        self.cur.execute.assert_called_once_with(
            "INSERT INTO table (basestring, none, datetime, int, float, str) VALUES (?,?,?,?,?,?)",
            ("'test_string", None, "2023-08-15T00:00:00", "1", "3.14", "str"),
        )

    def test_bulk_insert_rows_with_fields(self):
        rows = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        target_fields = ["col1", "col2", "col3"]
        self.test_db_hook.bulk_insert_rows("table", rows, target_fields)
        self.cur.executemany.assert_called_once_with(
            "INSERT INTO table (col1, col2, col3) VALUES (?, ?, ?)", rows
        )

    def test_bulk_insert_rows_with_commit_every(self):
        rows = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        target_fields = ["col1", "col2", "col3"]
        self.test_db_hook.bulk_insert_rows("table", rows, target_fields, commit_every=2)
        calls = [
            mock.call("INSERT INTO table (col1, col2, col3) values (1, 2, 3)"),
            mock.call("INSERT INTO table (col1, col2, col3) values (1, 2, 3)"),
        ]
        calls = [
            mock.call("INSERT INTO table (col1, col2, col3) VALUES (?, ?, ?)", rows[:2]),
            mock.call("INSERT INTO table (col1, col2, col3) VALUES (?, ?, ?)", rows[2:]),
        ]
        self.cur.executemany.assert_has_calls(calls, any_order=True)

    def test_bulk_insert_rows_without_fields(self):
        rows = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        self.test_db_hook.bulk_insert_rows("table", rows)
        self.cur.executemany.assert_called_once_with("INSERT INTO table  VALUES (?, ?, ?)", rows)

    def test_bulk_insert_rows_no_rows(self):
        rows = []
        with pytest.raises(ValueError):
            self.test_db_hook.bulk_insert_rows("table", rows)
