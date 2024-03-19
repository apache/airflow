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
from unittest.mock import patch

from airflow.models import Connection
from airflow.providers.vertica.hooks.vertica import VerticaHook


class TestVerticaHookConn:
    def setup_method(self):
        self.connection = Connection(
            login="login",
            password="password",
            host="host",
            schema="vertica",
        )

        class UnitTestVerticaHook(VerticaHook):
            conn_name_attr = "vertica_conn_id"

        self.db_hook = UnitTestVerticaHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @patch("airflow.providers.vertica.hooks.vertica.connect")
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            host="host", port=5433, database="vertica", user="login", password="password"
        )

    @patch("airflow.providers.vertica.hooks.vertica.connect")
    def test_get_conn_extra_parameters_no_cast(self, mock_connect):
        """Test if parameters are correctly passed to connection"""
        extra_dict = self.connection.extra_dejson
        bool_options = [
            "connection_load_balance",
            "binary_transfer",
            "disable_copy_local",
            "use_prepared_statements",
        ]
        for bo in bool_options:
            extra_dict.update({bo: True})
        extra_dict.update({"request_complex_types": False})

        std_options = [
            "session_label",
            "kerberos_host_name",
            "kerberos_service_name",
            "unicode_error",
            "workload",
            "ssl",
        ]
        for so in std_options:
            extra_dict.update({so: so})
        bck_server_node = ["1.2.3.4", "4.3.2.1"]
        conn_timeout = 30
        log_lvl = 40
        extra_dict.update({"backup_server_node": bck_server_node})
        extra_dict.update({"connection_timeout": conn_timeout})
        extra_dict.update({"log_level": log_lvl})
        self.connection.extra = json.dumps(extra_dict)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        for bo in bool_options:
            assert kwargs[bo] is True
        assert kwargs["request_complex_types"] is False
        for so in std_options:
            assert kwargs[so] == so
        assert bck_server_node[0] in kwargs["backup_server_node"]
        assert bck_server_node[1] in kwargs["backup_server_node"]
        assert kwargs["connection_timeout"] == conn_timeout
        assert kwargs["log_level"] == log_lvl
        assert kwargs["log_path"] is None

    @patch("airflow.providers.vertica.hooks.vertica.connect")
    def test_get_conn_extra_parameters_cast(self, mock_connect):
        """Test if parameters that can be passed either as string or int/bool
        like log_level are correctly converted when passed as string
        (while test_get_conn_extra_parameters_no_cast tests them passed as int/bool)"""
        import logging

        extra_dict = self.connection.extra_dejson
        bool_options = [
            "connection_load_balance",
            "binary_transfer",
            "disable_copy_local",
            "use_prepared_statements",
        ]
        for bo in bool_options:
            extra_dict.update({bo: "True"})
        extra_dict.update({"request_complex_types": "False"})
        extra_dict.update({"log_level": "Error"})
        self.connection.extra = json.dumps(extra_dict)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        for bo in bool_options:
            assert kwargs[bo] is True
        assert kwargs["request_complex_types"] is False
        assert kwargs["log_level"] == logging.ERROR
        assert kwargs["log_path"] is None


class TestVerticaHook:
    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0)
        self.cur.nextset.side_effect = [None]
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestVerticaHook(VerticaHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = UnitTestVerticaHook()

    @patch("airflow.providers.common.sql.hooks.sql.DbApiHook.insert_rows")
    def test_insert_rows(self, mock_insert_rows):
        table = "table"
        rows = [("hello",), ("world",)]
        target_fields = None
        commit_every = 10
        self.db_hook.insert_rows(table, rows, target_fields, commit_every)
        mock_insert_rows.assert_called_once_with(table, rows, None, 10)

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

    def test_get_pandas_df(self):
        statement = "SQL"
        column = "col"
        result_sets = [("row1",), ("row2",)]
        self.cur.description = [(column,)]
        self.cur.fetchall.return_value = result_sets
        df = self.db_hook.get_pandas_df(statement)

        assert column == df.columns[0]

        assert result_sets[0][0] == df.values.tolist()[0][0]
        assert result_sets[1][0] == df.values.tolist()[1][0]
