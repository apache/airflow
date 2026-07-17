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

from airflow.providers.common.compat.sdk import Connection
from airflow.providers.mysql.hooks.mysql import MySqlHook


class TestMySqlHookConnMySqlConnectorPython:
    def setup_method(self):
        self.connection = Connection(
            conn_id="test_conn_id",
            conn_type="mysql",
            login="login",
            password="password",
            host="host",
            schema="schema",
            extra='{"client": "mysql-connector-python"}',
        )

        self.db_hook = MySqlHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch("mysql.connector.connect")
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["user"] == "login"
        assert kwargs["password"] == "password"
        assert kwargs["host"] == "host"
        assert kwargs["database"] == "schema"

    @mock.patch("mysql.connector.connect")
    def test_get_conn_port(self, mock_connect):
        self.connection.port = 3307
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["port"] == 3307

    @mock.patch("mysql.connector.connect")
    def test_get_conn_allow_local_infile(self, mock_connect):
        extra_dict = self.connection.extra_dejson
        self.connection.extra = json.dumps(extra_dict)
        self.db_hook.local_infile = True
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["allow_local_infile"] == 1

    @mock.patch("mysql.connector.connect")
    def test_get_ssl_mode(self, mock_connect):
        extra_dict = self.connection.extra_dejson
        extra_dict.update(ssl_disabled=True)
        self.connection.extra = json.dumps(extra_dict)
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["ssl_disabled"] == 1

    @mock.patch("mysql.connector.connect")
    def test_get_conn_init_command(self, mock_connect):
        extra_dict = self.connection.extra_dejson
        self.connection.extra = json.dumps(extra_dict)
        self.db_hook.init_command = "SET time_zone = '+00:00';"
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        args, kwargs = mock_connect.call_args
        assert args == ()
        assert kwargs["init_command"] == "SET time_zone = '+00:00';"
