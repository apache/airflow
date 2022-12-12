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
import os
from unittest.mock import Mock, patch

import pytest
from pytest import param

from airflow.models import Connection
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.utils import db

jdbc_conn_mock = Mock(name="jdbc_conn")


class TestJdbcHook:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="jdbc_default",
                conn_type="jdbc",
                host="jdbc://localhost/",
                port=443,
                extra=json.dumps(
                    {
                        "extra__jdbc__drv_path": "/path1/test.jar,/path2/t.jar2",
                        "extra__jdbc__drv_clsname": "com.driver.main",
                    }
                ),
            )
        )

    @patch("airflow.providers.jdbc.hooks.jdbc.jaydebeapi.connect", autospec=True, return_value=jdbc_conn_mock)
    def test_jdbc_conn_connection(self, jdbc_mock):
        jdbc_hook = JdbcHook()
        jdbc_conn = jdbc_hook.get_conn()
        assert jdbc_mock.called
        assert isinstance(jdbc_conn, Mock)
        assert jdbc_conn.name == jdbc_mock.return_value.name

    @patch("airflow.providers.jdbc.hooks.jdbc.jaydebeapi.connect")
    def test_jdbc_conn_set_autocommit(self, _):
        jdbc_hook = JdbcHook()
        jdbc_conn = jdbc_hook.get_conn()
        jdbc_hook.set_autocommit(jdbc_conn, False)
        jdbc_conn.jconn.setAutoCommit.assert_called_once_with(False)

    @patch("airflow.providers.jdbc.hooks.jdbc.jaydebeapi.connect")
    def test_jdbc_conn_get_autocommit(self, _):
        jdbc_hook = JdbcHook()
        jdbc_conn = jdbc_hook.get_conn()
        jdbc_hook.get_autocommit(jdbc_conn)
        jdbc_conn.jconn.getAutoCommit.assert_called_once_with()

    @pytest.mark.parametrize(
        "uri",
        [
            param(
                "a://?extra__jdbc__drv_path=abc&extra__jdbc__drv_clsname=abc",
                id="prefix",
            ),
            param("a://?drv_path=abc&drv_clsname=abc", id="no-prefix"),
        ],
    )
    @patch("airflow.providers.jdbc.hooks.jdbc.jaydebeapi.connect")
    def test_backcompat_prefix_works(self, mock_connect, uri):
        with patch.dict(os.environ, {"AIRFLOW_CONN_MY_CONN": uri}):
            hook = JdbcHook("my_conn")
            hook.get_conn()
            mock_connect.assert_called_with(
                jclassname="abc",
                url="",
                driver_args=["None", "None"],
                jars="abc".split(","),
            )

    @patch("airflow.providers.jdbc.hooks.jdbc.jaydebeapi.connect")
    def test_backcompat_prefix_both_prefers_short(self, mock_connect):
        with patch.dict(
            os.environ,
            {"AIRFLOW_CONN_MY_CONN": "a://?drv_path=non-prefixed&extra__jdbc__drv_path=prefixed"},
        ):
            hook = JdbcHook("my_conn")
            hook.get_conn()
            mock_connect.assert_called_with(
                jclassname=None,
                url="",
                driver_args=["None", "None"],
                jars="non-prefixed".split(","),
            )
