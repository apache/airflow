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
import logging
from unittest import mock
from unittest.mock import Mock, patch

import pytest

from airflow.models import Connection
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.utils import db

pytestmark = pytest.mark.db_test


jdbc_conn_mock = Mock(name="jdbc_conn")


def get_hook(hook_params=None, conn_params=None):
    hook_params = hook_params or {}
    conn_params = conn_params or {}
    connection = Connection(
        **{
            **dict(login="login", password="password", host="host", schema="schema", port=1234),
            **conn_params,
        }
    )

    hook = JdbcHook(**hook_params)
    hook.get_connection = Mock()
    hook.get_connection.return_value = connection
    return hook


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
                        "driver_path": "/path1/test.jar,/path2/t.jar2",
                        "driver_class": "com.driver.main",
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

    def test_driver_hook_params(self):
        hook = get_hook(hook_params=dict(driver_path="Blah driver path", driver_class="Blah driver class"))
        assert hook.driver_path == "Blah driver path"
        assert hook.driver_class == "Blah driver class"

    def test_driver_in_extra_not_used(self):
        conn_params = dict(
            extra=json.dumps(dict(driver_path="ExtraDriverPath", driver_class="ExtraDriverClass"))
        )
        hook_params = {"driver_path": "ParamDriverPath", "driver_class": "ParamDriverClass"}
        hook = get_hook(conn_params=conn_params, hook_params=hook_params)
        assert hook.driver_path == "ParamDriverPath"
        assert hook.driver_class == "ParamDriverClass"

    def test_driver_extra_raises_warning_by_default(self, caplog):
        with caplog.at_level(logging.WARNING, logger="airflow.providers.jdbc.hooks.test_jdbc"):
            driver_path = get_hook(conn_params=dict(extra='{"driver_path": "Blah driver path"}')).driver_path
            assert (
                "You have supplied 'driver_path' via connection extra but it will not be used"
            ) in caplog.text
            assert driver_path is None

            driver_class = get_hook(
                conn_params=dict(extra='{"driver_class": "Blah driver class"}')
            ).driver_class
            assert (
                "You have supplied 'driver_class' via connection extra but it will not be used"
            ) in caplog.text
            assert driver_class is None

    @mock.patch.dict("os.environ", {"AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_PATH_IN_EXTRA": "TRUE"})
    @mock.patch.dict("os.environ", {"AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_CLASS_IN_EXTRA": "TRUE"})
    def test_driver_extra_works_when_allow_driver_extra(self):
        hook = get_hook(
            conn_params=dict(extra='{"driver_path": "Blah driver path", "driver_class": "Blah driver class"}')
        )
        assert hook.driver_path == "Blah driver path"
        assert hook.driver_class == "Blah driver class"

    def test_default_driver_set(self):
        with patch.object(JdbcHook, "default_driver_path", "Blah driver path") as _, patch.object(
            JdbcHook, "default_driver_class", "Blah driver class"
        ) as _:
            hook = get_hook()
            assert hook.driver_path == "Blah driver path"
            assert hook.driver_class == "Blah driver class"

    def test_driver_none_by_default(self):
        hook = get_hook()
        assert hook.driver_path is None
        assert hook.driver_class is None

    def test_driver_extra_raises_warning_and_returns_default_driver_by_default(self, caplog):
        with patch.object(JdbcHook, "default_driver_path", "Blah driver path"):
            with caplog.at_level(logging.WARNING, logger="airflow.providers.jdbc.hooks.test_jdbc"):
                driver_path = get_hook(
                    conn_params=dict(extra='{"driver_path": "Blah driver path2"}')
                ).driver_path
                assert (
                    "have supplied 'driver_path' via connection extra but it will not be used"
                ) in caplog.text
                assert driver_path == "Blah driver path"

        with patch.object(JdbcHook, "default_driver_class", "Blah driver class"):
            with caplog.at_level(logging.WARNING, logger="airflow.providers.jdbc.hooks.test_jdbc"):
                driver_class = get_hook(
                    conn_params=dict(extra='{"driver_class": "Blah driver class2"}')
                ).driver_class
                assert (
                    "have supplied 'driver_class' via connection extra but it will not be used"
                ) in caplog.text
                assert driver_class == "Blah driver class"
