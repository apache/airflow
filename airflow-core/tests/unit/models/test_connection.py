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

import re
import sys
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.models import Connection
from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
from airflow.sdk.execution_time.comms import ErrorResponse


class TestConnection:
    @pytest.mark.parametrize(
        "uri, expected_conn_type, expected_host, expected_login, expected_password,"
        " expected_port, expected_schema, expected_extra_dict, expected_exception_message",
        [
            (
                "type://user:pass@host:100/schema",
                "type",
                "host",
                "user",
                "pass",
                100,
                "schema",
                {},
                None,
            ),
            (
                "type://user:pass@host/schema",
                "type",
                "host",
                "user",
                "pass",
                None,
                "schema",
                {},
                None,
            ),
            (
                "type://user:pass@host/schema?param1=val1&param2=val2",
                "type",
                "host",
                "user",
                "pass",
                None,
                "schema",
                {"param1": "val1", "param2": "val2"},
                None,
            ),
            (
                "type://host",
                "type",
                "host",
                None,
                None,
                None,
                "",
                {},
                None,
            ),
            (
                "spark://mysparkcluster.com:80?deploy-mode=cluster&spark_binary=command&namespace=kube+namespace",
                "spark",
                "mysparkcluster.com",
                None,
                None,
                80,
                "",
                {"deploy-mode": "cluster", "spark_binary": "command", "namespace": "kube namespace"},
                None,
            ),
            (
                "spark://k8s://100.68.0.1:443?deploy-mode=cluster",
                "spark",
                "k8s://100.68.0.1",
                None,
                None,
                443,
                "",
                {"deploy-mode": "cluster"},
                None,
            ),
            (
                "type://protocol://user:pass@host:123?param=value",
                "type",
                "protocol://host",
                "user",
                "pass",
                123,
                "",
                {"param": "value"},
                None,
            ),
            (
                "type://user:pass@protocol://host:port?param=value",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                r"Invalid connection string: type://user:pass@protocol://host:port?param=value.",
            ),
        ],
    )
    def test_parse_from_uri(
        self,
        uri,
        expected_conn_type,
        expected_host,
        expected_login,
        expected_password,
        expected_port,
        expected_schema,
        expected_extra_dict,
        expected_exception_message,
    ):
        if expected_exception_message is not None:
            with pytest.raises(AirflowException, match=re.escape(expected_exception_message)):
                Connection(uri=uri)
        else:
            conn = Connection(uri=uri)
            assert conn.conn_type == expected_conn_type
            assert conn.login == expected_login
            assert conn.password == expected_password
            assert conn.host == expected_host
            assert conn.port == expected_port
            assert conn.schema == expected_schema
            assert conn.extra_dejson == expected_extra_dict

    @pytest.mark.parametrize(
        "connection, expected_uri",
        [
            (
                Connection(
                    conn_type="type",
                    login="user",
                    password="pass",
                    host="host",
                    port=100,
                    schema="schema",
                    extra={"param1": "val1", "param2": "val2"},
                ),
                "type://user:pass@host:100/schema?param1=val1&param2=val2",
            ),
            (
                Connection(
                    conn_type="type",
                    host="protocol://host",
                    port=100,
                    schema="schema",
                    extra={"param1": "val1", "param2": "val2"},
                ),
                "type://protocol://host:100/schema?param1=val1&param2=val2",
            ),
            (
                Connection(
                    conn_type="type",
                    login="user",
                    password="pass",
                    host="protocol://host",
                    port=100,
                    schema="schema",
                    extra={"param1": "val1", "param2": "val2"},
                ),
                "type://protocol://user:pass@host:100/schema?param1=val1&param2=val2",
            ),
        ],
    )
    def test_get_uri(self, connection, expected_uri):
        assert connection.get_uri() == expected_uri

    @pytest.mark.parametrize(
        "connection, expected_conn_id",
        [
            # a valid example of connection id
            (
                Connection(
                    conn_id="12312312312213___12312321",
                    conn_type="type",
                    login="user",
                    password="pass",
                    host="host",
                    port=100,
                    schema="schema",
                    extra={"param1": "val1", "param2": "val2"},
                ),
                "12312312312213___12312321",
            ),
            # an invalid example of connection id, which allows potential code execution
            (
                Connection(
                    conn_id="<script>alert(1)</script>",
                    conn_type="type",
                    host="protocol://host",
                    port=100,
                    schema="schema",
                    extra={"param1": "val1", "param2": "val2"},
                ),
                None,
            ),
            # a valid connection as well
            (
                Connection(
                    conn_id="a_valid_conn_id_!!##",
                    conn_type="type",
                    login="user",
                    password="pass",
                    host="protocol://host",
                    port=100,
                    schema="schema",
                    extra={"param1": "val1", "param2": "val2"},
                ),
                "a_valid_conn_id_!!##",
            ),
            # a valid connection as well testing dashes
            (
                Connection(
                    conn_id="a_-.11",
                    conn_type="type",
                    login="user",
                    password="pass",
                    host="protocol://host",
                    port=100,
                    schema="schema",
                    extra={"param1": "val1", "param2": "val2"},
                ),
                "a_-.11",
            ),
        ],
    )
    # Responsible for ensuring that the sanitized connection id
    # string works as expected.
    def test_sanitize_conn_id(self, connection, expected_conn_id):
        assert connection.conn_id == expected_conn_id

    def test_extra_dejson(self):
        extra = (
            '{"trust_env": false, "verify": false, "stream": true, "headers":'
            '{\r\n "Content-Type": "application/json",\r\n  "X-Requested-By": "Airflow"\r\n}}'
        )
        connection = Connection(
            conn_id="pokeapi",
            conn_type="http",
            login="user",
            password="pass",
            host="https://pokeapi.co/",
            port=100,
            schema="https",
            extra=extra,
        )

        assert connection.extra_dejson == {
            "trust_env": False,
            "verify": False,
            "stream": True,
            "headers": {"Content-Type": "application/json", "X-Requested-By": "Airflow"},
        }

    @mock.patch("airflow.sdk.Connection")
    def test_get_connection_from_secrets_task_sdk_success(self, mock_task_sdk_connection):
        """Test the get_connection_from_secrets method with Task SDK success path."""
        # Mock sys.modules for task_runner
        mock_task_runner = mock.MagicMock()
        mock_task_runner.SUPERVISOR_COMMS = True

        expected_connection = Connection(conn_id="test_conn", conn_type="test_type")
        mock_task_sdk_connection.get.return_value = expected_connection

        with mock.patch.dict(sys.modules, {"airflow.sdk.execution_time.task_runner": mock_task_runner}):
            result = Connection.get_connection_from_secrets("test_conn")
            assert result.conn_id == expected_connection.conn_id
            assert result.conn_type == expected_connection.conn_type

    @mock.patch("airflow.sdk.Connection")
    def test_get_connection_from_secrets_task_sdk_not_found(self, mock_task_sdk_connection):
        """Test the get_connection_from_secrets method with Task SDK not found path."""
        mock_task_runner = mock.MagicMock()
        mock_task_runner.SUPERVISOR_COMMS = True

        mock_task_sdk_connection.get.side_effect = AirflowRuntimeError(
            error=ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND)
        )

        with mock.patch.dict(sys.modules, {"airflow.sdk.execution_time.task_runner": mock_task_runner}):
            with pytest.raises(AirflowNotFoundException):
                Connection.get_connection_from_secrets("test_conn")

    @mock.patch.dict(sys.modules, {"airflow.sdk.execution_time.task_runner": None})
    @mock.patch("airflow.sdk.Connection")
    @mock.patch("airflow.secrets.environment_variables.EnvironmentVariablesBackend.get_connection")
    @mock.patch("airflow.secrets.metastore.MetastoreBackend.get_connection")
    def test_get_connection_from_secrets_metastore_backend(
        self, mock_db_backend, mock_env_backend, mock_task_sdk_connection
    ):
        """Test the get_connection_from_secrets should call all the backends."""

        mock_env_backend.return_value = None
        mock_db_backend.return_value = Connection(conn_id="test_conn", conn_type="test", password="pass")

        # Mock TaskSDK Connection to verify it is never imported
        mock_task_sdk_connection.get.side_effect = Exception("TaskSDKConnection should not be used")

        result = Connection.get_connection_from_secrets("test_conn")

        expected_connection = Connection(conn_id="test_conn", conn_type="test", password="pass")

        # Verify the result is from MetastoreBackend
        assert result.conn_id == expected_connection.conn_id
        assert result.conn_type == expected_connection.conn_type

        # Verify TaskSDKConnection was never used
        mock_task_sdk_connection.get.assert_not_called()

        # Verify the backends were called
        mock_env_backend.assert_called_once_with(conn_id="test_conn")
        mock_db_backend.assert_called_once_with(conn_id="test_conn")
