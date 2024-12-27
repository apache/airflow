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

from airflow.sdk.definitions.connection import Connection
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import ConnectionResult, ErrorResponse
from airflow.sdk.execution_time.context import ConnectionAccessor, _convert_connection_result_conn


def test_convert_connection_result_conn():
    """Test that the ConnectionResult is converted to a Connection object."""
    conn = ConnectionResult(
        conn_id="test_conn",
        conn_type="mysql",
        host="mysql",
        schema="airflow",
        login="root",
        password="password",
        port=1234,
        extra='{"extra_key": "extra_value"}',
    )
    conn = _convert_connection_result_conn(conn)
    assert conn == Connection(
        conn_id="test_conn",
        conn_type="mysql",
        host="mysql",
        schema="airflow",
        login="root",
        password="password",
        port=1234,
        extra='{"extra_key": "extra_value"}',
    )


class TestConnectionAccessor:
    def test_getattr_connection(self):
        """
        Test that the connection is fetched when accessed via __getattr__.

        The __getattr__ method is used for template rendering. Example: ``{{ conn.mysql_conn.host }}``.
        """
        accessor = ConnectionAccessor()

        # Conn from the supervisor / API Server
        conn_result = ConnectionResult(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)

        with mock.patch(
            "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
        ) as mock_supervisor_comms:
            mock_supervisor_comms.get_message.return_value = conn_result

            # Fetch the connection; triggers __getattr__
            conn = accessor.mysql_conn

            expected_conn = Connection(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)
            assert conn == expected_conn

    def test_get_method_valid_connection(self):
        """Test that the get method returns the requested connection using `conn.get`."""
        accessor = ConnectionAccessor()
        conn_result = ConnectionResult(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)

        with mock.patch(
            "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
        ) as mock_supervisor_comms:
            mock_supervisor_comms.get_message.return_value = conn_result

            conn = accessor.get("mysql_conn")
            assert conn == Connection(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)

    def test_get_method_with_default(self):
        """Test that the get method returns the default connection when the requested connection is not found."""
        accessor = ConnectionAccessor()
        default_conn = {"conn_id": "default_conn", "conn_type": "sqlite"}
        error_response = ErrorResponse(
            error=ErrorType.CONNECTION_NOT_FOUND, detail={"conn_id": "nonexistent_conn"}
        )

        with mock.patch(
            "airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", create=True
        ) as mock_supervisor_comms:
            mock_supervisor_comms.get_message.return_value = error_response

            conn = accessor.get("nonexistent_conn", default_conn=default_conn)
            assert conn == default_conn
