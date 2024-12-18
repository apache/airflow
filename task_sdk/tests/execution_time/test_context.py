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

from airflow.sdk.definitions.connection import Connection
from airflow.sdk.execution_time.comms import ConnectionResult
from airflow.sdk.execution_time.context import _convert_connection_result_conn


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
