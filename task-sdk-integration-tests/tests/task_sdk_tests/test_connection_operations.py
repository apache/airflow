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
"""
Integration tests for Connection operations.

These tests validate the Execution API endpoints for Connection operations:
- get(): Get connection details

Prerequisites:
- Connection "test_connection" is set via environment variable `AIRFLOW_CONN_TEST_CONNECTION`
"""

from __future__ import annotations

import pytest

from airflow.sdk.api.datamodels._generated import ConnectionResponse
from task_sdk_tests import console


def test_connection_get(sdk_client):
    """
    Test getting connection details.

    Expected: ConnectionResponse with connection details
    Endpoint: GET /execution/connections/{conn_id}
    """
    console.print("[yellow]Getting connection details...")

    response = sdk_client.connections.get("test_connection")

    console.print(" Connection Get Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Connection ID:[/] {response.conn_id}")
    console.print(f"[bright_blue]Connection Type:[/] {response.conn_type}")
    console.print(f"[bright_blue]Host:[/] {response.host}")
    console.print(f"[bright_blue]Schema:[/] {response.schema}")
    console.print("=" * 72)

    assert isinstance(response, ConnectionResponse)
    assert response.conn_id == "test_connection"
    assert response.conn_type == "postgres"
    assert response.host == "testhost"
    assert response.schema_ == "testdb"
    console.print("[green]✅ Connection get test passed!")


@pytest.mark.skip(reason="TODO: Implement Connection get (not found) test")
def test_connection_get_not_found(sdk_client):
    """
    Test getting a non-existent connection.

    Expected: ErrorResponse with CONNECTION_NOT_FOUND error
    Endpoint: GET /execution/connections/{conn_id}
    """
    console.print("[yellow]TODO: Implement test_connection_get_not_found")
    raise NotImplementedError("test_connection_get_not_found not implemented")
