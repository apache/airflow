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
"""
Integration tests for Variable operations.

These tests validate the Execution API endpoints for Variable operations:
- get(): Get Variable value (positive, negative)
- set(): Set Variable value
- delete(): Delete Variable value
"""

from __future__ import annotations

from airflow.sdk.api.datamodels._generated import VariableResponse
from airflow.sdk.execution_time.comms import ErrorResponse, OKResponse
from task_sdk_tests import console


def test_variable_get(sdk_client):
    """
    Test getting variable value.

    Expected: VariableResponse with key and value for existing variable
    Endpoint: GET /execution/variables/{key}
    """
    console.print("[yellow]Getting variable value...")

    response = sdk_client.variables.get("test_variable_key")

    console.print(" Variable Get Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Key:[/] {response.key}")
    console.print(f"[bright_blue]Value:[/] {response.value}")
    console.print("=" * 72)

    assert isinstance(response, VariableResponse)
    assert response.key == "test_variable_key"
    assert response.value == "test_variable_value"
    console.print("[green]✅ Variable get test passed!")


def test_variable_get_not_found(sdk_client):
    """
    Test getting non-existent variable.

    Expected: ErrorResponse with VARIABLE_NOT_FOUND error
    Endpoint: GET /execution/variables/{key}
    """
    console.print("[yellow]Getting non-existent variable...")

    response = sdk_client.variables.get("non_existent_variable_key")

    console.print(" Variable Get Error Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Error Type:[/] {response.error}")
    console.print(f"[bright_blue]Detail:[/] {response.detail}")
    console.print("=" * 72)

    assert isinstance(response, ErrorResponse)
    assert str(response.error).find("VARIABLE_NOT_FOUND") != -1
    console.print("[green]✅ Variable get (not found) test passed!")


def test_variable_set(sdk_client):
    """
    Test setting variable value.

    Expected: OKResponse with ok=True
    Endpoint: PUT /execution/variables/{key}
    """
    response = sdk_client.variables.set("test_variable_key", "test_variable_value")

    console.print(" Variable Set Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Status:[/] {response.ok}")
    console.print("=" * 72)

    assert isinstance(response, OKResponse)
    assert response.ok is True

    console.print("[green]✅ Variable set test passed!")


def test_variable_delete(sdk_client):
    """
    Test deleting variable value.

    Expected: OKResponse with ok=True
    Endpoint: DELETE /execution/variables/{key}
    """
    console.print("[yellow]Deleting variable...")

    # When using the test_variable_key, we noticed an issue where it appears that the variable that was
    # being set/deleted was still available when retrieved later. Per @amoghrajesh, in Docker Compose [we've]
    # defined an environment variable for test_variable_key and secrets backends are read-only, so when [our]
    # test calls Variable.set(), the value is written to the database — and when you delete it, Airflow
    # correctly removes it from the DB... However, a calling Variable.get() once the Variable had been
    # deleted pointed to the Secrets Backend, which still had that key available.
    key: str = "test_variable_delete_key"

    # First, set the variable
    sdk_client.variables.set(key, "test_variable_value")

    # Now, delete the variable
    response = sdk_client.variables.delete(key)

    console.print(" Variable Delete Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Status:[/] {response.ok}")
    console.print("=" * 72)

    assert isinstance(response, OKResponse)
    assert response.ok is True

    # Validate that the Variable has in fact been deleted
    get_response = sdk_client.variables.get(key)

    console.print(" Variable Get After Delete ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(get_response).__name__}")
    console.print(f"[bright_blue]Error Type:[/] {get_response.error}")
    console.print(f"[bright_blue]Detail:[/] {get_response.detail}")
    console.print("=" * 72)

    assert isinstance(get_response, ErrorResponse)
    assert str(get_response.error).find("VARIABLE_NOT_FOUND") != -1

    console.print("[green]✅ Variable delete test passed!")
