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
- get(): Get variable value
"""

from __future__ import annotations

import pytest

from airflow.sdk.api.datamodels._generated import VariableResponse
from airflow.sdk.execution_time.comms import ErrorResponse
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


@pytest.mark.skip(reason="TODO: Implement Variable set test")
def test_variable_set(sdk_client):
    """
    Test setting variable value.

    Expected: OKResponse with ok=True
    Endpoint: PUT /execution/variables/{key}
    """
    console.print("[yellow]TODO: Implement test_variable_set")
    raise NotImplementedError("test_variable_set not implemented")


@pytest.mark.skip(reason="TODO: Implement Variable delete test")
def test_variable_delete(sdk_client):
    """
    Test deleting variable value.

    Expected: OKResponse with ok=True
    Endpoint: DELETE /execution/variables/{key}
    """
    console.print("[yellow]TODO: Implement test_variable_delete")
    raise NotImplementedError("test_variable_delete not implemented")
