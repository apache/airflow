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
Integration tests for XCom operations for task execution time.

These tests validate the Execution API endpoints for XCom operations:
- get(): Get XCom value
- set(): Set XCom value
- delete(): Delete XCom value
"""

from __future__ import annotations

import pytest

from airflow.sdk.api.datamodels._generated import XComResponse
from airflow.sdk.execution_time.comms import OKResponse
from task_sdk_tests import console


def test_get_xcom(sdk_client, dag_info):
    """
    Test getting existing XCom value from `return_tuple_task`.

    Note: XCom APIs return data in serialized format and that is what we are testing.
    """
    console.print("[yellow]Getting existing XCom from return_tuple_task...")

    response = sdk_client.xcoms.get(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="return_tuple_task",
        key="return_value",
    )

    console.print(" XCom Get Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Key:[/] {response.key}")
    console.print(f"[bright_blue]Value Type:[/] {type(response.value).__name__}")
    console.print(f"[bright_blue]Value:[/] {response.value}")
    console.print("=" * 72)

    assert isinstance(response, XComResponse)
    assert response.key == "return_value"
    assert response.value == {
        "__classname__": "builtins.tuple",
        "__version__": 1,
        "__data__": [1, "test_value"],
    }
    console.print("[green]✅ XCom get test passed!")


@pytest.mark.skip(reason="TODO: Implement XCom get (not found) test")
def test_get_xcom_not_found(sdk_client, dag_info):
    """
    Test getting non-existent XCom value.

    Expected: XComResponse with value=None or ErrorResponse
    Endpoint: GET /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}
    """
    console.print("[yellow]TODO: Implement test_get_xcom_not_found")
    raise NotImplementedError("test_get_xcom_not_found not implemented")


def test_set_xcom(sdk_client, dag_info):
    """
    Test setting XCom value and then getting it to ensure set worked.
    """
    console.print("[yellow]Setting XCom value...")

    test_key = "test_xcom_key"
    test_value = {"test": "data", "number": 42}

    set_response = sdk_client.xcoms.set(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="long_running_task",
        key=test_key,
        value=test_value,
    )

    console.print(" XCom Set Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(set_response).__name__}")
    console.print(f"[bright_blue]Status:[/] {set_response.ok}")
    console.print("=" * 72)

    assert isinstance(set_response, OKResponse)
    assert set_response.ok is True

    console.print("[yellow]Getting XCom value...")
    get_response = sdk_client.xcoms.get(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="long_running_task",
        key=test_key,
    )

    console.print(" XCom Get Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(get_response).__name__}")
    console.print(f"[bright_blue]Key:[/] {get_response.key}")
    console.print(f"[bright_blue]Value:[/] {get_response.value}")
    console.print("=" * 72)

    assert isinstance(get_response, XComResponse)
    assert get_response.key == test_key
    assert get_response.value == test_value
    console.print("[green]✅ XCom set and get test passed!")


def test_xcom_delete(sdk_client, dag_info):
    """
    Test deleting XCom value.
    """
    console.print("[yellow]Deleting XCom value...")

    test_key = "test_xcom_key_delete"

    # Set XCom first
    sdk_client.xcoms.set(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="long_running_task",
        key=test_key,
        value="to_be_deleted",
    )

    # Delete XCom
    delete_response = sdk_client.xcoms.delete(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="long_running_task",
        key=test_key,
    )

    console.print(" XCom Delete Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(delete_response).__name__}")
    console.print(f"[bright_blue]Status:[/] {delete_response.ok}")
    console.print("=" * 72)

    assert isinstance(delete_response, OKResponse)
    assert delete_response.ok is True

    console.print("[yellow]Verifying XCom was deleted...")
    get_response = sdk_client.xcoms.get(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="long_running_task",
        key=test_key,
    )

    console.print(" XCom Get After Delete ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(get_response).__name__}")
    console.print(f"[bright_blue]Key:[/] {get_response.key}")
    console.print(f"[bright_blue]Value:[/] {get_response.value}")
    console.print("=" * 72)

    assert isinstance(get_response, XComResponse)
    assert get_response.key == test_key
    assert get_response.value is None
    console.print("[green]✅ XCom delete test passed!")


@pytest.mark.skip(reason="TODO: Implement XCom head test")
def test_xcom_head(sdk_client, dag_info):
    """
    Test getting count of mapped XCom values.

    Expected: XComCountResponse with len field in it (should be ideally equal to number of mapped tasks, since we have None it might throw RuntimeError)
    Endpoint: HEAD /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}
    """
    console.print("[yellow]TODO: Implement test_xcom_head")
    raise NotImplementedError("test_xcom_head not implemented")


@pytest.mark.skip(reason="TODO: Implement XCom get_sequence_item test")
def test_xcom_get_sequence_item(sdk_client, dag_info):
    """
    Test getting XCom sequence item by offset.

    Expected: XComSequenceIndexResponse with value
    Endpoint: GET /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}/item/{offset}
    """
    console.print("[yellow]TODO: Implement test_xcom_get_sequence_item")
    raise NotImplementedError("test_xcom_get_sequence_item not implemented")


@pytest.mark.skip(reason="TODO: Implement XCom get_sequence_item (not found) test")
def test_xcom_get_sequence_item_not_found(sdk_client, dag_info):
    """
    Test getting non-existent XCom sequence item.

    Expected: ErrorResponse with XCOM_NOT_FOUND error
    Endpoint: GET /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}/item/{offset}
    """
    console.print("[yellow]TODO: Implement test_xcom_get_sequence_item_not_found")
    raise NotImplementedError("test_xcom_get_sequence_item_not_found not implemented")


@pytest.mark.skip(reason="TODO: Implement XCom get_sequence_slice test")
def test_xcom_get_sequence_slice(sdk_client, dag_info):
    """
    Test getting XCom sequence slice.

    Expected: XComSequenceSliceResponse with list of values
    Endpoint: GET /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}/slice
    """
    console.print("[yellow]TODO: Implement test_xcom_get_sequence_slice")
    raise NotImplementedError("test_xcom_get_sequence_slice not implemented")


@pytest.mark.skip(reason="TODO: Implement XCom get_sequence_slice (not found) test")
def test_xcom_get_sequence_slice_not_found(sdk_client, dag_info):
    """
    Test getting slice for non-existent XCom key.

    Expected: XComSequenceSliceResponse as empty list
    Endpoint: GET /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}/slice
    """
    console.print("[yellow]TODO: Implement test_xcom_get_sequence_slice_not_found")
    raise NotImplementedError("test_xcom_get_sequence_slice_not_found not implemented")
