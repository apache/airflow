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

from airflow.sdk.api.datamodels._generated import (
    XComResponse,
    XComSequenceIndexResponse,
    XComSequenceSliceResponse,
)
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import ErrorResponse, OKResponse, XComCountResponse
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


def test_get_xcom_not_found(sdk_client, dag_info):
    """
    Test getting non-existent XCom value.

    Expected: XComResponse with value=None or ErrorResponse
    Endpoint: GET /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}
    """
    missing_key = "non_existent_xcom_key_for_test"
    console.print("[yellow]Getting non-existent XCom key...")

    response = sdk_client.xcoms.get(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="get_task_instance_id",
        key=missing_key,
    )

    console.print(" XCom Get (Not Found) Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Key:[/] {response.key}")
    console.print(f"[bright_blue]Value:[/] {response.value}")
    console.print("=" * 72)

    assert isinstance(response, XComResponse)
    assert response.key == missing_key
    assert response.value is None
    console.print("[green]✅ XCom not-found test passed!")


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

    sdk_client.xcoms.set(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="long_running_task",
        key=test_key,
        value="to_be_deleted",
    )

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


def test_xcom_head(sdk_client, dag_info):
    """
    Test getting count of mapped XCom values.

    Expected: XComCountResponse with len field in it (should be ideally equal to number of mapped tasks, since we have None it might throw RuntimeError)
    Endpoint: HEAD /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}
    """
    console.print("[yellow]Testing XCom head for non-mapped task...")

    response_single = sdk_client.xcoms.head(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="return_tuple_task",
        key="return_value",
    )

    console.print(" XCom Head Response (Non-Mapped) ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response_single).__name__}")
    console.print(f"[bright_blue]Count:[/] {response_single.len}")
    console.print("=" * 72)

    assert isinstance(response_single, XComCountResponse)
    assert response_single.len == 1

    console.print("[yellow]Testing XCom head for mapped task...")

    response_mapped = sdk_client.xcoms.head(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="mapped_task",
        key="return_value",
    )

    console.print(" XCom Head Response (Mapped) ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response_mapped).__name__}")
    console.print(f"[bright_blue]Count:[/] {response_mapped.len}")
    console.print("=" * 72)

    assert isinstance(response_mapped, XComCountResponse)
    assert response_mapped.len == 4
    console.print("[green]✅ XCom head test passed!")


def test_xcom_get_sequence_item(sdk_client, dag_info):
    """
    Test getting XCom sequence item by offset.

    Expected: XComSequenceIndexResponse with value
    Endpoint: GET /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}/item/{offset}
    """
    console.print("[yellow]Testing XCom sequence item access...")

    response_0 = sdk_client.xcoms.get_sequence_item(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="mapped_task",
        key="return_value",
        offset=0,
    )

    console.print(" XCom Sequence Item [offset=0] ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response_0).__name__}")
    console.print(f"[bright_blue]Value:[/] {response_0.root}")
    console.print("=" * 72)

    assert isinstance(response_0, XComSequenceIndexResponse)
    assert response_0.root == "processed_alpha"

    response_neg1 = sdk_client.xcoms.get_sequence_item(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="mapped_task",
        key="return_value",
        offset=-1,
    )

    console.print(" XCom Sequence Item [offset=-1] ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response_neg1).__name__}")
    console.print(f"[bright_blue]Value:[/] {response_neg1.root}")
    console.print("=" * 72)

    assert isinstance(response_neg1, XComSequenceIndexResponse)
    assert response_neg1.root == "processed_delta"

    response_2 = sdk_client.xcoms.get_sequence_item(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="mapped_task",
        key="return_value",
        offset=2,
    )

    console.print(" XCom Sequence Item [offset=2] ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response_2).__name__}")
    console.print(f"[bright_blue]Value:[/] {response_2.root}")
    console.print("=" * 72)

    assert isinstance(response_2, XComSequenceIndexResponse)
    assert response_2.root == "processed_gamma"  # Third mapped instance (index 2)

    console.print("[green]✅ XCom get_sequence_item test passed!")


def test_xcom_get_sequence_item_not_found(sdk_client, dag_info):
    """
    Test getting non-existent XCom sequence item.

    Expected: ErrorResponse with XCOM_NOT_FOUND error
    Endpoint: GET /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}/item/{offset}
    """
    console.print("[yellow]Testing XCom sequence item not found...")

    response = sdk_client.xcoms.get_sequence_item(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="mapped_task",
        key="return_value",
        offset=10,
    )

    console.print(" XCom Sequence Item Not Found Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Error:[/] {response.error}")
    console.print(f"[bright_blue]Detail:[/] {response.detail}")
    console.print("=" * 72)

    assert isinstance(response, ErrorResponse)
    assert response.error == ErrorType.XCOM_NOT_FOUND
    assert response.detail["key"] == "return_value"
    assert response.detail["offset"] == 10

    response_bad_key = sdk_client.xcoms.get_sequence_item(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="mapped_task",
        key="non_existent_key",
        offset=0,
    )

    console.print(" XCom Sequence Item (Bad Key) Response ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response_bad_key).__name__}")
    console.print(f"[bright_blue]Error:[/] {response_bad_key.error}")
    console.print("=" * 72)

    assert isinstance(response_bad_key, ErrorResponse)
    assert response_bad_key.error == ErrorType.XCOM_NOT_FOUND

    console.print("[green]✅ XCom get_sequence_item_not_found test passed!")


def test_xcom_get_sequence_slice(sdk_client, dag_info):
    """
    Test getting XCom sequence slice.

    Expected: XComSequenceSliceResponse with list of values
    Endpoint: GET /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}/slice
    """
    console.print("[yellow]Testing XCom sequence slice access...")

    response_full = sdk_client.xcoms.get_sequence_slice(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="mapped_task",
        key="return_value",
        start=None,
        stop=None,
        step=None,
    )

    console.print(" XCom Sequence Slice (full) ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response_full).__name__}")
    console.print(f"[bright_blue]Values:[/] {response_full.root}")
    console.print("=" * 72)

    assert isinstance(response_full, XComSequenceSliceResponse)
    assert response_full.root == ["processed_alpha", "processed_beta", "processed_gamma", "processed_delta"]

    response_slice = sdk_client.xcoms.get_sequence_slice(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="mapped_task",
        key="return_value",
        start=1,
        stop=3,
        step=None,
    )

    console.print(" XCom Sequence Slice [start=1,stop=3] ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response_slice).__name__}")
    console.print(f"[bright_blue]Values:[/] {response_slice.root}")
    console.print("=" * 72)

    assert isinstance(response_slice, XComSequenceSliceResponse)
    assert response_slice.root == ["processed_beta", "processed_gamma"]

    response_step = sdk_client.xcoms.get_sequence_slice(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="mapped_task",
        key="return_value",
        start=0,
        stop=4,
        step=2,
    )

    console.print(" XCom Sequence Slice [step=2] ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response_step).__name__}")
    console.print(f"[bright_blue]Values:[/] {response_step.root}")
    console.print("=" * 72)

    assert isinstance(response_step, XComSequenceSliceResponse)
    assert response_step.root == ["processed_alpha", "processed_gamma"]

    console.print("[green]✅ XCom get_sequence_slice test passed!")


def test_xcom_get_sequence_slice_not_found(sdk_client, dag_info):
    """
    Test getting slice for non-existent XCom key.

    Expected: XComSequenceSliceResponse as empty list
    Endpoint: GET /execution/xcoms/{dag_id}/{run_id}/{task_id}/{key}/slice
    """
    console.print("[yellow]Testing XCom sequence slice not found...")

    response = sdk_client.xcoms.get_sequence_slice(
        dag_id=dag_info["dag_id"],
        run_id=dag_info["dag_run_id"],
        task_id="mapped_task",
        key="non_existent_key",
        start=0,
        stop=10,
        step=None,
    )

    console.print(" XCom Sequence Slice (Not Found) ".center(72, "="))
    console.print(f"[bright_blue]Response Type:[/] {type(response).__name__}")
    console.print(f"[bright_blue]Values:[/] {getattr(response, 'root', None)}")
    console.print("=" * 72)

    assert isinstance(response, XComSequenceSliceResponse)
    assert response.root == []
    console.print("[green]✅ XCom get_sequence_slice_not_found test passed!")
