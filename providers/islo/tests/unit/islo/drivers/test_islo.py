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

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from airflow.providers.common.sandbox.exceptions import (
    SandboxInvalidHandleError,
    SandboxLaunchUnfencedError,
)
from airflow.providers.common.sandbox.models import SandboxHandle, SandboxLaunchRequest, SandboxState
from airflow.providers.islo.drivers.islo import IsloSandboxDriver
from airflow.providers.islo.exceptions import IsloProtocolError
from airflow.providers.islo.hooks.islo import AsyncIsloClient
from airflow.providers.islo.models import (
    IsloExecutionResult,
    IsloExecutionStart,
    IsloExecutionState,
    IsloSandboxConfig,
    IsloSandboxHandle,
    sandbox_name_from_request_id,
)


def make_driver() -> tuple[IsloSandboxDriver, AsyncMock]:
    driver = IsloSandboxDriver.__new__(IsloSandboxDriver)
    client = AsyncMock(spec=AsyncIsloClient)
    driver._client = client
    driver._fence_targets = {}
    return driver, client


def make_request() -> SandboxLaunchRequest:
    return SandboxLaunchRequest(
        request_id=str(uuid4()),
        command=("python", "-m", "airflow.sdk.execution_time.execute_workload"),
        env={"AIRFLOW__CORE__EXECUTION_API_SERVER_URL": "https://airflow.example/execution/"},
        provider_config=IsloSandboxConfig(snapshot_name="runtime", vcpus=4).to_json(),
        workdir="/workspace",
        timeout_seconds=300,
        ttl_seconds=600,
    )


@pytest.mark.asyncio
async def test_launch_maps_portable_request_to_islo_api() -> None:
    driver, client = make_driver()
    request = make_request()
    sandbox_name = sandbox_name_from_request_id(request.request_id)
    client.create_sandbox.return_value = (sandbox_name, "sandbox-id")
    client.execute.return_value = IsloExecutionStart("exec-id", "sandbox-id")

    handle = await driver.launch(request)

    spec = client.create_sandbox.await_args.args[0]
    assert spec.name == sandbox_name
    assert spec.config.snapshot_name == "runtime"
    assert spec.config.vcpus == 4
    assert spec.ttl_seconds == 600
    client.execute.assert_awaited_once_with(
        sandbox_name,
        list(request.command),
        request.env,
        workdir="/workspace",
        timeout_seconds=300,
    )
    assert IsloSandboxHandle.from_common(handle).execution_id == "exec-id"


@pytest.mark.asyncio
async def test_launch_rejects_mismatched_exec_sandbox() -> None:
    driver, client = make_driver()
    request = make_request()
    sandbox_name = sandbox_name_from_request_id(request.request_id)
    client.create_sandbox.return_value = (sandbox_name, "sandbox-id")
    client.execute.return_value = IsloExecutionStart("exec-id", "other-sandbox-id")

    with pytest.raises(IsloProtocolError, match="expected"):
        await driver.launch(request)


def test_handle_validation_checks_vendor_schema_and_request_id() -> None:
    driver, _ = make_driver()
    request = make_request()
    with pytest.raises(SandboxInvalidHandleError, match="fields"):
        driver.validate_handle(
            SandboxHandle({"request_id": request.request_id}), request_id=request.request_id
        )

    other_request_id = str(uuid4())
    handle = IsloSandboxHandle(
        other_request_id,
        sandbox_name_from_request_id(other_request_id),
        "sandbox-id",
        "exec-id",
    ).to_common()
    with pytest.raises(SandboxInvalidHandleError, match="does not match"):
        driver.validate_handle(handle, request_id=request.request_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("islo_state", "sandbox_state", "exit_code"),
    [
        (IsloExecutionState.PENDING, SandboxState.PENDING, 7),
        (IsloExecutionState.RUNNING, SandboxState.RUNNING, 7),
        (IsloExecutionState.SUCCEEDED, SandboxState.SUCCEEDED, 0),
        (IsloExecutionState.FAILED, SandboxState.FAILED, 7),
        (IsloExecutionState.GONE, SandboxState.GONE, 7),
    ],
)
async def test_status_mapping(
    islo_state: IsloExecutionState, sandbox_state: SandboxState, exit_code: int
) -> None:
    driver, client = make_driver()
    request = make_request()
    handle = IsloSandboxHandle(
        request.request_id,
        sandbox_name_from_request_id(request.request_id),
        "sandbox-id",
        "exec-id",
    ).to_common()
    client.execution_result.return_value = IsloExecutionResult(islo_state, exit_code)

    assert (await driver.get_status(handle)).state is sandbox_state


@pytest.mark.asyncio
async def test_failure_status_does_not_copy_task_output_into_scheduler_info() -> None:
    driver, client = make_driver()
    request = make_request()
    handle = IsloSandboxHandle(
        request.request_id,
        sandbox_name_from_request_id(request.request_id),
        "sandbox-id",
        "exec-id",
    ).to_common()
    client.execution_result.return_value = IsloExecutionResult(
        IsloExecutionState.FAILED,
        7,
        stdout="possibly sensitive output",
        stderr="possibly sensitive error",
    )

    result = await driver.get_status(handle)

    assert result.message is None


@pytest.mark.asyncio
async def test_output_reuses_full_execution_result() -> None:
    driver, client = make_driver()
    request = make_request()
    handle = IsloSandboxHandle(
        request.request_id,
        sandbox_name_from_request_id(request.request_id),
        "sandbox-id",
        "exec-id",
    ).to_common()
    client.execution_result.return_value = IsloExecutionResult(
        IsloExecutionState.RUNNING,
        stdout="task output",
        stderr="task error",
        truncated=True,
    )

    output = await driver.get_output(handle)

    assert output.stdout == "task output"
    assert output.stderr == "task error"
    assert output.truncated is True
    client.execution_result.assert_awaited_once()


@pytest.mark.asyncio
async def test_mismatched_created_name_must_be_deleted_or_reported_unfenced() -> None:
    driver, client = make_driver()
    request = make_request()
    client.create_sandbox.return_value = ("unexpected-name", "sandbox-id")
    client.delete_sandbox.side_effect = OSError("delete failed")

    with pytest.raises(SandboxLaunchUnfencedError):
        await driver.launch(request)

    client.delete_sandbox.assert_awaited_once_with("unexpected-name")

    client.delete_sandbox.reset_mock(side_effect=True)
    await driver.fence(request.request_id)
    assert {call.args[0] for call in client.delete_sandbox.await_args_list} == {
        "unexpected-name",
        sandbox_name_from_request_id(request.request_id),
    }


@pytest.mark.asyncio
async def test_terminate_and_fence_are_idempotent_delete_operations() -> None:
    driver, client = make_driver()
    request = make_request()
    handle = IsloSandboxHandle(
        request.request_id,
        sandbox_name_from_request_id(request.request_id),
        "sandbox-id",
        "exec-id",
    ).to_common()
    client.get_sandbox_id.return_value = "sandbox-id"

    await driver.terminate(handle)
    await driver.fence(request.request_id)

    expected_name = sandbox_name_from_request_id(request.request_id)
    assert [call.args[0] for call in client.delete_sandbox.await_args_list] == [
        expected_name,
        expected_name,
    ]


@pytest.mark.asyncio
async def test_terminate_refuses_a_reused_sandbox_name() -> None:
    driver, client = make_driver()
    request = make_request()
    handle = IsloSandboxHandle(
        request.request_id,
        sandbox_name_from_request_id(request.request_id),
        "original-id",
        "exec-id",
    ).to_common()
    client.get_sandbox_id.return_value = "replacement-id"

    with pytest.raises(IsloProtocolError, match="stable ID does not match"):
        await driver.terminate(handle)

    client.delete_sandbox.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminate_is_idempotent_when_sandbox_is_absent() -> None:
    driver, client = make_driver()
    request = make_request()
    handle = IsloSandboxHandle(
        request.request_id,
        sandbox_name_from_request_id(request.request_id),
        "sandbox-id",
        "exec-id",
    ).to_common()
    client.get_sandbox_id.return_value = None

    await driver.terminate(handle)

    client.delete_sandbox.assert_not_awaited()
