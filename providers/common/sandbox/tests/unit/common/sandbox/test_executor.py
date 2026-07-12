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

import asyncio
from concurrent.futures import Future
from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from airflow.executors.workloads import BundleInfo, ExecuteTask, TaskInstanceDTO
from airflow.providers.common.sandbox.driver import SandboxDriver
from airflow.providers.common.sandbox.exceptions import (
    SandboxConfigurationError,
    SandboxInvalidHandleError,
    SandboxLaunchUnfencedError,
)
from airflow.providers.common.sandbox.executor import BaseSandboxExecutor, _SandboxExecutorManager
from airflow.providers.common.sandbox.models import (
    RunningSandbox,
    SandboxExecutionRef,
    SandboxHandle,
    SandboxLaunchConfig,
    SandboxLaunchRequest,
    SandboxResult,
    SandboxState,
)
from airflow.utils.state import TaskInstanceState


class FakeManager:
    def __init__(self) -> None:
        self.launches: list[tuple[Future, SandboxLaunchRequest, bool]] = []
        self.statuses: list[tuple[Future, SandboxExecutionRef]] = []
        self.terminations: list[tuple[Future, SandboxExecutionRef]] = []
        self.fences: list[tuple[Future, str]] = []
        self.recoveries: list[tuple[Future, str]] = []
        self.recovery_result: RunningSandbox | None = None
        self.validation_error: Exception | None = None
        self.started = False
        self.closed = False

    def start(self) -> None:
        self.started = True

    def submit_health_check(self) -> Future:
        future: Future[None] = Future()
        future.set_result(None)
        return future

    def submit_launch(self, request: SandboxLaunchRequest) -> Future:
        future: Future[RunningSandbox] = Future()
        self.launches.append((future, request, request.keep))
        return future

    def submit_status(self, ref: SandboxExecutionRef) -> Future:
        future: Future[SandboxResult] = Future()
        self.statuses.append((future, ref))
        return future

    def submit_terminate(self, ref: SandboxExecutionRef) -> Future:
        future: Future[None] = Future()
        self.terminations.append((future, ref))
        return future

    def submit_fence(self, request_id: str) -> Future:
        future: Future[None] = Future()
        self.fences.append((future, request_id))
        return future

    def submit_validate(self, ref: SandboxExecutionRef) -> Future:
        future: Future[None] = Future()
        if self.validation_error is None:
            future.set_result(None)
        else:
            future.set_exception(self.validation_error)
        return future

    def submit_recover(self, request_id: str) -> Future:
        future: Future[RunningSandbox | None] = Future()
        future.set_result(self.recovery_result)
        self.recoveries.append((future, request_id))
        return future

    def close(self) -> None:
        self.closed = True


class _FakeSandboxExecutor(BaseSandboxExecutor):
    driver_id = "fake"
    config_section = "fake_sandbox"

    def get_driver_factory(self):
        raise AssertionError("tests inject a manager")

    def build_launch_config(self, workload: ExecuteTask, request_id: str) -> SandboxLaunchConfig:
        del workload, request_id
        return SandboxLaunchConfig(provider_config={"runtime": "fake"})


def make_workload(*, task_id: str = "task") -> ExecuteTask:
    return ExecuteTask(
        ti=TaskInstanceDTO(
            id=uuid4(),
            dag_version_id=uuid4(),
            task_id=task_id,
            dag_id="dag",
            run_id="run",
            try_number=1,
            map_index=-1,
            pool_slots=1,
            queue="default",
            priority_weight=1,
            external_executor_id=str(uuid4()),
            executor_config=None,
        ),
        dag_rel_path=PurePosixPath("dag.py"),
        token="jwt",
        bundle_info=BundleInfo(name="dags-folder", version=None),
        log_path="dag/task.log",
    )


def make_ref(request_id: str, *, keep: bool = False) -> SandboxExecutionRef:
    return SandboxExecutionRef(
        driver="fake",
        request_id=request_id,
        handle=SandboxHandle({"job_id": "job"}, display_name="fake-job"),
        keep=keep,
    )


@pytest.fixture
def executor(monkeypatch) -> tuple[_FakeSandboxExecutor, FakeManager]:
    monkeypatch.setenv("AIRFLOW__LOGGING__REMOTE_LOGGING", "True")
    monkeypatch.setenv("AIRFLOW__FAKE_SANDBOX__CHECK_HEALTH_ON_STARTUP", "False")
    manager = FakeManager()
    instance = _FakeSandboxExecutor(parallelism=4)
    instance._manager = cast("_SandboxExecutorManager", manager)
    return instance, manager


def test_workload_lifecycle_uses_task_sdk_and_reports_terminal_state(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    key = workload.ti.key
    instance.queued_tasks[key] = workload

    instance._process_workloads([workload])

    assert key in instance.running
    launch_future, request, keep = manager.launches[0]
    assert request.request_id == workload.ti.external_executor_id
    assert request.command[:4] == (
        "python",
        "-m",
        "airflow.sdk.execution_time.execute_workload",
        "--json-string",
    )
    assert request.env["AIRFLOW_IS_EXECUTOR_CONTAINER"] == "true"
    assert keep is False

    ref = make_ref(request.request_id)
    launch_future.set_result(RunningSandbox(ref))
    instance.sync()
    assert instance.event_buffer[key] == (TaskInstanceState.RUNNING, ref.encode())

    manager.statuses[0][0].set_result(SandboxResult(SandboxState.SUCCEEDED, 0))
    instance.sync()
    assert instance.event_buffer[key] == (TaskInstanceState.SUCCESS, None)
    assert key not in instance.running
    assert manager.terminations[-1][1] == ref


def test_transient_status_errors_back_off_without_failing(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    key = workload.ti.key
    ref = make_ref(str(workload.ti.external_executor_id))
    instance.running.add(key)
    instance._active[key] = RunningSandbox(ref)
    instance._next_poll[key] = 0

    instance._schedule_statuses()
    manager.statuses[-1][0].set_exception(TimeoutError("provider timeout"))
    instance._drain_statuses()

    assert key in instance.running
    assert key not in instance.event_buffer
    assert instance._poll_failures[key] == 1


def test_unfenced_launch_is_fenced_before_failure(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    key = workload.ti.key
    instance.queued_tasks[key] = workload
    instance._process_workloads([workload])
    launch_future, request, _ = manager.launches[0]
    launch_future.set_exception(
        SandboxLaunchUnfencedError(
            request.request_id,
            TimeoutError("launch response lost"),
            OSError("fence failed"),
        )
    )

    instance.sync()

    assert key in instance.running
    assert key not in instance.event_buffer
    fence_future, fenced_id = manager.fences[-1]
    assert fenced_id == request.request_id
    fence_future.set_result(None)
    instance.sync()
    assert instance.event_buffer[key][0] == TaskInstanceState.FAILED


def test_adoption_restores_full_ref_and_recovers_bare_request(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    ref = make_ref(str(workload.ti.external_executor_id))
    adopted_ti = SimpleNamespace(key=workload.ti.key, external_executor_id=ref.encode())
    bare_id = str(uuid4())
    bare_ti = SimpleNamespace(key=make_workload(task_id="bare").ti.key, external_executor_id=bare_id)

    not_adopted = instance.try_adopt_task_instances([adopted_ti, bare_ti])

    assert adopted_ti.key in instance.running
    assert instance._active[adopted_ti.key].ref == ref
    assert not_adopted == [bare_ti]
    assert manager.recoveries[-1][1] == bare_id


def test_adoption_can_persist_handle_recovered_by_driver(executor) -> None:
    instance, manager = executor
    request_id = str(uuid4())
    ti = SimpleNamespace(key=make_workload().ti.key, external_executor_id=request_id)
    recovered_ref = SandboxExecutionRef(
        driver="fake",
        request_id=request_id,
        handle=SandboxHandle({"job_id": "recovered"}, "recovered-job"),
        keep=True,
    )
    manager.recovery_result = RunningSandbox(recovered_ref)

    assert instance.try_adopt_task_instances([ti]) == []

    ref = instance._active[ti.key].ref
    assert ref.request_id == request_id
    assert ref.handle == recovered_ref.handle
    assert ref.keep is True
    assert instance.event_buffer[ti.key] == (TaskInstanceState.RUNNING, ref.encode())


def test_keep_survives_adoption_and_skips_terminal_cleanup(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    key = workload.ti.key
    ref = make_ref(str(workload.ti.external_executor_id), keep=True)
    ti = SimpleNamespace(key=key, external_executor_id=ref.encode())
    assert instance.try_adopt_task_instances([ti]) == []
    instance._schedule_statuses()
    manager.statuses[-1][0].set_result(SandboxResult(SandboxState.SUCCEEDED, 0))

    instance.sync()

    assert not manager.terminations


def test_revoke_retries_with_deterministic_fence_without_state_event(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    key = workload.ti.key
    ref = make_ref(str(workload.ti.external_executor_id))
    instance.running.add(key)
    instance._active[key] = RunningSandbox(ref)
    ti = SimpleNamespace(key=key, external_executor_id=ref.encode())

    instance.revoke_task(ti=ti)
    manager.terminations[0][0].set_exception(TimeoutError("delete timed out"))
    instance.sync()
    instance._fences[key].next_attempt = 0
    instance.sync()
    fence_future, request_id = manager.fences[-1]
    assert request_id == ref.request_id
    fence_future.set_result(None)
    instance.sync()

    assert key not in instance.event_buffer
    assert key not in instance._fences


def test_terminal_cleanup_failure_retries_with_request_fence(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    key = workload.ti.key
    ref = make_ref(str(workload.ti.external_executor_id))
    instance.running.add(key)
    instance._active[key] = RunningSandbox(ref)
    instance._next_poll[key] = 0
    instance._schedule_statuses()
    manager.statuses[-1][0].set_result(SandboxResult(SandboxState.SUCCEEDED, 0))
    instance.sync()

    manager.terminations[-1][0].set_exception(TimeoutError("delete timed out"))
    instance.sync()
    instance._cleanup_fences[ref.request_id].next_attempt = 0
    instance.sync()

    fence_future, request_id = manager.fences[-1]
    assert request_id == ref.request_id
    fence_future.set_result(None)
    instance.sync()
    assert ref.request_id not in instance._cleanup_fences
    assert instance.event_buffer[key][0] == TaskInstanceState.SUCCESS


def test_invalid_persisted_handle_is_fenced_instead_of_polled(executor) -> None:
    instance, manager = executor
    request_id = str(uuid4())
    ti = SimpleNamespace(
        key=make_workload(task_id="invalid-handle").ti.key,
        external_executor_id=make_ref(request_id).encode(),
    )
    manager.validation_error = SandboxInvalidHandleError("bad vendor schema")

    assert instance.try_adopt_task_instances([ti]) == []
    assert ti.key not in instance._active
    fence_future, fenced_id = manager.fences[-1]
    assert fenced_id == request_id
    fence_future.set_result(None)
    instance.sync()
    assert instance.event_buffer[ti.key][0] == TaskInstanceState.FAILED


@pytest.mark.asyncio
@pytest.mark.parametrize("fence_fails", [False, True])
async def test_manager_conservatively_fences_failed_launch(fence_fails: bool) -> None:
    driver = AsyncMock(spec=SandboxDriver)
    driver.driver_id = "fake"
    driver.launch.side_effect = TimeoutError("launch response lost")
    if fence_fails:
        driver.fence.side_effect = OSError("fence failed")
    manager = _SandboxExecutorManager(
        lambda: driver,
        expected_driver_id="fake",
        launch_concurrency=1,
        status_concurrency=1,
    )
    manager._driver = driver
    manager._launch_semaphore = asyncio.Semaphore(1)
    request = SandboxLaunchRequest(
        request_id=str(uuid4()),
        command=("true",),
        env={},
        provider_config={},
        workdir=None,
        timeout_seconds=10,
        ttl_seconds=20,
    )

    expected_error = SandboxLaunchUnfencedError if fence_fails else TimeoutError
    with pytest.raises(expected_error):
        await manager._launch(request)

    driver.fence.assert_awaited_once_with(request.request_id)


@pytest.mark.asyncio
async def test_manager_fences_oversized_driver_handle() -> None:
    driver = AsyncMock(spec=SandboxDriver)
    driver.driver_id = "fake"
    driver.launch.return_value = SandboxHandle({"payload": "x" * 70_000})
    manager = _SandboxExecutorManager(
        lambda: driver,
        expected_driver_id="fake",
        launch_concurrency=1,
        status_concurrency=1,
    )
    manager._driver = driver
    manager._launch_semaphore = asyncio.Semaphore(1)
    request = SandboxLaunchRequest(
        request_id=str(uuid4()),
        command=("true",),
        env={},
        provider_config={},
        workdir=None,
        timeout_seconds=10,
        ttl_seconds=20,
    )

    with pytest.raises(SandboxConfigurationError, match="too large"):
        await manager._launch(request)

    driver.fence.assert_awaited_once_with(request.request_id)


def test_manager_driver_mismatch_preserves_startup_error() -> None:
    driver = AsyncMock(spec=SandboxDriver)
    driver.driver_id = "wrong"
    manager = _SandboxExecutorManager(
        lambda: driver,
        expected_driver_id="expected",
        launch_concurrency=1,
        status_concurrency=1,
    )

    with pytest.raises(RuntimeError, match="failed to start") as error:
        manager.start()
    assert isinstance(error.value.__cause__, SandboxConfigurationError)
    manager.close()
