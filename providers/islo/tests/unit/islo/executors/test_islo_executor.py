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
from airflow.providers.islo.exceptions import IsloUnfencedLaunchError
from airflow.providers.islo.executors.islo_executor import IsloExecutor, IsloExecutorManager
from airflow.providers.islo.hooks.islo import AsyncIsloClient, IsloClientConfig
from airflow.providers.islo.models import (
    IsloExecutionRef,
    IsloExecutionResult,
    IsloExecutionState,
    IsloSandboxSpec,
    RunningIsloSandbox,
)
from airflow.utils.state import TaskInstanceState


class FakeManager:
    def __init__(self) -> None:
        self.launches: list[tuple[Future, object, list[str], dict[str, str]]] = []
        self.statuses: list[tuple[Future, IsloExecutionRef]] = []
        self.deletes: list[tuple[Future, str]] = []
        self.started = False
        self.closed = False

    def start(self) -> None:
        self.started = True

    def submit_health_check(self) -> Future:
        future: Future[None] = Future()
        future.set_result(None)
        return future

    def submit_launch(self, spec, command, env) -> Future:
        future: Future[RunningIsloSandbox] = Future()
        self.launches.append((future, spec, command, env))
        return future

    def submit_status(self, ref: IsloExecutionRef) -> Future:
        future: Future[IsloExecutionResult] = Future()
        self.statuses.append((future, ref))
        return future

    def submit_delete(self, sandbox_name: str) -> Future:
        future: Future[None] = Future()
        self.deletes.append((future, sandbox_name))
        return future

    def close(self) -> None:
        self.closed = True


def make_workload(*, task_id: str = "task", executor_config: dict | None = None) -> ExecuteTask:
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
            executor_config=executor_config,
        ),
        dag_rel_path=PurePosixPath("dag.py"),
        token="jwt",
        bundle_info=BundleInfo(name="dags-folder", version=None),
        log_path="dag/task.log",
    )


@pytest.fixture
def executor(monkeypatch) -> tuple[IsloExecutor, FakeManager]:
    monkeypatch.setenv("AIRFLOW__LOGGING__REMOTE_LOGGING", "True")
    monkeypatch.setenv("AIRFLOW__ISLO__DEFAULT_SNAPSHOT_NAME", "airflow-runtime")
    monkeypatch.setenv("AIRFLOW__ISLO__CHECK_HEALTH_ON_STARTUP", "False")
    manager = FakeManager()
    return IsloExecutor(parallelism=4, manager=cast("IsloExecutorManager", manager)), manager


def test_capabilities() -> None:
    assert IsloExecutor.pre_assigns_external_executor_id is True
    assert IsloExecutor.supports_ad_hoc_ti_run is False
    assert IsloExecutor.is_local is False


def test_workload_lifecycle_is_queued_once_and_reports_terminal_state(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    key = workload.ti.key
    instance.queued_tasks[key] = workload

    instance._process_workloads([workload])

    assert key not in instance.queued_tasks
    assert key in instance.running
    assert len(manager.launches) == 1
    launch_future, spec, command, env = manager.launches[0]
    assert spec.name == f"airflow-{workload.ti.external_executor_id}"
    assert command[:4] == ["python", "-m", "airflow.sdk.execution_time.execute_workload", "--json-string"]
    assert env["AIRFLOW_IS_EXECUTOR_CONTAINER"] == "true"

    ref = IsloExecutionRef(str(workload.ti.external_executor_id), spec.name, "sandbox-id", "exec-id")
    launch_future.set_result(RunningIsloSandbox(ref))
    instance.sync()

    assert instance.event_buffer[key] == (TaskInstanceState.RUNNING, ref.encode())
    assert len(manager.statuses) == 1
    manager.statuses[0][0].set_result(IsloExecutionResult(IsloExecutionState.SUCCEEDED, 0))
    instance.sync()

    assert instance.event_buffer[key] == (TaskInstanceState.SUCCESS, None)
    assert key not in instance.running
    assert manager.deletes[-1][1] == spec.name


def test_transient_status_errors_never_become_task_failure(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    key = workload.ti.key
    ref = IsloExecutionRef(str(workload.ti.external_executor_id), "sandbox", "sandbox-id", "exec-id")
    instance.running.add(key)
    instance._active[key] = RunningIsloSandbox(ref)
    instance._next_poll[key] = 0

    for _ in range(5):
        instance._next_poll[key] = 0
        instance._schedule_statuses()
        manager.statuses[-1][0].set_result(IsloExecutionResult(IsloExecutionState.UNKNOWN))
        instance._drain_statuses()

    assert key in instance.running
    assert key not in instance.event_buffer


def test_ambiguous_launch_is_fenced_before_failure(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    key = workload.ti.key
    instance.queued_tasks[key] = workload
    instance._process_workloads([workload])
    launch_future, spec, _, _ = manager.launches[0]
    launch_future.set_exception(
        IsloUnfencedLaunchError(spec.name, TimeoutError("exec response lost"), OSError("delete failed"))
    )

    instance.sync()

    assert key in instance.running
    assert key not in instance.event_buffer
    delete_future, deleted_name = manager.deletes[-1]
    assert deleted_name == spec.name
    delete_future.set_result(None)
    instance.sync()
    assert instance.event_buffer[key][0] == TaskInstanceState.FAILED


def test_adoption_restores_full_ref_and_fences_bare_dispatch_id(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    ref = IsloExecutionRef(str(workload.ti.external_executor_id), "sandbox", "sandbox-id", "exec-id")
    adopted_ti = SimpleNamespace(key=workload.ti.key, external_executor_id=ref.encode())
    bare_id = str(uuid4())
    bare_ti = SimpleNamespace(key=make_workload(task_id="bare").ti.key, external_executor_id=bare_id)

    # Complete the concurrent launch-phase fence while adoption waits.
    original_submit = manager.submit_delete

    def submit_completed_delete(name: str) -> Future:
        future = original_submit(name)
        future.set_result(None)
        return future

    manager.submit_delete = submit_completed_delete

    not_adopted = instance.try_adopt_task_instances([adopted_ti, bare_ti])

    assert adopted_ti.key in instance.running
    assert instance._active[adopted_ti.key].ref == ref
    assert not_adopted == [bare_ti]
    assert manager.deletes[-1][1] == f"airflow-{bare_id}"


def test_adoption_preserves_keep_flag(executor) -> None:
    instance, _ = executor
    workload = make_workload()
    ref = IsloExecutionRef(
        str(workload.ti.external_executor_id), "sandbox", "sandbox-id", "exec-id", keep=True
    )
    ti = SimpleNamespace(key=workload.ti.key, external_executor_id=ref.encode())

    assert instance.try_adopt_task_instances([ti]) == []
    assert instance._active[ti.key].keep is True


def test_revoke_deletes_active_sandbox_without_state_event(executor) -> None:
    instance, manager = executor
    workload = make_workload()
    key = workload.ti.key
    ref = IsloExecutionRef(str(workload.ti.external_executor_id), "sandbox", "sandbox-id", "exec-id")
    instance.running.add(key)
    instance._active[key] = RunningIsloSandbox(ref)
    ti = SimpleNamespace(key=key, external_executor_id=ref.encode())

    instance.revoke_task(ti=ti)

    assert key not in instance.running
    assert key not in instance.event_buffer
    assert all(name == "sandbox" for _, name in manager.deletes)

    first_delete = manager.deletes[0][0]
    first_delete.set_exception(TimeoutError("delete timed out"))
    instance.sync()
    instance._fences[key].next_attempt = 0
    instance.sync()
    retry_delete = manager.deletes[-1][0]
    retry_delete.set_result(None)
    instance.sync()

    assert key not in instance._fences
    assert key not in instance.event_buffer


@pytest.mark.asyncio
@pytest.mark.parametrize("delete_fails", [False, True])
async def test_manager_fences_sandbox_after_ambiguous_exec_submission(delete_fails: bool) -> None:
    client = AsyncMock(spec=AsyncIsloClient)
    client.create_sandbox.return_value = ("sandbox", "sandbox-id")
    client.execute.side_effect = TimeoutError("exec response lost")
    if delete_fails:
        client.delete_sandbox.side_effect = OSError("delete failed")

    manager = IsloExecutorManager(
        IsloClientConfig(access_key="ak_test"),
        launch_concurrency=1,
        status_concurrency=1,
    )
    manager._client = client
    manager._launch_semaphore = asyncio.Semaphore(1)
    spec = IsloSandboxSpec(
        name="sandbox",
        request_id=str(uuid4()),
        snapshot_name="airflow-runtime",
    )

    expected_error = IsloUnfencedLaunchError if delete_fails else TimeoutError
    with pytest.raises(expected_error):
        await manager._launch(spec, ["true"], {})

    client.delete_sandbox.assert_awaited_once_with("sandbox")
