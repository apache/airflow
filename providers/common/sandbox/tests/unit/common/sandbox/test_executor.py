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

from concurrent.futures import Future
from pathlib import PurePosixPath
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from airflow.executors.workloads import BundleInfo, ExecuteTask, TaskInstanceDTO
from airflow.providers.common.sandbox.exceptions import (
    SandboxConfigurationError,
    SandboxInvalidHandleError,
    SandboxLaunchUnfencedError,
    SandboxProtocolError,
)
from airflow.providers.common.sandbox.executor import BaseSandboxExecutor
from airflow.providers.common.sandbox.executor_runner import (
    SandboxRunnerOperation,
    SandboxRunnerResult,
    _SandboxExecutorRunner,
)
from airflow.providers.common.sandbox.models import (
    SandboxExecutionRef,
    SandboxHandle,
    SandboxLaunchConfig,
    SandboxLaunchOutcome,
    SandboxLaunchRequest,
    SandboxResult,
    SandboxState,
)
from airflow.utils.state import TaskInstanceState


class FakeRunner:
    def __init__(self) -> None:
        self.launches: list[tuple[Any, SandboxLaunchRequest]] = []
        self.statuses: list[tuple[Any, SandboxExecutionRef]] = []
        self.terminations: list[tuple[SandboxExecutionRef, Any | None, bool]] = []
        self.fences: list[tuple[Any, str]] = []
        self.recovery_results: dict[str, object | BaseException | None] = {}
        self.validation_error: BaseException | None = None
        self.accept_best_effort_cleanup = True
        self.close_calls: list[float] = []

    def submit_launch(self, key, request: SandboxLaunchRequest) -> None:
        self.launches.append((key, request))

    def submit_status(self, key, ref: SandboxExecutionRef) -> None:
        self.statuses.append((key, ref))

    def submit_terminate(
        self,
        ref: SandboxExecutionRef,
        *,
        key=None,
        required: bool,
    ) -> bool:
        if not required and not self.accept_best_effort_cleanup:
            return False
        self.terminations.append((ref, key, required))
        return True

    def submit_fence(self, key, request_id: str) -> None:
        self.fences.append((key, request_id))

    def submit_validate(self, ref: SandboxExecutionRef) -> Future[None]:
        future: Future[None] = Future()
        if self.validation_error is None:
            future.set_result(None)
        else:
            future.set_exception(self.validation_error)
        return future

    def submit_recover(self, request_id: str) -> Future[SandboxLaunchOutcome | None]:
        future: Future[SandboxLaunchOutcome | None] = Future()
        result = self.recovery_results.get(request_id)
        if isinstance(result, BaseException):
            future.set_exception(result)
        else:
            future.set_result(cast("SandboxLaunchOutcome | None", result))
        return future

    def close(self, timeout: float) -> bool:
        self.close_calls.append(timeout)
        return True


class _FakeSandboxExecutor(BaseSandboxExecutor):
    driver_id = "fake"

    def get_driver_factory(self):
        raise AssertionError("tests inject a runner")

    def build_launch_config(self, workload: ExecuteTask) -> SandboxLaunchConfig:
        del workload
        return SandboxLaunchConfig(provider_config={"runtime": "fake"})


def make_workload(
    *,
    task_id: str = "task",
    request_id: str | None = None,
    ti_id: UUID | None = None,
) -> ExecuteTask:
    return ExecuteTask(
        ti=TaskInstanceDTO(
            id=ti_id or uuid4(),
            dag_version_id=uuid4(),
            task_id=task_id,
            dag_id="dag",
            run_id="run",
            try_number=1,
            map_index=-1,
            pool_slots=1,
            queue="default",
            priority_weight=1,
            external_executor_id=request_id or str(uuid4()),
            executor_config=None,
        ),
        dag_rel_path=PurePosixPath("dag.py"),
        token="jwt",
        bundle_info=BundleInfo(name="dags-folder", version=None),
        log_path="dag/task.log",
    )


def make_ref(request_id: str, *, driver: str = "fake", keep: bool = False) -> SandboxExecutionRef:
    return SandboxExecutionRef(
        driver=driver,
        request_id=request_id,
        handle=SandboxHandle({"job_id": request_id}, display_name=f"{driver}-job"),
        keep=keep,
    )


def make_ti(*, task_id: str, external_executor_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        key=make_workload(task_id=task_id).ti.key,
        external_executor_id=external_executor_id,
    )


def send_result(
    executor: _FakeSandboxExecutor,
    operation: SandboxRunnerOperation,
    *,
    key,
    request_id: str,
    value: SandboxLaunchOutcome | SandboxResult | None = None,
    error: BaseException | None = None,
) -> None:
    executor._result_queue.put(
        SandboxRunnerResult(
            operation=operation,
            key=key,
            request_id=request_id,
            value=value,
            error=error,
        )
    )
    executor.sync()


def submit_workload(
    executor: _FakeSandboxExecutor,
    workload: ExecuteTask,
) -> None:
    executor.queue_workload(workload, session=cast("Any", None))
    executor._process_workloads([workload])


def complete_launch(
    executor: _FakeSandboxExecutor,
    workload: ExecuteTask,
    *,
    keep: bool = False,
) -> SandboxExecutionRef:
    request_id = cast("str", workload.ti.external_executor_id)
    ref = make_ref(request_id, keep=keep)
    send_result(
        executor,
        SandboxRunnerOperation.LAUNCH,
        key=workload.ti.key,
        request_id=request_id,
        value=SandboxLaunchOutcome(ref),
    )
    return ref


@pytest.fixture
def executor(monkeypatch) -> tuple[_FakeSandboxExecutor, FakeRunner]:
    monkeypatch.setenv("AIRFLOW__LOGGING__REMOTE_LOGGING", "True")
    monkeypatch.setenv(
        "AIRFLOW__LOGGING__REMOTE_TASK_HANDLER_KWARGS",
        '{"secret_option": "must-not-cross-the-boundary"}',
    )
    runner = FakeRunner()
    instance = _FakeSandboxExecutor(parallelism=4)
    instance._runner = cast("_SandboxExecutorRunner", runner)
    return instance, runner


def test_workload_lifecycle_uses_task_sdk_and_reports_terminal_state(executor) -> None:
    instance, runner = executor
    workload = make_workload()

    submit_workload(instance, workload)

    assert workload.ti.key in instance.running
    key, request = runner.launches[0]
    assert key == workload.ti.key
    assert request.request_id == workload.ti.external_executor_id
    assert request.command[:4] == (
        "python",
        "-m",
        "airflow.sdk.execution_time.execute_workload",
        "--json-string",
    )
    assert request.env["AIRFLOW_IS_EXECUTOR_CONTAINER"] == "true"
    assert request.env["AIRFLOW_SANDBOX_DRIVER"] == "fake"
    assert "AIRFLOW__LOGGING__REMOTE_TASK_HANDLER_KWARGS" not in request.env

    ref = complete_launch(instance, workload)
    assert instance.event_buffer[workload.ti.key] == (TaskInstanceState.RUNNING, ref.encode())

    send_result(
        instance,
        SandboxRunnerOperation.STATUS,
        key=workload.ti.key,
        request_id=ref.request_id,
        value=SandboxResult(SandboxState.SUCCEEDED, 0),
    )

    assert instance.event_buffer[workload.ti.key] == (TaskInstanceState.SUCCESS, None)
    assert workload.ti.key not in instance.running
    assert runner.terminations[-1] == (ref, None, False)


def test_pending_launches_are_bounded_by_launch_concurrency(executor) -> None:
    instance, runner = executor
    instance._launch_concurrency = 1
    first = make_workload(task_id="first")
    second = make_workload(task_id="second")
    instance.queue_workload(first, session=cast("Any", None))
    instance.queue_workload(second, session=cast("Any", None))

    instance._process_workloads([first, second])

    assert [request.request_id for _, request in runner.launches] == [first.ti.external_executor_id]
    assert second.ti.key in instance.queued_tasks


def test_same_key_successor_waits_for_predecessor_and_stale_results_are_ignored(executor) -> None:
    instance, runner = executor
    ti_id = uuid4()
    predecessor = make_workload(request_id=str(uuid4()), ti_id=ti_id)
    successor = make_workload(request_id=str(uuid4()), ti_id=ti_id)
    key = predecessor.ti.key

    submit_workload(instance, predecessor)
    predecessor_ref = complete_launch(instance, predecessor)
    instance.get_event_buffer()

    submit_workload(instance, successor)

    assert len(runner.launches) == 1
    assert instance.queued_tasks[key] is successor

    send_result(
        instance,
        SandboxRunnerOperation.STATUS,
        key=key,
        request_id=predecessor_ref.request_id,
        value=SandboxResult(SandboxState.SUCCEEDED, 0),
    )
    assert instance.event_buffer[key] == (TaskInstanceState.SUCCESS, None)
    instance.get_event_buffer()

    instance._process_workloads([successor])
    assert len(runner.launches) == 2
    assert runner.launches[-1][1].request_id == successor.ti.external_executor_id

    send_result(
        instance,
        SandboxRunnerOperation.STATUS,
        key=key,
        request_id=predecessor_ref.request_id,
        value=SandboxResult(SandboxState.FAILED, 7),
    )
    assert key not in instance.event_buffer
    assert instance._generations[key].request_id == successor.ti.external_executor_id

    send_result(
        instance,
        SandboxRunnerOperation.LAUNCH,
        key=key,
        request_id=predecessor_ref.request_id,
        value=SandboxLaunchOutcome(predecessor_ref),
    )
    assert runner.terminations[-1] == (predecessor_ref, None, False)
    assert instance._generations[key].request_id == successor.ti.external_executor_id


def test_same_key_successor_waits_until_predecessor_is_fenced(executor) -> None:
    instance, runner = executor
    predecessor = make_workload(request_id=str(uuid4()))
    successor = make_workload(request_id=str(uuid4()), ti_id=predecessor.ti.id)
    key = predecessor.ti.key
    submit_workload(instance, predecessor)
    predecessor_ref = complete_launch(instance, predecessor)
    instance.get_event_buffer()
    submit_workload(instance, successor)

    send_result(
        instance,
        SandboxRunnerOperation.STATUS,
        key=key,
        request_id=predecessor_ref.request_id,
        error=SandboxProtocolError("invalid provider status"),
    )

    assert len(runner.launches) == 1
    assert runner.fences == [(key, predecessor_ref.request_id)]
    assert instance.queued_tasks[key] is successor

    send_result(
        instance,
        SandboxRunnerOperation.FENCE,
        key=key,
        request_id=predecessor_ref.request_id,
    )
    instance.get_event_buffer()
    instance._process_workloads([successor])

    assert len(runner.launches) == 2
    assert runner.launches[-1][1].request_id == successor.ti.external_executor_id


def test_ambiguous_launch_is_fenced_before_task_failure(executor) -> None:
    instance, runner = executor
    workload = make_workload()
    key = workload.ti.key
    request_id = cast("str", workload.ti.external_executor_id)
    submit_workload(instance, workload)

    launch_error = TimeoutError("launch response lost")
    send_result(
        instance,
        SandboxRunnerOperation.LAUNCH,
        key=key,
        request_id=request_id,
        error=SandboxLaunchUnfencedError(request_id, launch_error, OSError("fence failed")),
    )

    assert runner.fences == [(key, request_id)]
    assert key in instance.running
    assert key not in instance.event_buffer

    send_result(
        instance,
        SandboxRunnerOperation.FENCE,
        key=key,
        request_id=request_id,
    )
    assert instance.event_buffer[key] == (TaskInstanceState.FAILED, str(launch_error))
    assert key not in instance.running


def test_revoke_during_launch_waits_for_handle_then_terminates_without_state_event(executor) -> None:
    instance, runner = executor
    workload = make_workload()
    key = workload.ti.key
    submit_workload(instance, workload)

    revoked = instance.revoke_task(
        ti=SimpleNamespace(key=key, external_executor_id=workload.ti.external_executor_id)
    )
    assert not revoked
    assert runner.terminations == []
    assert runner.fences == []

    ref = complete_launch(instance, workload)
    assert runner.terminations == [(ref, key, True)]
    assert key not in instance.event_buffer

    send_result(
        instance,
        SandboxRunnerOperation.TERMINATE,
        key=key,
        request_id=ref.request_id,
    )
    assert key not in instance.running
    assert key not in instance.event_buffer
    assert instance._generations[key].phase.value == "revoked"
    assert instance.revoke_task(
        ti=SimpleNamespace(key=key, external_executor_id=workload.ti.external_executor_id)
    )
    assert key not in instance._generations


def test_revoke_waits_for_required_termination(executor) -> None:
    instance, runner = executor
    request_id = str(uuid4())
    ref = make_ref(request_id)
    ti = make_ti(task_id="terminate", external_executor_id=ref.encode())

    def submit_terminate(ref, *, key=None, required: bool) -> bool:
        runner.terminations.append((ref, key, required))
        instance._result_queue.put(
            SandboxRunnerResult(
                operation=SandboxRunnerOperation.TERMINATE,
                key=key,
                request_id=ref.request_id,
            )
        )
        return True

    runner.submit_terminate = submit_terminate

    assert not instance.revoke_task(ti=ti)
    assert runner.terminations == [(ref, ti.key, True)]
    assert instance.revoke_task(ti=ti)
    assert ti.key not in instance._generations
    assert ti.key not in instance.running


def test_revoke_fences_bare_request_before_reporting_safe(executor) -> None:
    instance, runner = executor
    request_id = str(uuid4())
    ti = make_ti(task_id="fence", external_executor_id=request_id)

    def submit_fence(key, request_id: str) -> None:
        runner.fences.append((key, request_id))
        instance._result_queue.put(
            SandboxRunnerResult(
                operation=SandboxRunnerOperation.FENCE,
                key=key,
                request_id=request_id,
            )
        )

    runner.submit_fence = submit_fence

    assert not instance.revoke_task(ti=ti)
    assert runner.fences == [(ti.key, request_id)]
    assert instance.revoke_task(ti=ti)
    assert ti.key not in instance._generations
    assert ti.key not in instance.running


@pytest.mark.parametrize(
    ("external_executor_id", "reason"),
    [
        pytest.param("sandbox:v1:not-valid-base64", "malformed", id="malformed"),
        pytest.param("corrupted-id", "unknown format", id="unknown-format"),
        pytest.param(
            make_ref(str(uuid4()), driver="other").encode(),
            "belongs to driver 'other'",
            id="foreign-driver",
        ),
    ],
)
def test_revoke_quarantines_unowned_references(executor, external_executor_id: str, reason: str) -> None:
    instance, runner = executor
    ti = make_ti(task_id="quarantined-revoke", external_executor_id=external_executor_id)

    assert not instance.revoke_task(ti=ti)
    assert ti.key in instance.running
    assert instance._generations[ti.key].phase.value == "quarantined"
    assert reason in instance._generations[ti.key].failure_info
    assert runner.terminations == []
    assert runner.fences == []


def test_revoke_returns_without_waiting_for_provider_cleanup(executor) -> None:
    instance, runner = executor
    ref = make_ref(str(uuid4()))
    ti = make_ti(task_id="timeout", external_executor_id=ref.encode())

    assert not instance.revoke_task(ti=ti)
    assert runner.terminations == [(ref, ti.key, True)]
    assert ti.key in instance.running
    assert instance._generations[ti.key].phase.value == "terminating"


def test_status_protocol_error_is_fenced_before_failure(executor) -> None:
    instance, runner = executor
    workload = make_workload()
    submit_workload(instance, workload)
    ref = complete_launch(instance, workload)
    instance.get_event_buffer()

    send_result(
        instance,
        SandboxRunnerOperation.STATUS,
        key=workload.ti.key,
        request_id=ref.request_id,
        error=SandboxProtocolError("unknown provider state"),
    )

    assert runner.fences == [(workload.ti.key, ref.request_id)]
    assert workload.ti.key not in instance.event_buffer

    send_result(
        instance,
        SandboxRunnerOperation.FENCE,
        key=workload.ti.key,
        request_id=ref.request_id,
    )
    state, info = instance.event_buffer[workload.ti.key]
    assert state == TaskInstanceState.FAILED
    assert "unknown provider state" in info


def test_repeated_status_errors_are_fenced_at_the_configured_limit(executor) -> None:
    instance, runner = executor
    instance._max_status_errors = 2
    workload = make_workload()
    submit_workload(instance, workload)
    ref = complete_launch(instance, workload)
    instance.get_event_buffer()

    send_result(
        instance,
        SandboxRunnerOperation.STATUS,
        key=workload.ti.key,
        request_id=ref.request_id,
        error=TimeoutError("first timeout"),
    )
    assert runner.fences == []

    send_result(
        instance,
        SandboxRunnerOperation.STATUS,
        key=workload.ti.key,
        request_id=ref.request_id,
        error=TimeoutError("second timeout"),
    )
    assert runner.fences == [(workload.ti.key, ref.request_id)]


def test_terminal_state_is_not_blocked_when_best_effort_cleanup_is_full(executor) -> None:
    instance, runner = executor
    runner.accept_best_effort_cleanup = False
    workload = make_workload()
    submit_workload(instance, workload)
    ref = complete_launch(instance, workload)
    instance.get_event_buffer()

    send_result(
        instance,
        SandboxRunnerOperation.STATUS,
        key=workload.ti.key,
        request_id=ref.request_id,
        value=SandboxResult(SandboxState.SUCCEEDED, 0),
    )

    assert instance.event_buffer[workload.ti.key] == (TaskInstanceState.SUCCESS, None)
    assert runner.terminations == []


@pytest.mark.parametrize(
    ("launch_config", "message"),
    [
        (
            SandboxLaunchConfig(provider_config={}, timeout_seconds=60, ttl_seconds=121),
            "cannot exceed the deployment maximum",
        ),
        (
            SandboxLaunchConfig(provider_config={}, timeout_seconds=60, ttl_seconds=120, keep=True),
            "allow_keep=True",
        ),
    ],
)
def test_deployment_policy_bounds_task_retention(executor, launch_config, message: str) -> None:
    instance, _ = executor
    instance._max_ttl_seconds = 120
    instance._allow_keep = False

    with pytest.raises(SandboxConfigurationError, match=message):
        instance._validate_launch_policy(launch_config)


def test_required_terminal_cleanup_fences_before_reporting_success(executor) -> None:
    instance, runner = executor
    instance.requires_terminal_cleanup = True
    workload = make_workload()
    submit_workload(instance, workload)
    ref = complete_launch(instance, workload)
    instance.get_event_buffer()

    send_result(
        instance,
        SandboxRunnerOperation.STATUS,
        key=workload.ti.key,
        request_id=ref.request_id,
        value=SandboxResult(SandboxState.SUCCEEDED, 0),
    )

    assert workload.ti.key not in instance.event_buffer
    assert runner.terminations == [(ref, workload.ti.key, True)]

    send_result(
        instance,
        SandboxRunnerOperation.TERMINATE,
        key=workload.ti.key,
        request_id=ref.request_id,
        error=OSError("cleanup failed"),
    )

    assert workload.ti.key not in instance.event_buffer
    assert runner.fences == [(workload.ti.key, ref.request_id)]

    send_result(
        instance,
        SandboxRunnerOperation.FENCE,
        key=workload.ti.key,
        request_id=ref.request_id,
    )

    assert instance.event_buffer[workload.ti.key] == (TaskInstanceState.SUCCESS, None)


def test_adoption_accepts_a_valid_persisted_reference(executor) -> None:
    instance, _ = executor
    ref = make_ref(str(uuid4()), keep=True)
    ti = make_ti(task_id="valid", external_executor_id=ref.encode())

    assert instance.try_adopt_task_instances([ti]) == []
    assert instance._generations[ti.key].ref == ref
    assert ti.key in instance.running


def test_adoption_recovers_and_persists_a_bare_request(executor) -> None:
    instance, runner = executor
    request_id = str(uuid4())
    ref = make_ref(request_id, keep=True)
    runner.recovery_results[request_id] = SandboxLaunchOutcome(ref)
    ti = make_ti(task_id="recovered", external_executor_id=request_id)

    assert instance.try_adopt_task_instances([ti]) == []
    assert instance._generations[ti.key].ref == ref
    assert instance.event_buffer[ti.key] == (TaskInstanceState.RUNNING, ref.encode())


def test_adoption_resets_a_bare_request_only_after_recovery_confirms_absence(executor) -> None:
    instance, runner = executor
    request_id = str(uuid4())
    runner.recovery_results[request_id] = None
    ti = make_ti(task_id="absent", external_executor_id=request_id)

    assert instance.try_adopt_task_instances([ti]) == [ti]
    assert ti.key not in instance.running
    assert ti.key not in instance._generations


@pytest.mark.parametrize(
    "external_executor_id",
    [
        pytest.param("sandbox:v1:not-valid-base64", id="malformed"),
        pytest.param("corrupted-id", id="unknown-format"),
        pytest.param(make_ref(str(uuid4()), driver="other").encode(), id="foreign-driver"),
    ],
)
def test_adoption_quarantines_unowned_persisted_references(executor, external_executor_id: str) -> None:
    instance, runner = executor
    ti = make_ti(task_id="quarantined", external_executor_id=external_executor_id)

    assert instance.try_adopt_task_instances([ti]) == []
    assert ti.key in instance.running
    assert instance._generations[ti.key].request_id is None
    assert runner.fences == []


def test_adoption_fences_a_recovery_error_before_failure(executor) -> None:
    instance, runner = executor
    request_id = str(uuid4())
    runner.recovery_results[request_id] = SandboxProtocolError("malformed recovery response")
    ti = make_ti(task_id="recovery-error", external_executor_id=request_id)

    assert instance.try_adopt_task_instances([ti]) == []
    assert runner.fences == [(ti.key, request_id)]
    assert ti.key not in instance.event_buffer

    send_result(
        instance,
        SandboxRunnerOperation.FENCE,
        key=ti.key,
        request_id=request_id,
    )
    assert instance.event_buffer[ti.key][0] == TaskInstanceState.FAILED


def test_adoption_fences_an_invalid_provider_handle(executor) -> None:
    instance, runner = executor
    ref = make_ref(str(uuid4()))
    runner.validation_error = SandboxInvalidHandleError("bad vendor schema")
    ti = make_ti(task_id="invalid-handle", external_executor_id=ref.encode())

    assert instance.try_adopt_task_instances([ti]) == []
    assert runner.fences == [(ti.key, ref.request_id)]


def test_end_leaves_running_sandbox_for_scheduler_adoption(executor) -> None:
    instance, runner = executor
    workload = make_workload()
    submit_workload(instance, workload)
    complete_launch(instance, workload)

    instance.end()

    assert runner.fences == []
    assert runner.terminations == []
    assert workload.ti.key in instance.running
    assert len(runner.close_calls) == 1
    assert instance._runner is None
