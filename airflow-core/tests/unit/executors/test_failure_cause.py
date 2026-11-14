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

from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from airflow._shared.state import TaskFailureKind
from airflow.callbacks.callback_requests import TaskCallbackRequest
from airflow.executors.workloads.types import TaskInstanceUuid
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.listeners import hookimpl
from airflow.models.dagrun import DagRunState
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.mock_executor import MockExecutor

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance

    from tests_common.pytest_plugin import DagMaker

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize(
    ("name", "failure_info"),
    [
        ("infra", (TaskFailureKind.INFRA, "PreemptionByScheduler")),
        ("application", (TaskFailureKind.APPLICATION, None)),
        ("timeout", (TaskFailureKind.TIMEOUT, None)),
        ("manual", (TaskFailureKind.MANUAL, None)),
        *[
            (f"reason_{reason}", (None, reason))
            for reason in ("SIGTERM", "SIGKILL", "OOMKilled", "WorkerLostError", "Evicted")
        ],
        ("unclassified", None),
    ],
)
@pytest.mark.parametrize("retries", [0, 1])
@pytest.mark.parametrize("uuid_key", [False, True], ids=["legacy", "uuid"])
def test_executor_cause_reaches_scheduler_and_consumers(
    name: str,
    failure_info: tuple[TaskFailureKind | None, str | None] | None,
    retries: int,
    uuid_key: bool,
    dag_maker: DagMaker,
    listener_manager: Callable[[object], None],
    session: Session,
) -> None:
    received: list[tuple[TaskFailureKind | None, str | None, str, int]] = []
    legacy_received: list[tuple[str, int]] = []

    class FailureListener:
        @hookimpl
        def on_task_instance_failed(
            self,
            previous_state: TaskInstanceState | None,
            task_instance: TaskInstance,
            error: str | BaseException | None,
            failure_kind: TaskFailureKind | None,
            reason: str | None,
        ) -> None:
            received.append((failure_kind, reason, str(task_instance.id), task_instance.try_number))

    class LegacyListener:
        @hookimpl
        def on_task_instance_failed(
            self,
            previous_state: TaskInstanceState | None,
            task_instance: TaskInstance,
            error: str | BaseException | None,
        ) -> None:
            legacy_received.append((str(task_instance.id), task_instance.try_number))

    listener_manager(FailureListener())
    listener_manager(LegacyListener())
    with dag_maker(dag_id=f"failure_cause_{name}_{retries}", fileloc=f"/{name}/"):
        task = EmptyOperator(
            task_id="task",
            retries=retries,
            on_retry_callback=lambda context: None,
            on_failure_callback=lambda context: None,
        )
    ti = dag_maker.create_dagrun(state=DagRunState.RUNNING).get_task_instance(
        task.task_id,
        session=session,
    )
    assert ti is not None
    ti.state = TaskInstanceState.RUNNING
    ti.try_number = 1
    ti.queued_by_job_id = 1
    session.flush()
    failed_attempt_id = str(ti.id)
    key = TaskInstanceUuid(ti.id) if uuid_key else ti.key

    executor = MockExecutor(do_update=False)
    runner = SchedulerJobRunner(job=Job(), executors=[executor])
    failure_kind, reason = failure_info or (None, None)
    executor._register_task(ti=ti)
    executor.fail(
        key=key,
        failure_kind=failure_kind,
        reason=reason,
    )

    SchedulerJobRunner.process_executor_events(
        executor=executor,
        job_id=1,
        scheduler_dag_bag=runner.scheduler_dag_bag,
        session=session,
    )
    ti.refresh_from_db(session=session)
    if retries:
        ti = ti.dag_run.get_task_instance(task.task_id, session=session)
    assert ti is not None

    expected_state = TaskInstanceState.UP_FOR_RETRY if retries else TaskInstanceState.FAILED
    assert (ti.state, ti.max_tries) == (expected_state, retries)
    assert received == [(*(failure_info or (None, None)), failed_attempt_id, 1)]
    assert legacy_received == [(failed_attempt_id, 1)]
    assert executor.get_task_failure_info(key) is None
    assert executor.event_buffer == {}

    assert executor.callback_sink is not None
    send_callback = executor.callback_sink.send
    assert isinstance(send_callback, Mock)
    assert send_callback.call_args is not None
    request = send_callback.call_args.args[0]
    assert isinstance(request, TaskCallbackRequest)
    assert request.task_callback_type == expected_state
    assert request.context_from_server is not None
    assert "failure_kind" not in type(request.context_from_server).model_fields
    assert "failure_reason" not in type(request.context_from_server).model_fields
