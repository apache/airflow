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

import pytest

from airflow._shared.state import INFRA_RETRIES_USED_STATE_KEY, TaskFailureKind, TaskScope
from airflow.callbacks.callback_requests import TaskCallbackRequest
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.listeners import hookimpl
from airflow.models.dagrun import DagRunState
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.state.metastore import _get_db_backend
from airflow.utils.state import State, TaskInstanceState

from tests_common.test_utils.mock_executor import MockExecutor

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize(
    ("name", "failure_info", "expected_state", "expected_max_tries"),
    [
        (
            "infra",
            (TaskFailureKind.INFRA, "Evicted"),
            TaskInstanceState.UP_FOR_RETRY,
            1,
        ),
        (
            "application",
            (TaskFailureKind.APPLICATION, "OOMKilled"),
            TaskInstanceState.FAILED,
            0,
        ),
        ("reason_only", (None, "WorkerLost"), TaskInstanceState.FAILED, 0),
        ("unclassified", None, TaskInstanceState.FAILED, 0),
    ],
)
def test_executor_cause_reaches_scheduler_and_consumers(
    name,
    failure_info,
    expected_state,
    expected_max_tries,
    dag_maker,
    listener_manager,
    session,
):
    received: list[tuple[TaskFailureKind | None, str | None]] = []

    class FailureListener:
        @hookimpl
        def on_task_instance_failed(
            self,
            previous_state,
            task_instance,
            error,
            failure_kind,
            reason,
        ):
            received.append((failure_kind, reason))

    listener_manager(FailureListener())
    with dag_maker(dag_id=f"failure_cause_{name}", fileloc=f"/{name}/"):
        task = EmptyOperator(
            task_id="task",
            retries=0,
            infra_retries=1,
            on_retry_callback=lambda context: None,
            on_failure_callback=lambda context: None,
        )
    ti = dag_maker.create_dagrun(state=DagRunState.RUNNING).get_task_instance(
        task.task_id,
        session=session,
    )
    ti.state = State.QUEUED
    ti.queued_by_job_id = 1
    session.flush()

    executor = MockExecutor(do_update=False)
    runner = SchedulerJobRunner(job=Job(), executors=[executor])
    executor.event_buffer[ti.key] = State.FAILED, None
    if failure_info is not None:
        executor.task_failure_info[ti.key] = failure_info

    SchedulerJobRunner.process_executor_events(
        executor=executor,
        job_id=1,
        scheduler_dag_bag=runner.scheduler_dag_bag,
        session=session,
    )
    ti.refresh_from_db(session=session)

    expected_failure_info = failure_info or (None, None)
    assert (ti.state, ti.max_tries) == (expected_state, expected_max_tries)
    assert received == [expected_failure_info]

    request = executor.callback_sink.send.call_args[0][0]
    assert isinstance(request, TaskCallbackRequest)
    assert request.task_callback_type == expected_state
    assert request.context_from_server.failure_kind == (
        expected_failure_info[0].value if expected_failure_info[0] is not None else None
    )
    assert request.context_from_server.failure_reason == expected_failure_info[1]

    scope = TaskScope(
        dag_id=ti.dag_id,
        run_id=ti.run_id,
        task_id=ti.task_id,
        map_index=ti.map_index,
    )
    expected_count = "1" if expected_failure_info[0] == TaskFailureKind.INFRA else None
    assert _get_db_backend().get(scope, INFRA_RETRIES_USED_STATE_KEY, session=session) == expected_count
