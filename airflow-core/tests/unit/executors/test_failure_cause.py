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

from airflow._shared.state import TaskFailureKind
from airflow.callbacks.callback_requests import TaskCallbackRequest
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.listeners import hookimpl
from airflow.models.dagrun import DagRunState
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.mock_executor import MockExecutor

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize(
    ("name", "failure_info"),
    [
        ("infra", (TaskFailureKind.INFRA, "PreemptionByScheduler")),
        ("application", (TaskFailureKind.APPLICATION, None)),
        ("timeout", (TaskFailureKind.TIMEOUT, None)),
        ("manual", (TaskFailureKind.MANUAL, None)),
        ("reason_only", (None, "WorkerLost")),
        ("unclassified", None),
    ],
)
@pytest.mark.parametrize("retries", [0, 1])
def test_executor_cause_reaches_scheduler_and_consumers(
    name,
    failure_info,
    retries,
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
    ti.state = TaskInstanceState.RUNNING
    ti.try_number = 1
    ti.queued_by_job_id = 1
    session.flush()

    executor = MockExecutor(do_update=False)
    runner = SchedulerJobRunner(job=Job(), executors=[executor])
    failure_kind, reason = failure_info or (None, None)
    executor.fail(
        key=ti.key,
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

    expected_state = TaskInstanceState.UP_FOR_RETRY if retries else TaskInstanceState.FAILED
    assert (ti.state, ti.max_tries) == (expected_state, retries)
    assert received == [failure_info or (None, None)]
    assert executor.get_task_failure_info(ti.key) is None

    request = executor.callback_sink.send.call_args[0][0]
    assert isinstance(request, TaskCallbackRequest)
    assert request.task_callback_type == expected_state
    assert "failure_kind" not in type(request.context_from_server).model_fields
    assert "failure_reason" not in type(request.context_from_server).model_fields
