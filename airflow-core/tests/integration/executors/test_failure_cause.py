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
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import create_session
from airflow.utils.state import State, TaskInstanceState

from tests_common.test_utils.config import conf_vars
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
@conf_vars({("core", "max_infra_retries"): "1"})
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
            on_retry_callback=lambda context: None,
            on_failure_callback=lambda context: None,
        )
    ti = dag_maker.create_dagrun(state=DagRunState.RUNNING).get_task_instance(
        task.task_id,
        session=session,
    )
    ti.state = State.RUNNING
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

    expected_failure_info = failure_info or (None, None)
    assert (ti.state, ti.max_tries) == (expected_state, expected_max_tries)
    assert received == [expected_failure_info]

    request = executor.callback_sink.send.call_args[0][0]
    assert isinstance(request, TaskCallbackRequest)
    assert request.task_callback_type == expected_state
    assert "failure_kind" not in type(request.context_from_server).model_fields
    assert "failure_reason" not in type(request.context_from_server).model_fields


@pytest.mark.backend("postgres")
@conf_vars({("core", "max_infra_retries"): "1"})
def test_event_batch_preserves_a_manual_failure_after_an_earlier_commit(dag_maker, session):
    with dag_maker(dag_id="failure_batch_manual_stop"):
        for task_id in ("first", "second"):
            EmptyOperator(
                task_id=task_id,
                retries=0,
                on_retry_callback=lambda context: None,
                on_failure_callback=lambda context: None,
            )
    dag_run = dag_maker.create_dagrun(state=DagRunState.RUNNING)
    task_instances = dag_run.get_task_instances(session=session)
    executor = MockExecutor(do_update=False)
    runner = SchedulerJobRunner(job=Job(), executors=[executor])
    for ti in task_instances:
        ti.state = TaskInstanceState.RUNNING
        ti.try_number = 1
        ti.queued_by_job_id = 1
        executor.fail(key=ti.key, failure_kind=TaskFailureKind.INFRA, reason="Evicted")
    session.commit()
    stopped_tasks: list[str] = []

    def stop_other_task(request: TaskCallbackRequest) -> None:
        if stopped_tasks:
            return
        other_task_id = next(ti.task_id for ti in task_instances if ti.task_id != request.ti.task_id)
        with create_session(scoped=False) as other_session:
            assert other_session is not session
            stopped_ti = TaskInstance.get_task_instance(
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
                task_id=other_task_id,
                map_index=-1,
                lock_for_update=True,
                session=other_session,
            )
            assert stopped_ti is not None
            stopped_ti.set_state(state=TaskInstanceState.FAILED, session=other_session)
        stopped_tasks.append(other_task_id)

    executor.callback_sink.send.side_effect = stop_other_task
    SchedulerJobRunner.process_executor_events(
        executor=executor,
        job_id=1,
        scheduler_dag_bag=runner.scheduler_dag_bag,
        session=session,
    )

    assert len(stopped_tasks) == 1
    stopped_ti = next(ti for ti in task_instances if ti.task_id == stopped_tasks[0])
    stopped_ti.refresh_from_db(session=session)
    assert (stopped_ti.state, stopped_ti.max_tries) == (TaskInstanceState.FAILED, 0)
    executor.callback_sink.send.assert_called_once()
