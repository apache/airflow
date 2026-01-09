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

import contextlib
import logging
import os

import pytest

from airflow._shared.timezones import timezone
from airflow.exceptions import AirflowException
from airflow.jobs.job import Job, run_job
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, TaskInstanceState

from unit.listeners import (
    class_listener,
    full_listener,
    lifecycle_listener,
    partial_listener,
    throwing_listener,
)
from unit.utils.test_helpers import MockJobRunner

pytestmark = pytest.mark.db_test


LISTENERS = [
    class_listener,
    full_listener,
    lifecycle_listener,
    partial_listener,
    throwing_listener,
]

DAG_ID = "test_listener_dag"
TASK_ID = "test_listener_task"
LOGICAL_DATE = timezone.utcnow()

TEST_DAG_FOLDER = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]


@pytest.fixture(autouse=True)
def clean_listener_state():
    """Clear listener state after each test."""
    yield
    for listener in LISTENERS:
        listener.clear()


@provide_session
def test_listener_gets_calls(create_task_instance, session, listener_manager):
    listener_manager(full_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    # Using ti.run() instead of ti._run_raw_task() to capture state change to RUNNING
    # that only happens on `check_and_change_state_before_execution()` that is called before
    # `run()` calls `_run_raw_task()`
    ti.run()

    assert len(full_listener.get_listener_state().state) == 2
    assert full_listener.get_listener_state().state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS]


@provide_session
def test_multiple_listeners(create_task_instance, session, listener_manager):
    listener_manager(full_listener)
    listener_manager(lifecycle_listener)
    class_based_listener = class_listener.ClassBasedListener()
    listener_manager(class_based_listener)

    job = Job()
    job_runner = MockJobRunner(job=job)
    with contextlib.suppress(NotImplementedError):
        # suppress NotImplementedError: just for lifecycle
        run_job(job=job, execute_callable=job_runner._execute)

    assert full_listener.get_listener_state().started_component is job
    assert lifecycle_listener.get_listener_state().started_component is job
    assert full_listener.get_listener_state().stopped_component is job
    assert lifecycle_listener.get_listener_state().stopped_component is job
    assert class_based_listener.state == [DagRunState.RUNNING, DagRunState.SUCCESS]


@provide_session
def test_listener_gets_only_subscribed_calls(create_task_instance, session, listener_manager):
    listener_manager(partial_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    # Using ti.run() instead of ti._run_raw_task() to capture state change to RUNNING
    # that only happens on `check_and_change_state_before_execution()` that is called before
    # `run()` calls `_run_raw_task()`
    ti.run()

    assert len(partial_listener.state) == 1
    assert partial_listener.state == [TaskInstanceState.RUNNING]


@provide_session
def test_listener_suppresses_exceptions(create_task_instance, session, cap_structlog, listener_manager):
    listener_manager(throwing_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    ti.run()
    assert "error calling listener" in cap_structlog


@provide_session
def test_listener_captures_failed_taskinstances(create_task_instance_of_operator, session, listener_manager):
    listener_manager(full_listener)

    ti = create_task_instance_of_operator(
        BashOperator, dag_id=DAG_ID, logical_date=LOGICAL_DATE, task_id=TASK_ID, bash_command="exit 1"
    )
    with pytest.raises(AirflowException):
        ti.run()

    assert full_listener.get_listener_state().state == [TaskInstanceState.RUNNING, TaskInstanceState.FAILED]
    assert len(full_listener.get_listener_state().state) == 2


@provide_session
def test_listener_captures_longrunning_taskinstances(
    create_task_instance_of_operator, session, listener_manager
):
    listener_manager(full_listener)

    ti = create_task_instance_of_operator(
        BashOperator, dag_id=DAG_ID, logical_date=LOGICAL_DATE, task_id=TASK_ID, bash_command="sleep 5"
    )
    ti.run()

    assert full_listener.get_listener_state().state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS]
    assert len(full_listener.get_listener_state().state) == 2


@provide_session
def test_class_based_listener(create_task_instance, session, listener_manager):
    listener = class_listener.ClassBasedListener()
    listener_manager(listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    ti.run()

    assert listener.state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS, DagRunState.SUCCESS]


def test_listener_logs_call(caplog, create_task_instance, session, listener_manager):
    caplog.set_level(logging.DEBUG, logger="airflow.sdk._shared.listeners.listener")
    listener_manager(full_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    ti.run()

    listener_logs = [r for r in caplog.record_tuples if r[0] == "airflow.sdk._shared.listeners.listener"]
    assert all(r[:-1] == ("airflow.sdk._shared.listeners.listener", logging.DEBUG) for r in listener_logs)
    assert listener_logs[0][-1].startswith("Calling 'on_task_instance_running' with {'")
    assert listener_logs[1][-1].startswith("Hook impls: [<HookImpl plugin")
    assert listener_logs[2][-1] == "Result from 'on_task_instance_running': []"
    assert listener_logs[3][-1].startswith("Calling 'on_task_instance_success' with {'")
    assert listener_logs[4][-1].startswith("Hook impls: [<HookImpl plugin")
    assert listener_logs[5][-1] == "Result from 'on_task_instance_success': []"
