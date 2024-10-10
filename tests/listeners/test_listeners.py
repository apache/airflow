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

from airflow.exceptions import AirflowException
from airflow.jobs.job import Job, run_job
from airflow.listeners.listener import get_listener_manager
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, TaskInstanceState
from tests.listeners import (
    class_listener,
    full_listener,
    lifecycle_listener,
    partial_listener,
    throwing_listener,
)
from tests.utils.test_helpers import MockJobRunner

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
EXECUTION_DATE = timezone.utcnow()

TEST_DAG_FOLDER = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]


@pytest.fixture(autouse=True)
def clean_listener_manager():
    lm = get_listener_manager()
    lm.clear()
    yield
    lm = get_listener_manager()
    lm.clear()
    for listener in LISTENERS:
        listener.clear()


@pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
@provide_session
def test_listener_gets_calls(create_task_instance, session=None):
    lm = get_listener_manager()
    lm.add_listener(full_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    # Using ti.run() instead of ti._run_raw_task() to capture state change to RUNNING
    # that only happens on `check_and_change_state_before_execution()` that is called before
    # `run()` calls `_run_raw_task()`
    ti.run()

    assert len(full_listener.state) == 2
    assert full_listener.state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS]


@provide_session
def test_multiple_listeners(create_task_instance, session=None):
    lm = get_listener_manager()
    lm.add_listener(full_listener)
    lm.add_listener(lifecycle_listener)
    class_based_listener = class_listener.ClassBasedListener()
    lm.add_listener(class_based_listener)

    job = Job()
    job_runner = MockJobRunner(job=job)
    with contextlib.suppress(NotImplementedError):
        # suppress NotImplementedError: just for lifecycle
        run_job(job=job, execute_callable=job_runner._execute)

    assert full_listener.started_component is job
    assert lifecycle_listener.started_component is job
    assert full_listener.stopped_component is job
    assert lifecycle_listener.stopped_component is job
    assert class_based_listener.state == [DagRunState.RUNNING, DagRunState.SUCCESS]


@pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
@provide_session
def test_listener_gets_only_subscribed_calls(create_task_instance, session=None):
    lm = get_listener_manager()
    lm.add_listener(partial_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    # Using ti.run() instead of ti._run_raw_task() to capture state change to RUNNING
    # that only happens on `check_and_change_state_before_execution()` that is called before
    # `run()` calls `_run_raw_task()`
    ti.run()

    assert len(partial_listener.state) == 1
    assert partial_listener.state == [TaskInstanceState.RUNNING]


@pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
@provide_session
def test_listener_throws_exceptions(create_task_instance, session=None):
    lm = get_listener_manager()
    lm.add_listener(throwing_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    with pytest.raises(RuntimeError):
        ti._run_raw_task()


@pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
@provide_session
def test_listener_captures_failed_taskinstances(create_task_instance_of_operator, session=None):
    lm = get_listener_manager()
    lm.add_listener(full_listener)

    ti = create_task_instance_of_operator(
        BashOperator, dag_id=DAG_ID, execution_date=EXECUTION_DATE, task_id=TASK_ID, bash_command="exit 1"
    )
    with pytest.raises(AirflowException):
        ti._run_raw_task()

    assert full_listener.state == [TaskInstanceState.RUNNING, TaskInstanceState.FAILED]
    assert len(full_listener.state) == 2


@provide_session
def test_listener_captures_longrunning_taskinstances(create_task_instance_of_operator, session=None):
    lm = get_listener_manager()
    lm.add_listener(full_listener)

    ti = create_task_instance_of_operator(
        BashOperator, dag_id=DAG_ID, execution_date=EXECUTION_DATE, task_id=TASK_ID, bash_command="sleep 5"
    )
    ti._run_raw_task()

    assert full_listener.state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS]
    assert len(full_listener.state) == 2


@pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
@provide_session
def test_class_based_listener(create_task_instance, session=None):
    lm = get_listener_manager()
    listener = class_listener.ClassBasedListener()
    lm.add_listener(listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    # Using ti.run() instead of ti._run_raw_task() to capture state change to RUNNING
    # that only happens on `check_and_change_state_before_execution()` that is called before
    # `run()` calls `_run_raw_task()`
    ti.run()

    assert len(listener.state) == 2
    assert listener.state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS]


def test_listener_logs_call(caplog, create_task_instance, session):
    caplog.set_level(logging.DEBUG, logger="airflow.listeners.listener")
    lm = get_listener_manager()
    lm.add_listener(full_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    ti._run_raw_task()

    listener_logs = [r for r in caplog.record_tuples if r[0] == "airflow.listeners.listener"]
    assert len(listener_logs) == 6
    assert all(r[:-1] == ("airflow.listeners.listener", logging.DEBUG) for r in listener_logs)
    assert listener_logs[0][-1].startswith("Calling 'on_task_instance_running' with {'")
    assert listener_logs[1][-1].startswith("Hook impls: [<HookImpl plugin")
    assert listener_logs[2][-1] == "Result from 'on_task_instance_running': []"
    assert listener_logs[3][-1].startswith("Calling 'on_task_instance_success' with {'")
    assert listener_logs[4][-1].startswith("Hook impls: [<HookImpl plugin")
    assert listener_logs[5][-1] == "Result from 'on_task_instance_success': []"
