#
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

import pytest as pytest

from airflow import AirflowException
from airflow.listeners import events, hookimpl
from airflow.listeners.listener import get_listener_manager
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import State
from tests.listeners import full_listener, partial_listener, throwing_listener

DAG_ID = "test_listener_dag"
TASK_ID = "test_listener_task"
EXECUTION_DATE = timezone.utcnow()


@pytest.fixture(scope="module", autouse=True)
def register_events():
    events.register_task_instance_state_events()
    yield
    events.unregister_task_instance_state_events()


@pytest.fixture(autouse=True)
def clean_listener_manager():
    lm = get_listener_manager()
    lm.clear()
    yield
    lm = get_listener_manager()
    lm.clear()
    full_listener.state = []


@provide_session
def test_listener_gets_calls(create_task_instance, session=None):
    lm = get_listener_manager()
    lm.add_listener(full_listener)

    ti = create_task_instance(session=session, state=State.QUEUED)
    # Using ti.run() instead of ti._run_raw_task() to capture state change to RUNNING
    # that only happens on `check_and_change_state_before_execution()` that is called before
    # `run()` calls `_run_raw_task()`
    ti.run()

    assert len(full_listener.state) == 2
    assert full_listener.state == [State.RUNNING, State.SUCCESS]


@provide_session
def test_listener_gets_only_subscribed_calls(create_task_instance, session=None):
    lm = get_listener_manager()
    lm.add_listener(partial_listener)

    ti = create_task_instance(session=session, state=State.QUEUED)
    # Using ti.run() instead of ti._run_raw_task() to capture state change to RUNNING
    # that only happens on `check_and_change_state_before_execution()` that is called before
    # `run()` calls `_run_raw_task()`
    ti.run()

    assert len(partial_listener.state) == 1
    assert partial_listener.state == [State.RUNNING]


@provide_session
def test_listener_throws_exceptions(create_task_instance, session=None):
    lm = get_listener_manager()
    lm.add_listener(throwing_listener)

    ti = create_task_instance(session=session, state=State.QUEUED)
    with pytest.raises(RuntimeError):
        ti._run_raw_task()


@provide_session
def test_listener_captures_failed_taskinstances(create_task_instance_of_operator, session=None):
    lm = get_listener_manager()
    lm.add_listener(full_listener)

    ti = create_task_instance_of_operator(
        BashOperator, dag_id=DAG_ID, execution_date=EXECUTION_DATE, task_id=TASK_ID, bash_command="exit 1"
    )
    with pytest.raises(AirflowException):
        ti._run_raw_task()

    assert full_listener.state == [State.FAILED]
    assert len(full_listener.state) == 1


def test_non_module_listener_is_not_registered():
    class NotAListener:
        @hookimpl
        def on_task_instance_running(self, previous_state, task_instance, session):
            pass

    lm = get_listener_manager()
    with pytest.raises(TypeError):
        lm.add_listener(NotAListener())

    assert not lm.has_listeners
