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
import pluggy
import pytest as pytest

from airflow import AirflowException
from airflow.listeners.events import register_task_instance_state_events
from airflow.listeners.listener import Listener, get_listener_manager
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import State

DAG_ID = "test_listener_dag"
TASK_ID = "test_listener_task"
EXECUTION_DATE = timezone.utcnow()


hookimpl = pluggy.HookimplMarker("airflow")


class CollectingListener(Listener):
    def __init__(self):
        self.states = []


class PartialListener(CollectingListener):
    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance, session):
        self.states.append(State.RUNNING)


class FullListener(PartialListener):
    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance, session):
        self.states.append(State.SUCCESS)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, session):
        self.states.append(State.FAILED)


class ThrowingListener(Listener):
    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance, session):
        raise RuntimeError()


@pytest.fixture(scope="module", autouse=True)
def register_events():
    register_task_instance_state_events()


@pytest.fixture(autouse=True)
def clean_listener_manager():
    lm = get_listener_manager()
    lm.clear()
    yield
    lm = get_listener_manager()
    lm.clear()


@provide_session
def test_listener_gets_calls(create_task_instance, session=None):
    lm = get_listener_manager()
    listener = FullListener()
    lm.add_listener(listener)

    ti = create_task_instance(session=session, state=State.QUEUED)
    ti.run()

    assert len(listener.states) == 2
    assert listener.states == [State.RUNNING, State.SUCCESS]


@provide_session
def test_listener_gets_only_subscribed_calls(create_task_instance, session=None):
    lm = get_listener_manager()
    listener = PartialListener()
    lm.add_listener(listener)

    ti = create_task_instance(session=session, state=State.QUEUED)
    ti.run()

    assert len(listener.states) == 1
    assert listener.states == [State.RUNNING]


@provide_session
def test_listener_throws_exceptions(create_task_instance, session=None):
    lm = get_listener_manager()
    listener = ThrowingListener()
    lm.add_listener(listener)

    with pytest.raises(RuntimeError):
        ti = create_task_instance(session=session, state=State.QUEUED)
        ti.run()


def test_listener_needs_to_subclass_listener():
    lm = get_listener_manager()

    class Dummy:
        @hookimpl
        def on_task_instance_running(self, previous_state, task_instance, session):
            pass

    lm.add_listener(Dummy())
    assert not lm.has_listeners()


@provide_session
def test_listener_captures_failed_taskinstances(create_task_instance_of_operator, session=None):
    lm = get_listener_manager()
    listener = FullListener()
    lm.add_listener(listener)

    with pytest.raises(AirflowException):
        ti = create_task_instance_of_operator(
            BashOperator, dag_id=DAG_ID, execution_date=EXECUTION_DATE, task_id=TASK_ID, bash_command="exit 1"
        )
        ti.run()

    assert len(listener.states) == 2
    assert listener.states == [State.RUNNING, State.FAILED]
