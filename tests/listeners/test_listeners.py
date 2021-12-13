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

from airflow.executors.debug_executor import DebugExecutor
from airflow.listeners.listener import Listener, get_listener_manager
from airflow.utils.session import provide_session
from airflow.utils.state import State


hookimpl = pluggy.HookimplMarker("airflow")


class CollectingListener(Listener):
    def __init__(self):
        self.states = []


class PartialListener(CollectingListener):
    @hookimpl
    def on_task_instance_start(self, previous_state, state, task_instance, session):
        self.states.append(state)

    @hookimpl
    def on_task_instance_success(self, previous_state, state, task_instance, session):
        self.states.append(state)


class FullListener(PartialListener):
    @hookimpl
    def on_task_instance_failed(self, previous_state, state, task_instance, session):
        self.states.append(state)


class ThrowingListener(Listener):
    @hookimpl
    def on_task_instance_start(self, previous_state, state, task_instance, session):
        raise RuntimeError()


@pytest.fixture(autouse=True)
def clean_listener_manager():
    yield
    lm = get_listener_manager()
    lm.clear()


@provide_session
def test_listener_gets_calls(create_task_instance, session=None):
    lm = get_listener_manager()
    listener = FullListener()
    lm.add_listener(listener)

    ti = create_task_instance(session=session, state=State.RUNNING)

    ti.notify_listener(State.QUEUED, State.RUNNING, session)
    ti.notify_listener(State.RUNNING, State.SUCCESS, session)
    ti.notify_listener(State.SUCCESS, State.RUNNING, session)
    ti.notify_listener(State.RUNNING, State.FAILED, session)

    assert listener.states == [State.RUNNING, State.SUCCESS, State.RUNNING, State.FAILED]


@provide_session
def test_listener_gets_only_subscribed_calls(create_task_instance, session=None):
    lm = get_listener_manager()
    listener = PartialListener()
    lm.add_listener(listener)

    ti = create_task_instance(session=session, state=State.RUNNING)

    ti.notify_listener(State.QUEUED, State.RUNNING, session)
    ti.notify_listener(State.RUNNING, State.SUCCESS, session)
    ti.notify_listener(State.SUCCESS, State.RUNNING, session)
    ti.notify_listener(State.RUNNING, State.FAILED, session)

    assert listener.states == [State.RUNNING, State.SUCCESS, State.RUNNING]


@provide_session
def test_listener_throws_exceptions(create_task_instance, session=None):
    lm = get_listener_manager()
    listener = ThrowingListener()
    lm.add_listener(listener)

    ti = create_task_instance(session=session, state=State.RUNNING)

    with pytest.raises(RuntimeError):
        ti.notify_listener(State.QUEUED, State.RUNNING, session)


@provide_session
def test_executor_running_task_instance_notifies_listeners(create_task_instance, session=None):
    lm = get_listener_manager()
    listener = FullListener()
    lm.add_listener(listener)

    ti = create_task_instance(session=session, state=State.QUEUED)
    ti.run()

    assert listener.states == [State.RUNNING, State.SUCCESS]
