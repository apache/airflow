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
from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow._shared.listeners.listener import ListenerManager
from airflow._shared.timezones import timezone
from airflow.exceptions import AirflowException
from airflow.jobs.job import Job, run_job
from airflow.listeners import hookimpl, listener as listener_module
from airflow.plugins_manager import AirflowPlugin, integrate_listener_plugins
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.mock_plugins import mock_plugin_manager
from unit.listeners import (
    class_listener,
    full_listener,
    lifecycle_listener,
    partial_listener,
    throwing_listener,
)
from unit.utils.test_helpers import MockJobRunner

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

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
def test_listener_gets_calls(create_task_instance, listener_manager, *, session: Session):
    listener_manager(full_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    # Using ti.run() instead of ti._run_raw_task() to capture state change to RUNNING
    # that only happens on `check_and_change_state_before_execution()` that is called before
    # `run()` calls `_run_raw_task()`
    ti.run()

    assert len(full_listener.get_listener_state().state) == 2
    assert full_listener.get_listener_state().state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS]


@provide_session
def test_multiple_listeners(create_task_instance, listener_manager, *, session: Session):
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
def test_listener_gets_only_subscribed_calls(create_task_instance, listener_manager, *, session: Session):
    listener_manager(partial_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    # Using ti.run() instead of ti._run_raw_task() to capture state change to RUNNING
    # that only happens on `check_and_change_state_before_execution()` that is called before
    # `run()` calls `_run_raw_task()`
    ti.run()

    assert len(partial_listener.state) == 1
    assert partial_listener.state == [TaskInstanceState.RUNNING]


@provide_session
def test_listener_suppresses_exceptions(
    create_task_instance, cap_structlog, listener_manager, *, session: Session
):
    listener_manager(throwing_listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    ti.run()
    assert "error calling listener" in cap_structlog


@provide_session
def test_listener_captures_failed_taskinstances(
    create_task_instance_of_operator, listener_manager, *, session: Session
):
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
    create_task_instance_of_operator, listener_manager, *, session: Session
):
    listener_manager(full_listener)

    ti = create_task_instance_of_operator(
        BashOperator, dag_id=DAG_ID, logical_date=LOGICAL_DATE, task_id=TASK_ID, bash_command="sleep 5"
    )
    ti.run()

    assert full_listener.get_listener_state().state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS]
    assert len(full_listener.get_listener_state().state) == 2


@provide_session
def test_class_based_listener(create_task_instance, listener_manager, *, session: Session):
    listener = class_listener.ClassBasedListener()
    listener_manager(listener)

    ti = create_task_instance(session=session, state=TaskInstanceState.QUEUED)
    ti.run()

    assert listener.state == [TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS, DagRunState.SUCCESS]


def test_listener_logs_call(caplog, create_task_instance, listener_manager, *, session: Session):
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


class _RecordingListener:
    """Minimal task-instance listener used to verify registration/isolation."""

    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance):
        pass


def _make_team_plugin(name: str, team_name: str | None, listener: object) -> AirflowPlugin:
    plugin = AirflowPlugin()
    plugin.name = name
    plugin.team_name = team_name
    plugin.listeners = [listener]
    return plugin


@pytest.fixture
def team_listener_plugins():
    global_listener = _RecordingListener()
    team_a_listener = _RecordingListener()
    team_b_listener = _RecordingListener()
    plugins = [
        _make_team_plugin("global_plugin", None, global_listener),
        _make_team_plugin("team_a_plugin", "team_a", team_a_listener),
        _make_team_plugin("team_b_plugin", "team_b", team_b_listener),
    ]
    return plugins, global_listener, team_a_listener, team_b_listener


class TestIntegrateListenerPluginsTeamFiltering:
    @conf_vars({("core", "multi_team"): "True"})
    def test_team_manager_gets_global_and_own_team_only(self, team_listener_plugins):
        plugins, global_listener, team_a_listener, team_b_listener = team_listener_plugins
        manager = ListenerManager()
        with mock_plugin_manager(plugins=plugins):
            integrate_listener_plugins(manager, team_name="team_a")

        registered = set(manager.pm.get_plugins())
        assert global_listener in registered
        assert team_a_listener in registered
        assert team_b_listener not in registered

    @conf_vars({("core", "multi_team"): "True"})
    def test_global_manager_gets_only_global(self, team_listener_plugins):
        plugins, global_listener, team_a_listener, team_b_listener = team_listener_plugins
        manager = ListenerManager()
        with mock_plugin_manager(plugins=plugins):
            integrate_listener_plugins(manager, team_name=None)

        registered = set(manager.pm.get_plugins())
        assert global_listener in registered
        assert team_a_listener not in registered
        assert team_b_listener not in registered

    @conf_vars({("core", "multi_team"): "False"})
    def test_multi_team_disabled_registers_all_listeners(self, team_listener_plugins):
        plugins, global_listener, team_a_listener, team_b_listener = team_listener_plugins
        manager = ListenerManager()
        with mock_plugin_manager(plugins=plugins):
            integrate_listener_plugins(manager, team_name="team_a")

        registered = set(manager.pm.get_plugins())
        assert {global_listener, team_a_listener, team_b_listener} <= registered


class TestGetListenerManagerTeamKeying:
    def test_global_calls_return_same_instance(self):
        listener_module._build_listener_manager.cache_clear()
        default_manager = listener_module.get_listener_manager()
        assert default_manager is listener_module.get_listener_manager(None)
        assert default_manager is listener_module.get_listener_manager(team_name=None)

    @conf_vars({("core", "multi_team"): "True"})
    def test_distinct_team_returns_distinct_instance(self):
        listener_module._build_listener_manager.cache_clear()
        with mock_plugin_manager(plugins=[]):
            global_manager = listener_module.get_listener_manager()
            team_manager = listener_module.get_listener_manager("team_a")
        assert global_manager is not team_manager


class TestGetListenerManagerForDag:
    @conf_vars({("core", "multi_team"): "False"})
    def test_returns_global_manager_when_multi_team_disabled(self):
        with mock.patch("airflow.models.dag.DagModel.get_team_name") as mock_get_team_name:
            resolved = listener_module.get_listener_manager_for_dag("some_dag")
        assert resolved is listener_module.get_listener_manager()
        mock_get_team_name.assert_not_called()

    @conf_vars({("core", "multi_team"): "True"})
    def test_resolves_team_and_returns_team_manager(self):
        listener_module._build_listener_manager.cache_clear()
        with mock_plugin_manager(plugins=[]):
            with mock.patch(
                "airflow.models.dag.DagModel.get_team_name", return_value="team_a"
            ) as mock_get_team_name:
                resolved = listener_module.get_listener_manager_for_dag("some_dag")
            assert resolved is listener_module.get_listener_manager("team_a")
        mock_get_team_name.assert_called_once()
