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

import logging
from datetime import timedelta
from unittest import mock
from uuid import UUID

import pendulum
import pytest
import time_machine

from airflow._shared.timezones import timezone
from airflow.callbacks.callback_requests import CallbackRequest
from airflow.cli.cli_config import DefaultHelpParser, GroupCommand
from airflow.cli.cli_parser import AirflowHelpFormatter
from airflow.executors import workloads
from airflow.executors.base_executor import BaseExecutor, RunningRetryAttemptType
from airflow.executors.local_executor import LocalExecutor
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.sdk import BaseOperator
from airflow.utils.state import State, TaskInstanceState

from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker


def test_sentry_integration():
    assert not BaseExecutor.sentry_integration


def test_is_local_default_value():
    assert not BaseExecutor.is_local


def test_is_production_default_value():
    assert BaseExecutor.is_production


def test_invalid_slotspool():
    with pytest.raises(ValueError, match="parallelism is set to 0 or lower"):
        BaseExecutor(0)


def test_get_task_log():
    executor = BaseExecutor()
    ti = TaskInstance(task=BaseOperator(task_id="dummy"), dag_version_id=mock.MagicMock(spec=UUID))
    assert executor.get_task_log(ti=ti, try_number=1) == ([], [])


def test_serve_logs_default_value():
    assert not BaseExecutor.serve_logs


def test_no_cli_commands_vended():
    assert not BaseExecutor.get_cli_commands()


def test_get_event_buffer():
    executor = BaseExecutor()

    date = timezone.utcnow()
    try_number = 1
    key1 = TaskInstanceKey("my_dag1", "my_task1", date, try_number)
    key2 = TaskInstanceKey("my_dag2", "my_task1", date, try_number)
    key3 = TaskInstanceKey("my_dag2", "my_task2", date, try_number)
    state = State.SUCCESS
    executor.event_buffer[key1] = state, None
    executor.event_buffer[key2] = state, None
    executor.event_buffer[key3] = state, None

    assert len(executor.get_event_buffer(("my_dag1",))) == 1
    assert len(executor.get_event_buffer()) == 2
    assert len(executor.event_buffer) == 0


def test_fail_and_success():
    executor = BaseExecutor()

    date = timezone.utcnow()
    try_number = 1
    success_state = State.SUCCESS
    fail_state = State.FAILED
    key1 = TaskInstanceKey("my_dag1", "my_task1", date, try_number)
    key2 = TaskInstanceKey("my_dag2", "my_task1", date, try_number)
    key3 = TaskInstanceKey("my_dag2", "my_task2", date, try_number)
    executor.fail(key1, fail_state)
    executor.fail(key2, fail_state)
    executor.success(key3, success_state)

    assert len(executor.running) == 0
    assert executor.slots_occupied == 0
    assert len(executor.get_event_buffer()) == 3


@mock.patch("airflow.executors.base_executor.BaseExecutor.sync")
@mock.patch("airflow.executors.base_executor.BaseExecutor.trigger_tasks")
@mock.patch("airflow.executors.base_executor.Stats.gauge")
def test_gauge_executor_metrics_single_executor(mock_stats_gauge, mock_trigger_tasks, mock_sync):
    executor = BaseExecutor()
    executor.heartbeat()
    calls = [
        mock.call("executor.open_slots", value=mock.ANY, tags={"status": "open", "name": "BaseExecutor"}),
        mock.call("executor.queued_tasks", value=mock.ANY, tags={"status": "queued", "name": "BaseExecutor"}),
        mock.call(
            "executor.running_tasks", value=mock.ANY, tags={"status": "running", "name": "BaseExecutor"}
        ),
    ]
    mock_stats_gauge.assert_has_calls(calls)


@pytest.mark.parametrize(
    ("executor_class", "executor_name"),
    [(LocalExecutor, "LocalExecutor")],
)
@mock.patch("airflow.executors.local_executor.LocalExecutor.sync")
@mock.patch("airflow.executors.base_executor.BaseExecutor.trigger_tasks")
@mock.patch("airflow.executors.base_executor.Stats.gauge")
@mock.patch("airflow.executors.base_executor.ExecutorLoader.get_executor_names")
def test_gauge_executor_metrics_with_multiple_executors(
    mock_get_executor_names,
    mock_stats_gauge,
    mock_trigger_tasks,
    mock_local_sync,
    executor_class,
    executor_name,
):
    # The names of the executors aren't relevant for this test, so long as a list of length > 1
    # is returned. This forces the executor to use the multiple executors gauge logic.
    mock_get_executor_names.return_value = ["Exec1", "Exec2"]
    executor = executor_class()
    executor.heartbeat()

    calls = [
        mock.call(
            f"executor.open_slots.{executor_name}",
            value=mock.ANY,
            tags={"status": "open", "name": executor_name},
        ),
        mock.call(
            f"executor.queued_tasks.{executor_name}",
            value=mock.ANY,
            tags={"status": "queued", "name": executor_name},
        ),
        mock.call(
            f"executor.running_tasks.{executor_name}",
            value=mock.ANY,
            tags={"status": "running", "name": executor_name},
        ),
    ]
    mock_stats_gauge.assert_has_calls(calls)


def setup_dagrun(dag_maker):
    date = timezone.utcnow()
    start_date = date - timedelta(days=2)

    with dag_maker("test_try_adopt_task_instances"):
        BaseOperator(task_id="task_1", start_date=start_date)
        BaseOperator(task_id="task_2", start_date=start_date)
        BaseOperator(task_id="task_3", start_date=start_date)

    return dag_maker.create_dagrun(logical_date=date)


@pytest.mark.db_test
def test_try_adopt_task_instances(dag_maker):
    dagrun = setup_dagrun(dag_maker)
    tis = dagrun.task_instances
    assert {ti.task_id for ti in tis} == {"task_1", "task_2", "task_3"}
    assert BaseExecutor().try_adopt_task_instances(tis) == tis


def setup_trigger_tasks(dag_maker, parallelism=None):
    dagrun = setup_dagrun(dag_maker)
    if parallelism:
        executor = BaseExecutor(parallelism=parallelism)
    else:
        executor = BaseExecutor()

    executor._process_workloads = mock.Mock(spec=lambda workloads: None)

    for task_instance in dagrun.task_instances:
        workload = workloads.ExecuteTask.make(task_instance)
        executor.queued_tasks[task_instance.key] = workload

    return executor, dagrun


@pytest.mark.db_test
def test_trigger_queued_tasks(dag_maker):
    """Test that trigger_tasks() calls _process_workloads() when there are queued workloads."""
    executor, dagrun = setup_trigger_tasks(dag_maker)

    # Verify tasks are queued
    assert len(executor.queued_tasks) == 3

    # Call trigger_tasks with enough slots
    executor.trigger_tasks(open_slots=10)

    executor._process_workloads.assert_called_once()

    # Verify it was called with the expected workloads
    call_args = executor._process_workloads.call_args[0][0]
    assert len(call_args) == 3


@pytest.mark.db_test
def test_trigger_running_tasks(dag_maker):
    """Test that trigger_tasks() works when tasks are re-queued."""
    executor, dagrun = setup_trigger_tasks(dag_maker)

    executor.trigger_tasks(open_slots=10)
    executor._process_workloads.assert_called_once()

    # Reset mock for second call
    executor._process_workloads.reset_mock()

    # Re-queue one task (simulates retry scenario)
    ti = dagrun.task_instances[0]

    workload = workloads.ExecuteTask.make(ti)
    executor.queued_tasks[ti.key] = workload

    executor.trigger_tasks(open_slots=10)

    # Verify _process_workloads was called again
    executor._process_workloads.assert_called_once()


def test_debug_dump(caplog):
    executor = BaseExecutor()
    with caplog.at_level(logging.INFO):
        executor.debug_dump()
    assert "executor.queued" in caplog.text
    assert "executor.running" in caplog.text
    assert "executor.event_buffer" in caplog.text


def test_base_executor_cannot_send_callback():
    executor = BaseExecutor()
    with pytest.raises(ValueError, match="Callback sink is not ready"):
        executor.send_callback(mock.Mock(spec=CallbackRequest))


@skip_if_force_lowest_dependencies_marker
def test_parser_and_formatter_class():
    executor = BaseExecutor(42)
    parser = executor._get_parser()
    assert isinstance(parser, DefaultHelpParser)
    assert parser.formatter_class is AirflowHelpFormatter


@mock.patch("airflow.cli.cli_parser._add_command")
@mock.patch(
    "airflow.executors.base_executor.BaseExecutor.get_cli_commands",
    return_value=[
        GroupCommand(
            name="some_name",
            help="some_help",
            subcommands=["A", "B", "C"],
            description="some_description",
            epilog="some_epilog",
        )
    ],
)
def test_parser_add_command(mock_add_command, mock_get_cli_command):
    executor = BaseExecutor()
    executor._get_parser()
    mock_add_command.assert_called_once()


@pytest.mark.parametrize(("loop_duration", "total_tries"), [(0.5, 12), (1.0, 7), (1.7, 4), (10, 2)])
def test_running_retry_attempt_type(loop_duration, total_tries):
    """
    Verify can_try_again returns True until at least 5 seconds have passed.

    For faster loops, we total tries will be higher.  If loops take longer than 5 seconds, still should
    end up trying 2 times.
    """
    min_seconds_for_test = 5

    with time_machine.travel(pendulum.now("UTC"), tick=False) as t:
        # set MIN_SECONDS so tests don't break if the value is changed
        RunningRetryAttemptType.MIN_SECONDS = min_seconds_for_test
        a = RunningRetryAttemptType()
        while True:
            if not a.can_try_again():
                break
            t.shift(loop_duration)
        assert a.elapsed > min_seconds_for_test
    assert a.total_tries == total_tries
    assert a.tries_after_min == 1


def test_state_fail():
    executor = BaseExecutor()
    key = TaskInstanceKey("my_dag1", "my_task1", timezone.utcnow(), 1)
    executor.running.add(key)
    info = "info"
    executor.fail(key, info=info)
    assert not executor.running
    assert executor.event_buffer[key] == (TaskInstanceState.FAILED, info)


def test_state_success():
    executor = BaseExecutor()
    key = TaskInstanceKey("my_dag1", "my_task1", timezone.utcnow(), 1)
    executor.running.add(key)
    info = "info"
    executor.success(key, info=info)
    assert not executor.running
    assert executor.event_buffer[key] == (TaskInstanceState.SUCCESS, info)


def test_state_queued():
    executor = BaseExecutor()
    key = TaskInstanceKey("my_dag1", "my_task1", timezone.utcnow(), 1)
    executor.running.add(key)
    info = "info"
    executor.queued(key, info=info)
    assert not executor.running
    assert executor.event_buffer[key] == (TaskInstanceState.QUEUED, info)


def test_state_generic():
    executor = BaseExecutor()
    key = TaskInstanceKey("my_dag1", "my_task1", timezone.utcnow(), 1)
    executor.running.add(key)
    info = "info"
    executor.queued(key, info=info)
    assert not executor.running
    assert executor.event_buffer[key] == (TaskInstanceState.QUEUED, info)


def test_state_running():
    executor = BaseExecutor()
    key = TaskInstanceKey("my_dag1", "my_task1", timezone.utcnow(), 1)
    executor.running.add(key)
    info = "info"
    executor.running_state(key, info=info)
    # Running state should not remove a command as running
    assert executor.running
    assert executor.event_buffer[key] == (TaskInstanceState.RUNNING, info)
