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

from datetime import timedelta
from unittest import mock

import pendulum
import pytest
import time_machine
from pytest import mark

from airflow.executors.base_executor import BaseExecutor, RunningRetryAttemptType
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils import timezone
from airflow.utils.state import State


def test_supports_sentry():
    assert not BaseExecutor.supports_sentry


def test_supports_pickling():
    assert BaseExecutor.supports_pickling


def test_is_local_default_value():
    assert not BaseExecutor.is_local


def test_serve_logs_default_value():
    assert not BaseExecutor.serve_logs


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


@mock.patch("airflow.executors.base_executor.BaseExecutor.sync")
@mock.patch("airflow.executors.base_executor.BaseExecutor.trigger_tasks")
@mock.patch("airflow.executors.base_executor.Stats.gauge")
def test_gauge_executor_metrics(mock_stats_gauge, mock_trigger_tasks, mock_sync):
    executor = BaseExecutor()
    executor.heartbeat()
    calls = [
        mock.call("executor.open_slots", mock.ANY),
        mock.call("executor.queued_tasks", mock.ANY),
        mock.call("executor.running_tasks", mock.ANY),
    ]
    mock_stats_gauge.assert_has_calls(calls)


def setup_dagrun(dag_maker):
    date = timezone.utcnow()
    start_date = date - timedelta(days=2)

    with dag_maker("test_try_adopt_task_instances"):
        BaseOperator(task_id="task_1", start_date=start_date)
        BaseOperator(task_id="task_2", start_date=start_date)
        BaseOperator(task_id="task_3", start_date=start_date)

    return dag_maker.create_dagrun(execution_date=date)


def test_try_adopt_task_instances(dag_maker):
    dagrun = setup_dagrun(dag_maker)
    tis = dagrun.task_instances
    assert {ti.task_id for ti in tis} == {"task_1", "task_2", "task_3"}
    assert BaseExecutor().try_adopt_task_instances(tis) == tis


def enqueue_tasks(executor, dagrun):
    for task_instance in dagrun.task_instances:
        executor.queue_command(task_instance, ["airflow"])


def setup_trigger_tasks(dag_maker):
    dagrun = setup_dagrun(dag_maker)
    executor = BaseExecutor()
    executor.execute_async = mock.Mock()
    enqueue_tasks(executor, dagrun)
    return executor, dagrun


@mark.parametrize("open_slots", [1, 2, 3])
def test_trigger_queued_tasks(dag_maker, open_slots):
    executor, _ = setup_trigger_tasks(dag_maker)
    executor.trigger_tasks(open_slots)
    assert executor.execute_async.call_count == open_slots


@pytest.mark.parametrize(
    "can_try_num, change_state_num, second_exec",
    [
        (2, 3, False),
        (3, 3, True),
        (4, 3, True),
    ],
)
@mock.patch("airflow.executors.base_executor.RunningRetryAttemptType.can_try_again")
def test_trigger_running_tasks(can_try_mock, dag_maker, can_try_num, change_state_num, second_exec):
    can_try_mock.side_effect = [True for _ in range(can_try_num)] + [False]
    executor, dagrun = setup_trigger_tasks(dag_maker)
    open_slots = 100
    executor.trigger_tasks(open_slots)
    expected_calls = len(dagrun.task_instances)  # initially `execute_async` called for each task
    assert executor.execute_async.call_count == expected_calls

    # All the tasks are now "running", so while we enqueue them again here,
    # they won't be executed again until the executor has been notified of a state change.
    ti = dagrun.task_instances[0]
    assert ti.key in executor.running
    assert ti.key not in executor.queued_tasks
    executor.queue_command(ti, ["airflow"])

    # this is the problem we're dealing with: ti.key both queued and running
    assert ti.key in executor.queued_tasks and ti.key in executor.running
    assert len(executor.attempts) == 0
    executor.trigger_tasks(open_slots)

    # first trigger call after queueing again creates an attempt object
    assert len(executor.attempts) == 1
    assert ti.key in executor.attempts

    for attempt in range(2, change_state_num + 2):
        executor.trigger_tasks(open_slots)
        if attempt <= min(can_try_num, change_state_num):
            assert ti.key in executor.queued_tasks and ti.key in executor.running
        # On the configured attempt, we notify the executor that the task has succeeded.
        if attempt == change_state_num:
            executor.change_state(ti.key, State.SUCCESS)
            assert ti.key not in executor.running
    # retry was ok when state changed, ti.key will be in running (for the second time)
    if can_try_num >= change_state_num:
        assert ti.key in executor.running
    else:  # otherwise, it won't be
        assert ti.key not in executor.running
    # either way, ti.key not in queued -- it was either removed because never left running
    # or it was moved out when run 2nd time
    assert ti.key not in executor.queued_tasks
    assert not executor.attempts

    # we expect one more "execute_async" if TI was marked successful
    # this would move it out of running set and free the queued TI to be executed again
    if second_exec is True:
        expected_calls += 1

    assert executor.execute_async.call_count == expected_calls


def test_validate_airflow_tasks_run_command(dag_maker):
    dagrun = setup_dagrun(dag_maker)
    tis = dagrun.task_instances
    dag_id, task_id = BaseExecutor.validate_airflow_tasks_run_command(tis[0].command_as_list())
    assert dag_id == dagrun.dag_id and task_id == tis[0].task_id


@pytest.mark.parametrize("loop_duration, total_tries", [(0.5, 12), (1.0, 7), (1.7, 4), (10, 2)])
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
