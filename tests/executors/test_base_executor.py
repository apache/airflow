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

from pytest import mark

from airflow.executors.base_executor import QUEUEING_ATTEMPTS, BaseExecutor
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils import timezone
from airflow.utils.state import State


def test_is_local_default_value():
    assert not BaseExecutor.is_local


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
    assert len(executor.execute_async.mock_calls) == open_slots


@mark.parametrize("change_state_attempt", range(QUEUEING_ATTEMPTS + 2))
def test_trigger_running_tasks(dag_maker, change_state_attempt):
    executor, dagrun = setup_trigger_tasks(dag_maker)
    open_slots = 100
    executor.trigger_tasks(open_slots)
    expected_calls = len(dagrun.task_instances)  # initially `execute_async` called for each task
    assert len(executor.execute_async.mock_calls) == expected_calls

    # All the tasks are now "running", so while we enqueue them again here,
    # they won't be executed again until the executor has been notified of a state change.
    enqueue_tasks(executor, dagrun)

    for attempt in range(QUEUEING_ATTEMPTS + 2):
        # On the configured attempt, we notify the executor that the task has succeeded.
        if attempt == change_state_attempt:
            executor.change_state(dagrun.task_instances[0].key, State.SUCCESS)
            # If we have not exceeded QUEUEING_ATTEMPTS, we should expect an additional "execute" call
            if attempt < QUEUEING_ATTEMPTS:
                expected_calls += 1
        executor.trigger_tasks(open_slots)
        assert len(executor.execute_async.mock_calls) == expected_calls
    if change_state_attempt < QUEUEING_ATTEMPTS:
        assert len(executor.execute_async.mock_calls) == len(dagrun.task_instances) + 1
    else:
        assert len(executor.execute_async.mock_calls) == len(dagrun.task_instances)


def test_validate_airflow_tasks_run_command(dag_maker):
    dagrun = setup_dagrun(dag_maker)
    tis = dagrun.task_instances
    dag_id, task_id = BaseExecutor.validate_airflow_tasks_run_command(tis[0].command_as_list())
    assert dag_id == dagrun.dag_id and task_id == tis[0].task_id
