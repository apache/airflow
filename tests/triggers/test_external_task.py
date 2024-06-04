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

import asyncio
import datetime
import time
from unittest import mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from airflow.exceptions import RemovedInAirflow3Warning
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.triggers.base import TriggerEvent
from airflow.triggers.external_task import DagStateTrigger, TaskStateTrigger, WorkflowTrigger
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.timezone import utcnow


class TestWorkflowTrigger:
    DAG_ID = "external_task"
    TASK_ID = "external_task_op"
    RUN_ID = "external_task_run_id"
    STATES = ["success", "fail"]

    @mock.patch("airflow.triggers.external_task._get_count")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_success(self, mock_get_count):
        """check the db count get called correctly."""
        mock_get_count.side_effect = mocked_get_count
        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            execution_dates=[timezone.datetime(2022, 1, 1)],
            external_task_ids=[self.TASK_ID],
            allowed_states=self.STATES,
            poke_interval=0.2,
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        fake_task = asyncio.create_task(fake_async_fun())
        await trigger_task
        assert fake_task.done()  # confirm that get_count is done in an async fashion
        assert trigger_task.done()
        result = trigger_task.result()
        assert result.payload == {"status": "success"}
        mock_get_count.assert_called_once_with(
            dttm_filter=[timezone.datetime(2022, 1, 1)],
            external_task_ids=["external_task_op"],
            external_task_group_id=None,
            external_dag_id="external_task",
            states=["success", "fail"],
        )
        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @mock.patch("airflow.triggers.external_task._get_count")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_failed(self, mock_get_count):
        mock_get_count.side_effect = mocked_get_count
        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            execution_dates=[timezone.datetime(2022, 1, 1)],
            external_task_ids=[self.TASK_ID],
            failed_states=self.STATES,
            poke_interval=0.2,
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        fake_task = asyncio.create_task(fake_async_fun())
        await trigger_task
        assert fake_task.done()  # confirm that get_count is done in an async fashion
        assert trigger_task.done()
        result = trigger_task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "failed"}
        mock_get_count.assert_called_once_with(
            dttm_filter=[timezone.datetime(2022, 1, 1)],
            external_task_ids=["external_task_op"],
            external_task_group_id=None,
            external_dag_id="external_task",
            states=["success", "fail"],
        )
        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @mock.patch("airflow.triggers.external_task._get_count")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_fail_count_eq_0(self, mock_get_count):
        mock_get_count.return_value = 0
        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            execution_dates=[timezone.datetime(2022, 1, 1)],
            external_task_ids=[self.TASK_ID],
            failed_states=self.STATES,
            poke_interval=0.2,
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        await trigger_task
        assert trigger_task.done()
        result = trigger_task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "success"}
        mock_get_count.assert_called_once_with(
            dttm_filter=[timezone.datetime(2022, 1, 1)],
            external_task_ids=["external_task_op"],
            external_task_group_id=None,
            external_dag_id="external_task",
            states=["success", "fail"],
        )
        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @mock.patch("airflow.triggers.external_task._get_count")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_skipped(self, mock_get_count):
        mock_get_count.side_effect = mocked_get_count
        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            execution_dates=[timezone.datetime(2022, 1, 1)],
            external_task_ids=[self.TASK_ID],
            skipped_states=self.STATES,
            poke_interval=0.2,
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        fake_task = asyncio.create_task(fake_async_fun())
        await trigger_task
        assert fake_task.done()  # confirm that get_count is done in an async fashion
        assert trigger_task.done()
        result = trigger_task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "skipped"}
        mock_get_count.assert_called_once_with(
            dttm_filter=[timezone.datetime(2022, 1, 1)],
            external_task_ids=["external_task_op"],
            external_task_group_id=None,
            external_dag_id="external_task",
            states=["success", "fail"],
        )

    @mock.patch("airflow.triggers.external_task._get_count")
    @mock.patch("asyncio.sleep")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_sleep_success(self, mock_sleep, mock_get_count):
        mock_get_count.side_effect = [0, 1]
        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            execution_dates=[timezone.datetime(2022, 1, 1)],
            external_task_ids=[self.TASK_ID],
            poke_interval=0.2,
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        await trigger_task
        assert trigger_task.done()
        result = trigger_task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "success"}
        mock_get_count.assert_called()
        assert mock_get_count.call_count == 2

        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

        mock_sleep.assert_awaited()
        assert mock_sleep.await_count == 1

    def test_serialization(self):
        """
        Asserts that the WorkflowTrigger correctly serializes its arguments and classpath.
        """
        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            execution_dates=[timezone.datetime(2022, 1, 1)],
            external_task_ids=[self.TASK_ID],
            allowed_states=self.STATES,
            poke_interval=5,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.triggers.external_task.WorkflowTrigger"
        assert kwargs == {
            "external_dag_id": self.DAG_ID,
            "execution_dates": [timezone.datetime(2022, 1, 1)],
            "external_task_ids": [self.TASK_ID],
            "external_task_group_id": None,
            "failed_states": None,
            "skipped_states": None,
            "allowed_states": self.STATES,
            "poke_interval": 5,
            "soft_fail": False,
        }


class TestTaskStateTrigger:
    DAG_ID = "external_task"
    TASK_ID = "external_task_op"
    RUN_ID = "external_task_run_id"
    STATES = ["success", "fail"]

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_task_state_trigger_success(self, session):
        """
        Asserts that the TaskStateTrigger only goes off on or after a TaskInstance
        reaches an allowed state (i.e. SUCCESS).
        """
        trigger_start_time = utcnow()
        dag = DAG(self.DAG_ID, start_date=timezone.datetime(2022, 1, 1))
        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_type="manual",
            execution_date=timezone.datetime(2022, 1, 1),
            run_id=self.RUN_ID,
        )
        session.add(dag_run)
        session.commit()

        external_task = EmptyOperator(task_id=self.TASK_ID, dag=dag)
        instance = TaskInstance(external_task, run_id=self.RUN_ID)
        session.add(instance)
        session.commit()

        with pytest.warns(RemovedInAirflow3Warning, match="TaskStateTrigger has been deprecated"):
            trigger = TaskStateTrigger(
                dag_id=dag.dag_id,
                task_id=instance.task_id,
                states=self.STATES,
                execution_dates=[timezone.datetime(2022, 1, 1)],
                poll_interval=0.2,
                trigger_start_time=trigger_start_time,
            )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # It should not have produced a result
        assert task.done() is False

        # Progress the task to a "success" state so that run() yields a TriggerEvent
        instance.state = TaskInstanceState.SUCCESS
        session.commit()
        await asyncio.sleep(0.5)
        assert task.done() is True

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @mock.patch("airflow.triggers.external_task.utcnow")
    @pytest.mark.asyncio
    async def test_task_state_trigger_timeout(self, mock_utcnow):
        trigger_start_time = utcnow()
        mock_utcnow.return_value = trigger_start_time + datetime.timedelta(seconds=61)

        with pytest.warns(RemovedInAirflow3Warning, match="TaskStateTrigger has been deprecated"):
            trigger = TaskStateTrigger(
                dag_id="dag1",
                task_id="task1",
                states=self.STATES,
                execution_dates=[timezone.datetime(2022, 1, 1)],
                poll_interval=0.2,
                trigger_start_time=trigger_start_time,
            )

        trigger.count_running_dags = mock.AsyncMock()
        trigger.count_running_dags.return_value = 0

        gen = trigger.run()
        task = asyncio.create_task(gen.__anext__())
        await task

        result = task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "timeout"}
        assert task.done() is True

        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @mock.patch("airflow.triggers.external_task.utcnow")
    @mock.patch("airflow.triggers.external_task.asyncio.sleep")
    @pytest.mark.asyncio
    async def test_task_state_trigger_timeout_sleep_success(self, mock_sleep, mock_utcnow):
        trigger_start_time = utcnow()
        mock_utcnow.return_value = trigger_start_time + datetime.timedelta(seconds=20)

        with pytest.warns(RemovedInAirflow3Warning, match="TaskStateTrigger has been deprecated"):
            trigger = TaskStateTrigger(
                dag_id="dag1",
                task_id="task1",
                states=self.STATES,
                execution_dates=[timezone.datetime(2022, 1, 1)],
                poll_interval=0.2,
                trigger_start_time=trigger_start_time,
            )

        trigger.count_running_dags = mock.AsyncMock()
        trigger.count_running_dags.return_value = 0

        trigger.count_tasks = mock.AsyncMock()
        trigger.count_tasks.return_value = 1

        gen = trigger.run()
        task = asyncio.create_task(gen.__anext__())
        await task

        mock_sleep.assert_awaited()
        assert mock_sleep.await_count == 1

        result = task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "success"}
        assert task.done() is True

        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @mock.patch("airflow.triggers.external_task.utcnow")
    @mock.patch("airflow.triggers.external_task.asyncio.sleep")
    @pytest.mark.asyncio
    async def test_task_state_trigger_failed_exception(self, mock_sleep, mock_utcnow):
        """
        Asserts that the TaskStateTrigger only goes off on or after a TaskInstance
        reaches an allowed state (i.e. SUCCESS).
        """
        trigger_start_time = utcnow()
        mock_utcnow.return_value = +datetime.timedelta(seconds=61)

        mock_utcnow.side_effect = [
            trigger_start_time,
            trigger_start_time + datetime.timedelta(seconds=20),
        ]

        with pytest.warns(RemovedInAirflow3Warning, match="TaskStateTrigger has been deprecated"):
            trigger = TaskStateTrigger(
                dag_id="dag1",
                task_id="task1",
                states=self.STATES,
                execution_dates=[timezone.datetime(2022, 1, 1)],
                poll_interval=0.2,
                trigger_start_time=trigger_start_time,
            )

        trigger.count_running_dags = mock.AsyncMock()
        trigger.count_running_dags.side_effect = [SQLAlchemyError]

        gen = trigger.run()
        task = asyncio.create_task(gen.__anext__())
        await task

        result = task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "failed"}
        assert task.done() is True

    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger_start_time = utcnow()
        with pytest.warns(RemovedInAirflow3Warning, match="TaskStateTrigger has been deprecated"):
            trigger = TaskStateTrigger(
                dag_id=self.DAG_ID,
                task_id=self.TASK_ID,
                states=self.STATES,
                execution_dates=[timezone.datetime(2022, 1, 1)],
                poll_interval=5,
                trigger_start_time=trigger_start_time,
            )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.triggers.external_task.TaskStateTrigger"
        assert kwargs == {
            "dag_id": self.DAG_ID,
            "task_id": self.TASK_ID,
            "states": self.STATES,
            "execution_dates": [timezone.datetime(2022, 1, 1)],
            "poll_interval": 5,
            "trigger_start_time": trigger_start_time,
        }


class TestDagStateTrigger:
    DAG_ID = "test_dag_state_trigger"
    RUN_ID = "external_task_run_id"
    STATES = ["success", "fail"]

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_dag_state_trigger(self, session):
        """
        Assert that the DagStateTrigger only goes off on or after a DagRun
        reaches an allowed state (i.e. SUCCESS).
        """
        dag = DAG(self.DAG_ID, start_date=timezone.datetime(2022, 1, 1))
        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_type="manual",
            execution_date=timezone.datetime(2022, 1, 1),
            run_id=self.RUN_ID,
        )
        session.add(dag_run)
        session.commit()

        trigger = DagStateTrigger(
            dag_id=dag.dag_id,
            states=self.STATES,
            execution_dates=[timezone.datetime(2022, 1, 1)],
            poll_interval=0.2,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # It should not have produced a result
        assert task.done() is False

        # Progress the dag to a "success" state so that yields a TriggerEvent
        dag_run.state = DagRunState.SUCCESS
        session.commit()
        await asyncio.sleep(0.5)
        assert task.done() is True

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    def test_serialization(self):
        """Asserts that the DagStateTrigger correctly serializes its arguments and classpath."""
        trigger = DagStateTrigger(
            dag_id=self.DAG_ID,
            states=self.STATES,
            execution_dates=[timezone.datetime(2022, 1, 1)],
            poll_interval=5,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.triggers.external_task.DagStateTrigger"
        assert kwargs == {
            "dag_id": self.DAG_ID,
            "states": self.STATES,
            "execution_dates": [timezone.datetime(2022, 1, 1)],
            "poll_interval": 5,
        }


def mocked_get_count(*args, **kwargs):
    time.sleep(0.0001)
    return 1


async def fake_async_fun():
    await asyncio.sleep(0.00005)
