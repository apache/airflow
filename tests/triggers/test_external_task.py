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

import pytest

from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.triggers.external_task import DagStateTrigger, TaskStateTrigger
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.timezone import utcnow


class TestTaskStateTrigger:
    DAG_ID = "external_task"
    TASK_ID = "external_task_op"
    RUN_ID = "external_task_run_id"
    STATES = ["success", "fail"]

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_task_state_trigger(self, session):
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
        instance = TaskInstance(external_task, timezone.datetime(2022, 1, 1))
        session.add(instance)
        session.commit()

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

    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger_start_time = utcnow()
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
