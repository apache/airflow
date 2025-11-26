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
import time
from unittest import mock

import pytest

from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.providers.standard.triggers.external_task import DagStateTrigger, WorkflowTrigger
from airflow.triggers.base import TriggerEvent
from airflow.utils import timezone
from airflow.utils.state import DagRunState

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

_DATES = (
    {"run_ids": ["external_task_run_id"]}
    if AIRFLOW_V_3_0_PLUS
    else {"execution_dates": [timezone.datetime(2022, 1, 1)]}
)
key, value = next(iter(_DATES.items()))


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 3")
class TestWorkflowTrigger:
    DAG_ID = "external_task"
    TASK_ID = "external_task_op"
    RUN_ID = "external_task_run_id"
    STATES = ["success", "fail"]
    LOGICAL_DATE = timezone.datetime(2022, 1, 1)

    @pytest.mark.flaky(reruns=5)
    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_ti_count")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_success(self, mock_get_count):
        """check the db count get called correctly."""
        mock_get_count.side_effect = mocked_get_count

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            logical_dates=[self.LOGICAL_DATE],
            external_task_ids=[self.TASK_ID],
            allowed_states=self.STATES,
            poke_interval=0.2,
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        fake_task = asyncio.create_task(fake_async_fun())
        await trigger_task
        await fake_task
        assert fake_task.done()  # confirm that get_count is done in an async fashion
        assert trigger_task.done()
        result = trigger_task.result()
        assert result.payload == {"status": "success"}

        mock_get_count.assert_called_once_with(
            dag_id="external_task",
            task_ids=["external_task_op"],
            logical_dates=[self.LOGICAL_DATE],
            run_ids=None,
            states=["success", "fail"],
        )
        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @pytest.mark.flaky(reruns=5)
    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_ti_count")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_failed(self, mock_get_count):
        mock_get_count.side_effect = mocked_get_count

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            logical_dates=[self.LOGICAL_DATE],
            run_ids=[self.RUN_ID],
            external_task_ids=[self.TASK_ID],
            failed_states=self.STATES,
            poke_interval=0.2,
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        fake_task = asyncio.create_task(fake_async_fun())
        await trigger_task
        await fake_task
        assert fake_task.done()  # confirm that get_count is done in an async fashion
        assert trigger_task.done()
        result = trigger_task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "failed"}
        mock_get_count.assert_called_once_with(
            dag_id="external_task",
            task_ids=["external_task_op"],
            logical_dates=[self.LOGICAL_DATE],
            run_ids=[self.RUN_ID],
            states=["success", "fail"],
        )
        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @pytest.mark.asyncio
    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_ti_count")
    async def test_task_workflow_trigger_fail_count_eq_0(self, mock_get_count):
        mock_get_count.side_effect = [0, 1]  # First 0 for failed_states, then 1 for allowed_states

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            logical_dates=[self.LOGICAL_DATE],
            run_ids=[self.RUN_ID],
            external_task_ids=[self.TASK_ID],
            failed_states=self.STATES,
            allowed_states=self.STATES,
            poke_interval=0.2,
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        await trigger_task
        assert trigger_task.done()
        result = trigger_task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "success"}

        # Verify both calls were made
        assert mock_get_count.call_count == 2
        mock_get_count.assert_has_calls(
            [
                mock.call(
                    dag_id="external_task",
                    task_ids=["external_task_op"],
                    logical_dates=[self.LOGICAL_DATE],
                    run_ids=[self.RUN_ID],
                    states=["success", "fail"],
                ),
            ]
            * 2
        )

        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @pytest.mark.flaky(reruns=5)
    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_ti_count")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_skipped(self, mock_get_count):
        mock_get_count.side_effect = mocked_get_count

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            logical_dates=[self.LOGICAL_DATE],
            external_task_ids=[self.TASK_ID],
            skipped_states=self.STATES,
            poke_interval=0.2,
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        fake_task = asyncio.create_task(fake_async_fun())
        await trigger_task
        await fake_task
        assert fake_task.done()  # confirm that get_count is done in an async fashion
        assert trigger_task.done()
        result = trigger_task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "skipped"}
        mock_get_count.assert_called_once_with(
            dag_id="external_task",
            task_ids=["external_task_op"],
            logical_dates=[self.LOGICAL_DATE],
            run_ids=None,
            states=["success", "fail"],
        )

    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_ti_count")
    @mock.patch("asyncio.sleep")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_sleep_success(self, mock_sleep, mock_get_count):
        mock_get_count.side_effect = [0, 1]

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            logical_dates=[self.LOGICAL_DATE],
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

    @pytest.mark.parametrize(
        (
            "task_ids",
            "task_group_id",
            "states",
            "logical_dates",
            "mock_ti_count",
            "mock_task_states",
            "mock_dag_count",
            "expected",
        ),
        [
            (
                ["task_id_one", "task_id_two"],
                None,
                ["success"],
                [
                    timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                    timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                ],
                4,
                None,
                None,
                2,
            ),
            (
                [],
                "task_group_id",
                ["success"],
                [
                    timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                    timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                ],
                None,
                {"run_id_one": {"group1.task_id": "success"}, "run_id_two": {"group1.task_id": "success"}},
                None,
                2,
            ),
            (
                [],
                "task_group_id",
                ["success"],
                [
                    timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                    timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                ],
                None,
                {
                    "run_id_one": {"task_group_id.task_id": "success"},
                    "run_id_two": {"task_group_id.task_id": "failed"},
                },
                None,
                1,
            ),
            (
                [],
                "task_group_id",
                ["success"],
                [
                    timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                    timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                ],
                None,
                {
                    "run_id_one": {"task_group_id.task_id": "success"},
                    "run_id_two": {"task_group_id.task_id": "skipped"},
                },
                None,
                1,
            ),
            (
                [],
                None,
                ["success"],
                [
                    timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                    timezone.datetime(2020, 7, 6, 13, tzinfo=timezone.utc),
                ],
                None,
                None,
                2,
                2,
            ),
        ],
        ids=[
            "with_task_ids",
            "with task_group_id only",
            "with task_group_id and some task_ids failed",
            "without task_group_id and some task_ids skipped",
            "no task_ids or task_group_id",
        ],
    )
    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_ti_count")
    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_task_states")
    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_dr_count")
    @pytest.mark.asyncio
    async def test_get_count_af_3(
        self,
        mock_get_dr_count,
        mock_get_task_states,
        mock_get_ti_count,
        task_ids,
        task_group_id,
        states,
        logical_dates,
        mock_ti_count,
        mock_task_states,
        mock_dag_count,
        expected,
    ):
        """
        case1: when provided two task_ids, and two dag runs, the get_ti_count should return 4(each dag run returns two tasks)
                and normalized count becomes 2
        case2: when provided task_group_id, and two dag runs, the get_ti_count should return 2(each dag run returns 1 task group)
        case3: when not provided any task_ids or task_group_id, the get_dr_count should return 2(total dag runs 2)
        """

        mock_get_ti_count.return_value = mock_ti_count
        mock_get_dr_count.return_value = mock_dag_count
        mock_get_task_states.return_value = mock_task_states

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            logical_dates=logical_dates,
            external_task_ids=task_ids,
            external_task_group_id=task_group_id,
            allowed_states=states,
            poke_interval=0.2,
        )

        get_count_af_3 = await trigger._get_count_af_3(states)
        assert get_count_af_3 == expected

        if task_ids:
            mock_get_ti_count.assert_called_once()
            assert mock_get_ti_count.call_count == 1
            mock_get_task_states.assert_not_called()
            assert mock_get_task_states.call_count == 0
            mock_get_dr_count.assert_not_called()
            assert mock_get_dr_count.call_count == 0

        elif task_group_id:
            mock_get_task_states.assert_called_once()
            assert mock_get_task_states.call_count == 1
            mock_get_ti_count.assert_not_called()
            assert mock_get_ti_count.call_count == 0
            mock_get_dr_count.assert_not_called()
            assert mock_get_dr_count.call_count == 0

        if not task_ids and not task_group_id:
            mock_get_dr_count.assert_called_once()
            assert mock_get_dr_count.call_count == 1
            mock_get_ti_count.assert_not_called()
            assert mock_get_ti_count.call_count == 0
            mock_get_task_states.assert_not_called()
            assert mock_get_task_states.call_count == 0

    def test_serialization(self):
        """
        Asserts that the WorkflowTrigger correctly serializes its arguments and classpath.
        """
        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            logical_dates=[self.LOGICAL_DATE],
            run_ids=[self.RUN_ID],
            failed_states=["failed"],
            skipped_states=["skipped"],
            external_task_ids=[self.TASK_ID],
            allowed_states=self.STATES,
            poke_interval=5,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.standard.triggers.external_task.WorkflowTrigger"
        assert kwargs == {
            "external_dag_id": self.DAG_ID,
            "logical_dates": [self.LOGICAL_DATE],
            "run_ids": [self.RUN_ID],
            "external_task_ids": [self.TASK_ID],
            "external_task_group_id": None,
            "failed_states": ["failed"],
            "skipped_states": ["skipped"],
            "allowed_states": self.STATES,
            "poke_interval": 5,
            "soft_fail": False,
        }


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only for Airflow 2")
class TestWorkflowTriggerAF2:
    DAG_ID = "external_task"
    TASK_ID = "external_task_op"
    RUN_ID = "external_task_run_id"
    STATES = ["success", "fail"]

    @pytest.mark.flaky(reruns=5)
    @mock.patch("airflow.providers.standard.triggers.external_task._get_count")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_success(self, mock_get_count):
        """check the db count get called correctly."""
        mock_get_count.side_effect = mocked_get_count

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            **_DATES,
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
            dttm_filter=value,
            external_task_ids=["external_task_op"],
            external_task_group_id=None,
            external_dag_id="external_task",
            states=["success", "fail"],
        )
        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @pytest.mark.flaky(reruns=5)
    @mock.patch("airflow.providers.standard.triggers.external_task._get_count")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_failed(self, mock_get_count):
        mock_get_count.side_effect = mocked_get_count

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            **_DATES,
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
            dttm_filter=value,
            external_task_ids=["external_task_op"],
            external_task_group_id=None,
            external_dag_id="external_task",
            states=["success", "fail"],
        )
        # test that it returns after yielding
        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @mock.patch("airflow.providers.standard.triggers.external_task._get_count")
    @mock.patch("asyncio.sleep")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_fail_count_eq_0(self, mock_sleep, mock_get_count):
        mock_get_count.side_effect = [
            0,
            1,
        ]

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            **_DATES,
            external_task_ids=[self.TASK_ID],
            failed_states=self.STATES,
            allowed_states=self.STATES,
            poke_interval=0.2,
        )

        gen = trigger.run()
        trigger_task = asyncio.create_task(gen.__anext__())
        await trigger_task
        assert trigger_task.done()
        result = trigger_task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == {"status": "success"}

        assert mock_get_count.call_count == 2
        mock_get_count.assert_has_calls(
            [
                mock.call(
                    dttm_filter=value,
                    external_task_ids=["external_task_op"],
                    external_task_group_id=None,
                    external_dag_id="external_task",
                    states=["success", "fail"],
                ),
            ]
            * 2
        )

        mock_sleep.assert_not_called()

        with pytest.raises(StopAsyncIteration):
            await gen.__anext__()

    @pytest.mark.flaky(reruns=5)
    @mock.patch("airflow.providers.standard.triggers.external_task._get_count")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_skipped(self, mock_get_count):
        mock_get_count.side_effect = mocked_get_count

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            **_DATES,
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
            dttm_filter=value,
            external_task_ids=["external_task_op"],
            external_task_group_id=None,
            external_dag_id="external_task",
            states=["success", "fail"],
        )

    @mock.patch("airflow.providers.standard.triggers.external_task._get_count")
    @mock.patch("asyncio.sleep")
    @pytest.mark.asyncio
    async def test_task_workflow_trigger_sleep_success(self, mock_sleep, mock_get_count):
        mock_get_count.side_effect = [0, 1]

        trigger = WorkflowTrigger(
            external_dag_id=self.DAG_ID,
            **_DATES,
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
            **_DATES,
            external_task_ids=[self.TASK_ID],
            allowed_states=self.STATES,
            poke_interval=5,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.standard.triggers.external_task.WorkflowTrigger"
        assert kwargs == {
            "external_dag_id": self.DAG_ID,
            **_DATES,
            "external_task_ids": [self.TASK_ID],
            "external_task_group_id": None,
            "failed_states": None,
            "skipped_states": None,
            "allowed_states": self.STATES,
            "poke_interval": 5,
            "soft_fail": False,
        }


class TestDagStateTrigger:
    DAG_ID = "test_dag_state_trigger"
    RUN_ID = "external_task_run_id"
    STATES = ["success", "fail"]
    EXECUTION_DATE = timezone.datetime(2022, 1, 1)

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 3 had a different implementation")
    async def test_dag_state_trigger(self, session):
        """
        Assert that the DagStateTrigger only goes off on or after a DagRun
        reaches an allowed state (i.e. SUCCESS).
        """
        dag = DAG(self.DAG_ID, schedule=None, start_date=timezone.datetime(2022, 1, 1))
        run_id_or_execution_date = (
            {"run_id": "external_task_run_id"}
            if AIRFLOW_V_3_0_PLUS
            else {"execution_date": timezone.datetime(2022, 1, 1), "run_id": "external_task_run_id"}
        )
        dag_run = DagRun(dag_id=dag.dag_id, run_type="manual", **run_id_or_execution_date)
        session.add(dag_run)
        session.commit()

        trigger = DagStateTrigger(
            dag_id=dag.dag_id,
            states=self.STATES,
            **_DATES,
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

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 2 had a different implementation")
    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_dr_count")
    async def test_dag_state_trigger_af_3(self, mock_get_dag_run_count, session):
        """
        Assert that the DagStateTrigger only goes off on or after a DagRun
        reaches an allowed state (i.e. SUCCESS).
        """

        # Mock the get_dag_run_count_by_run_ids_and_states function to return 0 first time
        mock_get_dag_run_count.return_value = 0
        dag = DAG(self.DAG_ID, schedule=None, start_date=timezone.datetime(2022, 1, 1))

        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_type="manual",
            run_id="external_task_run_id",
            logical_date=timezone.datetime(2022, 1, 1),
        )
        session.add(dag_run)
        session.commit()

        trigger = DagStateTrigger(
            dag_id=dag.dag_id,
            states=self.STATES,
            run_ids=["external_task_run_id"],
            poll_interval=0.2,
            execution_dates=[timezone.datetime(2022, 1, 1)],
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # It should not have produced a result
        assert task.done() is False

        # Progress the dag to a "success" state so that yields a TriggerEvent
        dag_run.state = DagRunState.SUCCESS
        session.commit()

        # Mock the get_dag_run_count_by_run_ids_and_states function to return 1 second time
        mock_get_dag_run_count.return_value = 1
        await asyncio.sleep(0.5)
        assert task.done() is True

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 2 had a different implementation")
    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_dr_count")
    @mock.patch("airflow.sdk.execution_time.task_runner.RuntimeTaskInstance.get_dagrun_state")
    async def test_dag_state_trigger_af_3_return_type(
        self, mock_get_dagrun_state, mock_get_dag_run_count, session
    ):
        """
        Assert that the DagStateTrigger returns a tuple with classpath and event_data.
        """
        mock_get_dag_run_count.return_value = 1
        mock_get_dagrun_state.return_value = DagRunState.SUCCESS
        dag = DAG(f"{self.DAG_ID}_return_type", schedule=None, start_date=timezone.datetime(2022, 1, 1))

        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_type="manual",
            run_id="external_task_run_id",
            logical_date=timezone.datetime(2022, 1, 1),
        )
        dag_run.state = DagRunState.SUCCESS
        session.add(dag_run)
        session.commit()

        trigger = DagStateTrigger(
            dag_id=dag.dag_id,
            states=self.STATES,
            run_ids=["external_task_run_id"],
            poll_interval=0.2,
            execution_dates=[timezone.datetime(2022, 1, 1)],
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is True

        result = task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == (
            "airflow.providers.standard.triggers.external_task.DagStateTrigger",
            {
                "dag_id": "test_dag_state_trigger_return_type",
                "execution_dates": [
                    timezone.datetime(2022, 1, 1, 0, 0, tzinfo=timezone.utc),
                ],
                "external_task_run_id": DagRunState.SUCCESS,
                "poll_interval": 0.2,
                "run_ids": ["external_task_run_id"],
                "states": ["success", "fail"],
            },
        )
        asyncio.get_event_loop().stop()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test only AF2 implementation.")
    async def test_dag_state_trigger_af_2_return_type(self, session):
        """
        Assert that the DagStateTrigger returns a tuple with classpath and event_data.
        """
        dag = DAG(f"{self.DAG_ID}_return_type", schedule=None, start_date=timezone.datetime(2022, 1, 1))

        dag_run = DagRun(
            dag_id=dag.dag_id,
            run_type="manual",
            run_id="external_task_run_id",
            execution_date=timezone.datetime(2022, 1, 1),
        )
        dag_run.state = DagRunState.SUCCESS
        session.add(dag_run)
        session.commit()

        trigger = DagStateTrigger(
            dag_id=dag.dag_id,
            states=self.STATES,
            run_ids=["external_task_run_id"],
            poll_interval=0.2,
            execution_dates=[timezone.datetime(2022, 1, 1)],
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is True

        result = task.result()
        assert isinstance(result, TriggerEvent)
        assert result.payload == (
            "airflow.providers.standard.triggers.external_task.DagStateTrigger",
            {
                "dag_id": "test_dag_state_trigger_return_type",
                "execution_dates": [
                    timezone.datetime(2022, 1, 1, 0, 0, tzinfo=timezone.utc),
                ],
                # 'external_task_run_id': DagRunState.SUCCESS,  # This is only appended in AF3
                "poll_interval": 0.2,
                "run_ids": ["external_task_run_id"],
                "states": ["success", "fail"],
            },
        )
        asyncio.get_event_loop().stop()

    def test_serialization(self):
        """Asserts that the DagStateTrigger correctly serializes its arguments and classpath."""
        trigger = DagStateTrigger(
            dag_id=self.DAG_ID,
            states=self.STATES,
            run_ids=[TestDagStateTrigger.RUN_ID],
            execution_dates=[TestDagStateTrigger.EXECUTION_DATE],
            poll_interval=5,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.standard.triggers.external_task.DagStateTrigger"
        assert kwargs == {
            "dag_id": self.DAG_ID,
            "states": self.STATES,
            "run_ids": [TestDagStateTrigger.RUN_ID],
            "execution_dates": [TestDagStateTrigger.EXECUTION_DATE],
            "poll_interval": 5,
        }


def mocked_get_count(*args, **kwargs):
    time.sleep(0.0001)
    return 1


async def fake_async_fun():
    await asyncio.sleep(0.00005)
