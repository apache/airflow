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

from unittest import mock
from unittest.mock import AsyncMock

import pytest

from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook
from airflow.providers.amazon.aws.triggers.mwaa import MwaaDagRunCompletedTrigger, MwaaTaskCompletedTrigger
from airflow.triggers.base import TriggerEvent
from airflow.utils.state import DagRunState, TaskInstanceState

from unit.amazon.aws.utils.test_waiter import assert_expected_waiter_type

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.mwaa."
TRIGGER_DAG_RUN_KWARGS = {
    "external_env_name": "test_env",
    "external_dag_id": "test_dag",
    "external_dag_run_id": "test_run_id",
}

TRIGGER_TASK_KWARGS = {
    "external_env_name": "test_env",
    "external_dag_id": "test_dag",
    "external_dag_run_id": "test_run_id",
    "external_task_id": "test_task_id",
}


class TestMwaaDagRunCompletedTrigger:
    def test_init_states(self):
        trigger = MwaaDagRunCompletedTrigger(**TRIGGER_DAG_RUN_KWARGS)
        assert trigger.success_states == {DagRunState.SUCCESS.value}
        assert trigger.failure_states == {DagRunState.FAILED.value}
        acceptors = trigger.waiter_config_overrides["acceptors"]
        expected_acceptors = [
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": DagRunState.SUCCESS.value,
                "state": "success",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": DagRunState.FAILED.value,
                "state": "failure",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": DagRunState.RUNNING.value,
                "state": "retry",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": DagRunState.QUEUED.value,
                "state": "retry",
            },
        ]
        assert len(acceptors) == len(DagRunState)
        assert {tuple(sorted(a.items())) for a in acceptors} == {
            tuple(sorted(a.items())) for a in expected_acceptors
        }

    def test_init_fail(self):
        with pytest.raises(ValueError, match=r".*success_states.*failure_states.*"):
            MwaaDagRunCompletedTrigger(
                **TRIGGER_DAG_RUN_KWARGS, success_states=("a", "b"), failure_states=("b", "c")
            )

    def test_overwritten_conn_passed_to_hook(self):
        OVERWRITTEN_CONN = "new-conn-id"
        op = MwaaDagRunCompletedTrigger(**TRIGGER_DAG_RUN_KWARGS, aws_conn_id=OVERWRITTEN_CONN)
        assert op.hook().aws_conn_id == OVERWRITTEN_CONN

    def test_no_conn_passed_to_hook(self):
        DEFAULT_CONN = "aws_default"
        op = MwaaDagRunCompletedTrigger(**TRIGGER_DAG_RUN_KWARGS)
        assert op.hook().aws_conn_id == DEFAULT_CONN

    def test_serialization(self):
        success_states = ["a", "b"]
        failure_states = ["c", "d"]
        trigger = MwaaDagRunCompletedTrigger(
            **TRIGGER_DAG_RUN_KWARGS, success_states=success_states, failure_states=failure_states
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "MwaaDagRunCompletedTrigger"
        assert kwargs.get("external_env_name") == TRIGGER_DAG_RUN_KWARGS["external_env_name"]
        assert kwargs.get("external_dag_id") == TRIGGER_DAG_RUN_KWARGS["external_dag_id"]
        assert kwargs.get("external_dag_run_id") == TRIGGER_DAG_RUN_KWARGS["external_dag_run_id"]
        assert kwargs.get("success_states") == success_states
        assert kwargs.get("failure_states") == failure_states

    @pytest.mark.asyncio
    @mock.patch.object(MwaaHook, "get_waiter")
    @mock.patch.object(MwaaHook, "get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = MwaaDagRunCompletedTrigger(**TRIGGER_DAG_RUN_KWARGS)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "dag_run_id": TRIGGER_DAG_RUN_KWARGS["external_dag_run_id"]}
        )
        assert_expected_waiter_type(mock_get_waiter, "mwaa_dag_run_complete")
        mock_get_waiter().wait.assert_called_once()


class TestMwaaTaskCompletedTrigger:
    def test_init_states(self):
        trigger = MwaaTaskCompletedTrigger(**TRIGGER_TASK_KWARGS)
        # Two defensive assertions that should fail if more success or failure states are added to make sure the expected set of success and failure states is kept in sync.
        assert trigger.success_states == {
            TaskInstanceState.SUCCESS.value,
            TaskInstanceState.SKIPPED.value,
        }
        assert trigger.failure_states == {TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED}
        acceptors = trigger.waiter_config_overrides["acceptors"]
        expected_acceptors = [
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.SUCCESS.value,
                "state": "success",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.FAILED.value,
                "state": "failure",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.RUNNING.value,
                "state": "retry",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.QUEUED.value,
                "state": "retry",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.DEFERRED.value,
                "state": "retry",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.REMOVED.value,
                "state": "retry",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.RESTARTING.value,
                "state": "retry",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.SCHEDULED.value,
                "state": "retry",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.SKIPPED.value,
                "state": "success",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.UP_FOR_RESCHEDULE.value,
                "state": "retry",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.UP_FOR_RETRY.value,
                "state": "retry",
            },
            {
                "matcher": "path",
                "argument": "RestApiResponse.state",
                "expected": TaskInstanceState.UPSTREAM_FAILED.value,
                "state": "failure",
            },
        ]
        assert len(acceptors) == len(TaskInstanceState)
        assert {tuple(sorted(a.items())) for a in acceptors} == {
            tuple(sorted(a.items())) for a in expected_acceptors
        }

    def test_all_task_instance_states_are_categorized(self):
        """
        Defensive test to ensure all TaskInstanceState values are properly categorized.
        This test will fail if new states are added to TaskInstanceState without
        updating the state categorization logic.
        """
        expected_terminal_states = {
            TaskInstanceState.SUCCESS.value,
            TaskInstanceState.FAILED.value,
            TaskInstanceState.SKIPPED.value,
            TaskInstanceState.REMOVED.value,
        }

        # In-progress states - task is still active or could transition to other states
        expected_in_progress_states = {
            TaskInstanceState.SCHEDULED.value,
            TaskInstanceState.QUEUED.value,
            TaskInstanceState.RUNNING.value,
            TaskInstanceState.RESTARTING.value,
            TaskInstanceState.UP_FOR_RETRY.value,
            TaskInstanceState.UP_FOR_RESCHEDULE.value,
            TaskInstanceState.UPSTREAM_FAILED.value,
            TaskInstanceState.DEFERRED.value,
        }

        # Get all actual states
        all_actual_states = {s.value for s in TaskInstanceState}
        all_expected_states = expected_terminal_states | expected_in_progress_states

        # Test that we haven't missed any states and no unexpected states exist
        assert all_actual_states == all_expected_states, (
            f"TaskInstanceState enum has changed! "
            f"New states: {all_actual_states - all_expected_states}, "
            f"Removed states: {all_expected_states - all_actual_states}. "
            f"Please update the state categorization logic in the sensor class."
        )

        # Ensure terminal and in-progress states don't overlap
        assert not (expected_terminal_states & expected_in_progress_states), (
            "Terminal and in-progress states must not overlap"
        )

    def test_overwritten_conn_passed_to_hook(self):
        OVERWRITTEN_CONN = "new-conn-id"
        op = MwaaTaskCompletedTrigger(**TRIGGER_TASK_KWARGS, aws_conn_id=OVERWRITTEN_CONN)
        assert op.hook().aws_conn_id == OVERWRITTEN_CONN

    def test_no_conn_passed_to_hook(self):
        DEFAULT_CONN = "aws_default"
        op = MwaaTaskCompletedTrigger(**TRIGGER_TASK_KWARGS)
        assert op.hook().aws_conn_id == DEFAULT_CONN

    def test_init_fail(self):
        with pytest.raises(ValueError, match=r".*success_states.*failure_states.*"):
            MwaaTaskCompletedTrigger(
                **TRIGGER_TASK_KWARGS, success_states=("a", "b"), failure_states=("b", "c")
            )

    def test_serialization(self):
        success_states = ["a", "b"]
        failure_states = ["c", "d"]
        trigger = MwaaTaskCompletedTrigger(
            **TRIGGER_TASK_KWARGS, success_states=success_states, failure_states=failure_states
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "MwaaTaskCompletedTrigger"
        assert kwargs.get("external_env_name") == TRIGGER_TASK_KWARGS["external_env_name"]
        assert kwargs.get("external_dag_id") == TRIGGER_TASK_KWARGS["external_dag_id"]
        assert kwargs.get("external_dag_run_id") == TRIGGER_TASK_KWARGS["external_dag_run_id"]
        assert kwargs.get("external_task_id") == TRIGGER_TASK_KWARGS["external_task_id"]
        assert kwargs.get("success_states") == success_states
        assert kwargs.get("failure_states") == failure_states

    @pytest.mark.asyncio
    @mock.patch.object(MwaaHook, "get_waiter")
    @mock.patch.object(MwaaHook, "get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = MwaaTaskCompletedTrigger(**TRIGGER_TASK_KWARGS)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "task_id": TRIGGER_TASK_KWARGS["external_task_id"]}
        )
        assert_expected_waiter_type(mock_get_waiter, "mwaa_task_complete")
        mock_get_waiter().wait.assert_called_once()
