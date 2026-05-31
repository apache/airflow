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

import time
from unittest import mock

import pytest
from azure.batch import models as batch_models

from airflow.providers.microsoft.azure.triggers.batch import AzureBatchTrigger
from airflow.triggers.base import TriggerEvent

AZURE_BATCH_CONN_ID = "azure_batch_default"
JOB_ID = "test-job"
POKE_INTERVAL = 5
BATCH_END_TIME = time.time() + 60 * 60 * 24 * 7
MODULE = "airflow.providers.microsoft.azure"


class TestAzureBatchTrigger:
    TRIGGER = AzureBatchTrigger(
        job_id=JOB_ID,
        azure_batch_conn_id=AZURE_BATCH_CONN_ID,
        poll_interval=POKE_INTERVAL,
        end_time=BATCH_END_TIME,
    )

    def test_batch_trigger_serialization(self):
        classpath, kwargs = self.TRIGGER.serialize()

        assert classpath == f"{MODULE}.triggers.batch.AzureBatchTrigger"

        assert kwargs == {
            "job_id": JOB_ID,
            "azure_batch_conn_id": AZURE_BATCH_CONN_ID,
            "poll_interval": POKE_INTERVAL,
            "end_time": BATCH_END_TIME,
        }

    def test_build_trigger_event_success(self):
        completed_task = mock.MagicMock()
        completed_task.id = "task1"
        completed_task.state = batch_models.TaskState.completed
        completed_task.execution_info.result = batch_models.TaskExecutionResult.success

        event = self.TRIGGER._build_trigger_event([completed_task])

        assert event is not None

        assert event.payload == {
            "status": "success",
            "message": f"Azure Batch job {JOB_ID} completed successfully.",
            "job_id": JOB_ID,
        }

    def test_build_trigger_event_failure(self):
        failed_task = mock.MagicMock()
        failed_task.id = "task1"
        failed_task.state = batch_models.TaskState.completed
        failed_task.execution_info.result = batch_models.TaskExecutionResult.failure

        event = self.TRIGGER._build_trigger_event([failed_task])

        assert event is not None

        assert event.payload == {
            "status": "error",
            "message": f"Azure Batch job {JOB_ID} failed.",
            "job_id": JOB_ID,
            "failed_tasks": ["task1"],
        }

    def test_build_trigger_event_mixed_states(self):
        completed_task = mock.MagicMock()
        completed_task.id = "task1"
        completed_task.state = batch_models.TaskState.completed
        completed_task.execution_info.result = batch_models.TaskExecutionResult.success

        running_task = mock.MagicMock()
        running_task.id = "task2"
        running_task.state = batch_models.TaskState.running

        event = self.TRIGGER._build_trigger_event([completed_task, running_task])

        assert event is None

    def test_build_trigger_event_empty_tasks(self):
        event = self.TRIGGER._build_trigger_event([])

        assert event is not None

        assert event.payload == {
            "status": "error",
            "message": f"Azure Batch job {JOB_ID} contains no tasks.",
            "job_id": JOB_ID,
        }

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.batch.asyncio.sleep")
    @mock.patch(f"{MODULE}.triggers.batch.asyncio.to_thread")
    async def test_trigger_run_non_terminal_sleeps(
        self,
        mock_to_thread,
        mock_sleep,
    ):
        running_task = mock.MagicMock()
        running_task.id = "task1"
        running_task.state = batch_models.TaskState.running

        completed_task = mock.MagicMock()
        completed_task.id = "task1"
        completed_task.state = batch_models.TaskState.completed
        completed_task.execution_info.result = batch_models.TaskExecutionResult.success

        mock_to_thread.side_effect = [
            [running_task],
            [completed_task],
        ]

        events = [event async for event in self.TRIGGER.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "success",
                    "message": f"Azure Batch job {JOB_ID} completed successfully.",
                    "job_id": JOB_ID,
                }
            )
        ]

        mock_sleep.assert_awaited_once_with(POKE_INTERVAL)

    def test_build_trigger_event_non_terminal(self):
        running_task = mock.MagicMock()
        running_task.id = "task1"
        running_task.state = batch_models.TaskState.running

        event = self.TRIGGER._build_trigger_event([running_task])

        assert event is None

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.batch.asyncio.to_thread")
    async def test_trigger_run_success(self, mock_to_thread):
        completed_task = mock.MagicMock()
        completed_task.id = "task1"
        completed_task.state = batch_models.TaskState.completed
        completed_task.execution_info.result = batch_models.TaskExecutionResult.success

        mock_to_thread.return_value = [completed_task]

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)

        assert actual == TriggerEvent(
            {
                "status": "success",
                "message": f"Azure Batch job {JOB_ID} completed successfully.",
                "job_id": JOB_ID,
            }
        )

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.batch.asyncio.to_thread")
    async def test_trigger_run_failure(self, mock_to_thread):
        failed_task = mock.MagicMock()
        failed_task.id = "task1"
        failed_task.state = batch_models.TaskState.completed
        failed_task.execution_info.result = batch_models.TaskExecutionResult.failure

        mock_to_thread.return_value = [failed_task]

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)

        assert actual == TriggerEvent(
            {
                "status": "error",
                "message": f"Azure Batch job {JOB_ID} failed.",
                "job_id": JOB_ID,
                "failed_tasks": ["task1"],
            }
        )

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.batch.asyncio.to_thread")
    async def test_trigger_exception(self, mock_to_thread):
        mock_to_thread.side_effect = Exception("API failure")

        events = [event async for event in self.TRIGGER.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "message": "API failure",
                    "job_id": JOB_ID,
                }
            )
        ]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.batch.asyncio.to_thread")
    async def test_trigger_run_empty_tasks(self, mock_to_thread):
        mock_to_thread.return_value = []

        events = [event async for event in self.TRIGGER.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "error",
                    "message": f"Azure Batch job {JOB_ID} contains no tasks.",
                    "job_id": JOB_ID,
                }
            )
        ]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.batch.time")
    @mock.patch(f"{MODULE}.triggers.batch.asyncio.to_thread")
    async def test_trigger_timeout_job_already_succeeded(
        self,
        mock_to_thread,
        mock_time,
    ):
        completed_task = mock.MagicMock()
        completed_task.id = "task1"
        completed_task.state = batch_models.TaskState.completed
        completed_task.execution_info.result = batch_models.TaskExecutionResult.success

        mock_to_thread.return_value = [completed_task]

        mock_time.time.return_value = BATCH_END_TIME + 60

        events = [event async for event in self.TRIGGER.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "success",
                    "message": f"Azure Batch job {JOB_ID} completed successfully.",
                    "job_id": JOB_ID,
                }
            )
        ]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.triggers.batch.time")
    @mock.patch(f"{MODULE}.triggers.batch.asyncio.to_thread")
    async def test_trigger_timeout(self, mock_to_thread, mock_time):
        running_task = mock.MagicMock()
        running_task.id = "task1"
        running_task.state = batch_models.TaskState.running

        mock_to_thread.return_value = [running_task]

        mock_time.time.return_value = BATCH_END_TIME + 60

        events = [event async for event in self.TRIGGER.run()]

        assert events == [
            TriggerEvent(
                {
                    "status": "timeout",
                    "message": f"Timeout waiting for Azure Batch job {JOB_ID}.",
                    "job_id": JOB_ID,
                }
            )
        ]
