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
import json
from unittest import mock

import pytest
from azure.batch import models as batch_models

from airflow.providers.microsoft.azure.triggers.batch import AzureBatchJobTrigger
from airflow.triggers.base import TriggerEvent


def test_azure_batch_job_trigger_serialize():
    trigger = AzureBatchJobTrigger(
        job_id="job-123",
        azure_batch_conn_id="azure_batch_conn",
        timeout=42,
        poll_interval=9,
    )

    class_path, payload = trigger.serialize()

    assert class_path == "airflow.providers.microsoft.azure.triggers.batch.AzureBatchJobTrigger"
    assert set(payload.keys()) == {"job_id", "azure_batch_conn_id", "timeout", "poll_interval"}
    assert payload["job_id"] == "job-123"
    assert payload["azure_batch_conn_id"] == "azure_batch_conn"
    assert payload["timeout"] == 42
    assert payload["poll_interval"] == 9
    json.dumps(payload)
    assert all(isinstance(value, (str, int, float, bool, type(None))) for value in payload.values())


@pytest.mark.asyncio
async def test_trigger_run_success_all_tasks_completed():
    """Test trigger emits success when all tasks complete successfully."""
    trigger = AzureBatchJobTrigger(
        job_id="test-job",
        azure_batch_conn_id="azure_batch_default",
        timeout=1,  # 1 minute
        poll_interval=1,  # 1 second
    )

    # Mock task with completed state and success result
    mock_task1 = mock.MagicMock()
    mock_task1.id = "task1"
    mock_task1.state = batch_models.TaskState.completed
    mock_task1.execution_info = mock.MagicMock()
    mock_task1.execution_info.result = batch_models.TaskExecutionResult.success

    mock_task2 = mock.MagicMock()
    mock_task2.id = "task2"
    mock_task2.state = batch_models.TaskState.completed
    mock_task2.execution_info = mock.MagicMock()
    mock_task2.execution_info.result = batch_models.TaskExecutionResult.success

    with mock.patch("airflow.providers.microsoft.azure.triggers.batch.AzureBatchHook") as mock_hook_class:
        mock_hook = mock_hook_class.return_value
        mock_hook.connection.task.list.return_value = [mock_task1, mock_task2]

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["status"] == "success"
        assert events[0].payload["fail_task_ids"] == []


@pytest.mark.asyncio
async def test_trigger_run_failure_some_tasks_failed():
    """Test trigger emits failure when some tasks fail."""
    trigger = AzureBatchJobTrigger(
        job_id="test-job",
        azure_batch_conn_id="azure_batch_default",
        timeout=1,
        poll_interval=1,
    )

    # Mock tasks with mixed results
    mock_task1 = mock.MagicMock()
    mock_task1.id = "task1"
    mock_task1.state = batch_models.TaskState.completed
    mock_task1.execution_info = mock.MagicMock()
    mock_task1.execution_info.result = batch_models.TaskExecutionResult.success

    mock_task2 = mock.MagicMock()
    mock_task2.id = "task2"
    mock_task2.state = batch_models.TaskState.completed
    mock_task2.execution_info = mock.MagicMock()
    mock_task2.execution_info.result = batch_models.TaskExecutionResult.failure

    mock_task3 = mock.MagicMock()
    mock_task3.id = "task3"
    mock_task3.state = batch_models.TaskState.completed
    mock_task3.execution_info = mock.MagicMock()
    mock_task3.execution_info.result = batch_models.TaskExecutionResult.failure

    with mock.patch("airflow.providers.microsoft.azure.triggers.batch.AzureBatchHook") as mock_hook_class:
        mock_hook = mock_hook_class.return_value
        mock_hook.connection.task.list.return_value = [mock_task1, mock_task2, mock_task3]

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["status"] == "failure"
        assert set(events[0].payload["fail_task_ids"]) == {"task2", "task3"}


@pytest.mark.asyncio
async def test_trigger_run_timeout():
    """Test trigger emits timeout when polling exceeds timeout."""
    trigger = AzureBatchJobTrigger(
        job_id="test-job",
        azure_batch_conn_id="azure_batch_default",
        timeout=0.001,  # Very short timeout (0.001 minutes = 0.06 seconds)
        poll_interval=1,
    )

    # Mock task that never completes
    mock_task = mock.MagicMock()
    mock_task.id = "task1"
    mock_task.state = batch_models.TaskState.running

    with mock.patch("airflow.providers.microsoft.azure.triggers.batch.AzureBatchHook") as mock_hook_class:
        mock_hook = mock_hook_class.return_value
        mock_hook.connection.task.list.return_value = [mock_task]

        # Advance time to trigger timeout
        with mock.patch("airflow.providers.microsoft.azure.triggers.batch.timezone") as mock_timezone:
            from datetime import datetime, timedelta

            start_time = datetime(2024, 1, 1, 0, 0, 0)
            mock_timezone.utcnow.side_effect = [
                start_time,  # Initial time
                start_time + timedelta(minutes=1),  # After timeout
            ]

            events = []
            async for event in trigger.run():
                events.append(event)

            assert len(events) == 1
            assert events[0].payload["status"] == "timeout"
            assert "Timed out waiting for tasks to complete" in events[0].payload["message"]


@pytest.mark.asyncio
async def test_trigger_run_error_exception():
    """Test trigger emits error when Azure client raises exception."""
    trigger = AzureBatchJobTrigger(
        job_id="test-job",
        azure_batch_conn_id="azure_batch_default",
        timeout=1,
        poll_interval=1,
    )

    with mock.patch("airflow.providers.microsoft.azure.triggers.batch.AzureBatchHook") as mock_hook_class:
        mock_hook = mock_hook_class.return_value
        mock_hook.connection.task.list.side_effect = Exception("Azure API error")

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
        assert "Azure API error" in events[0].payload["message"]


@pytest.mark.asyncio
async def test_trigger_poll_interval_respected():
    """Test trigger respects poll_interval between checks."""
    trigger = AzureBatchJobTrigger(
        job_id="test-job",
        azure_batch_conn_id="azure_batch_default",
        timeout=1,
        poll_interval=5,  # 5 seconds
    )

    # Mock task that completes after one poll
    mock_task = mock.MagicMock()
    mock_task.id = "task1"
    mock_task.state = batch_models.TaskState.running

    mock_completed_task = mock.MagicMock()
    mock_completed_task.id = "task1"
    mock_completed_task.state = batch_models.TaskState.completed
    mock_completed_task.execution_info = mock.MagicMock()
    mock_completed_task.execution_info.result = batch_models.TaskExecutionResult.success

    call_count = 0

    def task_list_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return [mock_task]  # First call: running
        return [mock_completed_task]  # Second call: completed

    with mock.patch("airflow.providers.microsoft.azure.triggers.batch.AzureBatchHook") as mock_hook_class:
        mock_hook = mock_hook_class.return_value
        mock_hook.connection.task.list.side_effect = task_list_side_effect

        with mock.patch("airflow.providers.microsoft.azure.triggers.batch.asyncio.sleep") as mock_sleep:
            mock_sleep.return_value = None

            events = []
            async for event in trigger.run():
                events.append(event)

            # Verify sleep was called with correct interval
            mock_sleep.assert_called_once_with(5)

            assert len(events) == 1
            assert events[0].payload["status"] == "success"


@pytest.mark.asyncio
async def test_trigger_exits_after_first_terminal_event():
    """Test trigger exits immediately after emitting first terminal event."""
    trigger = AzureBatchJobTrigger(
        job_id="test-job",
        azure_batch_conn_id="azure_batch_default",
        timeout=10,
        poll_interval=1,
    )

    mock_task = mock.MagicMock()
    mock_task.id = "task1"
    mock_task.state = batch_models.TaskState.completed
    mock_task.execution_info = mock.MagicMock()
    mock_task.execution_info.result = batch_models.TaskExecutionResult.success

    with mock.patch("airflow.providers.microsoft.azure.triggers.batch.AzureBatchHook") as mock_hook_class:
        mock_hook = mock_hook_class.return_value
        # This should only be called once before trigger exits
        mock_hook.connection.task.list.return_value = [mock_task]

        events = []
        async for event in trigger.run():
            events.append(event)

        # Should emit exactly one event and stop
        assert len(events) == 1
        assert events[0].payload["status"] == "success"

        # Verify list was only called once
        assert mock_hook.connection.task.list.call_count == 1
