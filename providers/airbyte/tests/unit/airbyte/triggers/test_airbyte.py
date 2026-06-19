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
from contextlib import suppress
from unittest import mock

import pytest
from airbyte_api.models import JobStatusEnum

from airflow.models import Connection
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.providers.airbyte.triggers.airbyte import AirbyteSyncTrigger
from airflow.triggers.base import TriggerEvent


class TestAirbyteSyncTrigger:
    DAG_ID = "airbyte_sync_run"
    TASK_ID = "airbyte_sync_run_task_op"
    JOB_ID = 1234
    CONN_ID = "airbyte_default"
    POLL_INTERVAL = 3.0

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(conn_id=self.CONN_ID, conn_type="airbyte", host="http://test-airbyte")
        )

    @pytest.fixture
    def end_time(self):
        return time.time() + 60 * 60 * 24 * 7

    @pytest.fixture
    def execution_deadline(self):
        return time.time() + 60 * 60 * 24 * 7

    def test_serialization(self, end_time, execution_deadline):
        """Assert TestAirbyteSyncTrigger correctly serializes its arguments and classpath."""
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            job_id=self.JOB_ID,
            execution_deadline=execution_deadline,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger"
        assert kwargs == {
            "job_id": self.JOB_ID,
            "conn_id": self.CONN_ID,
            "end_time": end_time,
            "poll_interval": self.POLL_INTERVAL,
            "execution_deadline": execution_deadline,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_run_sync_trigger(self, mock_get_job_status, end_time):
        """Test AirbyteSyncTrigger is triggered with mocked details and run successfully."""
        mock_get_job_status.return_value = JobStatusEnum.RUNNING

        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            job_id=self.JOB_ID,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_value", "mock_status", "mock_message"),
        [
            (JobStatusEnum.SUCCEEDED, "success", "Job run 1234 has completed successfully."),
        ],
    )
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_for_terminal_status_success(
        self, mock_get_job_status, mock_value, mock_status, mock_message, end_time
    ):
        """Assert that run trigger success message in case of job success"""
        mock_get_job_status.return_value = mock_value
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            job_id=self.JOB_ID,
        )
        expected_result = {
            "status": mock_status,
            "message": mock_message,
            "job_id": self.JOB_ID,
        }

        events = [e async for e in trigger.run()]
        assert len(events) == 1
        assert TriggerEvent(expected_result) == events[0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_value", "mock_status", "mock_message"),
        [
            (JobStatusEnum.CANCELLED, "cancelled", "Job run 1234 has been cancelled."),
        ],
    )
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_for_terminal_status_cancelled(
        self, mock_get_job_status, mock_value, mock_status, mock_message, end_time
    ):
        """Assert that run trigger success message in case of job success"""
        mock_get_job_status.return_value = mock_value
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            job_id=self.JOB_ID,
        )
        expected_result = {
            "status": mock_status,
            "message": mock_message,
            "job_id": self.JOB_ID,
        }

        events = [e async for e in trigger.run()]
        assert len(events) == 1
        assert TriggerEvent(expected_result) == events[0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_value", "mock_status", "mock_message"),
        [
            (JobStatusEnum.FAILED, "error", "Job run 1234 has failed."),
        ],
    )
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_for_terminal_status_error(
        self, mock_get_job_status, mock_value, mock_status, mock_message, end_time
    ):
        """Assert that run trigger success message in case of job success"""
        mock_get_job_status.return_value = mock_value
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            job_id=self.JOB_ID,
        )
        expected_result = {
            "status": mock_status,
            "message": mock_message,
            "job_id": self.JOB_ID,
        }

        events = [e async for e in trigger.run()]
        assert len(events) == 1
        assert TriggerEvent(expected_result) == events[0]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_exception(self, mock_get_job_status, end_time):
        """Assert that run catch exception if Airbyte Sync job API throw exception"""
        mock_get_job_status.side_effect = Exception("Test exception")
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            job_id=self.JOB_ID,
        )

        events = [e async for e in trigger.run()]

        expected_result = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
                "job_id": self.JOB_ID,
            }
        )
        assert len(events) == 1
        assert expected_result in events

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_timeout(self, mock_get_job_status, end_time):
        """Assert that run timeout after end_time elapsed"""
        mock_get_job_status.side_effect = JobStatusEnum.RUNNING

        end_time = time.time()
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            job_id=self.JOB_ID,
        )

        events = [e async for e in trigger.run()]

        expected_result = TriggerEvent(
            {
                "status": "error",
                "message": f"Job run {self.JOB_ID} has not reached a terminal status "
                f"after {end_time} seconds.",
                "job_id": self.JOB_ID,
            }
        )

        assert len(events) == 1
        assert expected_result in events

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_run_execution_timeout(self, mock_get_job_status, end_time):
        """Assert that run timeout after execution_deadline has elapsed"""
        mock_get_job_status.side_effect = JobStatusEnum.RUNNING
        execution_deadline = time.time() - 1

        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            execution_deadline=execution_deadline,
            job_id=self.JOB_ID,
        )

        events = [e async for e in trigger.run()]

        expected_result = TriggerEvent(
            {
                "status": "timeout",
                "message": f"Job run {self.JOB_ID} has reached execution timeout.",
                "job_id": self.JOB_ID,
            }
        )

        assert len(events) == 1
        assert expected_result in events

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_execution_timeout_takes_precedence_over_success_status(
        self,
        mock_get_job_status,
        end_time,
    ):
        """Execution timeout should take precedence over terminal job states."""

        mock_get_job_status.return_value = JobStatusEnum.SUCCEEDED

        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            execution_deadline=time.time() - 1,
            job_id=self.JOB_ID,
        )

        events = [e async for e in trigger.run()]

        expected_result = TriggerEvent(
            {
                "status": "timeout",
                "message": f"Job run {self.JOB_ID} has reached execution timeout.",
                "job_id": self.JOB_ID,
            }
        )

        assert len(events) == 1
        assert expected_result in events

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.triggers.airbyte.time")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_run_trigger_uses_wall_clock_end_time(
        self,
        mock_get_job_status,
        mock_time,
        end_time,
        execution_deadline,
    ):
        """Assert serialized deadlines are compared to wall-clock time."""

        mock_time.time.return_value = end_time - 60

        mock_get_job_status.return_value = JobStatusEnum.RUNNING

        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            execution_deadline=execution_deadline,
            job_id=self.JOB_ID,
        )

        task = asyncio.create_task(trigger.run().__anext__())

        await asyncio.sleep(0)

        assert task.done() is False

        mock_time.time.assert_called()
        mock_time.monotonic.assert_not_called()
        mock_get_job_status.assert_called_once_with(self.JOB_ID)

        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_terminal_yields_only_once(self, mock_get_job_status):
        mock_get_job_status.return_value = JobStatusEnum.SUCCEEDED

        trigger = AirbyteSyncTrigger(
            conn_id="airbyte_default",
            poll_interval=1,
            end_time=time.time() + 100,
            job_id=1234,
        )

        events = [e async for e in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "success"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_response", "expected_status"),
        [
            (JobStatusEnum.SUCCEEDED, False),
        ],
    )
    async def test_airbyte_job_is_still_running_success(self, mock_response, expected_status, end_time):
        """Test is_still_running with mocked response job status and assert
        the return response with expected value"""
        hook = mock.AsyncMock(AirbyteHook)
        hook.get_job_status.return_value = mock_response
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            job_id=self.JOB_ID,
        )
        response = await trigger.is_still_running(hook)
        assert response == expected_status

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_response", "expected_status"),
        [
            (JobStatusEnum.RUNNING, True),
        ],
    )
    async def test_airbyte_sync_run_is_still_running(self, mock_response, expected_status, end_time):
        """Test is_still_running with mocked response job status and assert
        the return response with expected value"""
        airbyte_hook = mock.AsyncMock(AirbyteHook)
        airbyte_hook.get_job_status.return_value = mock_response
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            job_id=self.JOB_ID,
        )
        response = await trigger.is_still_running(airbyte_hook)
        assert response == expected_status
