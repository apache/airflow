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
    END_TIME = time.time() + 60 * 60 * 24 * 7
    POLL_INTERVAL = 3.0

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(conn_id=self.CONN_ID, conn_type="airbyte", host="http://test-airbyte")
        )

    def test_serialization(self):
        """Assert TestAirbyteSyncTrigger correctly serializes its arguments and classpath."""
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            job_id=self.JOB_ID,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger"
        assert kwargs == {
            "job_id": self.JOB_ID,
            "conn_id": self.CONN_ID,
            "end_time": self.END_TIME,
            "poll_interval": self.POLL_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger.is_still_running")
    async def test_airbyte_run_sync_trigger(self, mocked_is_still_running):
        """Test AirbyteSyncTrigger is triggered with mocked details and run successfully."""
        mocked_is_still_running.return_value = True
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
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
    @mock.patch("airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger.is_still_running")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_for_terminal_status_success(
        self, mock_get_job_status, mocked_is_still_running, mock_value, mock_status, mock_message
    ):
        """Assert that run trigger success message in case of job success"""
        mocked_is_still_running.return_value = False
        mock_get_job_status.return_value = mock_value
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            job_id=self.JOB_ID,
        )
        expected_result = {
            "status": mock_status,
            "message": mock_message,
            "job_id": self.JOB_ID,
        }
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert TriggerEvent(expected_result) == task.result()
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_value", "mock_status", "mock_message"),
        [
            (JobStatusEnum.CANCELLED, "cancelled", "Job run 1234 has been cancelled."),
        ],
    )
    @mock.patch("airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger.is_still_running")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_for_terminal_status_cancelled(
        self, mock_get_job_status, mocked_is_still_running, mock_value, mock_status, mock_message
    ):
        """Assert that run trigger success message in case of job success"""
        mocked_is_still_running.return_value = False
        mock_get_job_status.return_value = mock_value
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            job_id=self.JOB_ID,
        )
        expected_result = {
            "status": mock_status,
            "message": mock_message,
            "job_id": self.JOB_ID,
        }
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert TriggerEvent(expected_result) == task.result()
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_value", "mock_status", "mock_message"),
        [
            (JobStatusEnum.FAILED, "error", "Job run 1234 has failed."),
        ],
    )
    @mock.patch("airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger.is_still_running")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_for_terminal_status_error(
        self, mock_get_job_status, mocked_is_still_running, mock_value, mock_status, mock_message
    ):
        """Assert that run trigger success message in case of job success"""
        mocked_is_still_running.return_value = False
        mock_get_job_status.return_value = mock_value
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            job_id=self.JOB_ID,
        )
        expected_result = {
            "status": mock_status,
            "message": mock_message,
            "job_id": self.JOB_ID,
        }
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert TriggerEvent(expected_result) == task.result()
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger.is_still_running")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_exception(self, mock_get_job_status, mocked_is_still_running):
        """Assert that run catch exception if Airbyte Sync job API throw exception"""
        mocked_is_still_running.return_value = False
        mock_get_job_status.side_effect = Exception("Test exception")
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            job_id=self.JOB_ID,
        )
        task = [i async for i in trigger.run()]
        response = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
                "job_id": self.JOB_ID,
            }
        )
        assert len(task) == 1
        assert response in task

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger.is_still_running")
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_timeout(self, mock_get_job_status, mocked_is_still_running):
        """Assert that run timeout after end_time elapsed"""
        mocked_is_still_running.return_value = True
        mock_get_job_status.side_effect = Exception("Test exception")
        end_time = time.time()
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            job_id=self.JOB_ID,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        expected = TriggerEvent(
            {
                "status": "error",
                "message": f"Job run {self.JOB_ID} has not reached a terminal status "
                f"after {end_time} seconds.",
                "job_id": self.JOB_ID,
            }
        )
        assert expected == actual

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_response", "expected_status"),
        [
            (JobStatusEnum.SUCCEEDED, False),
        ],
    )
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_job_is_still_running_success(
        self, mock_get_job_status, mock_response, expected_status
    ):
        """Test is_still_running with mocked response job status and assert
        the return response with expected value"""
        hook = mock.AsyncMock(AirbyteHook)
        hook.get_job_status.return_value = mock_response
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
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
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job_status")
    async def test_airbyte_sync_run_is_still_running(
        self, mock_get_job_status, mock_response, expected_status
    ):
        """Test is_still_running with mocked response job status and assert
        the return response with expected value"""
        airbyte_hook = mock.AsyncMock(AirbyteHook)
        airbyte_hook.get_job_status.return_value = mock_response
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            job_id=self.JOB_ID,
        )
        response = await trigger.is_still_running(airbyte_hook)
        assert response == expected_status
