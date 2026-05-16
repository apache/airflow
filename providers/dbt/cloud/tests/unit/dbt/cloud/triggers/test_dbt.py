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
from unittest.mock import AsyncMock

import pytest

from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger
from airflow.triggers.base import TriggerEvent


class TestDbtCloudRunJobTrigger:
    DAG_ID = "dbt_cloud_run"
    TASK_ID = "dbt_cloud_run_task_op"
    RUN_ID = 1234
    CONN_ID = "dbt_cloud_default"
    ACCOUNT_ID = 12340
    POLL_INTERVAL = 3.0

    @pytest.fixture
    def end_time(self):
        return time.monotonic() + 60 * 60 * 24 * 7

    @pytest.fixture
    def execution_deadline(self):
        return time.monotonic() + 60 * 60 * 24 * 7

    def test_serialization(self, end_time, execution_deadline):
        """Assert DbtCloudRunJobTrigger correctly serializes its arguments and classpath."""
        trigger = DbtCloudRunJobTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            execution_deadline=execution_deadline,
            run_id=self.RUN_ID,
            account_id=self.ACCOUNT_ID,
            hook_params={"retry_delay": 10},
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.dbt.cloud.triggers.dbt.DbtCloudRunJobTrigger"
        assert kwargs == {
            "run_id": self.RUN_ID,
            "account_id": self.ACCOUNT_ID,
            "conn_id": self.CONN_ID,
            "end_time": end_time,
            "execution_deadline": execution_deadline,
            "poll_interval": self.POLL_INTERVAL,
            "hook_params": {"retry_delay": 10},
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_status")
    async def test_dbt_run_job_trigger(self, mock_get_job_status, end_time):
        """Test DbtCloudRunJobTrigger is triggered with mocked details and run successfully."""

        mock_get_job_status.return_value = DbtCloudJobRunStatus.RUNNING.value
        trigger = DbtCloudRunJobTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            run_id=self.RUN_ID,
            account_id=self.ACCOUNT_ID,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned.
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_value", "mock_status", "mock_message"),
        [
            (DbtCloudJobRunStatus.SUCCESS.value, "success", "Job run 1234 has completed successfully."),
            (DbtCloudJobRunStatus.CANCELLED.value, "cancelled", "Job run 1234 has been cancelled."),
            (DbtCloudJobRunStatus.ERROR.value, "error", "Job run 1234 has failed."),
        ],
    )
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_status")
    async def test_dbt_job_run_for_terminal_status(
        self, mock_get_job_status, mock_value, mock_status, mock_message, end_time
    ):
        """Assert run trigger messages when job reaches terminal status."""

        mock_get_job_status.return_value = mock_value
        trigger = DbtCloudRunJobTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            run_id=self.RUN_ID,
            account_id=self.ACCOUNT_ID,
        )
        expected_result = {
            "status": mock_status,
            "message": mock_message,
            "run_id": self.RUN_ID,
        }

        events = [e async for e in trigger.run()]
        assert len(events) == 1
        assert TriggerEvent(expected_result) == events[0]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_status")
    async def test_dbt_job_run_exception(self, mock_get_job_status, end_time):
        """Assert that run catch exception if dbt cloud job API throw exception."""

        mock_get_job_status.side_effect = Exception("Test exception")
        trigger = DbtCloudRunJobTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            run_id=self.RUN_ID,
            account_id=self.ACCOUNT_ID,
        )

        expected_result = {
            "status": "error",
            "message": "Test exception",
            "run_id": self.RUN_ID,
        }

        events = [e async for e in trigger.run()]
        assert len(events) == 1
        assert TriggerEvent(expected_result) == events[0]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_status")
    async def test_dbt_job_run_timeout(self, mock_get_job_status):
        """Assert that run timeout after end_time elapsed."""

        mock_get_job_status.return_value = DbtCloudJobRunStatus.RUNNING.value

        end_time = time.monotonic() - 1
        trigger = DbtCloudRunJobTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            run_id=self.RUN_ID,
            account_id=self.ACCOUNT_ID,
        )

        expected_result = {
            "status": "error",
            "message": f"Job run {self.RUN_ID} has not reached a terminal status "
            f"within the configured timeout.",
            "run_id": self.RUN_ID,
        }

        events = [e async for e in trigger.run()]
        assert len(events) == 1
        assert TriggerEvent(expected_result) == events[0]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_status")
    async def test_dbt_job_run_execution_timeout(self, mock_get_job_status, end_time):
        """Assert that run emits timeout event after execution_deadline elapsed."""

        mock_get_job_status.return_value = DbtCloudJobRunStatus.RUNNING.value

        execution_deadline = time.monotonic() - 1

        trigger = DbtCloudRunJobTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            execution_deadline=execution_deadline,
            run_id=self.RUN_ID,
            account_id=self.ACCOUNT_ID,
        )

        expected_result = {
            "status": "timeout",
            "message": f"Job run {self.RUN_ID} has timed out.",
            "run_id": self.RUN_ID,
        }

        events = [e async for e in trigger.run()]
        assert len(events) == 1
        assert TriggerEvent(expected_result) == events[0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mock_response", "expected_status"),
        [
            (DbtCloudJobRunStatus.SUCCESS.value, False),
            (DbtCloudJobRunStatus.RUNNING.value, True),
        ],
    )
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_status")
    async def test_dbt_job_run_is_still_running(
        self, mock_get_job_status, mock_response, expected_status, end_time
    ):
        """Test is_still_running with mocked response job status and assert
        the return response with expected value."""
        hook = AsyncMock(DbtCloudHook)
        hook.get_job_status.return_value = mock_response
        trigger = DbtCloudRunJobTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=end_time,
            run_id=self.RUN_ID,
            account_id=self.ACCOUNT_ID,
        )
        response = await trigger.is_still_running(hook)
        assert response == expected_status
