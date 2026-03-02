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
from unittest import mock

import pytest

from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshException,
    PowerBIDatasetRefreshStatus,
    PowerBIHook,
)
from airflow.providers.microsoft.azure.triggers.powerbi import (
    PowerBIDatasetListTrigger,
    PowerBITrigger,
    PowerBIWorkspaceListTrigger,
)
from airflow.triggers.base import TriggerEvent

from unit.microsoft.azure.test_utils import get_airflow_connection

POWERBI_CONN_ID = "powerbi_default"
DATASET_ID = "dataset_id"
GROUP_ID = "group_id"
DATASET_REFRESH_ID = "dataset_refresh_id"
DATASET_IDS = "dataset_ids"
WORKSPACE_IDS = "workspace_ids"
TIMEOUT = 5
MODULE = "airflow.providers.microsoft.azure"
CHECK_INTERVAL = 1
REQUEST_BODY = {
    "type": "full",
    "commitMode": "transactional",
    "objects": [{"table": "Customer", "partition": "Robert"}],
    "applyRefreshPolicy": "false",
    "timeout": "05:00:00",
}
API_VERSION = "v1.0"


@pytest.fixture
def powerbi_trigger(timeout=TIMEOUT, check_interval=CHECK_INTERVAL) -> PowerBITrigger:
    """Fixture for creating a PowerBITrigger with customizable timeout and check interval."""
    return PowerBITrigger(
        conn_id=POWERBI_CONN_ID,
        proxies=None,
        api_version=API_VERSION,
        dataset_id=DATASET_ID,
        dataset_refresh_id=DATASET_REFRESH_ID,
        group_id=GROUP_ID,
        check_interval=check_interval,
        wait_for_termination=True,
        timeout=timeout,
    )


@pytest.fixture
def powerbi_dataset_list_trigger(timeout=TIMEOUT, group_id=GROUP_ID) -> PowerBIDatasetListTrigger:
    """Fixture for creating a PowerBIDatasetListTrigger with customizable timeout."""
    return PowerBIDatasetListTrigger(
        conn_id=POWERBI_CONN_ID,
        proxies=None,
        api_version=API_VERSION,
        group_id=group_id,
        dataset_ids=None,
        timeout=timeout,
    )


@pytest.fixture
def powerbi_workspace_list_trigger(timeout=TIMEOUT) -> PowerBIWorkspaceListTrigger:
    """Fixture for creating a PowerBIWorkspaceListTrigger with customizable timeout."""
    return PowerBIWorkspaceListTrigger(
        conn_id=POWERBI_CONN_ID,
        proxies=None,
        api_version=API_VERSION,
        workspace_ids=None,
        timeout=timeout,
    )


class TestPowerBITrigger:
    @mock.patch.object(PowerBIHook, "get_connection", side_effect=get_airflow_connection)
    def test_powerbi_trigger_serialization(self, connection):
        """Asserts that the PowerBI Trigger correctly serializes its arguments and classpath."""
        powerbi_trigger = PowerBITrigger(
            conn_id=POWERBI_CONN_ID,
            proxies=None,
            api_version=API_VERSION,
            dataset_id=DATASET_ID,
            dataset_refresh_id=DATASET_REFRESH_ID,
            group_id=GROUP_ID,
            check_interval=CHECK_INTERVAL,
            wait_for_termination=True,
            timeout=TIMEOUT,
            request_body=REQUEST_BODY,
        )

        classpath, kwargs = powerbi_trigger.serialize()
        assert classpath == f"{PowerBITrigger.__module__}.PowerBITrigger"
        assert kwargs == {
            "conn_id": POWERBI_CONN_ID,
            "dataset_id": DATASET_ID,
            "dataset_refresh_id": DATASET_REFRESH_ID,
            "timeout": TIMEOUT,
            "group_id": GROUP_ID,
            "proxies": None,
            "api_version": API_VERSION,
            "check_interval": CHECK_INTERVAL,
            "wait_for_termination": True,
            "request_body": REQUEST_BODY,
        }

    @pytest.mark.asyncio
    @mock.patch.object(PowerBIHook, "get_refresh_details_by_refresh_id")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    async def test_powerbi_trigger_run_inprogress(
        self, mock_trigger_dataset_refresh, mock_get_refresh_details_by_refresh_id, powerbi_trigger
    ):
        """Assert task isn't completed until timeout if dataset refresh is in progress."""
        mock_get_refresh_details_by_refresh_id.return_value = {
            "status": PowerBIDatasetRefreshStatus.IN_PROGRESS,
            "error": None,
        }
        mock_trigger_dataset_refresh.return_value = DATASET_REFRESH_ID
        task = asyncio.create_task(powerbi_trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # Assert TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch.object(PowerBIHook, "get_refresh_details_by_refresh_id")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    async def test_powerbi_trigger_run_failed(
        self, mock_trigger_dataset_refresh, mock_get_refresh_details_by_refresh_id, powerbi_trigger
    ):
        """Assert event is triggered upon failed dataset refresh."""
        mock_get_refresh_details_by_refresh_id.return_value = {
            "status": PowerBIDatasetRefreshStatus.FAILED,
            "error": "Test error",
        }
        mock_trigger_dataset_refresh.return_value = DATASET_REFRESH_ID

        generator = powerbi_trigger.run()
        actual = await generator.asend(None)
        expected = TriggerEvent(
            {
                "status": "error",
                "dataset_refresh_status": PowerBIDatasetRefreshStatus.FAILED,
                "message": f"The dataset refresh {DATASET_REFRESH_ID} has "
                f"{PowerBIDatasetRefreshStatus.FAILED}. Error: Test error",
                "dataset_refresh_id": DATASET_REFRESH_ID,
            }
        )
        assert expected == actual

    @pytest.mark.asyncio
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    async def test_powerbi_trigger_run_trigger_refresh(self, mock_trigger_dataset_refresh, powerbi_trigger):
        """Assert event is triggered upon successful new refresh trigger."""
        powerbi_trigger.dataset_refresh_id = None
        mock_trigger_dataset_refresh.return_value = DATASET_REFRESH_ID

        task = [i async for i in powerbi_trigger.run()]
        response = TriggerEvent(
            {
                "status": "success",
                "dataset_refresh_status": None,
                "message": f"The dataset refresh {DATASET_REFRESH_ID} has been triggered.",
                "dataset_refresh_id": DATASET_REFRESH_ID,
            }
        )
        assert len(task) == 1
        assert response in task

    @pytest.mark.asyncio
    @mock.patch.object(PowerBIHook, "get_refresh_details_by_refresh_id")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    async def test_powerbi_trigger_run_completed(
        self, mock_trigger_dataset_refresh, mock_get_refresh_details_by_refresh_id, powerbi_trigger
    ):
        """Assert event is triggered upon successful dataset refresh completion."""
        mock_get_refresh_details_by_refresh_id.return_value = {
            "status": PowerBIDatasetRefreshStatus.COMPLETED,
            "error": None,
        }
        mock_trigger_dataset_refresh.return_value = DATASET_REFRESH_ID

        generator = powerbi_trigger.run()
        actual = await generator.asend(None)
        expected = TriggerEvent(
            {
                "status": "success",
                "dataset_refresh_status": PowerBIDatasetRefreshStatus.COMPLETED,
                "message": f"The dataset refresh {DATASET_REFRESH_ID} has "
                f"{PowerBIDatasetRefreshStatus.COMPLETED}.",
                "dataset_refresh_id": DATASET_REFRESH_ID,
            }
        )
        assert expected == actual

    @pytest.mark.asyncio
    @mock.patch.object(PowerBIHook, "cancel_dataset_refresh")
    @mock.patch.object(PowerBIHook, "get_refresh_details_by_refresh_id")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    async def test_powerbi_trigger_run_exception_during_refresh_check_loop(
        self,
        mock_trigger_dataset_refresh,
        mock_get_refresh_details_by_refresh_id,
        mock_cancel_dataset_refresh,
        powerbi_trigger,
    ):
        """Assert that run catch exception if Power BI API throw exception"""
        mock_get_refresh_details_by_refresh_id.side_effect = Exception("Test exception")
        mock_trigger_dataset_refresh.return_value = DATASET_REFRESH_ID

        task = [i async for i in powerbi_trigger.run()]
        response = TriggerEvent(
            {
                "status": "error",
                "dataset_refresh_status": None,
                "message": "An error occurred: Test exception",
                "dataset_refresh_id": DATASET_REFRESH_ID,
            }
        )
        assert len(task) == 1
        assert response in task
        mock_cancel_dataset_refresh.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch.object(PowerBIHook, "cancel_dataset_refresh")
    @mock.patch.object(PowerBIHook, "get_refresh_details_by_refresh_id")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    async def test_powerbi_trigger_run_PowerBIDatasetRefreshException_during_refresh_check_loop(
        self,
        mock_trigger_dataset_refresh,
        mock_get_refresh_details_by_refresh_id,
        mock_cancel_dataset_refresh,
        powerbi_trigger,
    ):
        """Assert that run catch PowerBIDatasetRefreshException and triggers retry mechanism"""
        mock_get_refresh_details_by_refresh_id.side_effect = PowerBIDatasetRefreshException("Test exception")
        mock_trigger_dataset_refresh.return_value = DATASET_REFRESH_ID

        task = [i async for i in powerbi_trigger.run()]
        response = TriggerEvent(
            {
                "status": "error",
                "dataset_refresh_status": None,
                "message": "An error occurred: Test exception",
                "dataset_refresh_id": DATASET_REFRESH_ID,
            }
        )
        assert mock_get_refresh_details_by_refresh_id.call_count == 3
        assert len(task) == 1
        assert response in task
        assert mock_cancel_dataset_refresh.call_count == 1

    @pytest.mark.asyncio
    @mock.patch.object(PowerBIHook, "cancel_dataset_refresh")
    @mock.patch.object(PowerBIHook, "get_refresh_details_by_refresh_id")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    async def test_powerbi_trigger_run_exception_during_refresh_cancellation(
        self,
        mock_trigger_dataset_refresh,
        mock_get_refresh_details_by_refresh_id,
        mock_cancel_dataset_refresh,
        powerbi_trigger,
    ):
        """Assert that run catch exception if Power BI API throw exception"""
        mock_get_refresh_details_by_refresh_id.side_effect = Exception("Test exception")
        mock_cancel_dataset_refresh.side_effect = Exception("Exception caused by cancel_dataset_refresh")
        mock_trigger_dataset_refresh.return_value = DATASET_REFRESH_ID

        task = [i async for i in powerbi_trigger.run()]
        response = TriggerEvent(
            {
                "status": "error",
                "dataset_refresh_status": None,
                "message": "An error occurred while canceling dataset: Exception caused by cancel_dataset_refresh",
                "dataset_refresh_id": DATASET_REFRESH_ID,
            }
        )

        assert len(task) == 1
        assert response in task
        mock_cancel_dataset_refresh.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch.object(PowerBIHook, "get_refresh_details_by_refresh_id")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    async def test_powerbi_trigger_run_exception_without_refresh_id(
        self, mock_trigger_dataset_refresh, mock_get_refresh_details_by_refresh_id, powerbi_trigger
    ):
        """Assert handling of exception when there is no dataset_refresh_id"""
        powerbi_trigger.dataset_refresh_id = None
        mock_get_refresh_details_by_refresh_id.side_effect = Exception(
            "Test exception for no dataset_refresh_id"
        )
        mock_trigger_dataset_refresh.return_value = None

        task = [i async for i in powerbi_trigger.run()]
        response = TriggerEvent(
            {
                "status": "error",
                "dataset_refresh_status": None,
                "message": "Failed to trigger the dataset refresh.",
                "dataset_refresh_id": None,
            }
        )
        assert len(task) == 1
        assert response in task

    @pytest.mark.asyncio
    @mock.patch.object(PowerBIHook, "get_refresh_details_by_refresh_id")
    @mock.patch.object(PowerBIHook, "trigger_dataset_refresh")
    async def test_powerbi_trigger_run_timeout(
        self, mock_trigger_dataset_refresh, mock_get_refresh_details_by_refresh_id, powerbi_trigger
    ):
        """Assert that powerbi run times out after end_time elapses"""
        mock_get_refresh_details_by_refresh_id.return_value = {
            "status": PowerBIDatasetRefreshStatus.IN_PROGRESS,
            "error": None,
        }
        mock_trigger_dataset_refresh.return_value = DATASET_REFRESH_ID

        generator = powerbi_trigger.run()
        actual = await generator.asend(None)
        expected = TriggerEvent(
            {
                "status": "error",
                "dataset_refresh_status": PowerBIDatasetRefreshStatus.IN_PROGRESS,
                "message": f"Timeout occurred while waiting for dataset refresh to complete: The dataset refresh {DATASET_REFRESH_ID} has status {PowerBIDatasetRefreshStatus.IN_PROGRESS}.",
                "dataset_refresh_id": DATASET_REFRESH_ID,
            }
        )

        assert expected == actual
