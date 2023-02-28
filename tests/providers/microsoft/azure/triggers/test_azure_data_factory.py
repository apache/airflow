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
import sys
import time

import pytest

from airflow.providers.microsoft.azure.triggers.data_factory import (
    ADFPipelineRunStatusSensorTrigger,
)
from airflow.triggers.base import TriggerEvent

if sys.version_info < (3, 8):
    # For compatibility with Python 3.7
    from asynctest import mock as async_mock
else:
    from unittest import mock as async_mock

RESOURCE_GROUP_NAME = "team_provider_resource_group_test"
DATAFACTORY_NAME = "ADFProvidersTeamDataFactory"
AZURE_DATA_FACTORY_CONN_ID = "azure_data_factory_default"
RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"
POKE_INTERVAL = 5
AZ_PIPELINE_RUN_ID = "123"
AZ_RESOURCE_GROUP_NAME = "test-rg"
AZ_FACTORY_NAME = "test-factory"
AZ_DATA_FACTORY_CONN_ID = "test-conn"
AZ_PIPELINE_END_TIME = time.time() + 60 * 60 * 24 * 7
MODULE = "airflow.providers.microsoft.azure"


class TestADFPipelineRunStatusSensorTrigger:
    TRIGGER = ADFPipelineRunStatusSensorTrigger(
        run_id=RUN_ID,
        azure_data_factory_conn_id=AZURE_DATA_FACTORY_CONN_ID,
        resource_group_name=RESOURCE_GROUP_NAME,
        factory_name=DATAFACTORY_NAME,
        poke_interval=POKE_INTERVAL,
    )

    def test_adf_pipeline_run_status_sensors_trigger_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """

        classpath, kwargs = self.TRIGGER.serialize()
        assert classpath == f"{MODULE}.triggers.data_factory.ADFPipelineRunStatusSensorTrigger"
        assert kwargs == {
            "run_id": RUN_ID,
            "azure_data_factory_conn_id": AZURE_DATA_FACTORY_CONN_ID,
            "resource_group_name": RESOURCE_GROUP_NAME,
            "factory_name": DATAFACTORY_NAME,
            "poke_interval": POKE_INTERVAL,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_status",
        [
            "Queued",
        ],
    )
    @async_mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_adf_pipeline_run_status_sensors_trigger_run_queued(self, mock_data_factory, mock_status):
        """
        Test if the task is run is in trigger successfully.
        """
        mock_data_factory.return_value = mock_status

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_status",
        [
            "InProgress",
        ],
    )
    @async_mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_adf_pipeline_run_status_sensors_trigger_run_inprogress(
        self, mock_data_factory, mock_status
    ):
        """
        Test if the task is run is in trigger successfully.
        """
        mock_data_factory.return_value = mock_status

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_status",
        ["Succeeded"],
    )
    @async_mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_adf_pipeline_run_status_sensors_trigger_completed(self, mock_data_factory, mock_status):
        """Test if the task pipeline status is in succeeded status."""
        mock_data_factory.return_value = mock_status

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        msg = f"Pipeline run {RUN_ID} has been Succeeded."
        assert TriggerEvent({"status": "success", "message": msg}) == actual

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_status, mock_message",
        [
            ("Failed", f"Pipeline run {RUN_ID} has Failed."),
        ],
    )
    @async_mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_adf_pipeline_run_status_sensors_trigger_failed(
        self, mock_data_factory, mock_status, mock_message
    ):
        """Test if the task is run is in trigger failure status."""
        mock_data_factory.return_value = mock_status

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": mock_message}) == actual

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_status, mock_message",
        [
            ("Cancelled", f"Pipeline run {RUN_ID} has been Cancelled."),
        ],
    )
    @async_mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_adf_pipeline_run_status_sensors_trigger_cancelled(
        self, mock_data_factory, mock_status, mock_message
    ):
        """Test if the task is run is in trigger failure status."""
        mock_data_factory.return_value = mock_status

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": mock_message}) == actual

    @pytest.mark.asyncio
    @async_mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_adf_pipeline_run_status_sensors_trigger_exception(self, mock_data_factory):
        """Test EMR container sensors with raise exception"""
        mock_data_factory.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
