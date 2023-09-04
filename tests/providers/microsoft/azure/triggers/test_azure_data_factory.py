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

from airflow.providers.microsoft.azure.triggers.data_factory import (
    ADFPipelineRunStatusSensorTrigger,
    AzureDataFactoryPipelineRunStatus,
    AzureDataFactoryTrigger,
)
from airflow.triggers.base import TriggerEvent

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
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
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
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
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
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
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
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
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
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_adf_pipeline_run_status_sensors_trigger_cancelled(
        self, mock_data_factory, mock_status, mock_message
    ):
        """Test if the task is run is in trigger failure status."""
        mock_data_factory.return_value = mock_status

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "error", "message": mock_message}) == actual

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.refresh_conn")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_adf_pipeline_run_status_sensors_trigger_exception(
        self, mock_data_factory, mock_refresh_token
    ):
        """Test EMR container sensors with raise exception"""
        mock_data_factory.side_effect = Exception("Test exception")
        mock_refresh_token.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


class TestAzureDataFactoryTrigger:
    TRIGGER = AzureDataFactoryTrigger(
        run_id=AZ_PIPELINE_RUN_ID,
        resource_group_name=AZ_RESOURCE_GROUP_NAME,
        factory_name=AZ_FACTORY_NAME,
        azure_data_factory_conn_id=AZ_DATA_FACTORY_CONN_ID,
        end_time=AZ_PIPELINE_END_TIME,
    )

    def test_azure_data_factory_trigger_serialization(self):
        """Asserts that the AzureDataFactoryTrigger correctly serializes its arguments and classpath."""

        classpath, kwargs = self.TRIGGER.serialize()
        assert classpath == f"{MODULE}.triggers.data_factory.AzureDataFactoryTrigger"
        assert kwargs == {
            "run_id": AZ_PIPELINE_RUN_ID,
            "resource_group_name": AZ_RESOURCE_GROUP_NAME,
            "factory_name": AZ_FACTORY_NAME,
            "azure_data_factory_conn_id": AZ_DATA_FACTORY_CONN_ID,
            "end_time": AZ_PIPELINE_END_TIME,
            "wait_for_termination": True,
            "check_interval": 60,
        }

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_azure_data_factory_trigger_run_without_wait(self, mock_pipeline_run_status):
        """Assert that run trigger without waiting if wait_for_termination is set to false"""
        mock_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.SUCCEEDED
        trigger = AzureDataFactoryTrigger(
            run_id=AZ_PIPELINE_RUN_ID,
            resource_group_name=AZ_RESOURCE_GROUP_NAME,
            factory_name=AZ_FACTORY_NAME,
            azure_data_factory_conn_id=AZ_DATA_FACTORY_CONN_ID,
            wait_for_termination=False,
            end_time=AZ_PIPELINE_END_TIME,
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        expected = TriggerEvent(
            {
                "status": "success",
                "message": f"The pipeline run {AZ_PIPELINE_RUN_ID} has "
                f"{AzureDataFactoryPipelineRunStatus.SUCCEEDED} status.",
                "run_id": AZ_PIPELINE_RUN_ID,
            }
        )
        assert actual == expected

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_azure_data_factory_trigger_run_queued(self, mock_pipeline_run_status):
        """Assert that run wait if pipeline run is in queued state"""
        mock_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.QUEUED

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_azure_data_factory_trigger_run_inprogress(self, mock_pipeline_run_status):
        """Assert that run wait if pipeline run is in progress state"""
        mock_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.IN_PROGRESS

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_azure_data_factory_trigger_run_canceling(self, mock_pipeline_run_status):
        """Assert that run wait if pipeline run is in canceling state"""
        mock_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.CANCELING

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_azure_data_factory_trigger_run_success(self, mock_pipeline_run_status):
        """Assert that the trigger generates success event in case of pipeline success"""
        mock_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.SUCCEEDED

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        expected = TriggerEvent(
            {
                "status": "success",
                "message": f"The pipeline run {AZ_PIPELINE_RUN_ID} has "
                f"{AzureDataFactoryPipelineRunStatus.SUCCEEDED}.",
                "run_id": AZ_PIPELINE_RUN_ID,
            }
        )
        assert expected == actual

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_azure_data_factory_trigger_run_failed(self, mock_pipeline_run_status):
        """Assert that run trigger error message in case of pipeline fail"""
        mock_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.FAILED

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        expected = TriggerEvent(
            {
                "status": "error",
                "message": f"The pipeline run {AZ_PIPELINE_RUN_ID} has "
                f"{AzureDataFactoryPipelineRunStatus.FAILED}.",
                "run_id": AZ_PIPELINE_RUN_ID,
            }
        )
        assert expected == actual

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_azure_data_factory_trigger_run_cancelled(self, mock_pipeline_run_status):
        """Assert that run trigger error message in case of pipeline fail"""
        mock_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.CANCELLED

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        expected = TriggerEvent(
            {
                "status": "error",
                "message": f"The pipeline run {AZ_PIPELINE_RUN_ID} has "
                f"{AzureDataFactoryPipelineRunStatus.CANCELLED}.",
                "run_id": AZ_PIPELINE_RUN_ID,
            }
        )
        assert expected == actual

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.cancel_pipeline_run")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_azure_data_factory_trigger_run_exception(
        self, mock_pipeline_run_status, mock_cancel_pipeline_run
    ):
        """Assert that run catch exception if Azure API throw exception"""
        mock_pipeline_run_status.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        response = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
                "run_id": AZ_PIPELINE_RUN_ID,
            }
        )
        assert len(task) == 1
        assert response in task
        mock_cancel_pipeline_run.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryAsyncHook.get_adf_pipeline_run_status")
    async def test_azure_data_factory_trigger_run_timeout(self, mock_pipeline_run_status):
        """Assert that pipeline run times out after end_time elapses"""
        mock_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.QUEUED
        trigger = AzureDataFactoryTrigger(
            run_id=AZ_PIPELINE_RUN_ID,
            resource_group_name=AZ_RESOURCE_GROUP_NAME,
            factory_name=AZ_FACTORY_NAME,
            azure_data_factory_conn_id=AZ_DATA_FACTORY_CONN_ID,
            end_time=time.time(),
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        expected = TriggerEvent(
            {
                "status": "error",
                "message": f"Timeout: The pipeline run {AZ_PIPELINE_RUN_ID} "
                f"has {AzureDataFactoryPipelineRunStatus.QUEUED}.",
                "run_id": AZ_PIPELINE_RUN_ID,
            }
        )

        assert expected == actual
