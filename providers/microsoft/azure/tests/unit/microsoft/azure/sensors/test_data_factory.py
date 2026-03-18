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
from unittest.mock import patch

import pytest

from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.providers.microsoft.azure.triggers.data_factory import ADFPipelineRunStatusSensorTrigger


class TestAzureDataFactoryPipelineRunStatusSensor:
    def setup_method(self):
        self.config = {
            "azure_data_factory_conn_id": "azure_data_factory_test",
            "run_id": "run_id",
            "resource_group_name": "resource-group-name",
            "factory_name": "factory-name",
            "timeout": 100,
            "poke_interval": 15,
        }
        self.sensor = AzureDataFactoryPipelineRunStatusSensor(task_id="pipeline_run_sensor", **self.config)
        self.defered_sensor = AzureDataFactoryPipelineRunStatusSensor(
            task_id="pipeline_run_sensor_defered", deferrable=True, **self.config
        )

    def test_init(self):
        assert self.sensor.azure_data_factory_conn_id == self.config["azure_data_factory_conn_id"]
        assert self.sensor.run_id == self.config["run_id"]
        assert self.sensor.resource_group_name == self.config["resource_group_name"]
        assert self.sensor.factory_name == self.config["factory_name"]
        assert self.sensor.timeout == self.config["timeout"]
        assert self.sensor.poke_interval == self.config["poke_interval"]

    @pytest.mark.parametrize(
        ("pipeline_run_status", "expected_status"),
        [
            (AzureDataFactoryPipelineRunStatus.SUCCEEDED, True),
            (AzureDataFactoryPipelineRunStatus.FAILED, "exception"),
            (AzureDataFactoryPipelineRunStatus.CANCELLED, "exception"),
            (AzureDataFactoryPipelineRunStatus.CANCELING, False),
            (AzureDataFactoryPipelineRunStatus.QUEUED, False),
            (AzureDataFactoryPipelineRunStatus.IN_PROGRESS, False),
        ],
    )
    @patch.object(AzureDataFactoryHook, "get_pipeline_run")
    def test_poke(self, mock_pipeline_run, pipeline_run_status, expected_status):
        mock_pipeline_run.return_value.status = pipeline_run_status

        if expected_status != "exception":
            assert self.sensor.poke({}) == expected_status
        else:
            # The sensor should fail if the pipeline run status is "Failed" or "Cancelled".
            if pipeline_run_status == AzureDataFactoryPipelineRunStatus.FAILED:
                error_message = f"Pipeline run {self.config['run_id']} has failed."
            else:
                error_message = f"Pipeline run {self.config['run_id']} has been cancelled."

            with pytest.raises(AzureDataFactoryPipelineRunException, match=error_message):
                self.sensor.poke({})

    @mock.patch("airflow.providers.microsoft.azure.sensors.data_factory.AzureDataFactoryHook")
    def test_adf_pipeline_status_sensor_async(self, mock_hook):
        """Assert execute method defer for Azure Data factory pipeline run status sensor"""
        mock_hook.return_value.get_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.QUEUED
        with pytest.raises(TaskDeferred) as exc:
            self.defered_sensor.execute(mock.MagicMock())
        assert isinstance(exc.value.trigger, ADFPipelineRunStatusSensorTrigger), (
            "Trigger is not a ADFPipelineRunStatusSensorTrigger"
        )

    @mock.patch("airflow.providers.microsoft.azure.sensors.data_factory.AzureDataFactoryHook")
    @mock.patch(
        "airflow.providers.microsoft.azure.sensors.data_factory.AzureDataFactoryPipelineRunStatusSensor.defer"
    )
    def test_adf_pipeline_status_sensor_finish_before_deferred(self, mock_defer, mock_hook):
        mock_hook.return_value.get_pipeline_run_status.return_value = (
            AzureDataFactoryPipelineRunStatus.SUCCEEDED
        )
        self.defered_sensor.execute(mock.MagicMock())
        assert not mock_defer.called

    def test_adf_pipeline_status_sensor_execute_complete_success(self):
        """Assert execute_complete log success message when trigger fire with target status"""

        msg = f"Pipeline run {self.config['run_id']} has been succeeded."
        with mock.patch.object(self.defered_sensor.log, "info") as mock_log_info:
            self.defered_sensor.execute_complete(context={}, event={"status": "success", "message": msg})
        mock_log_info.assert_called_with(msg)

    def test_adf_pipeline_status_sensor_execute_complete_failure(self):
        """Assert execute_complete method fail"""

        with pytest.raises(AirflowException):
            self.defered_sensor.execute_complete(context={}, event={"status": "error", "message": ""})


class TestAzureDataFactoryPipelineRunStatusSensorWithAsync:
    RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"
    SENSOR = AzureDataFactoryPipelineRunStatusSensor(
        task_id="pipeline_run_sensor_async",
        run_id=RUN_ID,
        resource_group_name="resource-group-name",
        factory_name="factory-name",
        deferrable=True,
    )

    @mock.patch("airflow.providers.microsoft.azure.sensors.data_factory.AzureDataFactoryHook")
    def test_adf_pipeline_status_sensor_async(self, mock_hook):
        """Assert execute method defer for Azure Data factory pipeline run status sensor"""
        mock_hook.return_value.get_pipeline_run_status.return_value = AzureDataFactoryPipelineRunStatus.QUEUED
        with pytest.raises(TaskDeferred) as exc:
            self.SENSOR.execute({})
        assert isinstance(exc.value.trigger, ADFPipelineRunStatusSensorTrigger), (
            "Trigger is not a ADFPipelineRunStatusSensorTrigger"
        )

    def test_adf_pipeline_status_sensor_execute_complete_success(self):
        """Assert execute_complete log success message when trigger fire with target status"""

        msg = f"Pipeline run {self.RUN_ID} has been succeeded."
        with mock.patch.object(self.SENSOR.log, "info") as mock_log_info:
            self.SENSOR.execute_complete(context={}, event={"status": "success", "message": msg})
        mock_log_info.assert_called_with(msg)

    def test_adf_pipeline_status_sensor_execute_complete_failure(
        self,
    ):
        """Assert execute_complete method fail"""

        with pytest.raises(AirflowException):
            self.SENSOR.execute_complete(context={}, event={"status": "error", "message": ""})
