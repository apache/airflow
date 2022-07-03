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

import unittest
from unittest.mock import patch

import pytest
from parameterized import parameterized

from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHook,
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
)
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor


class TestPipelineRunStatusSensor(unittest.TestCase):
    def setUp(self):
        self.config = {
            "azure_data_factory_conn_id": "azure_data_factory_test",
            "run_id": "run_id",
            "resource_group_name": "resource-group-name",
            "factory_name": "factory-name",
            "timeout": 100,
            "poke_interval": 15,
        }
        self.sensor = AzureDataFactoryPipelineRunStatusSensor(task_id="pipeline_run_sensor", **self.config)

    def test_init(self):
        assert self.sensor.azure_data_factory_conn_id == self.config["azure_data_factory_conn_id"]
        assert self.sensor.run_id == self.config["run_id"]
        assert self.sensor.resource_group_name == self.config["resource_group_name"]
        assert self.sensor.factory_name == self.config["factory_name"]
        assert self.sensor.timeout == self.config["timeout"]
        assert self.sensor.poke_interval == self.config["poke_interval"]

    @parameterized.expand(
        [
            (AzureDataFactoryPipelineRunStatus.SUCCEEDED, True),
            (AzureDataFactoryPipelineRunStatus.FAILED, "exception"),
            (AzureDataFactoryPipelineRunStatus.CANCELLED, "exception"),
            (AzureDataFactoryPipelineRunStatus.CANCELING, False),
            (AzureDataFactoryPipelineRunStatus.QUEUED, False),
            (AzureDataFactoryPipelineRunStatus.IN_PROGRESS, False),
        ]
    )
    @patch.object(AzureDataFactoryHook, "get_pipeline_run")
    def test_poke(self, pipeline_run_status, expected_status, mock_pipeline_run):
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
