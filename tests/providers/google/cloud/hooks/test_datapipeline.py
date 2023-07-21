#
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

import copy
import re
import shlex
from asyncio import Future
from typing import Any
from unittest import mock
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.datapipeline import DataPipelineHook

TASK_ID = "test-datapipeline-operators"
TEST_NAME = "projects/test-datapipeline-operators/locations/test-location"
TEST_BODY = {
    "name": "projects/test-datapipeline-operators/locations/test-location/pipelines/test-pipeline",
            "type": "PIPELINE_TYPE_BATCH",
            "workload": {
                "dataflowFlexTemplateRequest": {
                "launchParameter": {
                    "containerSpecGcsPath": "gs://dataflow-templates-us-central1/latest/Word_Count_metadata",
                    "jobName": "test-job",
                    "environment": {
                    "tempLocation": "test-temp-location"
                    },
                    "parameters": {
                    "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
                    "output": "gs://test/output/my_output"
                    }
                },
                "projectId": "test-project-id",
                "location": "test-location"
                }
            }
}
TEST_LOCATION = "test-location"
TEST_PROJECTID = "test-project-id"
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_DATA_PIPELINE_NAME = "test_data_pipeline_name"
TEST_PARENT = "projects/test-datapipeline-operators/locations/test-location/pipelines/test-pipeline"

class TestDataPipelineHook:
    def setup_method(self):
        self.datapipeline_hook = DataPipelineHook(gcp_conn_id="google_cloud_default")

    @mock.patch("airflow.providers.google.cloud.hooks.datapipeline.DataPipelineHook.run_data_pipeline")
    @mock.patch("airflow.providers.google.cloud.hooks.datapipeline.get_conn")
    def test_datapipeline_hook_call_run_api(self, mock_datapipeline_hook, mock_connection):
        """Test that run_data_pipeline is called with correct parameters and
           calls Data Pipelines API
        """
        mock_locations = mock_connection.return_value.projects.return_value.locations #get location of mock
        mock_request = mock_locations.return_value.pipelines.return_value.run #mock run API
        mock_request.return_value.execute.return_value = {"job"}

        response = self.datapipeline_hook.run_data_pipeline(
            data_pipeline_name=TEST_DATA_PIPELINE_NAME,
            project_id=TEST_PROJECTID,
            location=TEST_LOCATION,
        )

        mock_request.assert_called_once_with(
            name = TEST_DATA_PIPELINE_NAME,
            body = {},
        )

        #notes:
        # call run_data_pipeline on class hook with test params
        # 