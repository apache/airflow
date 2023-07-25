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
TEST_GCP_CONN_ID = "test-gcp-conn-id"
TEST_DATA_PIPELINE_NAME = "test-data-pipeline-name"
TEST_PARENT = "projects/test-project-id/locations/test-location/pipelines/test-data-pipeline-name"
TEST_JOB_ID = "test-job-id"

class TestDataPipelineHook:
    def setup_method(self):
        self.datapipeline_hook = DataPipelineHook(gcp_conn_id="google_cloud_default")

    @mock.patch("airflow.providers.google.cloud.hooks.datapipeline.DataPipelineHook.get_conn")
    def test_run_data_pipeline(self, mock_connection):
        """Test that run_data_pipeline is called with correct parameters and
           calls Google Data Pipelines API
        """
        mock_request = mock_connection.return_value.projects.return_value.locations.return_value.pipelines.return_value.run
        mock_request.return_value.execute.return_value = {"job": {"id": TEST_JOB_ID}}
 
        result = self.datapipeline_hook.run_data_pipeline(
            data_pipeline_name=TEST_DATA_PIPELINE_NAME,
            project_id=TEST_PROJECTID,
            location=TEST_LOCATION,
        )
        assert result == {"job": {"id": TEST_JOB_ID}}

        mock_request.assert_called_once_with(
            name = TEST_PARENT,
            body = {},
        )