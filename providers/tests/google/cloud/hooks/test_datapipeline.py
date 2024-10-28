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

import warnings
from unittest import mock

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.datapipeline import DataPipelineHook

pytestmark = pytest.mark.db_test


TASK_ID = "test-datapipeline-operators"
TEST_PARENT_NAME = "projects/test-project-id/locations/test-location"
TEST_LOCATION = "test-location"
TEST_PROJECT_ID = "test-project-id"
TEST_DATA_PIPELINE_NAME = "test-data-pipeline-name"
TEST_JOB_ID = "test-job-id"
TEST_BODY = {
    "name": f"{TEST_PARENT_NAME}/pipelines/{TEST_DATA_PIPELINE_NAME}",
    "type": "PIPELINE_TYPE_BATCH",
    "workload": {
        "dataflowFlexTemplateRequest": {
            "launchParameter": {
                "containerSpecGcsPath": "gs://dataflow-templates-us-central1/latest/Word_Count_metadata",
                "jobName": "test-job",
                "environment": {"tempLocation": "test-temp-location"},
                "parameters": {
                    "inputFile": "gs://dataflow-samples/shakespeare/kinglear.txt",
                    "output": "gs://test/output/my_output",
                },
            },
            "projectId": TEST_PROJECT_ID,
            "location": TEST_LOCATION,
        }
    },
}


class TestDataPipelineHook:
    """
    Module meant to test the DataPipeline Hooks
    """

    def setup_method(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", AirflowProviderDeprecationWarning)
            self.datapipeline_hook = DataPipelineHook(gcp_conn_id="google_cloud_default")

    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.DataflowHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.build")
    def test_get_conn(self, mock_build, mock_authorize):
        """
        Test that get_conn is called with the correct params and
        returns the correct API address
        """
        connection = self.datapipeline_hook.get_conn()
        mock_build.assert_called_once_with(
            "datapipelines", "v1", http=mock_authorize.return_value, cache_discovery=False
        )
        assert mock_build.return_value == connection

    @mock.patch(
        "airflow.providers.google.cloud.hooks.dataflow.DataflowHook.get_pipelines_conn"
    )
    def test_create_data_pipeline(self, mock_connection):
        """
        Test that request are called with the correct params
        Test that request returns the correct value
        """
        mock_locations = mock_connection.return_value.projects.return_value.locations
        mock_request = mock_locations.return_value.pipelines.return_value.create
        mock_request.return_value.execute.return_value = {"name": TEST_PARENT_NAME}

        result = self.datapipeline_hook.create_data_pipeline(
            body=TEST_BODY,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )

        mock_request.assert_called_once_with(
            parent=TEST_PARENT_NAME,
            body=TEST_BODY,
        )
        assert result == {"name": TEST_PARENT_NAME}

    @mock.patch(
        "airflow.providers.google.cloud.hooks.dataflow.DataflowHook.get_pipelines_conn"
    )
    def test_run_data_pipeline(self, mock_connection):
        """
        Test that run_data_pipeline is called with correct parameters and
        calls Google Data Pipelines API
        """
        mock_request = mock_connection.return_value.projects.return_value.locations.return_value.pipelines.return_value.run
        mock_request.return_value.execute.return_value = {"job": {"id": TEST_JOB_ID}}

        result = self.datapipeline_hook.run_data_pipeline(
            data_pipeline_name=TEST_DATA_PIPELINE_NAME,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )

        mock_request.assert_called_once_with(
            name=f"{TEST_PARENT_NAME}/pipelines/{TEST_DATA_PIPELINE_NAME}",
            body={},
        )
        assert result == {"job": {"id": TEST_JOB_ID}}
