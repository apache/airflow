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

from unittest import mock

import pytest

from airflow.providers.google.cloud.operators.dataflow import (
    DataflowCreatePipelineOperator,
    DataflowRunPipelineOperator,
)

TASK_ID = "test-datapipeline-operators"
TEST_BODY = {
    "name": "projects/test-datapipeline-operators/locations/test-location/pipelines/test-pipeline",
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
            "projectId": "test-project-id",
            "location": "test-location",
        }
    },
}
TEST_LOCATION = "test-location"
TEST_PROJECT_ID = "test-project-id"
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_DATA_PIPELINE_NAME = "test_data_pipeline_name"


class TestCreateDataPipelineOperator:
    @pytest.fixture
    def create_operator(self):
        """
        Creates a mock create datapipeline operator to be used in testing.
        """
        return DataflowCreatePipelineOperator(
            task_id="test_create_datapipeline",
            body=TEST_BODY,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_execute(self, mock_hook, create_operator):
        """
        Test that the execute function creates and calls the DataPipeline hook with the correct parameters
        """
        create_operator.execute(mock.MagicMock())
        mock_hook.assert_called_once_with(
            gcp_conn_id="test_gcp_conn_id",
            impersonation_chain=None,
        )

        mock_hook.return_value.create_data_pipeline.assert_called_once_with(
            project_id=TEST_PROJECT_ID,
            body=TEST_BODY,
            location=TEST_LOCATION,
        )


@pytest.mark.db_test
class TestRunDataPipelineOperator:
    @pytest.fixture
    def run_operator(self):
        """
        Create a DataflowRunPipelineOperator instance with test data
        """
        return DataflowRunPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=TEST_DATA_PIPELINE_NAME,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_execute(self, data_pipeline_hook_mock, run_operator):
        """
        Test Run Operator execute with correct parameters
        """
        run_operator.execute(mock.MagicMock())
        data_pipeline_hook_mock.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=None,
        )

        data_pipeline_hook_mock.return_value.run_data_pipeline.assert_called_once_with(
            pipeline_name=TEST_DATA_PIPELINE_NAME,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )
