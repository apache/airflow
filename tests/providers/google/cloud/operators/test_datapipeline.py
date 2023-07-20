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
from unittest.mock import MagicMock

import pytest as pytest

import airflow
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.operators.datapipeline import (
    CreateDataPipelineOperator,
    RunDataPipelineOperator,
)
from airflow.providers.google.cloud.hooks.datapipeline import DataPipelineHook
from airflow.version import version

TASK_ID = "test-datapipeline-operators"
TEST_BODY = {
    "name": "projects/dataflow-interns/locations/us-central1/pipelines/dp-create-1642676351302-mp--1675461000",
            "type": "PIPELINE_TYPE_BATCH",
            "workload": {
                "dataflowFlexTemplateRequest": {
                "launchParameter": {
                    "containerSpecGcsPath": "gs://intern-bucket-1/templates/word-count.json",
                    "jobName": "word-count-test-intern1",
                    "environment": {
                    "tempLocation": "gs://intern-bucket-1/temp"
                    },
                    "parameters": {
                    "inputFile": "gs://intern-bucket-1/examples/kinglear.txt",
                    "output": "gs://intern-bucket-1/results/hello"
                    }
                },
                "projectId": "dataflow-interns",
                "location": "us-central1"
                }
            }
} # TODO change this to be different DAG testing
TEST_LOCATION = "test-location"
TEST_PROJECTID = "test-project-id"
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_DATA_PIPELINE_NAME = "test_data_pipeline_name"

class TestCreateDataPipelineOperator:
    @pytest.fixture
    def create_operator(self):
        return CreateDataPipelineOperator(
            task_id = "test_create_datapipeline",
            body = TEST_BODY,
            project_id = TEST_PROJECTID,
            location = TEST_LOCATION,
            gcp_conn_id = TEST_GCP_CONN_ID,
        )
    # TODO: Test the execute function 
    # TODO: Test Hook
    # TODO: Test all parameters are given

class TestRunDataPipelineOperator:
    @pytest.fixture
    def run_operator(self):
        """Create a RunDataPipelineOperator instance"""
        return RunDataPipelineOperator(
            task_id=TASK_ID,
            data_pipeline_name=TEST_DATA_PIPELINE_NAME,
            project_id=TEST_PROJECTID,
            location=TEST_LOCATION,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )

    @mock.patch("airflow.providers.google.cloud.operators.datapipeline.DataPipelineHook")
    def test_execute(self, data_pipeline_hook_mock, run_operator):
        """Test Run Operator execute with correct parameters"""
        run_operator.execute(mock.MagicMock())
        data_pipeline_hook_mock.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        
        data_pipeline_hook_mock.return_value.run_data_pipeline.assert_called_once_with(
            data_pipeline_name=TEST_DATA_PIPELINE_NAME,
            project_id=TEST_PROJECTID,
            location=TEST_LOCATION,
        )

    # TODO test execute errors