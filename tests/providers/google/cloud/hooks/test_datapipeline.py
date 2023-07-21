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
from airflow.providers.apache.beam.hooks.beam import BeamHook, run_beam_command
from airflow.providers.google.cloud.hooks.datapipeline import (
    DataPipelineHook,
    DEFAULT_DATAPIPELINE_LOCATION
)

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
    """
    Module meant to test the DataPipeline Hooks
    """
    
    def setup_method(self):
        self.datapipeline_hook = DataPipelineHook(gcp_conn_id = "test_google_cloud_default")

    @mock.patch("airflow.providers.google.cloud.hooks.datapipeline.DataPipelineHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.datapipeline.build")
    def test_datapipeline_client_creation(self, mock_build, mock_authorize):
        """
        Test that get_conn is called with the correct params and 
        returns the correct API address
        """
        result = self.datapipeline_hook.get_conn()
        mock_build.assert_called_once_with(
            "datapipelines", "v1", http=mock_authorize.return_value, cache_discovery=False
        )
        assert mock_build.return_value == result

    @mock.patch("airflow.providers.google.cloud.hooks.datapipeline.DataPipelineHook.build_parent_name")
    def test_build_parent_name(self, mock_build_parent_name):
        """
        Test that build_parent_name is called with the correct params and 
        returns the correct parent string
        """
        result = self.datapipeline_hook.build_parent_name(
            project_id = TEST_PROJECTID,
            location = TEST_LOCATION,
        )
        mock_build_parent_name.assert_called_with(
            project_id = TEST_PROJECTID,
            location = TEST_LOCATION,
        )
        assert mock_build_parent_name.return_value == result

    @mock.patch("airflow.providers.google.cloud.hooks.datapipeline.DataPipelineHook.create_data_pipeline")
    @mock.patch("airflow.providers.google.cloud.hooks.datapipeline.DataPipelineHook.get_conn")
    def test_create_data_pipeline(self, datapipeline, mock_connection):
        """
        Test that request are called with the correct params
        Test that request actually requests the api
        """
        mock_locations = mock_connection.return_value.projects.return_value.locations
        mock_request = mock_locations.return_value.pipelines.return_value.create
        mock_request.return_value.execute.return_value = {"name": TEST_PARENT}

        result = self.datapipeline_hook.create_data_pipeline(
            body = TEST_BODY,
            location = TEST_LOCATION,
            project_id = TEST_PROJECTID,
        )
        
        # assert that the api is requested 
        mock_request.assert_called_once_with(
            body = TEST_BODY,
            location = TEST_LOCATION,
            project_id = TEST_PROJECTID,
        )

        assert datapipeline.return_value == result
