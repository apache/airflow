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

from unittest import TestCase, mock

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.hooks.vertex_ai.auto_ml import AutoMLHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_PIPELINE_JOB: dict = {}
TEST_PIPELINE_JOB_ID: str = "test-pipeline-job-id"
TEST_TRAINING_PIPELINE: dict = {}
TEST_TRAINING_PIPELINE_NAME: str = "test-training-pipeline"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
CUSTOM_JOB_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.auto_ml.{}"


class TestAutoMLWithDefaultProjectIdHook(TestCase):
    def setUp(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = AutoMLHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(CUSTOM_JOB_STRING.format("AutoMLHook.get_pipeline_service_client"))
    def test_delete_training_pipeline(self, mock_client) -> None:
        self.hook.delete_training_pipeline(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            training_pipeline=TEST_TRAINING_PIPELINE_NAME,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_training_pipeline.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.training_pipeline_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.training_pipeline_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_TRAINING_PIPELINE_NAME
        )

    @mock.patch(CUSTOM_JOB_STRING.format("AutoMLHook.get_pipeline_service_client"))
    def test_get_training_pipeline(self, mock_client) -> None:
        self.hook.get_training_pipeline(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            training_pipeline=TEST_TRAINING_PIPELINE_NAME,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_training_pipeline.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.training_pipeline_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.training_pipeline_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_TRAINING_PIPELINE_NAME
        )

    @mock.patch(CUSTOM_JOB_STRING.format("AutoMLHook.get_pipeline_service_client"))
    def test_list_training_pipelines(self, mock_client) -> None:
        self.hook.list_training_pipelines(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_training_pipelines.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                page_size=None,
                page_token=None,
                filter=None,
                read_mask=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)


class TestAutoMLWithoutDefaultProjectIdHook(TestCase):
    def setUp(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = AutoMLHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(CUSTOM_JOB_STRING.format("AutoMLHook.get_pipeline_service_client"))
    def test_delete_training_pipeline(self, mock_client) -> None:
        self.hook.delete_training_pipeline(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            training_pipeline=TEST_TRAINING_PIPELINE_NAME,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_training_pipeline.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.training_pipeline_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.training_pipeline_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_TRAINING_PIPELINE_NAME
        )

    @mock.patch(CUSTOM_JOB_STRING.format("AutoMLHook.get_pipeline_service_client"))
    def test_get_training_pipeline(self, mock_client) -> None:
        self.hook.get_training_pipeline(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            training_pipeline=TEST_TRAINING_PIPELINE_NAME,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_training_pipeline.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.training_pipeline_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.training_pipeline_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_TRAINING_PIPELINE_NAME
        )

    @mock.patch(CUSTOM_JOB_STRING.format("AutoMLHook.get_pipeline_service_client"))
    def test_list_training_pipelines(self, mock_client) -> None:
        self.hook.list_training_pipelines(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_training_pipelines.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                page_size=None,
                page_token=None,
                filter=None,
                read_mask=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)
