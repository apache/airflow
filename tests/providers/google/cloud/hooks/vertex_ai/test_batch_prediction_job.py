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

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.hooks.vertex_ai.batch_prediction_job import BatchPredictionJobHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_BATCH_PREDICTION_JOB: dict = {}
TEST_MODEL_NAME = f"projects/{TEST_PROJECT_ID}/locations/{TEST_REGION}/models/test_model_id"
TEST_JOB_DISPLAY_NAME = "temp_create_batch_prediction_job_test"
TEST_BATCH_PREDICTION_JOB_ID = "test_batch_prediction_job_id"
TEST_UPDATE_MASK: dict = {}

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
BATCH_PREDICTION_JOB_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.batch_prediction_job.{}"


class TestBatchPredictionJobWithDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = BatchPredictionJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(BATCH_PREDICTION_JOB_STRING.format("BatchPredictionJobHook.get_job_service_client"))
    def test_delete_batch_prediction_job(self, mock_client) -> None:
        self.hook.delete_batch_prediction_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            batch_prediction_job=TEST_BATCH_PREDICTION_JOB,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_batch_prediction_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.batch_prediction_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.batch_prediction_job_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_REGION,
            TEST_BATCH_PREDICTION_JOB,
        )

    @mock.patch(BATCH_PREDICTION_JOB_STRING.format("BatchPredictionJobHook.get_job_service_client"))
    def test_get_batch_prediction_job(self, mock_client) -> None:
        self.hook.get_batch_prediction_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            batch_prediction_job=TEST_BATCH_PREDICTION_JOB,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_batch_prediction_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.batch_prediction_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.batch_prediction_job_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_REGION,
            TEST_BATCH_PREDICTION_JOB,
        )

    @mock.patch(BATCH_PREDICTION_JOB_STRING.format("BatchPredictionJobHook.get_job_service_client"))
    def test_list_batch_prediction_jobs(self, mock_client) -> None:
        self.hook.list_batch_prediction_jobs(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_batch_prediction_jobs.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                filter=None,
                page_size=None,
                page_token=None,
                read_mask=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)


class TestBatchPredictionJobWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = BatchPredictionJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(BATCH_PREDICTION_JOB_STRING.format("BatchPredictionJobHook.get_job_service_client"))
    def test_delete_batch_prediction_job(self, mock_client) -> None:
        self.hook.delete_batch_prediction_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            batch_prediction_job=TEST_BATCH_PREDICTION_JOB,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_batch_prediction_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.batch_prediction_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.batch_prediction_job_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_REGION,
            TEST_BATCH_PREDICTION_JOB,
        )

    @mock.patch(BATCH_PREDICTION_JOB_STRING.format("BatchPredictionJobHook.get_job_service_client"))
    def test_get_batch_prediction_job(self, mock_client) -> None:
        self.hook.get_batch_prediction_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            batch_prediction_job=TEST_BATCH_PREDICTION_JOB,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_batch_prediction_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.batch_prediction_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.batch_prediction_job_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_REGION,
            TEST_BATCH_PREDICTION_JOB,
        )

    @mock.patch(BATCH_PREDICTION_JOB_STRING.format("BatchPredictionJobHook.get_job_service_client"))
    def test_list_batch_prediction_jobs(self, mock_client) -> None:
        self.hook.list_batch_prediction_jobs(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_batch_prediction_jobs.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                filter=None,
                page_size=None,
                page_token=None,
                read_mask=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)
