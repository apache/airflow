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
from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.hooks.vertex_ai.hyperparameter_tuning_job import (
    HyperparameterTuningJobHook,
)
from tests.providers.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_HYPERPARAMETER_TUNING_JOB_ID = "test_hyperparameter_tuning_job_id"
TEST_UPDATE_MASK: dict = {}

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
HYPERPARAMETER_TUNING_JOB_STRING = (
    "airflow.providers.google.cloud.hooks.vertex_ai.hyperparameter_tuning_job.{}"
)


class TestHyperparameterTuningJobWithDefaultProjectIdHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            HyperparameterTuningJobHook(gcp_conn_id=TEST_GCP_CONN_ID, delegate_to="delegate_to")

    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = HyperparameterTuningJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(HYPERPARAMETER_TUNING_JOB_STRING.format("HyperparameterTuningJobHook.get_job_service_client"))
    def test_delete_hyperparameter_tuning_job(self, mock_client) -> None:
        self.hook.delete_hyperparameter_tuning_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            hyperparameter_tuning_job=TEST_HYPERPARAMETER_TUNING_JOB_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_hyperparameter_tuning_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.hyperparameter_tuning_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.hyperparameter_tuning_job_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_REGION,
            TEST_HYPERPARAMETER_TUNING_JOB_ID,
        )

    @mock.patch(HYPERPARAMETER_TUNING_JOB_STRING.format("HyperparameterTuningJobHook.get_job_service_client"))
    def test_get_hyperparameter_tuning_job(self, mock_client) -> None:
        self.hook.get_hyperparameter_tuning_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            hyperparameter_tuning_job=TEST_HYPERPARAMETER_TUNING_JOB_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_hyperparameter_tuning_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.hyperparameter_tuning_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.hyperparameter_tuning_job_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_REGION,
            TEST_HYPERPARAMETER_TUNING_JOB_ID,
        )

    @mock.patch(HYPERPARAMETER_TUNING_JOB_STRING.format("HyperparameterTuningJobHook.get_job_service_client"))
    def test_list_hyperparameter_tuning_jobs(self, mock_client) -> None:
        self.hook.list_hyperparameter_tuning_jobs(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_hyperparameter_tuning_jobs.assert_called_once_with(
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


class TestHyperparameterTuningJobWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = HyperparameterTuningJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(HYPERPARAMETER_TUNING_JOB_STRING.format("HyperparameterTuningJobHook.get_job_service_client"))
    def test_delete_hyperparameter_tuning_job(self, mock_client) -> None:
        self.hook.delete_hyperparameter_tuning_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            hyperparameter_tuning_job=TEST_HYPERPARAMETER_TUNING_JOB_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_hyperparameter_tuning_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.hyperparameter_tuning_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.hyperparameter_tuning_job_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_REGION,
            TEST_HYPERPARAMETER_TUNING_JOB_ID,
        )

    @mock.patch(HYPERPARAMETER_TUNING_JOB_STRING.format("HyperparameterTuningJobHook.get_job_service_client"))
    def test_get_hyperparameter_tuning_job(self, mock_client) -> None:
        self.hook.get_hyperparameter_tuning_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            hyperparameter_tuning_job=TEST_HYPERPARAMETER_TUNING_JOB_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_hyperparameter_tuning_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.hyperparameter_tuning_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.hyperparameter_tuning_job_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_REGION,
            TEST_HYPERPARAMETER_TUNING_JOB_ID,
        )

    @mock.patch(HYPERPARAMETER_TUNING_JOB_STRING.format("HyperparameterTuningJobHook.get_job_service_client"))
    def test_list_hyperparameter_tuning_jobs(self, mock_client) -> None:
        self.hook.list_hyperparameter_tuning_jobs(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_hyperparameter_tuning_jobs.assert_called_once_with(
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
