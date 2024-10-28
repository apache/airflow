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

from copy import deepcopy
from unittest import mock

import pytest
from google.api_core.gapic_v1.method import DEFAULT

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.cloud.aiplatform_v1 import JobState

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.batch_prediction_job import (
    BatchPredictionJobAsyncHook,
    BatchPredictionJobHook,
)

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_BATCH_PREDICTION_JOB: dict = {}
TEST_MODEL_NAME = (
    f"projects/{TEST_PROJECT_ID}/locations/{TEST_REGION}/models/test_model_id"
)
TEST_JOB_DISPLAY_NAME = "temp_create_batch_prediction_job_test"
TEST_BATCH_PREDICTION_JOB_ID = "test_batch_prediction_job_id"
TEST_UPDATE_MASK: dict = {}
TEST_INSTANCES_FORMAT = "jsonl"
TEST_PREDICTION_FORMAT = "jsonl"
TEST_CREATE_BATCH_PREDICTION_JOB_PARAMETERS = dict(
    project_id=TEST_PROJECT_ID,
    region=TEST_REGION,
    job_display_name=TEST_JOB_DISPLAY_NAME,
    model_name=TEST_MODEL_NAME,
    instances_format="jsonl",
    predictions_format="jsonl",
    gcs_source=None,
    bigquery_source=None,
    gcs_destination_prefix=None,
    bigquery_destination_prefix=None,
    model_parameters=None,
    machine_type=None,
    accelerator_type=None,
    accelerator_count=None,
    starting_replica_count=None,
    max_replica_count=None,
    generate_explanation=False,
    explanation_metadata=None,
    explanation_parameters=None,
    labels=None,
    encryption_spec_key_name=None,
    create_request_timeout=None,
    batch_size=None,
)

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
BATCH_PREDICTION_JOB_STRING = (
    "airflow.providers.google.cloud.hooks.vertex_ai.batch_prediction_job.{}"
)


class TestBatchPredictionJobWithDefaultProjectIdHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            BatchPredictionJobHook(
                gcp_conn_id=TEST_GCP_CONN_ID, delegate_to="delegate_to"
            )

    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = BatchPredictionJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(BATCH_PREDICTION_JOB_STRING.format("BatchPredictionJob.create"))
    @mock.patch(
        BATCH_PREDICTION_JOB_STRING.format("BatchPredictionJobHook.get_credentials")
    )
    def test_create_create_batch_prediction_job(self, mock_get_credentials, mock_create):
        expected_job = mock_create.return_value
        invoke_params = deepcopy(TEST_CREATE_BATCH_PREDICTION_JOB_PARAMETERS)
        invoke_params["sync"] = True

        expected_params = deepcopy(invoke_params)
        expected_params["credentials"] = mock_get_credentials.return_value
        expected_params["project"] = expected_params.pop("project_id")
        expected_params["location"] = expected_params.pop("region")

        actual_job = self.hook.create_batch_prediction_job(**invoke_params)

        mock_get_credentials.assert_called_once()
        mock_create.assert_called_once_with(**expected_params)
        assert actual_job == expected_job

    @mock.patch(
        BATCH_PREDICTION_JOB_STRING.format(
            "BatchPredictionJobHook.get_job_service_client"
        )
    )
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

    @mock.patch(
        BATCH_PREDICTION_JOB_STRING.format(
            "BatchPredictionJobHook.get_job_service_client"
        )
    )
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

    @mock.patch(
        BATCH_PREDICTION_JOB_STRING.format(
            "BatchPredictionJobHook.get_job_service_client"
        )
    )
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
        mock_client.return_value.common_location_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION
        )


class TestBatchPredictionJobWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = BatchPredictionJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(BATCH_PREDICTION_JOB_STRING.format("BatchPredictionJob.create"))
    @mock.patch(
        BATCH_PREDICTION_JOB_STRING.format("BatchPredictionJobHook.get_credentials")
    )
    def test_create_create_batch_prediction_job(self, mock_get_credentials, mock_create):
        expected_job = mock_create.return_value
        invoke_params = deepcopy(TEST_CREATE_BATCH_PREDICTION_JOB_PARAMETERS)
        invoke_params["sync"] = True

        expected_params = deepcopy(invoke_params)
        expected_params["credentials"] = mock_get_credentials.return_value
        expected_params["project"] = expected_params.pop("project_id")
        expected_params["location"] = expected_params.pop("region")

        actual_job = self.hook.create_batch_prediction_job(**invoke_params)

        mock_get_credentials.assert_called_once()
        mock_create.assert_called_once_with(**expected_params)
        assert actual_job == expected_job

    @mock.patch(
        BATCH_PREDICTION_JOB_STRING.format(
            "BatchPredictionJobHook.get_job_service_client"
        )
    )
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

    @mock.patch(
        BATCH_PREDICTION_JOB_STRING.format(
            "BatchPredictionJobHook.get_job_service_client"
        )
    )
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

    @mock.patch(
        BATCH_PREDICTION_JOB_STRING.format(
            "BatchPredictionJobHook.get_job_service_client"
        )
    )
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
        mock_client.return_value.common_location_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION
        )


class TestBatchPredictionJobAsyncHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = BatchPredictionJobAsyncHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @pytest.mark.asyncio
    @mock.patch(
        BATCH_PREDICTION_JOB_STRING.format(
            "BatchPredictionJobAsyncHook.get_job_service_client"
        )
    )
    async def test_get_batch_prediction_job(self, mock_get_job_service_client):
        mock_client = mock.MagicMock()
        mock_get_job_service_client.side_effect = mock.AsyncMock(return_value=mock_client)
        mock_job_name = mock_client.batch_prediction_job_path.return_value
        mock_job = mock.MagicMock()
        mock_async_get_batch_prediction_job = mock.AsyncMock(return_value=mock_job)
        mock_client.get_batch_prediction_job.side_effect = (
            mock_async_get_batch_prediction_job
        )

        result = await self.hook.get_batch_prediction_job(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            job_id=TEST_BATCH_PREDICTION_JOB_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

        mock_get_job_service_client.assert_called_once_with(region=TEST_REGION)
        mock_client.batch_prediction_job_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_BATCH_PREDICTION_JOB_ID
        )
        mock_async_get_batch_prediction_job.assert_awaited_once_with(
            request={"name": mock_job_name},
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        assert result == mock_job

    @pytest.mark.parametrize(
        "state",
        [
            JobState.JOB_STATE_CANCELLED,
            JobState.JOB_STATE_FAILED,
            JobState.JOB_STATE_PAUSED,
            JobState.JOB_STATE_SUCCEEDED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch(BATCH_PREDICTION_JOB_STRING.format("asyncio.sleep"))
    async def test_wait_hyperparameter_tuning_job(self, mock_sleep, state):
        mock_job = mock.MagicMock(state=state)
        mock_async_get_batch_prediction_job = mock.AsyncMock(return_value=mock_job)
        mock_get_batch_prediction_job = mock.MagicMock(
            side_effect=mock_async_get_batch_prediction_job
        )

        await_kwargs = dict(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            job_id=TEST_BATCH_PREDICTION_JOB_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        with mock.patch.object(
            self.hook, "get_batch_prediction_job", mock_get_batch_prediction_job
        ):
            result = await self.hook.wait_batch_prediction_job(**await_kwargs)

        mock_async_get_batch_prediction_job.assert_awaited_once_with(**await_kwargs)
        mock_sleep.assert_not_awaited()
        assert result == mock_job

    @pytest.mark.parametrize(
        "state",
        [
            JobState.JOB_STATE_UNSPECIFIED,
            JobState.JOB_STATE_QUEUED,
            JobState.JOB_STATE_PENDING,
            JobState.JOB_STATE_RUNNING,
            JobState.JOB_STATE_CANCELLING,
            JobState.JOB_STATE_EXPIRED,
            JobState.JOB_STATE_UPDATING,
            JobState.JOB_STATE_PARTIALLY_SUCCEEDED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch(BATCH_PREDICTION_JOB_STRING.format("asyncio.sleep"))
    async def test_wait_batch_prediction_job_waited(self, mock_sleep, state):
        mock_job_incomplete = mock.MagicMock(state=state)
        mock_job_complete = mock.MagicMock(state=JobState.JOB_STATE_SUCCEEDED)
        mock_async_get_batch_prediction_job = mock.AsyncMock(
            side_effect=[mock_job_incomplete, mock_job_complete]
        )
        mock_get_batch_prediction_job = mock.MagicMock(
            side_effect=mock_async_get_batch_prediction_job
        )

        await_kwargs = dict(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            job_id=TEST_BATCH_PREDICTION_JOB_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

        with mock.patch.object(
            self.hook, "get_batch_prediction_job", mock_get_batch_prediction_job
        ):
            result = await self.hook.wait_batch_prediction_job(**await_kwargs)

        mock_async_get_batch_prediction_job.assert_has_awaits(
            [
                mock.call(**await_kwargs),
                mock.call(**await_kwargs),
            ]
        )
        mock_sleep.assert_awaited_once()
        assert result == mock_job_complete

    @pytest.mark.asyncio
    async def test_wait_batch_prediction_job_exception(self):
        mock_get_batch_prediction_job = mock.MagicMock(side_effect=Exception)
        with mock.patch.object(
            self.hook, "get_batch_prediction_job", mock_get_batch_prediction_job
        ):
            with pytest.raises(AirflowException):
                await self.hook.wait_batch_prediction_job(
                    project_id=TEST_PROJECT_ID,
                    location=TEST_REGION,
                    job_id=TEST_BATCH_PREDICTION_JOB_ID,
                    retry=DEFAULT,
                    timeout=None,
                    metadata=(),
                )
