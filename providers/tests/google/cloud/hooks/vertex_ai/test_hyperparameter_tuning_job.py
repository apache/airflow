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

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud import aiplatform

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.cloud.aiplatform_v1 import JobState

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.hyperparameter_tuning_job import (
    HyperparameterTuningJobAsyncHook,
    HyperparameterTuningJobHook,
)

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_HYPERPARAMETER_TUNING_JOB_ID = "test_hyperparameter_tuning_job_id"
TEST_UPDATE_MASK: dict = {}
TEST_DISPLAY_NAME = "Test display name"
TEST_METRIC_SPECS = {
    "accuracy": "maximize",
}
TEST_PARAM_SPECS = {
    "learning_rate": aiplatform.hyperparameter_tuning.DoubleParameterSpec(min=0.01, max=1, scale="log"),
}
TEST_MAX_TRIAL_COUNT = 3
TEST_PARALLEL_TRIAL_COUNT = 3
# CustomJob param
TEST_WORKER_POOL_SPECS = [
    {
        "machine_spec": {
            "machine_type": "n1-standard-4",
            "accelerator_type": "ACCELERATOR_TYPE_UNSPECIFIED",
            "accelerator_count": 0,
        },
    }
]
TEST_BASE_OUTPUT_DIR = None
TEST_CUSTOM_JOB_LABELS = None
TEST_CUSTOM_JOB_ENCRYPTION_SPEC_KEY_NAME = None
TEST_STAGING_BUCKET = None
# CustomJob param
TEST_MAX_FAILED_TRIAL_COUNT = 0
TEST_SEARCH_ALGORITHM = None
TEST_MEASUREMENT_SELECTION = "best"
TEST_HYPERPARAMETER_TUNING_JOB_LABELS = None
TEST_HYPERPARAMETER_TUNING_JOB_ENCRYPTION_SPEC_KEY_NAME = None
# run param
TEST_SERVICE_ACCOUNT = None
TEST_NETWORK = None
TEST_TIMEOUT = None
TEST_RESTART_JOB_ON_WORKER_RESTART = False
TEST_ENABLE_WEB_ACCESS = False
TEST_TENSORBOARD = None
TEST_SYNC = True

TEST_WAIT_JOB_COMPLETED = True
TEST_CREATE_HYPERPARAMETER_TUNING_JOB_PARAMS = dict(
    project_id=TEST_PROJECT_ID,
    region=TEST_REGION,
    display_name=TEST_DISPLAY_NAME,
    metric_spec=TEST_METRIC_SPECS,
    parameter_spec=TEST_PARAM_SPECS,
    max_trial_count=TEST_MAX_TRIAL_COUNT,
    parallel_trial_count=TEST_PARALLEL_TRIAL_COUNT,
    # START: CustomJob param
    worker_pool_specs=TEST_WORKER_POOL_SPECS,
    base_output_dir=TEST_BASE_OUTPUT_DIR,
    custom_job_labels=TEST_CUSTOM_JOB_LABELS,
    custom_job_encryption_spec_key_name=TEST_CUSTOM_JOB_ENCRYPTION_SPEC_KEY_NAME,
    staging_bucket=TEST_STAGING_BUCKET,
    # END: CustomJob param
    max_failed_trial_count=TEST_MAX_FAILED_TRIAL_COUNT,
    search_algorithm=TEST_SEARCH_ALGORITHM,
    measurement_selection=TEST_MEASUREMENT_SELECTION,
    hyperparameter_tuning_job_labels=TEST_HYPERPARAMETER_TUNING_JOB_LABELS,
    hyperparameter_tuning_job_encryption_spec_key_name=TEST_HYPERPARAMETER_TUNING_JOB_ENCRYPTION_SPEC_KEY_NAME,
    # START: run param
    service_account=TEST_SERVICE_ACCOUNT,
    network=TEST_NETWORK,
    timeout=TEST_TIMEOUT,
    restart_job_on_worker_restart=TEST_RESTART_JOB_ON_WORKER_RESTART,
    enable_web_access=TEST_ENABLE_WEB_ACCESS,
    tensorboard=TEST_TENSORBOARD,
    sync=TEST_SYNC,
    # END: run param
    wait_job_completed=TEST_WAIT_JOB_COMPLETED,
)
BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
HYPERPARAMETER_TUNING_JOB_HOOK_PATH = (
    "airflow.providers.google.cloud.hooks.vertex_ai.hyperparameter_tuning_job.{}"
)
HYPERPARAMETER_TUNING_JOB_HOOK_STRING = (
    HYPERPARAMETER_TUNING_JOB_HOOK_PATH.format("HyperparameterTuningJobHook") + ".{}"
)
HYPERPARAMETER_TUNING_JOB_ASYNC_HOOK_STRING = (
    HYPERPARAMETER_TUNING_JOB_HOOK_PATH.format("HyperparameterTuningJobAsyncHook") + ".{}"
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

    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_STRING.format("get_job_service_client"))
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

    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_STRING.format("get_job_service_client"))
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

    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_STRING.format("get_job_service_client"))
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

    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_STRING.format("get_job_service_client"))
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

    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_STRING.format("get_job_service_client"))
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

    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_STRING.format("get_job_service_client"))
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


class TestHyperparameterTuningJobHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = HyperparameterTuningJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_STRING.format("get_hyperparameter_tuning_job_object"))
    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_STRING.format("get_custom_job_object"))
    def test_create_hyperparameter_tuning_job(
        self,
        mock_get_custom_job_object,
        mock_get_hyperparameter_tuning_job_object,
    ):
        mock_custom_job = mock_get_custom_job_object.return_value

        result = self.hook.create_hyperparameter_tuning_job(**TEST_CREATE_HYPERPARAMETER_TUNING_JOB_PARAMS)

        mock_get_custom_job_object.assert_called_once_with(
            project=TEST_PROJECT_ID,
            location=TEST_REGION,
            display_name=TEST_DISPLAY_NAME,
            worker_pool_specs=TEST_WORKER_POOL_SPECS,
            base_output_dir=TEST_BASE_OUTPUT_DIR,
            labels=TEST_CUSTOM_JOB_LABELS,
            encryption_spec_key_name=TEST_CUSTOM_JOB_ENCRYPTION_SPEC_KEY_NAME,
            staging_bucket=TEST_STAGING_BUCKET,
        )
        mock_get_hyperparameter_tuning_job_object.assert_called_once_with(
            project=TEST_PROJECT_ID,
            location=TEST_REGION,
            display_name=TEST_DISPLAY_NAME,
            custom_job=mock_custom_job,
            metric_spec=TEST_METRIC_SPECS,
            parameter_spec=TEST_PARAM_SPECS,
            max_trial_count=TEST_MAX_TRIAL_COUNT,
            parallel_trial_count=TEST_PARALLEL_TRIAL_COUNT,
            max_failed_trial_count=TEST_MAX_FAILED_TRIAL_COUNT,
            search_algorithm=TEST_SEARCH_ALGORITHM,
            measurement_selection=TEST_MEASUREMENT_SELECTION,
            labels=TEST_HYPERPARAMETER_TUNING_JOB_LABELS,
            encryption_spec_key_name=TEST_HYPERPARAMETER_TUNING_JOB_ENCRYPTION_SPEC_KEY_NAME,
        )
        self.hook._hyperparameter_tuning_job.run.assert_called_once_with(
            service_account=TEST_SERVICE_ACCOUNT,
            network=TEST_NETWORK,
            timeout=TEST_TIMEOUT,
            restart_job_on_worker_restart=TEST_RESTART_JOB_ON_WORKER_RESTART,
            enable_web_access=TEST_ENABLE_WEB_ACCESS,
            tensorboard=TEST_TENSORBOARD,
            sync=TEST_SYNC,
        )
        self.hook._hyperparameter_tuning_job.wait.assert_called_once()
        self.hook._hyperparameter_tuning_job._wait_for_resource_creation.assert_not_called()
        assert result == self.hook._hyperparameter_tuning_job

    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_STRING.format("get_hyperparameter_tuning_job_object"))
    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_STRING.format("get_custom_job_object"))
    def test_create_hyperparameter_tuning_job_no_wait(self, _, __):
        params = dict(TEST_CREATE_HYPERPARAMETER_TUNING_JOB_PARAMS)
        params["wait_job_completed"] = False

        result = self.hook.create_hyperparameter_tuning_job(**params)

        self.hook._hyperparameter_tuning_job.wait.assert_not_called()
        self.hook._hyperparameter_tuning_job._wait_for_resource_creation.assert_called_once()
        assert result == self.hook._hyperparameter_tuning_job


class TestHyperparameterTuningJobAsyncHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = HyperparameterTuningJobAsyncHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @pytest.mark.asyncio
    @mock.patch(HYPERPARAMETER_TUNING_JOB_ASYNC_HOOK_STRING.format("get_job_service_client"))
    async def test_get_hyperparameter_tuning_job(self, mock_get_job_service_client):
        mock_client = mock.MagicMock()
        mock_get_job_service_client.side_effect = mock.AsyncMock(return_value=mock_client)
        mock_job_name = mock_client.hyperparameter_tuning_job_path.return_value
        mock_job = mock.MagicMock()
        mock_async_get_hyperparameter_tuning_job = mock.AsyncMock(return_value=mock_job)
        mock_client.get_hyperparameter_tuning_job.side_effect = mock_async_get_hyperparameter_tuning_job

        result = await self.hook.get_hyperparameter_tuning_job(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            job_id=TEST_HYPERPARAMETER_TUNING_JOB_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

        mock_get_job_service_client.assert_called_once_with(region=TEST_REGION)
        mock_client.hyperparameter_tuning_job_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_HYPERPARAMETER_TUNING_JOB_ID
        )
        mock_async_get_hyperparameter_tuning_job.assert_awaited_once_with(
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
    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_PATH.format("asyncio.sleep"))
    async def test_wait_hyperparameter_tuning_job(self, mock_sleep, state):
        mock_job = mock.MagicMock(state=state)
        mock_async_get_hpt_job = mock.AsyncMock(return_value=mock_job)
        mock_get_hpt_job = mock.MagicMock(side_effect=mock_async_get_hpt_job)

        await_kwargs = dict(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            job_id=TEST_HYPERPARAMETER_TUNING_JOB_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
        with mock.patch.object(self.hook, "get_hyperparameter_tuning_job", mock_get_hpt_job):
            result = await self.hook.wait_hyperparameter_tuning_job(**await_kwargs)

        mock_async_get_hpt_job.assert_awaited_once_with(**await_kwargs)
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
    @mock.patch(HYPERPARAMETER_TUNING_JOB_HOOK_PATH.format("asyncio.sleep"))
    async def test_wait_hyperparameter_tuning_job_waited(self, mock_sleep, state):
        mock_job_incomplete = mock.MagicMock(state=state)
        mock_job_complete = mock.MagicMock(state=JobState.JOB_STATE_SUCCEEDED)
        mock_async_get_ht_job = mock.AsyncMock(side_effect=[mock_job_incomplete, mock_job_complete])
        mock_get_ht_job = mock.MagicMock(side_effect=mock_async_get_ht_job)

        await_kwargs = dict(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            job_id=TEST_HYPERPARAMETER_TUNING_JOB_ID,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

        with mock.patch.object(self.hook, "get_hyperparameter_tuning_job", mock_get_ht_job):
            result = await self.hook.wait_hyperparameter_tuning_job(**await_kwargs)

        mock_async_get_ht_job.assert_has_awaits(
            [
                mock.call(**await_kwargs),
                mock.call(**await_kwargs),
            ]
        )
        mock_sleep.assert_awaited_once()
        assert result == mock_job_complete

    @pytest.mark.asyncio
    async def test_wait_hyperparameter_tuning_job_exception(self):
        mock_get_ht_job = mock.MagicMock(side_effect=Exception)
        with mock.patch.object(self.hook, "get_hyperparameter_tuning_job", mock_get_ht_job):
            with pytest.raises(AirflowException):
                await self.hook.wait_hyperparameter_tuning_job(
                    project_id=TEST_PROJECT_ID,
                    location=TEST_REGION,
                    job_id=TEST_HYPERPARAMETER_TUNING_JOB_ID,
                    retry=DEFAULT,
                    timeout=None,
                    metadata=(),
                )
