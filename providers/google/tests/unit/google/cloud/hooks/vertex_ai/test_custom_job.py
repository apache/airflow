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

import asyncio
from unittest import mock

import pytest
import pytest_asyncio

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.aiplatform_v1 import JobServiceAsyncClient, PipelineServiceAsyncClient

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.custom_job import (
    CustomJobAsyncHook,
    CustomJobHook,
    JobState,
    PipelineState,
    types,
)

from unit.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_IMPERSONATION_CHAIN = [
    "TEST",
    "PERSONA",
]
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_PIPELINE_JOB: dict = {}
TEST_PIPELINE_JOB_ID: str = "test-pipeline-job-id"
TEST_TRAINING_PIPELINE: dict = {}
TEST_TRAINING_PIPELINE_NAME: str = "test-training-pipeline"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
CUSTOM_JOB_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.custom_job.{}"


@pytest_asyncio.fixture
async def test_async_hook():
    return CustomJobAsyncHook(
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest_asyncio.fixture
async def pipeline_service_async_client():
    return PipelineServiceAsyncClient(
        credentials=mock.MagicMock(),
    )


@pytest_asyncio.fixture
async def job_service_async_client():
    return JobServiceAsyncClient(
        credentials=mock.MagicMock(),
    )


@pytest.fixture
def test_training_pipeline_name(pipeline_service_async_client):
    return pipeline_service_async_client.training_pipeline_path(
        project=TEST_PROJECT_ID,
        location=TEST_REGION,
        training_pipeline=TEST_PIPELINE_JOB_ID,
    )


@pytest.fixture
def test_custom_job_name(job_service_async_client):
    return job_service_async_client.custom_job_path(
        project=TEST_PROJECT_ID,
        location=TEST_REGION,
        custom_job=TEST_PIPELINE_JOB_ID,
    )


class TestCustomJobWithDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = CustomJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobHook.get_pipeline_service_client"))
    def test_cancel_training_pipeline(self, mock_client) -> None:
        self.hook.cancel_training_pipeline(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            training_pipeline=TEST_TRAINING_PIPELINE_NAME,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.cancel_training_pipeline.assert_called_once_with(
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

    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobHook.get_pipeline_service_client"))
    def test_create_training_pipeline(self, mock_client) -> None:
        self.hook.create_training_pipeline(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            training_pipeline=TEST_TRAINING_PIPELINE,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.create_training_pipeline.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                training_pipeline=TEST_TRAINING_PIPELINE,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)

    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobHook.get_pipeline_service_client"))
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

    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobHook.get_pipeline_service_client"))
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

    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobHook.get_pipeline_service_client"))
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


class TestCustomJobWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = CustomJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobHook.get_pipeline_service_client"))
    def test_cancel_training_pipeline(self, mock_client) -> None:
        self.hook.cancel_training_pipeline(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            training_pipeline=TEST_TRAINING_PIPELINE_NAME,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.cancel_training_pipeline.assert_called_once_with(
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

    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobHook.get_pipeline_service_client"))
    def test_create_training_pipeline(self, mock_client) -> None:
        self.hook.create_training_pipeline(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            training_pipeline=TEST_TRAINING_PIPELINE,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.create_training_pipeline.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                training_pipeline=TEST_TRAINING_PIPELINE,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)

    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobHook.get_pipeline_service_client"))
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

    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobHook.get_pipeline_service_client"))
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

    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobHook.get_pipeline_service_client"))
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


class TestCustomJobAsyncHook:
    @pytest.mark.asyncio
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_pipeline_service_client"))
    async def test_get_training_pipeline(
        self, mock_pipeline_service_client, test_async_hook, test_training_pipeline_name
    ):
        mock_pipeline_service_client.return_value.training_pipeline_path = mock.MagicMock(
            return_value=test_training_pipeline_name
        )
        await test_async_hook.get_training_pipeline(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            pipeline_id=TEST_PIPELINE_JOB_ID,
        )
        mock_pipeline_service_client.assert_awaited_once_with(region=TEST_REGION)
        mock_pipeline_service_client.return_value.get_training_pipeline.assert_awaited_once_with(
            request={"name": test_training_pipeline_name},
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

    @pytest.mark.asyncio
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_job_service_client"))
    async def test_get_custom_job(
        self,
        mock_get_job_service_client,
        test_async_hook,
        test_custom_job_name,
    ):
        mock_get_job_service_client.return_value.custom_job_path = mock.MagicMock(
            return_value=test_custom_job_name
        )
        await test_async_hook.get_custom_job(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            job_id=TEST_PIPELINE_JOB_ID,
        )
        mock_get_job_service_client.assert_awaited_once_with(region=TEST_REGION)
        mock_get_job_service_client.return_value.get_custom_job.assert_awaited_once_with(
            request={"name": test_custom_job_name},
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=(),
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_CANCELLED,
            PipelineState.PIPELINE_STATE_FAILED,
            PipelineState.PIPELINE_STATE_PAUSED,
            PipelineState.PIPELINE_STATE_SUCCEEDED,
        ],
    )
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_pipeline_service_client"))
    async def test_wait_for_training_pipeline_returns_pipeline_if_in_complete_state(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        pipeline_state_value,
        test_async_hook,
        test_training_pipeline_name,
    ):
        expected_obj = types.TrainingPipeline(
            state=pipeline_state_value,
            name=test_training_pipeline_name,
        )
        mock_get_training_pipeline.return_value = expected_obj
        actual_obj = await test_async_hook.wait_for_training_pipeline(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            pipeline_id=TEST_PIPELINE_JOB_ID,
        )
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_REGION)
        assert actual_obj == expected_obj

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "job_state_value",
        [
            JobState.JOB_STATE_CANCELLED,
            JobState.JOB_STATE_FAILED,
            JobState.JOB_STATE_PAUSED,
            JobState.JOB_STATE_SUCCEEDED,
        ],
    )
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_custom_job"))
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_job_service_client"))
    async def test_wait_for_custom_job_returns_job_if_in_complete_state(
        self,
        mock_get_job_service_client,
        mock_get_custom_job,
        job_state_value,
        test_async_hook,
        test_custom_job_name,
    ):
        expected_obj = types.CustomJob(
            state=job_state_value,
            name=test_custom_job_name,
        )
        mock_get_custom_job.return_value = expected_obj
        actual_obj = await test_async_hook.wait_for_custom_job(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            job_id=TEST_PIPELINE_JOB_ID,
        )
        mock_get_job_service_client.assert_awaited_once_with(region=TEST_REGION)
        assert actual_obj == expected_obj

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_CANCELLING,
            PipelineState.PIPELINE_STATE_PENDING,
            PipelineState.PIPELINE_STATE_QUEUED,
            PipelineState.PIPELINE_STATE_RUNNING,
            PipelineState.PIPELINE_STATE_UNSPECIFIED,
        ],
    )
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_pipeline_service_client"))
    async def test_wait_for_training_pipeline_loop_is_still_running_if_in_incomplete_state(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        pipeline_state_value,
        test_async_hook,
    ):
        mock_get_training_pipeline.return_value = types.TrainingPipeline(state=pipeline_state_value)
        task = asyncio.create_task(
            test_async_hook.wait_for_training_pipeline(
                project_id=TEST_PROJECT_ID,
                location=TEST_REGION,
                pipeline_id=TEST_PIPELINE_JOB_ID,
            )
        )
        await asyncio.sleep(0.5)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_REGION)
        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "job_state_value",
        [
            JobState.JOB_STATE_CANCELLING,
            JobState.JOB_STATE_PENDING,
            JobState.JOB_STATE_QUEUED,
            JobState.JOB_STATE_RUNNING,
            JobState.JOB_STATE_UNSPECIFIED,
        ],
    )
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_custom_job"))
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_job_service_client"))
    async def test_wait_for_custom_job_loop_is_still_running_if_in_incomplete_state(
        self,
        mock_get_job_service_client,
        mock_get_custom_job,
        job_state_value,
        test_async_hook,
    ):
        mock_get_custom_job.return_value = types.CustomJob(state=job_state_value)
        task = asyncio.create_task(
            test_async_hook.wait_for_custom_job(
                project_id=TEST_PROJECT_ID,
                location=TEST_REGION,
                job_id=TEST_PIPELINE_JOB_ID,
            )
        )
        await asyncio.sleep(0.5)
        mock_get_job_service_client.assert_awaited_once_with(region=TEST_REGION)
        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_credentials"))
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_training_pipeline"))
    async def test_wait_for_training_pipeline_raises_exception(
        self, mock_get_training_pipeline, mock_get_credentials, test_async_hook
    ):
        mock_get_training_pipeline.side_effect = mock.AsyncMock(side_effect=Exception())
        mock_get_credentials.return_value = mock.AsyncMock()
        with pytest.raises(AirflowException):
            await test_async_hook.wait_for_training_pipeline(
                project_id=TEST_PROJECT_ID,
                location=TEST_REGION,
                pipeline_id=TEST_PIPELINE_JOB_ID,
            )

    @pytest.mark.asyncio
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_credentials"))
    @mock.patch(CUSTOM_JOB_STRING.format("CustomJobAsyncHook.get_custom_job"))
    async def test_wait_for_custom_job_raises_exception(
        self, mock_get_custom_job, mock_get_credentials, test_async_hook
    ):
        mock_get_custom_job.side_effect = mock.AsyncMock(side_effect=Exception())
        mock_get_credentials.return_value = mock.AsyncMock()
        with pytest.raises(AirflowException):
            await test_async_hook.wait_for_custom_job(
                project_id=TEST_PROJECT_ID,
                location=TEST_REGION,
                job_id=TEST_PIPELINE_JOB_ID,
            )
