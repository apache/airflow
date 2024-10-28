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

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.api_core.gapic_v1.method import DEFAULT

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.pipeline_job import (
    PipelineJobAsyncHook,
    PipelineJobHook,
    PipelineState,
    types,
)

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_IMPERSONATION_CHAIN = [
    "IMPERSONATE",
    "THIS",
]
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_PIPELINE_JOB: dict = {}
TEST_PIPELINE_JOB_ID = "test_pipeline_job_id"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
PIPELINE_JOB_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.pipeline_job.{}"


@pytest.fixture
def test_async_hook():
    return PipelineJobAsyncHook(
        gcp_conn_id=TEST_GCP_CONN_ID,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def test_pipeline_job_name():
    return f"projects/{TEST_PROJECT_ID}/locations/{TEST_REGION}/pipelineJobs/{TEST_PIPELINE_JOB_ID}"


class TestPipelineJobWithDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = PipelineJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobHook.get_pipeline_service_client"))
    def test_create_pipeline_job(self, mock_client) -> None:
        self.hook.create_pipeline_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            pipeline_job=TEST_PIPELINE_JOB,
            pipeline_job_id=TEST_PIPELINE_JOB_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.create_pipeline_job.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                pipeline_job=TEST_PIPELINE_JOB,
                pipeline_job_id=TEST_PIPELINE_JOB_ID,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION
        )

    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobHook.get_pipeline_service_client"))
    def test_delete_pipeline_job(self, mock_client) -> None:
        self.hook.delete_pipeline_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            pipeline_job_id=TEST_PIPELINE_JOB_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_pipeline_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.pipeline_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.pipeline_job_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_PIPELINE_JOB_ID
        )

    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobHook.get_pipeline_service_client"))
    def test_get_pipeline_job(self, mock_client) -> None:
        self.hook.get_pipeline_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            pipeline_job_id=TEST_PIPELINE_JOB_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_pipeline_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.pipeline_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.pipeline_job_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_PIPELINE_JOB_ID
        )

    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobHook.get_pipeline_service_client"))
    def test_list_pipeline_jobs(self, mock_client) -> None:
        self.hook.list_pipeline_jobs(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_pipeline_jobs.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                page_size=None,
                page_token=None,
                filter=None,
                order_by=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION
        )


class TestPipelineJobWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = PipelineJobHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobHook.get_pipeline_service_client"))
    def test_create_pipeline_job(self, mock_client) -> None:
        self.hook.create_pipeline_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            pipeline_job=TEST_PIPELINE_JOB,
            pipeline_job_id=TEST_PIPELINE_JOB_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.create_pipeline_job.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                pipeline_job=TEST_PIPELINE_JOB,
                pipeline_job_id=TEST_PIPELINE_JOB_ID,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION
        )

    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobHook.get_pipeline_service_client"))
    def test_delete_pipeline_job(self, mock_client) -> None:
        self.hook.delete_pipeline_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            pipeline_job_id=TEST_PIPELINE_JOB_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_pipeline_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.pipeline_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.pipeline_job_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_PIPELINE_JOB_ID
        )

    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobHook.get_pipeline_service_client"))
    def test_get_pipeline_job(self, mock_client) -> None:
        self.hook.get_pipeline_job(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            pipeline_job_id=TEST_PIPELINE_JOB_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_pipeline_job.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.pipeline_job_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.pipeline_job_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_PIPELINE_JOB_ID
        )

    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobHook.get_pipeline_service_client"))
    def test_list_pipeline_jobs(self, mock_client) -> None:
        self.hook.list_pipeline_jobs(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_pipeline_jobs.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                page_size=None,
                page_token=None,
                filter=None,
                order_by=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION
        )


class TestPipelineJobAsyncHook:
    @pytest.mark.asyncio
    @mock.patch(
        PIPELINE_JOB_STRING.format("PipelineJobAsyncHook.get_pipeline_service_client")
    )
    async def test_get_pipeline_job(
        self, mock_pipeline_service_client, test_async_hook, test_pipeline_job_name
    ):
        mock_pipeline_service_client.return_value.pipeline_job_path = mock.MagicMock(
            return_value=test_pipeline_job_name
        )
        await test_async_hook.get_pipeline_job(
            project_id=TEST_PROJECT_ID, location=TEST_REGION, job_id=TEST_PIPELINE_JOB_ID
        )
        mock_pipeline_service_client.assert_awaited_once_with(region=TEST_REGION)
        mock_pipeline_service_client.return_value.get_pipeline_job.assert_awaited_once_with(
            request={"name": test_pipeline_job_name},
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
    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobAsyncHook.get_pipeline_job"))
    async def test_wait_for_pipeline_job_returns_job_if_pipeline_in_complete_state(
        self,
        mock_get_pipeline_job,
        pipeline_state_value,
        test_async_hook,
        test_pipeline_job_name,
    ):
        expected_job = types.PipelineJob(
            state=pipeline_state_value,
            name=test_pipeline_job_name,
        )
        mock_get_pipeline_job.return_value = expected_job
        actual_job = await test_async_hook.wait_for_pipeline_job(
            project_id=TEST_PROJECT_ID,
            location=TEST_REGION,
            job_id=TEST_PIPELINE_JOB_ID,
        )
        assert actual_job == expected_job

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
    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobAsyncHook.get_pipeline_job"))
    async def test_wait_for_pipeline_job_loop_is_still_running_if_pipeline_in_incomplete_state(
        self,
        mock_get_pipeline_job,
        pipeline_state_value,
        test_async_hook,
    ):
        mock_get_pipeline_job.return_value = types.PipelineJob(state=pipeline_state_value)
        task = asyncio.create_task(
            test_async_hook.wait_for_pipeline_job(
                project_id=TEST_PROJECT_ID,
                location=TEST_REGION,
                job_id=TEST_PIPELINE_JOB_ID,
            )
        )
        await asyncio.sleep(0.5)
        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch(PIPELINE_JOB_STRING.format("PipelineJobAsyncHook.get_pipeline_job"))
    async def test_wait_for_pipeline_job_raises_exception(
        self, mock_get_pipeline_job, test_async_hook
    ):
        mock_get_pipeline_job.side_effect = mock.AsyncMock(side_effect=Exception())
        with pytest.raises(AirflowException):
            await test_async_hook.wait_for_pipeline_job(
                project_id=TEST_PROJECT_ID,
                location=TEST_REGION,
                job_id=TEST_PIPELINE_JOB_ID,
            )
