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
from google.cloud.batch import ListJobsRequest
from google.cloud.batch_v1 import CreateJobRequest, Job, JobStatus

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.cloud_batch import CloudBatchAsyncHook, CloudBatchHook

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

pytestmark = pytest.mark.db_test


class TestCloudBathHook:
    def dummy_get_credentials(self):
        pass

    @pytest.fixture
    def cloud_batch_hook(self):
        cloud_batch_hook = CloudBatchHook()
        cloud_batch_hook.get_credentials = self.dummy_get_credentials
        return cloud_batch_hook

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_submit(self, mock_batch_service_client, cloud_batch_hook):
        job = Job()
        job_name = "jobname"
        project_id = "test_project_id"
        region = "us-central1"

        cloud_batch_hook.submit_batch_job(
            job_name=job_name, job=Job.to_dict(job), region=region, project_id=project_id
        )

        create_request = CreateJobRequest()
        create_request.job = job
        create_request.job_id = job_name
        create_request.parent = f"projects/{project_id}/locations/{region}"

        cloud_batch_hook._client.create_job.assert_called_with(create_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_get_job(self, mock_batch_service_client, cloud_batch_hook):
        cloud_batch_hook.get_job("job1")
        cloud_batch_hook._client.get_job.assert_called_once_with(name="job1")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_delete_job(self, mock_batch_service_client, cloud_batch_hook):
        job_name = "job1"
        region = "us-east1"
        project_id = "test_project_id"
        cloud_batch_hook.delete_job(job_name=job_name, region=region, project_id=project_id)
        cloud_batch_hook._client.delete_job.assert_called_once_with(
            name=f"projects/{project_id}/locations/{region}/jobs/{job_name}"
        )

    @pytest.mark.parametrize("state", [JobStatus.State.SUCCEEDED])
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_wait_job_succeeded(self, mock_batch_service_client, state, cloud_batch_hook):
        mock_job = self._mock_job_with_status(state)
        mock_batch_service_client.return_value.get_job.return_value = mock_job
        actual_job = cloud_batch_hook.wait_for_job("job1")
        assert actual_job == mock_job

    @pytest.mark.parametrize("state", [JobStatus.State.FAILED, JobStatus.State.DELETION_IN_PROGRESS])
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_wait_job_does_not_succeed(self, mock_batch_service_client, state, cloud_batch_hook):
        mock_job = self._mock_job_with_status(state)
        mock_batch_service_client.return_value.get_job.return_value = mock_job
        with pytest.raises(AirflowException):
            cloud_batch_hook.wait_for_job("job1")

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_wait_job_timeout(self, mock_batch_service_client, cloud_batch_hook):
        mock_job = self._mock_job_with_status(JobStatus.State.RUNNING)
        mock_batch_service_client.return_value.get_job.return_value = mock_job

        exception_caught = False
        try:
            cloud_batch_hook.wait_for_job("job1", polling_period_seconds=0.01, timeout=0.02)
        except AirflowException:
            exception_caught = True

        assert exception_caught

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_list_jobs(self, mock_batch_service_client, cloud_batch_hook):
        number_of_jobs = 3
        region = "us-central1"
        project_id = "test_project_id"
        filter = "filter_description"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        jobs_list = cloud_batch_hook.list_jobs(region=region, project_id=project_id, filter=filter)

        for i in range(number_of_jobs):
            assert jobs_list[i].name == f"name{i}"

        expected_list_jobs_request: ListJobsRequest = ListJobsRequest(
            parent=f"projects/{project_id}/locations/{region}", filter=filter
        )
        mock_batch_service_client.return_value.list_jobs.assert_called_once_with(
            request=expected_list_jobs_request
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_list_jobs_with_limit(self, mock_batch_service_client, cloud_batch_hook):
        number_of_jobs = 3
        limit = 2
        region = "us-central1"
        project_id = "test_project_id"
        filter = "filter_description"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        jobs_list = cloud_batch_hook.list_jobs(
            region=region, project_id=project_id, filter=filter, limit=limit
        )

        assert len(jobs_list) == limit
        for i in range(limit):
            assert jobs_list[i].name == f"name{i}"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_list_jobs_with_limit_zero(self, mock_batch_service_client, cloud_batch_hook):
        number_of_jobs = 3
        limit = 0
        region = "us-central1"
        project_id = "test_project_id"
        filter = "filter_description"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        jobs_list = cloud_batch_hook.list_jobs(
            region=region, project_id=project_id, filter=filter, limit=limit
        )

        assert len(jobs_list) == 0

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_list_jobs_with_limit_greater_then_range(self, mock_batch_service_client, cloud_batch_hook):
        number_of_jobs = 3
        limit = 5
        region = "us-central1"
        project_id = "test_project_id"
        filter = "filter_description"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        jobs_list = cloud_batch_hook.list_jobs(
            region=region, project_id=project_id, filter=filter, limit=limit
        )

        assert len(jobs_list) == number_of_jobs
        for i in range(number_of_jobs):
            assert jobs_list[i].name == f"name{i}"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_list_jobs_with_limit_less_than_zero(self, mock_batch_service_client, cloud_batch_hook):
        number_of_jobs = 3
        limit = -1
        region = "us-central1"
        project_id = "test_project_id"
        filter = "filter_description"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        with pytest.raises(expected_exception=AirflowException):
            cloud_batch_hook.list_jobs(region=region, project_id=project_id, filter=filter, limit=limit)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_list_tasks_with_limit(self, mock_batch_service_client, cloud_batch_hook):
        number_of_tasks = 3
        limit = 2
        region = "us-central1"
        project_id = "test_project_id"
        filter = "filter_description"
        job_name = "test_job"

        page = self._mock_pager(number_of_tasks)
        mock_batch_service_client.return_value.list_tasks.return_value = page

        tasks_list = cloud_batch_hook.list_tasks(
            region=region, project_id=project_id, job_name=job_name, filter=filter, limit=limit
        )

        assert len(tasks_list) == limit
        for i in range(limit):
            assert tasks_list[i].name == f"name{i}"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_list_tasks_with_limit_greater_then_range(self, mock_batch_service_client, cloud_batch_hook):
        number_of_tasks = 3
        limit = 5
        region = "us-central1"
        project_id = "test_project_id"
        filter = "filter_description"
        job_name = "test_job"

        page = self._mock_pager(number_of_tasks)
        mock_batch_service_client.return_value.list_tasks.return_value = page

        tasks_list = cloud_batch_hook.list_tasks(
            region=region, project_id=project_id, filter=filter, job_name=job_name, limit=limit
        )

        assert len(tasks_list) == number_of_tasks
        for i in range(number_of_tasks):
            assert tasks_list[i].name == f"name{i}"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_list_tasks_with_limit_less_than_zero(self, mock_batch_service_client, cloud_batch_hook):
        number_of_tasks = 3
        limit = -1
        region = "us-central1"
        project_id = "test_project_id"
        filter = "filter_description"
        job_name = "test_job"

        page = self._mock_pager(number_of_tasks)
        mock_batch_service_client.return_value.list_tasks.return_value = page

        with pytest.raises(expected_exception=AirflowException):
            cloud_batch_hook.list_tasks(
                region=region, project_id=project_id, job_name=job_name, filter=filter, limit=limit
            )

    def _mock_job_with_status(self, status: JobStatus.State):
        job = mock.MagicMock()
        job.status.state = status
        return job

    def _mock_pager(self, number_of_jobs):
        mock_pager = []
        for i in range(number_of_jobs):
            mock_pager.append(Job(name=f"name{i}"))

        return mock_pager


class TestCloudBatchAsyncHook:
    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceAsyncClient")
    async def test_get_job(self, mock_client):
        expected_job = {"name": "somename"}

        async def _get_job(name):
            return expected_job

        job_name = "jobname"
        mock_client.return_value = mock.MagicMock()
        mock_client.return_value.get_job = _get_job

        hook = CloudBatchAsyncHook()
        hook.get_credentials = self._dummy_get_credentials

        returned_operation = await hook.get_batch_job(job_name=job_name)

        assert returned_operation == expected_job

    def _dummy_get_credentials(self):
        pass
