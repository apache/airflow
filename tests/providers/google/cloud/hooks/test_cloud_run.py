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
from google.cloud.run_v2 import (
    CreateJobRequest,
    CreateServiceRequest,
    DeleteJobRequest,
    DeleteServiceRequest,
    GetJobRequest,
    GetServiceRequest,
    Job,
    ListJobsRequest,
    RunJobRequest,
    Service,
    UpdateJobRequest,
)

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_run import (
    CloudRunAsyncHook,
    CloudRunHook,
    CloudRunServiceAsyncHook,
    CloudRunServiceHook,
)
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id


@pytest.mark.db_test
class TestCloudRunHook:
    def dummy_get_credentials(self):
        pass

    @pytest.fixture
    def cloud_run_hook(self):
        cloud_run_hook = CloudRunHook()
        cloud_run_hook.get_credentials = self.dummy_get_credentials
        return cloud_run_hook

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_get_job(self, mock_batch_service_client, cloud_run_hook):
        job_name = "job1"
        region = "region1"
        project_id = "projectid"

        get_job_request = GetJobRequest(name=f"projects/{project_id}/locations/{region}/jobs/{job_name}")

        cloud_run_hook.get_job(job_name=job_name, region=region, project_id=project_id)
        cloud_run_hook._client.get_job.assert_called_once_with(get_job_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_update_job(self, mock_batch_service_client, cloud_run_hook):
        job_name = "job1"
        region = "region1"
        project_id = "projectid"
        job = Job()
        job.name = f"projects/{project_id}/locations/{region}/jobs/{job_name}"

        update_request = UpdateJobRequest()
        update_request.job = job

        cloud_run_hook.update_job(
            job=Job.to_dict(job), job_name=job_name, region=region, project_id=project_id
        )

        cloud_run_hook._client.update_job.assert_called_once_with(update_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_create_job(self, mock_batch_service_client, cloud_run_hook):
        job_name = "job1"
        region = "region1"
        project_id = "projectid"
        job = Job()

        create_request = CreateJobRequest()
        create_request.job = job
        create_request.job_id = job_name
        create_request.parent = f"projects/{project_id}/locations/{region}"

        cloud_run_hook.create_job(
            job=Job.to_dict(job), job_name=job_name, region=region, project_id=project_id
        )

        cloud_run_hook._client.create_job.assert_called_once_with(create_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_execute_job(self, mock_batch_service_client, cloud_run_hook):
        job_name = "job1"
        region = "region1"
        project_id = "projectid"
        overrides = {
            "container_overrides": [{"args": ["python", "main.py"]}],
            "task_count": 1,
            "timeout": "60s",
        }
        run_job_request = RunJobRequest(
            name=f"projects/{project_id}/locations/{region}/jobs/{job_name}", overrides=overrides
        )

        cloud_run_hook.execute_job(
            job_name=job_name, region=region, project_id=project_id, overrides=overrides
        )
        cloud_run_hook._client.run_job.assert_called_once_with(request=run_job_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_list_jobs(self, mock_batch_service_client, cloud_run_hook):
        number_of_jobs = 3
        region = "us-central1"
        project_id = "test_project_id"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        jobs_list = cloud_run_hook.list_jobs(region=region, project_id=project_id)

        for i in range(number_of_jobs):
            assert jobs_list[i].name == f"name{i}"

        expected_list_jobs_request: ListJobsRequest = ListJobsRequest(
            parent=f"projects/{project_id}/locations/{region}"
        )
        mock_batch_service_client.return_value.list_jobs.assert_called_once_with(
            request=expected_list_jobs_request
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_list_jobs_show_deleted(self, mock_batch_service_client, cloud_run_hook):
        number_of_jobs = 3
        region = "us-central1"
        project_id = "test_project_id"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        jobs_list = cloud_run_hook.list_jobs(region=region, project_id=project_id, show_deleted=True)

        for i in range(number_of_jobs):
            assert jobs_list[i].name == f"name{i}"

        expected_list_jobs_request: ListJobsRequest = ListJobsRequest(
            parent=f"projects/{project_id}/locations/{region}", show_deleted=True
        )
        mock_batch_service_client.return_value.list_jobs.assert_called_once_with(
            request=expected_list_jobs_request
        )

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_list_jobs_with_limit(self, mock_batch_service_client, cloud_run_hook):
        number_of_jobs = 3
        limit = 2
        region = "us-central1"
        project_id = "test_project_id"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        jobs_list = cloud_run_hook.list_jobs(region=region, project_id=project_id, limit=limit)

        assert len(jobs_list) == limit
        for i in range(limit):
            assert jobs_list[i].name == f"name{i}"

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_list_jobs_with_limit_zero(self, mock_batch_service_client, cloud_run_hook):
        number_of_jobs = 3
        limit = 0
        region = "us-central1"
        project_id = "test_project_id"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        jobs_list = cloud_run_hook.list_jobs(region=region, project_id=project_id, limit=limit)

        assert len(jobs_list) == 0

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_list_jobs_with_limit_greater_then_range(self, mock_batch_service_client, cloud_run_hook):
        number_of_jobs = 3
        limit = 5
        region = "us-central1"
        project_id = "test_project_id"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        jobs_list = cloud_run_hook.list_jobs(region=region, project_id=project_id, limit=limit)

        assert len(jobs_list) == number_of_jobs
        for i in range(number_of_jobs):
            assert jobs_list[i].name == f"name{i}"

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_list_jobs_with_limit_less_than_zero(self, mock_batch_service_client, cloud_run_hook):
        number_of_jobs = 3
        limit = -1
        region = "us-central1"
        project_id = "test_project_id"

        page = self._mock_pager(number_of_jobs)
        mock_batch_service_client.return_value.list_jobs.return_value = page

        with pytest.raises(expected_exception=AirflowException):
            cloud_run_hook.list_jobs(region=region, project_id=project_id, limit=limit)

    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsClient")
    def test_delete_job(self, mock_batch_service_client, cloud_run_hook):
        job_name = "job1"
        region = "region1"
        project_id = "projectid"

        delete_request = DeleteJobRequest(name=f"projects/{project_id}/locations/{region}/jobs/{job_name}")

        cloud_run_hook.delete_job(job_name=job_name, region=region, project_id=project_id)
        cloud_run_hook._client.delete_job.assert_called_once_with(delete_request)

    def _mock_pager(self, number_of_jobs):
        mock_pager = []
        for i in range(number_of_jobs):
            mock_pager.append(Job(name=f"name{i}"))

        return mock_pager


class TestCloudRunAsyncHook:
    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.JobsAsyncClient")
    async def test_get_operation(self, mock_client):
        expected_operation = {"name": "somename"}
        operation_name = "operationname"
        mock_client.return_value = mock.MagicMock()
        mock_client.return_value.get_operation = self.mock_get_operation(expected_operation)
        hook = CloudRunAsyncHook()
        hook.get_credentials = self._dummy_get_credentials

        returned_operation = await hook.get_operation(operation_name=operation_name)

        mock_client.return_value.get_operation.assert_called_once_with(mock.ANY, timeout=120)
        assert returned_operation == expected_operation

    def mock_get_operation(self, expected_operation):
        get_operation_mock = mock.AsyncMock()
        get_operation_mock.return_value = expected_operation
        return get_operation_mock

    def _dummy_get_credentials(self):
        pass


@pytest.mark.db_test
class TestCloudRunServiceHook:
    def dummy_get_credentials(self):
        pass

    @pytest.fixture
    def cloud_run_service_hook(self):
        cloud_run_service_hook = CloudRunServiceHook()
        cloud_run_service_hook.get_credentials = self.dummy_get_credentials
        return cloud_run_service_hook

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.ServicesClient")
    def test_get_service(self, mock_batch_service_client, cloud_run_service_hook):
        service_name = "service1"
        region = "region1"
        project_id = "projectid"

        get_service_request = GetServiceRequest(
            name=f"projects/{project_id}/locations/{region}/services/{service_name}"
        )

        cloud_run_service_hook.get_service(service_name=service_name, region=region, project_id=project_id)
        cloud_run_service_hook._client.get_service.assert_called_once_with(get_service_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.ServicesClient")
    def test_create_service(self, mock_batch_service_client, cloud_run_service_hook):
        service_name = "service1"
        region = "region1"
        project_id = "projectid"
        service = Service()

        create_request = CreateServiceRequest()
        create_request.service = service
        create_request.service_id = service_name
        create_request.parent = f"projects/{project_id}/locations/{region}"

        cloud_run_service_hook.create_service(
            service=service, service_name=service_name, region=region, project_id=project_id
        )
        cloud_run_service_hook._client.create_service.assert_called_once_with(create_request)

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.ServicesClient")
    def test_delete_service(self, mock_batch_service_client, cloud_run_service_hook):
        service_name = "service1"
        region = "region1"
        project_id = "projectid"

        delete_request = DeleteServiceRequest(
            name=f"projects/{project_id}/locations/{region}/services/{service_name}"
        )

        cloud_run_service_hook.delete_service(service_name=service_name, region=region, project_id=project_id)
        cloud_run_service_hook._client.delete_service.assert_called_once_with(delete_request)


class TestCloudRunServiceAsyncHook:
    def dummy_get_credentials(self):
        pass

    def mock_service(self):
        return mock.AsyncMock()

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.ServicesAsyncClient")
    async def test_create_service(self, mock_client):
        mock_client.return_value = mock.MagicMock()
        mock_client.return_value.create_service = self.mock_service()

        hook = CloudRunServiceAsyncHook()
        hook.get_credentials = self.dummy_get_credentials

        await hook.create_service(
            service_name="service1",
            service=Service(),
            region="region1",
            project_id="projectid",
        )

        expected_request = CreateServiceRequest(
            service=Service(),
            service_id="service1",
            parent="projects/projectid/locations/region1",
        )

        mock_client.return_value.create_service.assert_called_once_with(expected_request)

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
        new=mock_base_gcp_hook_default_project_id,
    )
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_run.ServicesAsyncClient")
    async def test_delete_service(self, mock_client):
        mock_client.return_value = mock.MagicMock()
        mock_client.return_value.delete_service = self.mock_service()

        hook = CloudRunServiceAsyncHook()
        hook.get_credentials = self.dummy_get_credentials

        await hook.delete_service(
            service_name="service1",
            region="region1",
            project_id="projectid",
        )

        expected_request = DeleteServiceRequest(
            name="projects/projectid/locations/region1/services/service1",
        )

        mock_client.return_value.delete_service.assert_called_once_with(expected_request)
