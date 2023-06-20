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
from unittest import mock, TestCase
import pytest
from airflow.providers.google.cloud.hooks.cloud_batch import CloudBatchHook
from airflow.exceptions import AirflowException
from google.cloud.batch_v1 import Job, CreateJobRequest, JobStatus
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id


class TestCloudBathHook():

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__", new=mock_base_gcp_hook_default_project_id)
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_submit(self, mock_batch_service_client):
        cloud_batch_hook = CloudBatchHook()
        job = Job()
        job_name = 'jobname'
        project_id = 'test_project_id'
        region = 'us-central1'

        cloud_batch_hook.submit_build_job(job_name, Job(), region, project_id)

        create_request = CreateJobRequest()
        create_request.job = job
        create_request.job_id = job_name
        create_request.parent = f"projects/{project_id}/locations/{region}"

        cloud_batch_hook.client.create_job.assert_called_with(
            create_request)

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__", new=mock_base_gcp_hook_default_project_id)
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_get_job(self, mock_batch_service_client):
        cloud_batch_hook = CloudBatchHook()
        cloud_batch_hook.get_job("job1")
        cloud_batch_hook.client.get_job.assert_called_once_with(name="job1")

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__", new=mock_base_gcp_hook_default_project_id)
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_delete_job(self, mock_batch_service_client):
        job_name = 'job1'
        region = 'us-east1'
        project_id = 'test_project_id'
        cloud_batch_hook = CloudBatchHook()
        cloud_batch_hook.delete_job(job_name, region, project_id)
        cloud_batch_hook.client.delete_job.assert_called_once_with(
            name=f"projects/{project_id}/locations/{region}/jobs/{job_name}")

    @pytest.mark.parametrize(
        "state",
        [
            JobStatus.State.SUCCEEDED,
            JobStatus.State.FAILED,
            JobStatus.State.DELETION_IN_PROGRESS
           
        ]
    )
    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__", new=mock_base_gcp_hook_default_project_id)
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_wait_job_succeeded(self, mock_batch_service_client, state):
        mock_job = self._mock_job_with_status(state)
        mock_batch_service_client.return_value.get_job.return_value = mock_job
        cloud_batch_hook = CloudBatchHook()
        actual_job = cloud_batch_hook.wait_for_job('job1')
        assert actual_job == mock_job


    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__", new=mock_base_gcp_hook_default_project_id)
    @mock.patch("airflow.providers.google.cloud.hooks.cloud_batch.BatchServiceClient")
    def test_wait_job_timeout(self, mock_batch_service_client):
        mock_job = self._mock_job_with_status(JobStatus.State.RUNNING)
        mock_batch_service_client.return_value.get_job.return_value = mock_job
        cloud_batch_hook = CloudBatchHook()

        exception_caught = False
        try:
            cloud_batch_hook.wait_for_job('job1', polling_period_seconds=0.01, timeout=0.02)
        except AirflowException:
            exception_caught = True

        assert exception_caught
        

    def _mock_job_with_status(self, status: JobStatus.State):
        job = mock.MagicMock()
        job.status.state = status
        return job
