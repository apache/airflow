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
from airflow.providers.google.cloud.hooks.cloud_batch import CloudBatchHook
from google.cloud.batch_v1 import Job, CreateJobRequest
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id


class TestCloudBathHook:

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

       
