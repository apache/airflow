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

from airflow.providers.google.cloud.hooks.ray import RayJobHook

from unit.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_CLUSTER_NAME: str = "test-cluster-name"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
RAY_JOB_STRING = "airflow.providers.google.cloud.hooks.ray.{}"

TEST_CLUSTER_HOSTNAME = "ray.aiplatform-training.googleusercontent.com"
TEST_JOB_ID = "test-job-id"


class TestRayJobHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = RayJobHook(gcp_conn_id=TEST_GCP_CONN_ID)
            self.hook.get_credentials = mock.MagicMock()

    @mock.patch(RAY_JOB_STRING.format("JobSubmissionClient"))
    def test_submit_job(self, mock_client_cls) -> None:
        mock_client = mock_client_cls.return_value
        mock_client.submit_job.return_value = TEST_JOB_ID
        job_id = self.hook.submit_job(
            entrypoint="python3 heavy.py",
            cluster_address=TEST_CLUSTER_HOSTNAME,
            runtime_env={"a": "b"},
            metadata={"k": "v"},
            submission_id="sub-123",
            entrypoint_num_cpus=1,
            entrypoint_num_gpus=0,
            entrypoint_memory=1024,
            entrypoint_resources={"CPU": 1.0},
        )

        mock_client_cls.assert_called_once_with(f"vertex_ray://{TEST_CLUSTER_HOSTNAME}")
        mock_client.submit_job.assert_called_once_with(
            entrypoint="python3 heavy.py",
            runtime_env={"a": "b"},
            metadata={"k": "v"},
            submission_id="sub-123",
            entrypoint_num_cpus=1,
            entrypoint_num_gpus=0,
            entrypoint_memory=1024,
            entrypoint_resources={"CPU": 1.0},
        )
        assert job_id == TEST_JOB_ID

    @mock.patch(RAY_JOB_STRING.format("JobSubmissionClient"))
    def test_stop_job(self, mock_client_cls) -> None:
        mock_client = mock_client_cls.return_value
        mock_client.stop_job.return_value = True

        result = self.hook.stop_job(
            job_id=TEST_JOB_ID,
            cluster_address=TEST_CLUSTER_HOSTNAME,
        )

        mock_client_cls.assert_called_once_with(f"vertex_ray://{TEST_CLUSTER_HOSTNAME}")
        mock_client.stop_job.assert_called_once_with(job_id=TEST_JOB_ID)
        assert result is True

    @mock.patch(RAY_JOB_STRING.format("JobSubmissionClient"))
    def test_delete_job(self, mock_client_cls) -> None:
        mock_client = mock_client_cls.return_value
        mock_client.delete_job.return_value = True

        result = self.hook.delete_job(
            job_id=TEST_JOB_ID,
            cluster_address=TEST_CLUSTER_HOSTNAME,
        )

        mock_client_cls.assert_called_once_with(f"vertex_ray://{TEST_CLUSTER_HOSTNAME}")
        mock_client.delete_job.assert_called_once_with(job_id=TEST_JOB_ID)
        assert result is True

    @mock.patch(RAY_JOB_STRING.format("JobSubmissionClient"))
    def test_get_job_info(self, mock_client_cls) -> None:
        mock_client = mock_client_cls.return_value
        fake_job_details = object()
        mock_client.get_job_info.return_value = fake_job_details

        result = self.hook.get_job_info(
            job_id=TEST_JOB_ID,
            cluster_address=TEST_CLUSTER_HOSTNAME,
        )

        mock_client_cls.assert_called_once_with(f"vertex_ray://{TEST_CLUSTER_HOSTNAME}")
        mock_client.get_job_info.assert_called_once_with(job_id=TEST_JOB_ID)
        assert result is fake_job_details

    @mock.patch(RAY_JOB_STRING.format("JobSubmissionClient"))
    def test_list_jobs(self, mock_client_cls) -> None:
        mock_client = mock_client_cls.return_value
        fake_list = [object(), object()]
        mock_client.list_jobs.return_value = fake_list

        result = self.hook.list_jobs(
            cluster_address=TEST_CLUSTER_HOSTNAME,
        )

        mock_client_cls.assert_called_once_with(f"vertex_ray://{TEST_CLUSTER_HOSTNAME}")
        mock_client.list_jobs.assert_called_once_with()
        assert result is fake_list

    @mock.patch(RAY_JOB_STRING.format("JobSubmissionClient"))
    def test_get_job_status(self, mock_client_cls) -> None:
        mock_client = mock_client_cls.return_value
        fake_status = object()
        mock_client.get_job_status.return_value = fake_status

        result = self.hook.get_job_status(
            job_id=TEST_JOB_ID,
            cluster_address=TEST_CLUSTER_HOSTNAME,
        )

        mock_client_cls.assert_called_once_with(f"vertex_ray://{TEST_CLUSTER_HOSTNAME}")
        mock_client.get_job_status.assert_called_once_with(job_id=TEST_JOB_ID)
        assert result is fake_status

    @mock.patch(RAY_JOB_STRING.format("JobSubmissionClient"))
    def test_get_job_logs(self, mock_client_cls) -> None:
        mock_client = mock_client_cls.return_value
        fake_logs = "some logs"
        mock_client.get_job_logs.return_value = fake_logs

        result = self.hook.get_job_logs(
            job_id=TEST_JOB_ID,
            cluster_address=TEST_CLUSTER_HOSTNAME,
        )

        mock_client_cls.assert_called_once_with(f"vertex_ray://{TEST_CLUSTER_HOSTNAME}")
        mock_client.get_job_logs.assert_called_once_with(job_id=TEST_JOB_ID)
        assert result == fake_logs
