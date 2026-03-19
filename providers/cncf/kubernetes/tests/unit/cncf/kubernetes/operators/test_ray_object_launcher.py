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

from unittest.mock import MagicMock

import pytest
from kubernetes.client.rest import ApiException

from airflow.providers.cncf.kubernetes.operators.ray_object_launcher import (
    RayJobDeploymentStatus,
    RayJobStatus,
    RayObjectLauncher,
)
from airflow.providers.common.compat.sdk import AirflowException

SAMPLE_TEMPLATE = {
    "apiVersion": "ray.io/v1",
    "kind": "RayJob",
    "metadata": {"name": "test-ray-job", "namespace": "default"},
    "spec": {"entrypoint": "python script.py"},
}


class TestRayObjectLauncher:
    def setup_method(self):
        self.mock_kube_client = MagicMock()
        self.mock_custom_obj_api = MagicMock()

    def test_body_construction(self):
        launcher = RayObjectLauncher(
            name="test-job",
            namespace="default",
            kube_client=self.mock_kube_client,
            custom_obj_api=self.mock_custom_obj_api,
            template_body=SAMPLE_TEMPLATE,
        )
        assert launcher.body["metadata"]["name"] == "test-job"
        assert launcher.body["metadata"]["namespace"] == "default"
        assert launcher.body["metadata"]["labels"]["ray.io/job-name"] == "test-job"

    def test_name_sanitization(self):
        launcher = RayObjectLauncher(
            name="My_Test_Job",
            namespace="default",
            kube_client=self.mock_kube_client,
            custom_obj_api=self.mock_custom_obj_api,
            template_body=SAMPLE_TEMPLATE,
        )
        assert launcher.body["metadata"]["name"] == "my-test-job"

    def test_api_version_parsing(self):
        launcher = RayObjectLauncher(
            name="test-job",
            namespace="default",
            kube_client=self.mock_kube_client,
            custom_obj_api=self.mock_custom_obj_api,
            template_body=SAMPLE_TEMPLATE,
        )
        assert launcher.api_group == "ray.io"
        assert launcher.api_version == "v1"

    def test_no_template_body_raises(self):
        with pytest.raises(AirflowException, match="template_body is required"):
            RayObjectLauncher(
                name="test-job",
                namespace="default",
                kube_client=self.mock_kube_client,
                custom_obj_api=self.mock_custom_obj_api,
                template_body=None,
            )

    def test_no_name_raises(self):
        with pytest.raises(AirflowException, match="Job name cannot be None"):
            RayObjectLauncher(
                name=None,
                namespace="default",
                kube_client=self.mock_kube_client,
                custom_obj_api=self.mock_custom_obj_api,
                template_body=SAMPLE_TEMPLATE,
            )

    def test_delete_ray_job_success(self):
        launcher = RayObjectLauncher(
            name="test-job",
            namespace="default",
            kube_client=self.mock_kube_client,
            custom_obj_api=self.mock_custom_obj_api,
            template_body=SAMPLE_TEMPLATE,
        )
        launcher.delete_ray_job()
        self.mock_custom_obj_api.delete_namespaced_custom_object.assert_called_once()

    def test_delete_ray_job_already_deleted(self):
        self.mock_custom_obj_api.delete_namespaced_custom_object.side_effect = ApiException(status=404)
        launcher = RayObjectLauncher(
            name="test-job",
            namespace="default",
            kube_client=self.mock_kube_client,
            custom_obj_api=self.mock_custom_obj_api,
            template_body=SAMPLE_TEMPLATE,
        )
        # Should not raise
        launcher.delete_ray_job()

    def test_get_ray_job_status(self):
        self.mock_custom_obj_api.get_namespaced_custom_object_status.return_value = {
            "status": {"jobStatus": "RUNNING", "jobDeploymentStatus": "Running"}
        }
        launcher = RayObjectLauncher(
            name="test-job",
            namespace="default",
            kube_client=self.mock_kube_client,
            custom_obj_api=self.mock_custom_obj_api,
            template_body=SAMPLE_TEMPLATE,
        )
        status = launcher.get_ray_job_status()
        assert status["status"]["jobStatus"] == "RUNNING"

    def test_get_ray_job_status_not_found(self):
        self.mock_custom_obj_api.get_namespaced_custom_object_status.side_effect = ApiException(status=404)
        launcher = RayObjectLauncher(
            name="test-job",
            namespace="default",
            kube_client=self.mock_kube_client,
            custom_obj_api=self.mock_custom_obj_api,
            template_body=SAMPLE_TEMPLATE,
        )
        with pytest.raises(AirflowException, match="not found"):
            launcher.get_ray_job_status()

    def test_is_job_terminal_succeeded(self):
        self.mock_custom_obj_api.get_namespaced_custom_object_status.return_value = {
            "status": {
                "jobStatus": RayJobStatus.SUCCEEDED,
                "jobDeploymentStatus": RayJobDeploymentStatus.COMPLETE,
            }
        }
        launcher = RayObjectLauncher(
            name="test-job",
            namespace="default",
            kube_client=self.mock_kube_client,
            custom_obj_api=self.mock_custom_obj_api,
            template_body=SAMPLE_TEMPLATE,
        )
        assert launcher.is_job_terminal() is True

    def test_is_job_terminal_running(self):
        self.mock_custom_obj_api.get_namespaced_custom_object_status.return_value = {
            "status": {
                "jobStatus": RayJobStatus.RUNNING,
                "jobDeploymentStatus": RayJobDeploymentStatus.RUNNING,
            }
        }
        launcher = RayObjectLauncher(
            name="test-job",
            namespace="default",
            kube_client=self.mock_kube_client,
            custom_obj_api=self.mock_custom_obj_api,
            template_body=SAMPLE_TEMPLATE,
        )
        assert launcher.is_job_terminal() is False
