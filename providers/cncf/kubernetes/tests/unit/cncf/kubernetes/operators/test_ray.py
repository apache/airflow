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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.cncf.kubernetes.operators.ray import RayKubernetesOperator
from airflow.providers.common.compat.sdk import AirflowException

SAMPLE_TEMPLATE_SPEC = {
    "apiVersion": "ray.io/v1",
    "kind": "RayJob",
    "metadata": {"name": "test-ray-job", "namespace": "default"},
    "spec": {"entrypoint": "python script.py"},
}


class TestRayKubernetesOperator:
    def test_init_with_template_spec(self):
        op = RayKubernetesOperator(
            task_id="test_ray",
            template_spec=SAMPLE_TEMPLATE_SPEC,
            namespace="test-ns",
        )
        assert op.namespace == "test-ns"
        assert op.wait_for_completion is True
        assert op.delete_on_termination is True
        assert op.reattach_on_restart is True

    def test_init_with_application_file(self):
        op = RayKubernetesOperator(task_id="test_ray", application_file="ray_job.yaml")
        assert op.application_file == "ray_job.yaml"

    def test_requires_spec_or_file(self):
        op = RayKubernetesOperator(task_id="test_ray")
        with pytest.raises(AirflowException, match="Either application_file or template_spec"):
            op._parse_template()

    def test_template_fields(self):
        assert "application_file" in RayKubernetesOperator.template_fields
        assert "template_spec" in RayKubernetesOperator.template_fields
        assert "namespace" in RayKubernetesOperator.template_fields

    def test_parse_template_adds_defaults(self):
        op = RayKubernetesOperator(
            task_id="test_ray",
            template_spec={"spec": {"entrypoint": "python script.py"}},
        )
        result = op._parse_template()
        assert result["apiVersion"] == "ray.io/v1"
        assert result["kind"] == "RayJob"
        assert "metadata" in result

    @patch(
        "airflow.providers.cncf.kubernetes.operators.ray.add_unique_suffix",
        return_value="test-ray-abc123",
    )
    def test_create_job_name_with_suffix(self, mock_suffix):
        op = RayKubernetesOperator(
            task_id="test_ray",
            template_spec=SAMPLE_TEMPLATE_SPEC,
            random_name_suffix=True,
        )
        name = op._create_job_name()
        assert name == "test-ray-abc123"

    def test_create_job_name_without_suffix(self):
        op = RayKubernetesOperator(
            task_id="test_ray",
            name="my-ray-job",
            template_spec=SAMPLE_TEMPLATE_SPEC,
            random_name_suffix=False,
        )
        name = op._create_job_name()
        assert name == "my-ray-job"

    def test_get_ti_pod_labels_no_context(self):
        labels = RayKubernetesOperator._get_ti_pod_labels(None)
        assert labels == {}

    def test_get_ti_pod_labels_with_context(self):
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_task"
        mock_ti.try_number = 1
        mock_ti.map_index = -1

        context = {"ti": mock_ti, "run_id": "manual__2024-01-01"}
        labels = RayKubernetesOperator._get_ti_pod_labels(context)
        assert "dag-id" in labels
        assert "task-id" in labels
        assert "run-id" in labels
        assert "ray-kubernetes-operator" in labels
        assert "try-number" in labels

    def test_get_ti_pod_labels_all_string_values(self):
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_task"
        mock_ti.try_number = 3
        mock_ti.map_index = 5

        context = {"ti": mock_ti, "run_id": "run_123"}
        labels = RayKubernetesOperator._get_ti_pod_labels(context)
        for value in labels.values():
            assert isinstance(value, str)

    @patch("airflow.providers.cncf.kubernetes.operators.ray.KubernetesHook")
    def test_submit_uses_hook_create(self, mock_hook_cls):
        mock_hook = MagicMock()
        mock_hook_cls.return_value = mock_hook

        op = RayKubernetesOperator(
            task_id="test_ray",
            template_spec=SAMPLE_TEMPLATE_SPEC,
        )
        op.ray_job_name = "test-ray-job"
        mock_context = MagicMock()
        mock_context.__getitem__ = MagicMock(side_effect=KeyError)

        # Use empty labels when context doesn't have ti
        with patch.object(op, "_get_ti_pod_labels", return_value={}):
            op._submit_ray_job(mock_context)

        mock_hook.create_custom_object.assert_called_once()
        call_kwargs = mock_hook.create_custom_object.call_args
        assert call_kwargs.kwargs["group"] == "ray.io"
        assert call_kwargs.kwargs["version"] == "v1"
        assert call_kwargs.kwargs["plural"] == "rayjobs"

    @patch("airflow.providers.cncf.kubernetes.operators.ray.KubernetesHook")
    def test_delete_uses_hook_delete(self, mock_hook_cls):
        mock_hook = MagicMock()
        mock_hook_cls.return_value = mock_hook

        op = RayKubernetesOperator(task_id="test_ray", template_spec=SAMPLE_TEMPLATE_SPEC)
        op.ray_job_name = "test-ray-job"
        op._delete_ray_job()

        mock_hook.delete_custom_object.assert_called_once_with(
            group="ray.io",
            version="v1",
            plural="rayjobs",
            name="test-ray-job",
            namespace="default",
        )

    @patch("airflow.providers.cncf.kubernetes.operators.ray.KubernetesHook")
    def test_get_status_uses_hook_get(self, mock_hook_cls):
        mock_hook = MagicMock()
        mock_hook.get_custom_object.return_value = {
            "status": {"jobStatus": "RUNNING", "jobDeploymentStatus": "Running"}
        }
        mock_hook_cls.return_value = mock_hook

        op = RayKubernetesOperator(task_id="test_ray", template_spec=SAMPLE_TEMPLATE_SPEC)
        op.ray_job_name = "test-ray-job"
        job_status, dep_status = op._get_ray_job_status()

        assert job_status == "RUNNING"
        assert dep_status == "Running"

    def test_ui_color(self):
        assert RayKubernetesOperator.ui_color == "#028edd"


