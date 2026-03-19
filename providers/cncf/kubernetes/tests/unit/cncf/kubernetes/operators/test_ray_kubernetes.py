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

from airflow.providers.cncf.kubernetes.operators.ray_kubernetes import RayKubernetesOperator
from airflow.providers.common.compat.sdk import AirflowException

SAMPLE_TEMPLATE_SPEC = {
    "apiVersion": "ray.io/v1",
    "kind": "RayJob",
    "metadata": {"name": "test-ray-job", "namespace": "default"},
    "spec": {
        "entrypoint": "python script.py",
        "rayClusterSpec": {
            "headGroupSpec": {
                "template": {"spec": {"containers": [{"name": "ray-head", "image": "rayproject/ray:2.9.0"}]}}
            },
        },
    },
}


class TestRayKubernetesOperator:
    def test_operator_init_with_template_spec(self):
        op = RayKubernetesOperator(
            task_id="test_ray",
            template_spec=SAMPLE_TEMPLATE_SPEC,
            namespace="test-ns",
        )
        assert op.namespace == "test-ns"
        assert op.template_spec == SAMPLE_TEMPLATE_SPEC
        assert op.reattach_on_restart is True
        assert op.delete_on_termination is True

    def test_operator_init_with_application_file(self):
        op = RayKubernetesOperator(
            task_id="test_ray",
            application_file="ray_job.yaml",
        )
        assert op.application_file == "ray_job.yaml"

    def test_operator_requires_spec_or_file(self):
        op = RayKubernetesOperator(task_id="test_ray")
        with pytest.raises(AirflowException, match="Either application_file or template_spec"):
            op._manage_template_specs()

    def test_template_fields(self):
        assert "application_file" in RayKubernetesOperator.template_fields
        assert "template_spec" in RayKubernetesOperator.template_fields
        assert "namespace" in RayKubernetesOperator.template_fields

    def test_validate_and_enhance_template(self):
        op = RayKubernetesOperator(
            task_id="test_ray",
            template_spec={"spec": {"entrypoint": "python script.py"}},
        )
        result = op._manage_template_specs()
        assert result["apiVersion"] == "ray.io/v1"
        assert result["kind"] == "RayJob"
        assert "metadata" in result

    @patch(
        "airflow.providers.cncf.kubernetes.operators.ray_kubernetes.add_unique_suffix",
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
        mock_suffix.assert_called_once()

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

        context = {"ti": mock_ti, "run_id": "manual__2024-01-01", "dag": MagicMock(is_subdag=False)}

        labels = RayKubernetesOperator._get_ti_pod_labels(context)
        assert "dag-id" in labels
        assert "task-id" in labels
        assert "run-id" in labels
        assert "ray-kubernetes-operator" in labels
        assert "try-number" in labels

    def test_get_ti_pod_labels_string_values(self):
        """All label values must be strings for Kubernetes."""
        mock_ti = MagicMock()
        mock_ti.dag_id = "test_dag"
        mock_ti.task_id = "test_task"
        mock_ti.try_number = 3
        mock_ti.map_index = 5

        context = {"ti": mock_ti, "run_id": "run_123", "dag": MagicMock(is_subdag=False)}

        labels = RayKubernetesOperator._get_ti_pod_labels(context)
        for value in labels.values():
            assert isinstance(value, str), f"Label value {value!r} is not a string"

    def test_ui_color(self):
        assert RayKubernetesOperator.ui_color == "#028edd"
