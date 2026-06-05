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

pytest.importorskip("google.cloud.aiplatform.vertex_ray.util.resources")
from google.cloud.aiplatform.vertex_ray.util.resources import Resources

from airflow.providers.google.cloud.operators.vertex_ai.ray import CreateRayClusterOperator

TEST_GCP_CONN_ID = "test-gcp-conn-id"
TEST_LOCATION = "us-central1"
TEST_PROJECT_ID = "test-project-id"
TEST_PYTHON_VERSION = "3.10"
TEST_RAY_VERSION = "2.33"
TEST_CLUSTER_NAME = "test-cluster-name"

VERTEX_AI_RAY_OP_PATH = "airflow.providers.google.cloud.operators.vertex_ai.ray.{}"


class TestCreateRayClusterOperator:
    @mock.patch(VERTEX_AI_RAY_OP_PATH.format("RayHook"))
    def test_create_ray_cluster_with_explicit_head_node_type(self, mock_hook_cls):
        explicit_head = Resources()
        op = CreateRayClusterOperator(
            task_id="test-task",
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            python_version=TEST_PYTHON_VERSION,
            ray_version=TEST_RAY_VERSION,
            head_node_type=explicit_head,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        assert op.head_node_type is explicit_head

    @mock.patch(VERTEX_AI_RAY_OP_PATH.format("RayHook"))
    def test_create_ray_cluster_default_head_node_type_is_fresh_resources(self, mock_hook_cls):
        op1 = CreateRayClusterOperator(
            task_id="test-task-1",
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            python_version=TEST_PYTHON_VERSION,
            ray_version=TEST_RAY_VERSION,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        op2 = CreateRayClusterOperator(
            task_id="test-task-2",
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            python_version=TEST_PYTHON_VERSION,
            ray_version=TEST_RAY_VERSION,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        assert isinstance(op1.head_node_type, Resources)
        assert isinstance(op2.head_node_type, Resources)
        assert op1.head_node_type is not op2.head_node_type

    @mock.patch(VERTEX_AI_RAY_OP_PATH.format("VertexAIRayClusterLink"))
    @mock.patch(VERTEX_AI_RAY_OP_PATH.format("RayHook"))
    def test_execute_without_head_node_type_passes_default_resources(self, mock_hook_cls, mock_link):
        mock_hook = mock_hook_cls.return_value
        mock_hook.create_ray_cluster.return_value = (
            f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/persistentResources/{TEST_CLUSTER_NAME}"
        )
        mock_hook.extract_cluster_id.return_value = TEST_CLUSTER_NAME

        op = CreateRayClusterOperator(
            task_id="test-task",
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            python_version=TEST_PYTHON_VERSION,
            ray_version=TEST_RAY_VERSION,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )

        ti_mock = mock.MagicMock()
        context = {"ti": ti_mock, "task": mock.MagicMock()}
        op.execute(context=context)

        call_kwargs = mock_hook.create_ray_cluster.call_args.kwargs
        assert isinstance(call_kwargs["head_node_type"], Resources)
