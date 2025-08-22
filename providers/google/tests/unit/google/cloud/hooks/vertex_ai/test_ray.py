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

resources = pytest.importorskip("google.cloud.aiplatform.vertex_ray.util.resources")
from google.cloud.aiplatform.vertex_ray.util.resources import Cluster, Resources

from airflow.providers.google.cloud.hooks.vertex_ai.ray import RayHook

from unit.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_LOCATION: str = "test-location"
TEST_PROJECT_ID: str = "test-project-id"
TEST_NODE_RESOURCES: dict = {
    "machine_type": "n1-standard-8",
    "node_count": 1,
    "accelerator_type": "NVIDIA_TESLA_K80",
    "accelerator_count": 1,
    "custom_image": "us-docker.pkg.dev/my-project/ray-cpu-image.2.9:latest",
}
TEST_PYTHON_VERSION: str = "3.10"
TEST_RAY_VERSION: str = "2.33"
TEST_CLUSTER_NAME: str = "test-cluster-name"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
RAY_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.ray.{}"


class TestRayWithDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = RayHook(gcp_conn_id=TEST_GCP_CONN_ID)
            self.hook.get_credentials = mock.MagicMock()

    @mock.patch(RAY_STRING.format("vertex_ray.create_ray_cluster"))
    @mock.patch(RAY_STRING.format("aiplatform.init"))
    def test_create_ray_cluster(self, mock_aiplatform_init, mock_create_ray_cluster) -> None:
        self.hook.create_ray_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            head_node_type=TEST_NODE_RESOURCES,
            python_version=TEST_PYTHON_VERSION,
            ray_version=TEST_RAY_VERSION,
            network=None,
            service_account=None,
            cluster_name=TEST_CLUSTER_NAME,
            worker_node_types=[TEST_NODE_RESOURCES],
            custom_images=None,
            enable_metrics_collection=True,
            enable_logging=True,
            psc_interface_config=None,
            reserved_ip_ranges=None,
            labels=None,
        )
        mock_aiplatform_init.assert_called_once()
        mock_create_ray_cluster.assert_called_once_with(
            head_node_type=TEST_NODE_RESOURCES,
            python_version=TEST_PYTHON_VERSION,
            ray_version=TEST_RAY_VERSION,
            network=None,
            service_account=None,
            cluster_name=TEST_CLUSTER_NAME,
            worker_node_types=[TEST_NODE_RESOURCES],
            custom_images=None,
            enable_metrics_collection=True,
            enable_logging=True,
            psc_interface_config=None,
            reserved_ip_ranges=None,
            labels=None,
        )

    @mock.patch(RAY_STRING.format("vertex_ray.delete_ray_cluster"))
    @mock.patch(RAY_STRING.format("aiplatform.init"))
    @mock.patch(RAY_STRING.format("PersistentResourceServiceClient.persistent_resource_path"))
    def test_delete_ray_cluster(
        self, mock_persistent_resource_path, mock_aiplatform_init, mock_delete_ray_cluster
    ) -> None:
        self.hook.delete_ray_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_NAME,
        )
        mock_aiplatform_init.assert_called_once()
        mock_persistent_resource_path.assert_called_once_with(
            project=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            persistent_resource=TEST_CLUSTER_NAME,
        )
        mock_delete_ray_cluster.assert_called_once_with(
            cluster_resource_name=mock_persistent_resource_path.return_value,
        )

    @mock.patch(RAY_STRING.format("vertex_ray.get_ray_cluster"))
    @mock.patch(RAY_STRING.format("aiplatform.init"))
    @mock.patch(RAY_STRING.format("PersistentResourceServiceClient.persistent_resource_path"))
    def test_get_ray_cluster(
        self, mock_persistent_resource_path, mock_aiplatform_init, mock_get_ray_cluster
    ) -> None:
        self.hook.get_ray_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_NAME,
        )
        mock_aiplatform_init.assert_called_once()
        mock_persistent_resource_path.assert_called_once_with(
            project=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            persistent_resource=TEST_CLUSTER_NAME,
        )
        mock_get_ray_cluster.assert_called_once_with(
            cluster_resource_name=mock_persistent_resource_path.return_value,
        )

    @mock.patch(RAY_STRING.format("vertex_ray.update_ray_cluster"))
    @mock.patch(RAY_STRING.format("aiplatform.init"))
    @mock.patch(RAY_STRING.format("PersistentResourceServiceClient.persistent_resource_path"))
    def test_update_ray_cluster(
        self, mock_persistent_resource_path, mock_aiplatform_init, mock_update_ray_cluster
    ) -> None:
        self.hook.update_ray_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_NAME,
            worker_node_types=[TEST_NODE_RESOURCES],
        )
        mock_aiplatform_init.assert_called_once()
        mock_persistent_resource_path.assert_called_once_with(
            project=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            persistent_resource=TEST_CLUSTER_NAME,
        )
        mock_update_ray_cluster.assert_called_once_with(
            cluster_resource_name=mock_persistent_resource_path.return_value,
            worker_node_types=[TEST_NODE_RESOURCES],
        )

    @mock.patch(RAY_STRING.format("vertex_ray.list_ray_clusters"))
    @mock.patch(RAY_STRING.format("aiplatform.init"))
    def test_list_ray_clusters(self, mock_aiplatform_init, mock_list_ray_clusters) -> None:
        self.hook.list_ray_clusters(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )
        mock_aiplatform_init.assert_called_once()
        mock_list_ray_clusters.assert_called_once()

    @mock.patch(RAY_STRING.format("aiplatform.init"))
    def test_serialize_cluster_obj(self, mock_aiplatform_init) -> None:
        RESOURCE_SAMPLE = {
            "accelerator_count": 0,
            "accelerator_type": None,
            "autoscaling_spec": None,
            "boot_disk_size_gb": 100,
            "boot_disk_type": "pd-ssd",
            "custom_image": None,
            "machine_type": "n1-standard-16",
            "node_count": 1,
        }
        SAMPLE_CLUSTER_SERIALIZED = {
            "cluster_resource_name": TEST_CLUSTER_NAME,
            "dashboard_address": "dashboard_addr",
            "head_node_type": RESOURCE_SAMPLE,
            "labels": {"label1": "val1"},
            "network": "custom_network",
            "psc_interface_config": None,
            "python_version": TEST_PYTHON_VERSION,
            "ray_logs_enabled": True,
            "ray_metric_enabled": True,
            "ray_version": TEST_RAY_VERSION,
            "reserved_ip_ranges": [
                "172.16.0.0/16",
                "10.10.10.0/28",
            ],
            "service_account": None,
            "state": "RUNNING",
            "worker_node_types": [RESOURCE_SAMPLE, RESOURCE_SAMPLE],
        }
        cluster_obj = Cluster(
            cluster_resource_name=TEST_CLUSTER_NAME,
            state="RUNNING",  # type: ignore[arg-type]
            network="custom_network",
            reserved_ip_ranges=["172.16.0.0/16", "10.10.10.0/28"],
            python_version=TEST_PYTHON_VERSION,
            ray_version=TEST_RAY_VERSION,
            head_node_type=Resources(**RESOURCE_SAMPLE),  # type: ignore[arg-type]
            worker_node_types=[
                Resources(**RESOURCE_SAMPLE),  # type: ignore[arg-type]
                Resources(**RESOURCE_SAMPLE),  # type: ignore[arg-type]
            ],
            dashboard_address="dashboard_addr",
            labels={"label1": "val1"},
        )

        assert self.hook.serialize_cluster_obj(cluster_obj) == SAMPLE_CLUSTER_SERIALIZED


class TestRayWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = RayHook(gcp_conn_id=TEST_GCP_CONN_ID)
            self.hook.get_credentials = mock.MagicMock()

    @mock.patch(RAY_STRING.format("vertex_ray.create_ray_cluster"))
    @mock.patch(RAY_STRING.format("aiplatform.init"))
    def test_create_ray_cluster(self, mock_aiplatform_init, mock_create_ray_cluster) -> None:
        self.hook.create_ray_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            head_node_type=TEST_NODE_RESOURCES,
            python_version=TEST_PYTHON_VERSION,
            ray_version=TEST_RAY_VERSION,
            network=None,
            service_account=None,
            cluster_name=TEST_CLUSTER_NAME,
            worker_node_types=[TEST_NODE_RESOURCES],
            custom_images=None,
            enable_metrics_collection=True,
            enable_logging=True,
            psc_interface_config=None,
            reserved_ip_ranges=None,
            labels=None,
        )
        mock_aiplatform_init.assert_called_once()
        mock_create_ray_cluster.assert_called_once_with(
            head_node_type=TEST_NODE_RESOURCES,
            python_version=TEST_PYTHON_VERSION,
            ray_version=TEST_RAY_VERSION,
            network=None,
            service_account=None,
            cluster_name=TEST_CLUSTER_NAME,
            worker_node_types=[TEST_NODE_RESOURCES],
            custom_images=None,
            enable_metrics_collection=True,
            enable_logging=True,
            psc_interface_config=None,
            reserved_ip_ranges=None,
            labels=None,
        )

    @mock.patch(RAY_STRING.format("vertex_ray.delete_ray_cluster"))
    @mock.patch(RAY_STRING.format("aiplatform.init"))
    @mock.patch(RAY_STRING.format("PersistentResourceServiceClient.persistent_resource_path"))
    def test_delete_ray_cluster(
        self, mock_persistent_resource_path, mock_aiplatform_init, mock_delete_ray_cluster
    ) -> None:
        self.hook.delete_ray_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_NAME,
        )
        mock_aiplatform_init.assert_called_once()
        mock_persistent_resource_path.assert_called_once_with(
            project=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            persistent_resource=TEST_CLUSTER_NAME,
        )
        mock_delete_ray_cluster.assert_called_once_with(
            cluster_resource_name=mock_persistent_resource_path.return_value,
        )

    @mock.patch(RAY_STRING.format("vertex_ray.get_ray_cluster"))
    @mock.patch(RAY_STRING.format("aiplatform.init"))
    @mock.patch(RAY_STRING.format("PersistentResourceServiceClient.persistent_resource_path"))
    def test_get_ray_cluster(
        self, mock_persistent_resource_path, mock_aiplatform_init, mock_get_ray_cluster
    ) -> None:
        self.hook.get_ray_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_NAME,
        )
        mock_aiplatform_init.assert_called_once()
        mock_persistent_resource_path.assert_called_once_with(
            project=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            persistent_resource=TEST_CLUSTER_NAME,
        )
        mock_get_ray_cluster.assert_called_once_with(
            cluster_resource_name=mock_persistent_resource_path.return_value,
        )

    @mock.patch(RAY_STRING.format("vertex_ray.update_ray_cluster"))
    @mock.patch(RAY_STRING.format("aiplatform.init"))
    @mock.patch(RAY_STRING.format("PersistentResourceServiceClient.persistent_resource_path"))
    def test_update_ray_cluster(
        self, mock_persistent_resource_path, mock_aiplatform_init, mock_update_ray_cluster
    ) -> None:
        self.hook.update_ray_cluster(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            cluster_id=TEST_CLUSTER_NAME,
            worker_node_types=[TEST_NODE_RESOURCES],
        )
        mock_aiplatform_init.assert_called_once()
        mock_persistent_resource_path.assert_called_once_with(
            project=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            persistent_resource=TEST_CLUSTER_NAME,
        )
        mock_update_ray_cluster.assert_called_once_with(
            cluster_resource_name=mock_persistent_resource_path.return_value,
            worker_node_types=[TEST_NODE_RESOURCES],
        )

    @mock.patch(RAY_STRING.format("vertex_ray.list_ray_clusters"))
    @mock.patch(RAY_STRING.format("aiplatform.init"))
    def test_list_ray_clusters(self, mock_aiplatform_init, mock_list_ray_clusters) -> None:
        self.hook.list_ray_clusters(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
        )
        mock_aiplatform_init.assert_called_once()
        mock_list_ray_clusters.assert_called_once()
