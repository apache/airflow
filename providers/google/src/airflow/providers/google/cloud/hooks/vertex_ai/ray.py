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
"""This module contains a Google Cloud Vertex AI hook."""

from __future__ import annotations

import dataclasses
from collections.abc import MutableMapping
from typing import Any

import vertex_ray
from google.cloud import aiplatform
from google.cloud.aiplatform.vertex_ray.util import resources
from google.cloud.aiplatform_v1 import (
    PersistentResourceServiceClient,
)
from proto.marshal.collections.repeated import Repeated

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class RayHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Ray APIs."""

    def extract_cluster_id(self, cluster_path) -> str:
        """Extract cluster_id from cluster_path."""
        cluster_id = PersistentResourceServiceClient.parse_persistent_resource_path(cluster_path)[
            "persistent_resource"
        ]
        return cluster_id

    def serialize_cluster_obj(self, cluster_obj: resources.Cluster) -> dict:
        """Serialize Cluster dataclass to dict."""

        def __encode_value(value: Any) -> Any:
            if isinstance(value, (list, Repeated)):
                return [__encode_value(nested_value) for nested_value in value]
            if not isinstance(value, dict) and isinstance(value, MutableMapping):
                return {key: __encode_value(nested_value) for key, nested_value in dict(value).items()}
            if dataclasses.is_dataclass(value) and not isinstance(value, type):
                return dataclasses.asdict(value)
            return value

        return {
            field.name: __encode_value(getattr(cluster_obj, field.name))
            for field in dataclasses.fields(cluster_obj)
        }

    @GoogleBaseHook.fallback_to_default_project_id
    def create_ray_cluster(
        self,
        project_id: str,
        location: str,
        head_node_type: resources.Resources = resources.Resources(),
        python_version: str = "3.10",
        ray_version: str = "2.33",
        network: str | None = None,
        service_account: str | None = None,
        cluster_name: str | None = None,
        worker_node_types: list[resources.Resources] | None = None,
        custom_images: resources.NodeImages | None = None,
        enable_metrics_collection: bool = True,
        enable_logging: bool = True,
        psc_interface_config: resources.PscIConfig | None = None,
        reserved_ip_ranges: list[str] | None = None,
        labels: dict[str, str] | None = None,
    ) -> str:
        """
        Create a Ray cluster on the Vertex AI.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param head_node_type: The head node resource. Resources.node_count must be 1. If not set, default
            value of Resources() class will be used.
        :param python_version: Python version for the ray cluster.
        :param ray_version: Ray version for the ray cluster. Default is 2.33.0.
        :param network: Virtual private cloud (VPC) network. For Ray Client, VPC peering is required to
            connect to the Ray Cluster managed in the Vertex API service. For Ray Job API, VPC network is not
            required because Ray Cluster connection can be accessed through dashboard address.
        :param service_account: Service account to be used for running Ray programs on the cluster.
        :param cluster_name: This value may be up to 63 characters, and valid characters are `[a-z0-9_-]`.
            The first character cannot be a number or hyphen.
        :param worker_node_types: The list of Resources of the worker nodes. The same Resources object should
            not appear multiple times in the list.
        :param custom_images: The NodeImages which specifies head node and worker nodes images. All the
            workers will share the same image. If each Resource has a specific custom image, use
            `Resources.custom_image` for head/worker_node_type(s). Note that configuring
            `Resources.custom_image` will override `custom_images` here. Allowlist only.
        :param enable_metrics_collection: Enable Ray metrics collection for visualization.
        :param enable_logging: Enable exporting Ray logs to Cloud Logging.
        :param psc_interface_config: PSC-I config.
        :param reserved_ip_ranges: A list of names for the reserved IP ranges under the VPC network that can
            be used for this cluster. If set, we will deploy the cluster within the provided IP ranges.
            Otherwise, the cluster is deployed to any IP ranges under the provided VPC network.
            Example: ["vertex-ai-ip-range"].
        :param labels: The labels with user-defined metadata to organize Ray cluster.
            Label keys and values can be no longer than 64 characters (Unicode codepoints), can only contain
            lowercase letters, numeric characters, underscores and dashes. International characters are allowed.
            See https://goo.gl/xmQnxf for more information and examples of labels.
        """
        aiplatform.init(project=project_id, location=location, credentials=self.get_credentials())
        cluster_path = vertex_ray.create_ray_cluster(
            head_node_type=head_node_type,
            python_version=python_version,
            ray_version=ray_version,
            network=network,
            service_account=service_account,
            cluster_name=cluster_name,
            worker_node_types=worker_node_types,
            custom_images=custom_images,
            enable_metrics_collection=enable_metrics_collection,
            enable_logging=enable_logging,
            psc_interface_config=psc_interface_config,
            reserved_ip_ranges=reserved_ip_ranges,
            labels=labels,
        )
        return cluster_path

    @GoogleBaseHook.fallback_to_default_project_id
    def list_ray_clusters(
        self,
        project_id: str,
        location: str,
    ) -> list[resources.Cluster]:
        """
        List Ray clusters under the currently authenticated project.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        """
        aiplatform.init(project=project_id, location=location, credentials=self.get_credentials())
        ray_clusters = vertex_ray.list_ray_clusters()
        return ray_clusters

    @GoogleBaseHook.fallback_to_default_project_id
    def get_ray_cluster(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
    ) -> resources.Cluster:
        """
        Get Ray cluster.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param cluster_id: Cluster resource ID.
        """
        aiplatform.init(project=project_id, location=location, credentials=self.get_credentials())
        ray_cluster_name = PersistentResourceServiceClient.persistent_resource_path(
            project=project_id,
            location=location,
            persistent_resource=cluster_id,
        )
        ray_cluster = vertex_ray.get_ray_cluster(
            cluster_resource_name=ray_cluster_name,
        )
        return ray_cluster

    @GoogleBaseHook.fallback_to_default_project_id
    def update_ray_cluster(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
        worker_node_types: list[resources.Resources],
    ) -> str:
        """
        Update Ray cluster (currently support resizing node counts for worker nodes).

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param cluster_id: Cluster resource ID.
        :param worker_node_types: The list of Resources of the resized worker nodes. The same Resources
            object should not appear multiple times in the list.
        """
        aiplatform.init(project=project_id, location=location, credentials=self.get_credentials())
        ray_cluster_name = PersistentResourceServiceClient.persistent_resource_path(
            project=project_id,
            location=location,
            persistent_resource=cluster_id,
        )
        updated_ray_cluster_name = vertex_ray.update_ray_cluster(
            cluster_resource_name=ray_cluster_name, worker_node_types=worker_node_types
        )
        return updated_ray_cluster_name

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_ray_cluster(
        self,
        project_id: str,
        location: str,
        cluster_id: str,
    ) -> None:
        """
        Delete Ray cluster.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param cluster_id: Cluster resource ID.
        """
        aiplatform.init(project=project_id, location=location, credentials=self.get_credentials())
        ray_cluster_name = PersistentResourceServiceClient.persistent_resource_path(
            project=project_id,
            location=location,
            persistent_resource=cluster_id,
        )
        vertex_ray.delete_ray_cluster(cluster_resource_name=ray_cluster_name)
