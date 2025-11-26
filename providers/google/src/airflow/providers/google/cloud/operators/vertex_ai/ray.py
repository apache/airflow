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
"""This module contains Google Vertex AI Ray operators."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Literal

from google.api_core.exceptions import NotFound
from google.cloud.aiplatform.vertex_ray.util import resources

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.ray import RayHook
from airflow.providers.google.cloud.links.vertex_ai import (
    VertexAIRayClusterLink,
    VertexAIRayClusterListLink,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class RayBaseOperator(GoogleCloudBaseOperator):
    """
    Base class for Ray operators.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "location",
        "gcp_conn_id",
        "project_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        project_id: str,
        location: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.location = location
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    @cached_property
    def hook(self) -> RayHook:
        return RayHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class CreateRayClusterOperator(RayBaseOperator):
    """
    Create a Ray cluster on the Vertex AI.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param head_node_type: The head node resource. Resources.node_count must be 1. If not set, default
        value of Resources() class will be used.
    :param python_version: Required. Python version for the ray cluster.
    :param ray_version: Required. Ray version for the ray cluster.
        Currently only 3 version are available: 2.9.3, 2.33, 2.42. For more information please refer to
        https://github.com/googleapis/python-aiplatform/blob/main/setup.py#L101
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
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"head_node_type", "worker_node_types"} | set(RayBaseOperator.template_fields)
    )
    operator_extra_links = (VertexAIRayClusterLink(),)

    def __init__(
        self,
        python_version: str,
        ray_version: Literal["2.9.3", "2.33", "2.42"],
        head_node_type: resources.Resources = resources.Resources(),
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
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.head_node_type = head_node_type
        self.python_version = python_version
        self.ray_version = ray_version
        self.network = network
        self.service_account = service_account
        self.cluster_name = cluster_name
        self.worker_node_types = worker_node_types
        self.custom_images = custom_images
        self.enable_metrics_collection = enable_metrics_collection
        self.enable_logging = enable_logging
        self.psc_interface_config = psc_interface_config
        self.reserved_ip_ranges = reserved_ip_ranges
        self.labels = labels

    def execute(self, context: Context):
        self.log.info("Creating a Ray cluster.")
        try:
            cluster_path = self.hook.create_ray_cluster(
                project_id=self.project_id,
                location=self.location,
                head_node_type=self.head_node_type,
                python_version=self.python_version,
                ray_version=self.ray_version,
                network=self.network,
                service_account=self.service_account,
                cluster_name=self.cluster_name,
                worker_node_types=self.worker_node_types,
                custom_images=self.custom_images,
                enable_metrics_collection=self.enable_metrics_collection,
                enable_logging=self.enable_logging,
                psc_interface_config=self.psc_interface_config,
                reserved_ip_ranges=self.reserved_ip_ranges,
                labels=self.labels,
            )
            cluster_id = self.hook.extract_cluster_id(cluster_path)
            context["ti"].xcom_push(
                key="cluster_id",
                value=cluster_id,
            )
            VertexAIRayClusterLink.persist(
                context=context, location=self.location, cluster_id=cluster_id, project_id=self.project_id
            )
            self.log.info("Ray cluster was created.")
        except Exception as error:
            raise AirflowException(error)
        return cluster_path


class ListRayClustersOperator(RayBaseOperator):
    """
    List Ray clusters under the currently authenticated project.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    operator_extra_links = (VertexAIRayClusterListLink(),)

    def execute(self, context: Context):
        VertexAIRayClusterListLink.persist(context=context, project_id=self.project_id)
        self.log.info("Listing Clusters from location %s.", self.location)
        try:
            ray_cluster_list = self.hook.list_ray_clusters(
                project_id=self.project_id,
                location=self.location,
            )
            ray_cluster_dict_list = [
                self.hook.serialize_cluster_obj(ray_cluster) for ray_cluster in ray_cluster_list
            ]
        except Exception as error:
            raise AirflowException(error)
        return ray_cluster_dict_list


class GetRayClusterOperator(RayBaseOperator):
    """
    Get Ray cluster.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Cluster resource ID.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple({"cluster_id"} | set(RayBaseOperator.template_fields))
    operator_extra_links = (VertexAIRayClusterLink(),)

    def __init__(
        self,
        cluster_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id

    def execute(self, context: Context):
        VertexAIRayClusterLink.persist(
            context=context,
            location=self.location,
            cluster_id=self.cluster_id,
            project_id=self.project_id,
        )
        self.log.info("Getting Cluster: %s", self.cluster_id)
        try:
            ray_cluster = self.hook.get_ray_cluster(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
            )
            self.log.info("Cluster data has been retrieved.")
            ray_cluster_dict = self.hook.serialize_cluster_obj(ray_cluster)
            return ray_cluster_dict
        except NotFound as not_found_err:
            self.log.info("The Cluster %s does not exist.", self.cluster_id)
            raise AirflowException(not_found_err)


class UpdateRayClusterOperator(RayBaseOperator):
    """
    Update Ray cluster (currently support resizing node counts for worker nodes).

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Cluster resource ID.
    :param worker_node_types: The list of Resources of the resized worker nodes. The same Resources
        object should not appear multiple times in the list.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple(
        {"cluster_id", "worker_node_types"} | set(RayBaseOperator.template_fields)
    )
    operator_extra_links = (VertexAIRayClusterLink(),)

    def __init__(
        self,
        cluster_id: str,
        worker_node_types: list[resources.Resources],
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id
        self.worker_node_types = worker_node_types

    def execute(self, context: Context):
        VertexAIRayClusterLink.persist(
            context=context,
            location=self.location,
            cluster_id=self.cluster_id,
            project_id=self.project_id,
        )
        self.log.info("Updating a Ray cluster.")
        try:
            cluster_path = self.hook.update_ray_cluster(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
                worker_node_types=self.worker_node_types,
            )
            self.log.info("Ray cluster %s was updated.", self.cluster_id)
            return cluster_path
        except NotFound as not_found_err:
            self.log.info("The Cluster %s does not exist.", self.cluster_id)
            raise AirflowException(not_found_err)
        except Exception as error:
            raise AirflowException(error)


class DeleteRayClusterOperator(RayBaseOperator):
    """
    Delete Ray cluster.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud region that the service belongs to.
    :param cluster_id: Cluster resource ID.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = tuple({"cluster_id"} | set(RayBaseOperator.template_fields))

    def __init__(
        self,
        cluster_id: str,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.cluster_id = cluster_id

    def execute(self, context: Context):
        try:
            self.log.info("Deleting Ray cluster: %s", self.cluster_id)
            self.hook.delete_ray_cluster(
                project_id=self.project_id,
                location=self.location,
                cluster_id=self.cluster_id,
            )
            self.log.info("Ray cluster was deleted.")
        except NotFound as not_found_err:
            self.log.info("The Ray cluster ID %s does not exist.", self.cluster_id)
            raise AirflowException(not_found_err)
