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
"""This module contains a Google Kubernetes Engine Hook."""

from __future__ import annotations

import contextlib
import json
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.auth.transport import requests as google_requests

# not sure why but mypy complains on missing `container_v1` but it is clearly there and is importable
from google.cloud import exceptions
from google.cloud.container_v1 import ClusterManagerAsyncClient, ClusterManagerClient
from google.cloud.container_v1.types import Cluster, Operation
from kubernetes import client
from kubernetes_asyncio import client as async_client
from kubernetes_asyncio.config.kube_config import FileOrData

from airflow import version
from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook, KubernetesHook
from airflow.providers.cncf.kubernetes.kube_client import _enable_tcp_keepalive
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)

if TYPE_CHECKING:
    import google.auth.credentials
    from google.api_core.retry import Retry

OPERATIONAL_POLL_INTERVAL = 15


class GKEClusterConnection:
    """Helper for establishing connection to GKE cluster."""

    def __init__(
        self,
        cluster_url: str,
        ssl_ca_cert: str,
        credentials: google.auth.credentials.Credentials,
        enable_tcp_keepalive: bool = False,
        use_dns_endpoint: bool = False,
    ):
        self._cluster_url = cluster_url
        self._ssl_ca_cert = ssl_ca_cert
        self._credentials = credentials
        self.enable_tcp_keepalive = enable_tcp_keepalive
        self.use_dns_endpoint = use_dns_endpoint

    def get_conn(self) -> client.ApiClient:
        configuration = self._get_config()
        configuration.refresh_api_key_hook = self._refresh_api_key_hook
        if self.enable_tcp_keepalive:
            _enable_tcp_keepalive()
        return client.ApiClient(configuration)

    def _refresh_api_key_hook(self, configuration: client.configuration.Configuration):
        configuration.api_key = {"authorization": self._get_token(self._credentials)}

    def _get_config(self) -> client.configuration.Configuration:
        configuration = client.Configuration(
            host=self._cluster_url,
            api_key_prefix={"authorization": "Bearer"},
            api_key={"authorization": self._get_token(self._credentials)},
        )
        if not self.use_dns_endpoint:
            configuration.ssl_ca_cert = FileOrData(
                {
                    "certificate-authority-data": self._ssl_ca_cert,
                },
                file_key_name="certificate-authority",
            ).as_file()
        return configuration

    @staticmethod
    def _get_token(creds: google.auth.credentials.Credentials) -> str:
        if creds.token is None or creds.expired:
            auth_req = google_requests.Request()
            creds.refresh(auth_req)
        return creds.token


class GKEHook(GoogleBaseHook):
    """
    Google Kubernetes Engine cluster APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )
        self._client: ClusterManagerClient | None = None
        self.location = location

    def get_cluster_manager_client(self) -> ClusterManagerClient:
        """Create or get a ClusterManagerClient."""
        if self._client is None:
            self._client = ClusterManagerClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._client

    def wait_for_operation(self, operation: Operation, project_id: str = PROVIDE_PROJECT_ID) -> Operation:
        """
        Continuously fetch the status from Google Cloud.

        This is done until the given operation completes, or raises an error.

        :param operation: The Operation to wait for.
        :param project_id: Google Cloud project ID.
        :return: A new, updated operation fetched from Google Cloud.
        """
        self.log.info("Waiting for OPERATION_NAME %s", operation.name)
        time.sleep(OPERATIONAL_POLL_INTERVAL)
        while operation.status != Operation.Status.DONE:
            if operation.status in (Operation.Status.RUNNING, Operation.Status.PENDING):
                time.sleep(OPERATIONAL_POLL_INTERVAL)
            else:
                raise exceptions.GoogleCloudError(f"Operation has failed with status: {operation.status}")
            # To update status of operation
            operation = self.get_operation(operation.name, project_id=project_id or self.project_id)
        return operation

    def get_operation(self, operation_name: str, project_id: str = PROVIDE_PROJECT_ID) -> Operation:
        """
        Get an operation from Google Cloud.

        :param operation_name: Name of operation to fetch
        :param project_id: Google Cloud project ID
        :return: The new, updated operation from Google Cloud
        """
        return self.get_cluster_manager_client().get_operation(
            name=(
                f"projects/{project_id or self.project_id}"
                f"/locations/{self.location}/operations/{operation_name}"
            )
        )

    @staticmethod
    def _append_label(cluster_proto: Cluster, key: str, val: str) -> Cluster:
        """
        Append labels to provided Cluster Protobuf.

        Labels must fit the regex ``[a-z]([-a-z0-9]*[a-z0-9])?`` (current
         airflow version string follows semantic versioning spec: x.y.z).

        :param cluster_proto: The proto to append resource_label airflow
            version to
        :param key: The key label
        :param val:
        :return: The cluster proto updated with new label
        """
        val = val.replace(".", "-").replace("+", "-")
        cluster_proto.resource_labels.update({key: val})
        return cluster_proto

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_cluster(
        self,
        name: str,
        project_id: str = PROVIDE_PROJECT_ID,
        wait_to_complete: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
    ) -> Operation | None:
        """
        Delete the cluster, the Kubernetes endpoint, and all worker nodes.

        Firewalls and routes that were configured during cluster creation are
        also deleted. Other Google Compute Engine resources that might be in use
        by the cluster (e.g. load balancer resources) will not be deleted if
        they were not present at the initial create time.

        :param name: The name of the cluster to delete.
        :param project_id: Google Cloud project ID.
        :param wait_to_complete: If *True*, wait until the deletion is finished
            before returning.
        :param retry: Retry object used to determine when/if to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to
            each individual attempt.
        :return: The full url to the delete operation if successful, else None.
        """
        self.log.info("Deleting (project_id=%s, location=%s, cluster_id=%s)", project_id, self.location, name)

        try:
            operation = self.get_cluster_manager_client().delete_cluster(
                name=f"projects/{project_id}/locations/{self.location}/clusters/{name}",
                retry=retry,
                timeout=timeout,
            )
            if wait_to_complete:
                operation = self.wait_for_operation(operation, project_id)
            # Returns server-defined url for the resource
            return operation
        except NotFound as error:
            self.log.info("Assuming Success: %s", error.message)
            return None

    @GoogleBaseHook.fallback_to_default_project_id
    def create_cluster(
        self,
        cluster: dict | Cluster,
        project_id: str = PROVIDE_PROJECT_ID,
        wait_to_complete: bool = True,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
    ) -> Operation | Cluster:
        """
        Create a cluster.

        This should consist of the specified number, and the type of Google
        Compute Engine instances.

        :param cluster: A Cluster protobuf or dict. If dict is provided, it must
            be of the same form as the protobuf message
            :class:`google.cloud.container_v1.types.Cluster`.
        :param project_id: Google Cloud project ID.
        :param wait_to_complete: A boolean value which makes method to sleep
            while operation of creation is not finished.
        :param retry: A retry object (``google.api_core.retry.Retry``) used to
            retry requests. If None is specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to
            each individual attempt.
        :return: The full url to the new, or existing, cluster.
        :raises ParseError: On JSON parsing problems when trying to convert
            dict.
        :raises AirflowException: cluster is not dict type nor Cluster proto
            type.
        """
        if isinstance(cluster, dict):
            cluster = Cluster.from_json(json.dumps(cluster))
        elif not isinstance(cluster, Cluster):
            raise AirflowException("cluster is not instance of Cluster proto or python dict")

        self._append_label(cluster, "airflow-version", "v" + version.version)  # type: ignore

        self.log.info(
            "Creating (project_id=%s, location=%s, cluster_name=%s)",
            project_id,
            self.location,
            cluster.name,  # type: ignore
        )
        operation = self.get_cluster_manager_client().create_cluster(
            parent=f"projects/{project_id}/locations/{self.location}",
            cluster=cluster,  # type: ignore
            retry=retry,
            timeout=timeout,
        )

        if wait_to_complete:
            operation = self.wait_for_operation(operation, project_id)

        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def get_cluster(
        self,
        name: str,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
    ) -> Cluster:
        """
        Get details of specified cluster.

        :param name: The name of the cluster to retrieve.
        :param project_id: Google Cloud project ID.
        :param retry: A retry object used to retry requests. If None is
            specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request
            to complete. Note that if retry is specified, the timeout applies to
            each individual attempt.
        """
        self.log.info(
            "Fetching cluster (project_id=%s, location=%s, cluster_name=%s)",
            project_id or self.project_id,
            self.location,
            name,
        )

        return self.get_cluster_manager_client().get_cluster(
            name=f"projects/{project_id}/locations/{self.location}/clusters/{name}",
            retry=retry,
            timeout=timeout,
        )

    def check_cluster_autoscaling_ability(self, cluster: Cluster | dict):
        """
        Check if the specified Cluster has ability to autoscale.

        Cluster should be Autopilot, with Node Auto-provisioning or regular auto-scaled node pools.
        Returns True if the Cluster supports autoscaling, otherwise returns False.

        :param cluster: The Cluster object.
        """
        if isinstance(cluster, Cluster):
            cluster_dict_representation = Cluster.to_dict(cluster)
        elif not isinstance(cluster, dict):
            raise AirflowException("cluster is not instance of Cluster proto or python dict")
        else:
            cluster_dict_representation = cluster

        node_pools_autoscaled = False
        for node_pool in cluster_dict_representation["node_pools"]:
            try:
                if node_pool["autoscaling"]["enabled"] is True:
                    node_pools_autoscaled = True
                    break
            except KeyError:
                self.log.info("No autoscaling enabled in Node pools level.")
                break
        if (
            cluster_dict_representation["autopilot"]["enabled"]
            or cluster_dict_representation["autoscaling"]["enable_node_autoprovisioning"]
            or node_pools_autoscaled
        ):
            return True
        return False


class GKEAsyncHook(GoogleBaseAsyncHook):
    """Asynchronous client of GKE."""

    sync_hook_class = GKEHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )
        self._client: ClusterManagerAsyncClient | None = None
        self.location = location

    async def _get_client(self) -> ClusterManagerAsyncClient:
        if self._client is None:
            self._client = ClusterManagerAsyncClient(
                credentials=(await self.get_sync_hook()).get_credentials(),
                client_info=CLIENT_INFO,
            )
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    async def get_operation(
        self,
        operation_name: str,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> Operation:
        """
        Fetch an operation from Google Cloud.

        :param operation_name: Name of operation to fetch.
        :param project_id: Google Cloud project ID.
        :return: The new, updated operation from Google Cloud.
        """
        project_id = project_id or (await self.get_sync_hook()).project_id

        operation_path = f"projects/{project_id}/locations/{self.location}/operations/{operation_name}"
        client = await self._get_client()
        return await client.get_operation(
            name=operation_path,
        )


class GKEKubernetesHook(GoogleBaseHook, KubernetesHook):
    """
    GKE authenticated hook for standard Kubernetes API.

    This hook provides full set of the standard Kubernetes API provided by the KubernetesHook,
    and at the same time it provides a GKE authentication, so it makes it possible to KubernetesHook
    functionality against GKE clusters.
    """

    def __init__(
        self,
        cluster_url: str,
        ssl_ca_cert: str,
        enable_tcp_keepalive: bool = False,
        use_dns_endpoint: bool = False,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._cluster_url = cluster_url
        self._ssl_ca_cert = ssl_ca_cert
        self.enable_tcp_keepalive = enable_tcp_keepalive
        self.use_dns_endpoint = use_dns_endpoint

    def get_conn(self) -> client.ApiClient:
        return GKEClusterConnection(
            cluster_url=self._cluster_url,
            ssl_ca_cert=self._ssl_ca_cert,
            credentials=self.get_credentials(),
            enable_tcp_keepalive=self.enable_tcp_keepalive,
            use_dns_endpoint=self.use_dns_endpoint,
        ).get_conn()

    def apply_from_yaml_file(
        self,
        api_client: Any = None,
        yaml_file: str | None = None,
        yaml_objects: list[dict] | None = None,
        verbose: bool = False,
        namespace: str = "default",
    ):
        """
        Perform an action from a yaml file.

        :param api_client: A Kubernetes client application.
        :param yaml_file: Contains the path to yaml file.
        :param yaml_objects: List of YAML objects; used instead of reading the yaml_file.
        :param verbose: If True, print confirmation from create action. Default is False.
        :param namespace: Contains the namespace to create all resources inside. The namespace must
            preexist otherwise the resource creation will fail.
        """
        super().apply_from_yaml_file(
            api_client=api_client or self.get_conn(),
            yaml_file=yaml_file,
            yaml_objects=yaml_objects,
            verbose=verbose,
            namespace=namespace,
        )


class GKEKubernetesAsyncHook(GoogleBaseAsyncHook, AsyncKubernetesHook):
    """
    Async GKE authenticated hook for standard Kubernetes API.

    This hook provides full set of the standard Kubernetes API provided by the AsyncKubernetesHook,
    and at the same time it provides a GKE authentication, so it makes it possible to KubernetesHook
    functionality against GKE clusters.
    """

    sync_hook_class = GKEKubernetesHook
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    def __init__(
        self,
        cluster_url: str,
        ssl_ca_cert: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        enable_tcp_keepalive: bool = True,
        **kwargs,
    ) -> None:
        self._cluster_url = cluster_url
        self._ssl_ca_cert = ssl_ca_cert
        self.enable_tcp_keepalive = enable_tcp_keepalive
        super().__init__(
            cluster_url=cluster_url,
            ssl_ca_cert=ssl_ca_cert,
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )

    @contextlib.asynccontextmanager
    async def get_conn(self) -> async_client.ApiClient:
        kube_client = None
        try:
            kube_client = await self._load_config()
            yield kube_client
        finally:
            if kube_client is not None:
                await kube_client.close()

    async def _load_config(self) -> async_client.ApiClient:
        configuration = self._get_config()
        token = await self.get_token()
        access_token = await token.get()
        return async_client.ApiClient(
            configuration,
            header_name="Authorization",
            header_value=f"Bearer {access_token}",
        )

    def _get_config(self) -> async_client.configuration.Configuration:
        configuration = async_client.Configuration(
            host=self._cluster_url,
            ssl_ca_cert=FileOrData(
                {
                    "certificate-authority-data": self._ssl_ca_cert,
                },
                file_key_name="certificate-authority",
            ).as_file(),
        )
        return configuration
