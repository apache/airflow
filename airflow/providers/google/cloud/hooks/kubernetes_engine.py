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
"""
This module contains a Google Kubernetes Engine Hook.

.. spelling::

    gapic
    enums
"""
from __future__ import annotations

import contextlib
import json
import time
import warnings
from typing import Sequence

import google.auth.credentials
from gcloud.aio.auth import Token
from google.api_core.exceptions import NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry
from google.auth.transport import requests as google_requests

# not sure why but mypy complains on missing `container_v1` but it is clearly there and is importable
from google.cloud import container_v1, exceptions  # type: ignore[attr-defined]
from google.cloud.container_v1 import ClusterManagerAsyncClient, ClusterManagerClient
from google.cloud.container_v1.types import Cluster, Operation
from kubernetes import client
from kubernetes_asyncio import client as async_client
from kubernetes_asyncio.client.models import V1Pod
from kubernetes_asyncio.config.kube_config import FileOrData
from urllib3.exceptions import HTTPError

from airflow import version
from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.kubernetes.pod_generator_deprecated import PodDefaults
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)

OPERATIONAL_POLL_INTERVAL = 15


class GKEHook(GoogleBaseHook):
    """
    Hook for managing Google Kubernetes Engine cluster APIs.

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
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self._client: ClusterManagerClient | None = None
        self.location = location

    def get_cluster_manager_client(self) -> ClusterManagerClient:
        """Returns ClusterManagerClient."""
        if self._client is None:
            self._client = ClusterManagerClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._client

    # To preserve backward compatibility
    # TODO: remove one day
    def get_conn(self) -> container_v1.ClusterManagerClient:
        warnings.warn(
            "The get_conn method has been deprecated. You should use the get_cluster_manager_client method.",
            DeprecationWarning,
        )
        return self.get_cluster_manager_client()

    # To preserve backward compatibility
    # TODO: remove one day
    def get_client(self) -> ClusterManagerClient:
        warnings.warn(
            "The get_client method has been deprecated. You should use the get_conn method.",
            DeprecationWarning,
        )
        return self.get_conn()

    def wait_for_operation(self, operation: Operation, project_id: str | None = None) -> Operation:
        """
        Given an operation, continuously fetches the status from Google Cloud until either
        completion or an error occurring

        :param operation: The Operation to wait for
        :param project_id: Google Cloud project ID
        :return: A new, updated operation fetched from Google Cloud
        """
        self.log.info("Waiting for OPERATION_NAME %s", operation.name)
        time.sleep(OPERATIONAL_POLL_INTERVAL)
        while operation.status != Operation.Status.DONE:
            if operation.status == Operation.Status.RUNNING or operation.status == Operation.Status.PENDING:
                time.sleep(OPERATIONAL_POLL_INTERVAL)
            else:
                raise exceptions.GoogleCloudError(f"Operation has failed with status: {operation.status}")
            # To update status of operation
            operation = self.get_operation(operation.name, project_id=project_id or self.project_id)
        return operation

    def get_operation(self, operation_name: str, project_id: str | None = None) -> Operation:
        """
        Fetches the operation from Google Cloud

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
        Append labels to provided Cluster Protobuf

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
        Deletes the cluster, including the Kubernetes endpoint and all
        worker nodes. Firewalls and routes that were configured during
        cluster creation are also deleted. Other Google Compute Engine
        resources that might be in use by the cluster (e.g. load balancer
        resources) will not be deleted if they were not present at the
        initial create time.

        :param name: The name of the cluster to delete
        :param project_id: Google Cloud project ID
        :param wait_to_complete: A boolean value which makes method to sleep while
            operation of deletion is not finished.
        :param retry: Retry object used to determine when/if to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :return: The full url to the delete operation if successful, else None
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
        Creates a cluster, consisting of the specified number and type of Google Compute
        Engine instances.

        :param cluster: A Cluster protobuf or dict. If dict is provided, it must
            be of the same form as the protobuf message
            :class:`google.cloud.container_v1.types.Cluster`
        :param project_id: Google Cloud project ID
        :param wait_to_complete: A boolean value which makes method to sleep while
            operation of creation is not finished.
        :param retry: A retry object (``google.api_core.retry.Retry``) used to
            retry requests.
            If None is specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :return: The full url to the new, or existing, cluster
        :raises:
            ParseError: On JSON parsing problems when trying to convert dict
            AirflowException: cluster is not dict type nor Cluster proto type
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
        Gets details of specified cluster

        :param name: The name of the cluster to retrieve
        :param project_id: Google Cloud project ID
        :param retry: A retry object used to retry requests. If None is specified,
            requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :return: google.cloud.container_v1.types.Cluster
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


class GKEAsyncHook(GoogleBaseAsyncHook):
    """Hook implemented with usage of asynchronous client of GKE."""

    sync_hook_class = GKEHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
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
        Fetches the operation from Google Cloud.

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


class GKEPodHook(GoogleBaseHook):
    """Hook for managing Google Kubernetes Engine pod APIs."""

    def __init__(
        self,
        cluster_url: str,
        ssl_ca_cert: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._cluster_url = cluster_url
        self._ssl_ca_cert = ssl_ca_cert

    @cached_property
    def api_client(self) -> client.ApiClient:
        return self.get_conn()

    @cached_property
    def core_v1_client(self) -> client.CoreV1Api:
        return client.CoreV1Api(self.api_client)

    @property
    def is_in_cluster(self) -> bool:
        return False

    @staticmethod
    def get_xcom_sidecar_container_image():
        """Returns the xcom sidecar image that defined in the connection"""
        return PodDefaults.SIDECAR_CONTAINER.image

    def get_conn(self) -> client.ApiClient:
        configuration = self._get_config()
        return client.ApiClient(configuration)

    def _get_config(self) -> client.configuration.Configuration:
        configuration = client.Configuration(
            host=self._cluster_url,
            api_key_prefix={"authorization": "Bearer"},
            api_key={"authorization": self._get_token(self.get_credentials())},
        )
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

    def get_pod(self, name: str, namespace: str) -> V1Pod:
        """
        Gets pod's object.

        :param name: Name of the pod.
        :param namespace: Name of the pod's namespace.
        """
        return self.core_v1_client.read_namespaced_pod(
            name=name,
            namespace=namespace,
        )


class GKEPodAsyncHook(GoogleBaseAsyncHook):
    """
    Hook for managing Google Kubernetes Engine pods APIs in asynchronous way.

    :param cluster_url: The URL pointed to the cluster.
    :param ssl_ca_cert: SSL certificate that is used for authentication to the pod.
    """

    sync_hook_class = GKEPodHook
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    def __init__(
        self,
        cluster_url: str,
        ssl_ca_cert: str,
        **kwargs,
    ):

        self._cluster_url = cluster_url
        self._ssl_ca_cert = ssl_ca_cert

        kwargs.update(
            cluster_url=cluster_url,
            ssl_ca_cert=ssl_ca_cert,
        )
        super().__init__(**kwargs)

    @contextlib.asynccontextmanager
    async def get_conn(self, token: Token) -> async_client.ApiClient:  # type: ignore[override]
        kube_client = None
        try:
            kube_client = await self._load_config(token)
            yield kube_client
        finally:
            if kube_client is not None:
                await kube_client.close()

    async def _load_config(self, token: Token) -> async_client.ApiClient:
        configuration = self._get_config()
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

    async def get_pod(self, name: str, namespace: str) -> V1Pod:
        """
        Gets pod's object.

        :param name: Name of the pod.
        :param namespace: Name of the pod's namespace.
        """
        async with Token(scopes=self.scopes) as token:
            async with self.get_conn(token) as connection:
                v1_api = async_client.CoreV1Api(connection)
                pod: V1Pod = await v1_api.read_namespaced_pod(
                    name=name,
                    namespace=namespace,
                )
            return pod

    async def delete_pod(self, name: str, namespace: str):
        """
        Deletes pod's object.

        :param name: Name of the pod.
        :param namespace: Name of the pod's namespace.
        """
        async with Token(scopes=self.scopes) as token:
            async with self.get_conn(token) as connection:
                try:
                    v1_api = async_client.CoreV1Api(connection)
                    await v1_api.delete_namespaced_pod(
                        name=name,
                        namespace=namespace,
                        body=client.V1DeleteOptions(),
                    )
                except async_client.ApiException as e:
                    # If the pod is already deleted
                    if e.status != 404:
                        raise

    async def read_logs(self, name: str, namespace: str):
        """
        Reads logs inside the pod while starting containers inside. All the logs will be outputted with its
        timestamp to track the logs after the execution of the pod is completed. The method is used for async
        output of the logs only in the pod failed it execution or the task was cancelled by the user.

        :param name: Name of the pod.
        :param namespace: Name of the pod's namespace.
        """
        async with Token(scopes=self.scopes) as token:
            async with self.get_conn(token) as connection:
                try:
                    v1_api = async_client.CoreV1Api(connection)
                    logs = await v1_api.read_namespaced_pod_log(
                        name=name,
                        namespace=namespace,
                        follow=False,
                        timestamps=True,
                    )
                    logs = logs.splitlines()
                    for line in logs:
                        self.log.info("Container logs from %s", line)
                    return logs
                except HTTPError:
                    self.log.exception("There was an error reading the kubernetes API.")
                    raise
