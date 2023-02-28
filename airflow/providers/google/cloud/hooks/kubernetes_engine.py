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

import json
import time
import warnings
from typing import Sequence

from google.api_core.exceptions import AlreadyExists, NotFound
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.api_core.retry import Retry

# not sure why but mypy complains on missing `container_v1` but it is clearly there and is importable
from google.cloud import container_v1, exceptions  # type: ignore[attr-defined]
from google.cloud.container_v1 import ClusterManagerClient
from google.cloud.container_v1.types import Cluster, Operation

from airflow import version
from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

OPERATIONAL_POLL_INTERVAL = 15


class GKEHook(GoogleBaseHook):
    """
    Hook for Google Kubernetes Engine APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
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
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
    ) -> str | None:
        """
        Deletes the cluster, including the Kubernetes endpoint and all
        worker nodes. Firewalls and routes that were configured during
        cluster creation are also deleted. Other Google Compute Engine
        resources that might be in use by the cluster (e.g. load balancer
        resources) will not be deleted if they were not present at the
        initial create time.

        :param name: The name of the cluster to delete
        :param project_id: Google Cloud project ID
        :param retry: Retry object used to determine when/if to retry requests.
            If None is specified, requests will not be retried.
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :return: The full url to the delete operation if successful, else None
        """
        self.log.info("Deleting (project_id=%s, location=%s, cluster_id=%s)", project_id, self.location, name)

        try:
            resource = self.get_cluster_manager_client().delete_cluster(
                name=f"projects/{project_id}/locations/{self.location}/clusters/{name}",
                retry=retry,
                timeout=timeout,
            )
            resource = self.wait_for_operation(resource, project_id)
            # Returns server-defined url for the resource
            return resource.self_link
        except NotFound as error:
            self.log.info("Assuming Success: %s", error.message)
            return None

    @GoogleBaseHook.fallback_to_default_project_id
    def create_cluster(
        self,
        cluster: dict | Cluster | None,
        project_id: str = PROVIDE_PROJECT_ID,
        retry: Retry | _MethodDefault = DEFAULT,
        timeout: float | None = None,
    ) -> str:
        """
        Creates a cluster, consisting of the specified number and type of Google Compute
        Engine instances.

        :param cluster: A Cluster protobuf or dict. If dict is provided, it must
            be of the same form as the protobuf message
            :class:`google.cloud.container_v1.types.Cluster`
        :param project_id: Google Cloud project ID
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
        try:
            resource = self.get_cluster_manager_client().create_cluster(
                parent=f"projects/{project_id}/locations/{self.location}",
                cluster=cluster,  # type: ignore
                retry=retry,
                timeout=timeout,
            )
            resource = self.wait_for_operation(resource, project_id)

            return resource.target_link
        except AlreadyExists as error:
            self.log.info("Assuming Success: %s", error.message)
            return self.get_cluster(name=cluster.name, project_id=project_id)  # type: ignore

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

        return (
            self.get_cluster_manager_client()
            .get_cluster(
                name=f"projects/{project_id}/locations/{self.location}/clusters/{name}",
                retry=retry,
                timeout=timeout,
            )
            .self_link
        )
