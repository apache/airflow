# -*- coding: utf-8 -*-
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
#
"""
This module contains a Google Kubernetes Engine Hook.
"""

import time
import warnings
from typing import Dict, Optional, Union

from google.api_core.exceptions import AlreadyExists, NotFound
from google.api_core.gapic_v1.method import DEFAULT
from google.api_core.retry import Retry
from google.cloud import container_v1, exceptions
from google.cloud.container_v1.gapic.enums import Operation
from google.cloud.container_v1.types import Cluster
from google.protobuf.json_format import ParseDict

from airflow import AirflowException, version
from airflow.providers.google.cloud.hooks.base import CloudBaseHook

OPERATIONAL_POLL_INTERVAL = 15


class GKEHook(CloudBaseHook):
    """
    Hook for Google Kubernetes Engine APIs.

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.
    """

    def __init__(
        self,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        location: Optional[str] = None
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id, delegate_to=delegate_to)
        self._client = None
        self.location = location

    def get_conn(self) -> container_v1.ClusterManagerClient:
        """
        Returns ClusterManagerCLinet object.

        :rtype: google.cloud.container_v1.ClusterManagerClient
        """
        if self._client is None:
            credentials = self._get_credentials()
            self._client = container_v1.ClusterManagerClient(
                credentials=credentials,
                client_info=self.client_info
            )
        return self._client

    # To preserve backward compatibility
    # TODO: remove one day
    def get_client(self) -> container_v1.ClusterManagerClient:  # pylint: disable=missing-docstring
        warnings.warn("The get_client method has been deprecated. "
                      "You should use the get_conn method.", DeprecationWarning)
        return self.get_conn()

    def wait_for_operation(self, operation: Operation, project_id: Optional[str] = None) -> Operation:
        """
        Given an operation, continuously fetches the status from Google Cloud until either
        completion or an error occurring

        :param operation: The Operation to wait for
        :type operation: google.cloud.container_V1.gapic.enums.Operation
        :param project_id: Google Cloud Platform project ID
        :type project_id: str
        :return: A new, updated operation fetched from Google Cloud
        """
        self.log.info("Waiting for OPERATION_NAME %s", operation.name)
        time.sleep(OPERATIONAL_POLL_INTERVAL)
        while operation.status != Operation.Status.DONE:
            if operation.status == Operation.Status.RUNNING or operation.status == \
                    Operation.Status.PENDING:
                time.sleep(OPERATIONAL_POLL_INTERVAL)
            else:
                raise exceptions.GoogleCloudError(
                    "Operation has failed with status: %s" % operation.status)
            # To update status of operation
            operation = self.get_operation(operation.name, project_id=project_id or self.project_id)
        return operation

    def get_operation(self, operation_name: str, project_id: Optional[str] = None) -> Operation:
        """
        Fetches the operation from Google Cloud

        :param operation_name: Name of operation to fetch
        :type operation_name: str
        :param project_id: Google Cloud Platform project ID
        :type project_id: str
        :return: The new, updated operation from Google Cloud
        """
        return self.get_conn().get_operation(project_id=project_id or self.project_id,
                                             zone=self.location,
                                             operation_id=operation_name)

    @staticmethod
    def _append_label(cluster_proto: Cluster, key: str, val: str) -> Cluster:
        """
        Append labels to provided Cluster Protobuf

        Labels must fit the regex ``[a-z]([-a-z0-9]*[a-z0-9])?`` (current
         airflow version string follows semantic versioning spec: x.y.z).

        :param cluster_proto: The proto to append resource_label airflow
            version to
        :type cluster_proto: google.cloud.container_v1.types.Cluster
        :param key: The key label
        :type key: str
        :param val:
        :type val: str
        :return: The cluster proto updated with new label
        """
        val = val.replace('.', '-').replace('+', '-')
        cluster_proto.resource_labels.update({key: val})
        return cluster_proto

    @CloudBaseHook.fallback_to_default_project_id
    def delete_cluster(
        self,
        name: str,
        project_id: Optional[str] = None,
        retry: Retry = DEFAULT,
        timeout: float = DEFAULT
    ) -> Optional[str]:
        """
        Deletes the cluster, including the Kubernetes endpoint and all
        worker nodes. Firewalls and routes that were configured during
        cluster creation are also deleted. Other Google Compute Engine
        resources that might be in use by the cluster (e.g. load balancer
        resources) will not be deleted if they weren’t present at the
        initial create time.

        :param name: The name of the cluster to delete
        :type name: str
        :param project_id: Google Cloud Platform project ID
        :type project_id: str
        :param retry: Retry object used to determine when/if to retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :return: The full url to the delete operation if successful, else None
        """

        self.log.info(
            "Deleting (project_id=%s, zone=%s, cluster_id=%s)", project_id, self.location, name
        )

        try:
            resource = self.get_conn().delete_cluster(project_id=project_id,
                                                      zone=self.location,
                                                      cluster_id=name,
                                                      retry=retry,
                                                      timeout=timeout)
            resource = self.wait_for_operation(resource)
            # Returns server-defined url for the resource
            return resource.self_link
        except NotFound as error:
            self.log.info('Assuming Success: %s', error.message)
            return None

    @CloudBaseHook.fallback_to_default_project_id
    def create_cluster(
        self,
        cluster: Union[Dict, Cluster],
        project_id: Optional[str] = None,
        retry: Retry = DEFAULT,
        timeout: float = DEFAULT
    ) -> str:
        """
        Creates a cluster, consisting of the specified number and type of Google Compute
        Engine instances.

        :param cluster: A Cluster protobuf or dict. If dict is provided, it must
            be of the same form as the protobuf message
            :class:`google.cloud.container_v1.types.Cluster`
        :type cluster: dict or google.cloud.container_v1.types.Cluster
        :param project_id: Google Cloud Platform project ID
        :type project_id: str
        :param retry: A retry object (``google.api_core.retry.Retry``) used to
            retry requests.
            If None is specified, requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :return: The full url to the new, or existing, cluster
        :raises:
            ParseError: On JSON parsing problems when trying to convert dict
            AirflowException: cluster is not dict type nor Cluster proto type
        """

        if isinstance(cluster, dict):
            cluster_proto = Cluster()
            cluster = ParseDict(cluster, cluster_proto)
        elif not isinstance(cluster, Cluster):
            raise AirflowException(
                "cluster is not instance of Cluster proto or python dict")

        self._append_label(cluster, 'airflow-version', 'v' + version.version)

        self.log.info(
            "Creating (project_id=%s, zone=%s, cluster_name=%s)",
            project_id, self.location, cluster.name
        )
        try:
            resource = self.get_conn().create_cluster(project_id=project_id,
                                                      zone=self.location,
                                                      cluster=cluster,
                                                      retry=retry,
                                                      timeout=timeout)
            resource = self.wait_for_operation(resource)

            return resource.target_link
        except AlreadyExists as error:
            self.log.info('Assuming Success: %s', error.message)
            return self.get_cluster(name=cluster.name)

    @CloudBaseHook.fallback_to_default_project_id
    def get_cluster(
        self,
        name: str,
        project_id: Optional[str] = None,
        retry: Retry = DEFAULT,
        timeout: float = DEFAULT
    ) -> Cluster:
        """
        Gets details of specified cluster

        :param name: The name of the cluster to retrieve
        :type name: str
        :param project_id: Google Cloud Platform project ID
        :type project_id: str
        :param retry: A retry object used to retry requests. If None is specified,
            requests will not be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The amount of time, in seconds, to wait for the request to
            complete. Note that if retry is specified, the timeout applies to each
            individual attempt.
        :type timeout: float
        :return: google.cloud.container_v1.types.Cluster
        """
        self.log.info(
            "Fetching cluster (project_id=%s, zone=%s, cluster_name=%s)",
            project_id or self.project_id, self.location, name
        )

        return self.get_conn().get_cluster(project_id=project_id,
                                           zone=self.location,
                                           cluster_id=name,
                                           retry=retry,
                                           timeout=timeout).self_link
