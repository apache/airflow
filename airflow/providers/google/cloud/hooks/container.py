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

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud import container_v1
from google.cloud.container_v1.gapic.enums import Operation

from airflow.providers.google.cloud.hooks.base import CloudBaseHook


class ContainerHook(CloudBaseHook):
    def __init__(self, gcp_conn_id='google_cloud_default', delegates_to=None):
        super().__init__(gcp_conn_id, delegates_to)
        self._client = None
        self._operation_completion_status = [
            Operation.Status.DONE
        ]

    def _get_client(self):
        if not self._client:
            self._client = container_v1.ClusterManagerClient()
        return self._client

    @CloudBaseHook.fallback_to_default_project_id
    def _wait_for_operation_complete(
        self,
        operation,
        project_id=None
    ):
        client = self._get_client()
        if operation is not None:
            operation_id = operation.name
            operation_zone = operation.zone
            operation_status = operation.status
            while operation_status not in self._operation_completion_status:
                op = client.get_operation(
                    project_id=project_id,
                    zone=operation_zone,
                    operation_id=operation_id,
                )
                operation_status = op.status

    @CloudBaseHook.fallback_to_default_project_id
    def create_gke_cluster(
        self,
        zone,
        cluster,
        wait_for_completion=True,
        project_id=None,
        parent=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
    ):
        client = self._get_client()
        operation = client.create_cluster(
            project_id=project_id,
            zone=zone,
            cluster=cluster,
            parent=parent,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        if wait_for_completion:
            self._wait_for_operation_complete(operation=operation, project_id=project_id)

    @CloudBaseHook.fallback_to_default_project_id
    def create_node_pool(
        self,
        zone,
        cluster_id,
        node_pool,
        wait_for_completion=True,
        project_id=None,
        parent=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
    ):
        client = self._get_client()
        operation = client.create_node_pool(
            project_id=project_id,
            zone=zone,
            cluster_id=cluster_id,
            node_pool=node_pool,
            parent=parent,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        if wait_for_completion:
            self._wait_for_operation_complete(operation=operation, project_id=project_id)

    @CloudBaseHook.fallback_to_default_project_id
    def delete_gke_cluster(
        self,
        zone,
        cluster_id,
        name=None,
        wait_for_completion=True,
        project_id=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
    ):
        client = self._get_client()
        operation = client.delete_cluster(
            project_id=project_id,
            zone=zone,
            cluster_id=cluster_id,
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        if wait_for_completion:
            self._wait_for_operation_complete(operation=operation, project_id=project_id)

    @CloudBaseHook.fallback_to_default_project_id
    def delete_node_pool(
        self,
        zone,
        cluster_id,
        node_pool_id,
        name=None,
        wait_for_completion=True,
        project_id=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
    ):
        client = self._get_client()
        operation = client.delete_node_pool(
            project_id=project_id,
            zone=zone,
            cluster_id=cluster_id,
            node_pool_id=node_pool_id,
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        if wait_for_completion:
            self._wait_for_operation_complete(operation=operation, project_id=project_id)

    @CloudBaseHook.fallback_to_default_project_id
    # pylint: disable=too-many-arguments
    def set_node_pool_size(
        self,
        zone,
        cluster_id,
        node_pool_id,
        node_count,
        wait_for_completion=True,
        name=None,
        project_id=None,
        retry=DEFAULT,
        timeout=DEFAULT,
        metadata=None,
    ):
        client = self._get_client()
        operation = client.set_node_pool_size(
            project_id=project_id,
            zone=zone,
            cluster_id=cluster_id,
            node_pool_id=node_pool_id,
            node_count=node_count,
            name=name,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )

        if wait_for_completion:
            self._wait_for_operation_complete(operation=operation, project_id=project_id)
