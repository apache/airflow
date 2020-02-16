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
import unittest

import mock
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.container_v1.gapic import enums
from google.cloud.container_v1.proto import cluster_service_pb2

from airflow.providers.google.cloud.hooks import container

PROJECT_ID = 'test-project'
CREDENTIALS = 'test-credentials'
ZONE = 'asia-northeast1-a'
CLUSTER = {
    "name": "test-cluster",
    "nodePools": [
        {
            "name": "pool1",
            "initialNodeCount": 3
        },
        {
            "name": "pool2",
            "initialNodeCount": 5
        }
    ],
    "locations": [
        "asia-northeast1-a"
    ]
}
CLUSTER_ID = 'test-cluster'
NODE_POOL = {
    "name": "test-pool",
    "initialNodeCount": 3
}
NODE_POOL_ID = 'node-pool-id'
NODE_COUNT = 3


class TestContainerHookMethods(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
                return_value=(CREDENTIALS, PROJECT_ID))
    @mock.patch('airflow.providers.google.cloud.hooks.container.ContainerHook._get_client')
    def test_create_gke_cluster_with_wait_for_completion(self, mock_client, _):
        operation1 = cluster_service_pb2.Operation()
        operation1.name = 'operation-id'
        operation1.status = enums.Operation.Status.RUNNING
        operation1.zone = ZONE
        mock_client.return_value.create_cluster.return_value = operation1

        operation2 = cluster_service_pb2.Operation()
        operation2.name = 'operation-id'
        operation2.status = enums.Operation.Status.DONE
        operation2.zone = ZONE
        mock_client.return_value.get_operation.return_value = operation2

        hook = container.ContainerHook()
        hook.create_gke_cluster(
            zone=ZONE,
            cluster=CLUSTER
        )

        mock_client.return_value.create_cluster.assert_called_once_with(
            zone=ZONE,
            cluster=CLUSTER,
            project_id=PROJECT_ID,
            parent=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )

        mock_client.return_value.get_operation.assert_called_once_with(
            zone=ZONE,
            operation_id='operation-id',
            project_id=PROJECT_ID
        )

    @mock.patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
                return_value=(CREDENTIALS, PROJECT_ID))
    @mock.patch('airflow.providers.google.cloud.hooks.container.ContainerHook._get_client')
    def test_create_gke_cluster_without_wait_for_completion(self, mock_client, _):
        hook = container.ContainerHook()
        hook.create_gke_cluster(
            zone=ZONE,
            cluster=CLUSTER,
            wait_for_completion=False
        )
        mock_client.return_value.create_cluster.assert_called_once_with(
            zone=ZONE,
            cluster=CLUSTER,
            project_id=PROJECT_ID,
            parent=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )

        mock_client.return_value._wait_for_operation_complete.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
                return_value=(CREDENTIALS, PROJECT_ID))
    @mock.patch('airflow.providers.google.cloud.hooks.container.ContainerHook._get_client')
    def test_create_node_pool_with_wait_on_completion(self, mock_client, _):
        operation1 = cluster_service_pb2.Operation()
        operation1.name = 'operation-id'
        operation1.status = enums.Operation.Status.RUNNING
        operation1.zone = ZONE
        mock_client.return_value.create_node_pool.return_value = operation1

        operation2 = cluster_service_pb2.Operation()
        operation2.name = 'operation-id'
        operation2.status = enums.Operation.Status.DONE
        operation2.zone = ZONE
        mock_client.return_value.get_operation.return_value = operation2

        hook = container.ContainerHook()
        hook.create_node_pool(
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool=NODE_POOL
        )

        mock_client.return_value.create_node_pool.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool=NODE_POOL,
            parent=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )

        mock_client.return_value.get_operation.assert_called_once_with(
            zone=ZONE,
            operation_id='operation-id',
            project_id=PROJECT_ID
        )

    @mock.patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
                return_value=(CREDENTIALS, PROJECT_ID))
    @mock.patch('airflow.providers.google.cloud.hooks.container.ContainerHook._get_client')
    def test_create_node_pool_without_wait_on_completion(self, mock_client, _):
        hook = container.ContainerHook()
        hook.create_node_pool(
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool=NODE_POOL,
            wait_for_completion=False
        )
        mock_client.return_value.create_node_pool.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool=NODE_POOL,
            parent=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )
        mock_client.return_value._wait_for_operation_complete.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
                return_value=(CREDENTIALS, PROJECT_ID))
    @mock.patch('airflow.providers.google.cloud.hooks.container.ContainerHook._get_client')
    def test_delete_gke_cluster_with_wait_for_completion(self, mock_client, _):
        operation1 = cluster_service_pb2.Operation()
        operation1.name = 'operation-id'
        operation1.status = enums.Operation.Status.RUNNING
        operation1.zone = ZONE
        mock_client.return_value.delete_cluster.return_value = operation1

        operation2 = cluster_service_pb2.Operation()
        operation2.name = 'operation-id'
        operation2.status = enums.Operation.Status.DONE
        operation2.zone = ZONE
        mock_client.return_value.get_operation.return_value = operation2

        hook = container.ContainerHook()
        hook.delete_gke_cluster(
            zone=ZONE,
            cluster_id=CLUSTER_ID
        )

        mock_client.return_value.delete_cluster.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            name=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )

        mock_client.return_value.get_operation.assert_called_once_with(
            zone=ZONE,
            operation_id='operation-id',
            project_id=PROJECT_ID
        )

    @mock.patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
                return_value=(CREDENTIALS, PROJECT_ID))
    @mock.patch('airflow.providers.google.cloud.hooks.container.ContainerHook._get_client')
    def test_delete_gke_cluster_without_wait_on_completion(self, mock_client, _):
        hook = container.ContainerHook()
        hook.delete_gke_cluster(
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            wait_for_completion=False
        )
        mock_client.return_value.delete_cluster.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            name=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )
        mock_client.return_value._wait_for_operation_complete.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
                return_value=(CREDENTIALS, PROJECT_ID))
    @mock.patch('airflow.providers.google.cloud.hooks.container.ContainerHook._get_client')
    def test_delete_node_pool_with_wait_for_completion(self, mock_client, _):
        operation1 = cluster_service_pb2.Operation()
        operation1.name = 'operation-id'
        operation1.status = enums.Operation.Status.RUNNING
        operation1.zone = ZONE
        mock_client.return_value.delete_node_pool.return_value = operation1

        operation2 = cluster_service_pb2.Operation()
        operation2.name = 'operation-id'
        operation2.status = enums.Operation.Status.DONE
        operation2.zone = ZONE
        mock_client.return_value.get_operation.return_value = operation2

        hook = container.ContainerHook()
        hook.delete_node_pool(
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID
        )

        mock_client.return_value.delete_node_pool.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID,
            name=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )

        mock_client.return_value.get_operation.assert_called_once_with(
            zone=ZONE,
            operation_id='operation-id',
            project_id=PROJECT_ID
        )

    @mock.patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
                return_value=(CREDENTIALS, PROJECT_ID))
    @mock.patch('airflow.providers.google.cloud.hooks.container.ContainerHook._get_client')
    def test_delete_node_pool_without_wait_on_completion(self, mock_client, _):
        hook = container.ContainerHook()
        hook.delete_node_pool(
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID,
            wait_for_completion=False
        )
        mock_client.return_value.delete_node_pool.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID,
            name=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )
        mock_client.return_value._wait_for_operation_complete.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
                return_value=(CREDENTIALS, PROJECT_ID))
    @mock.patch('airflow.providers.google.cloud.hooks.container.ContainerHook._get_client')
    def test_set_node_pool_size_with_wait_on_completion(self, mock_client, _):
        operation1 = cluster_service_pb2.Operation()
        operation1.name = 'operation-id'
        operation1.status = enums.Operation.Status.RUNNING
        operation1.zone = ZONE
        mock_client.return_value.set_node_pool_size.return_value = operation1

        operation2 = cluster_service_pb2.Operation()
        operation2.name = 'operation-id'
        operation2.status = enums.Operation.Status.DONE
        operation2.zone = ZONE
        mock_client.return_value.get_operation.return_value = operation2

        hook = container.ContainerHook()
        hook.set_node_pool_size(
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID,
            node_count=NODE_COUNT
        )

        mock_client.return_value.set_node_pool_size.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID,
            node_count=NODE_COUNT,
            name=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )

        mock_client.return_value.get_operation.assert_called_once_with(
            zone=ZONE,
            operation_id='operation-id',
            project_id=PROJECT_ID
        )

    @mock.patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook._get_credentials_and_project_id',
                return_value=(CREDENTIALS, PROJECT_ID))
    @mock.patch('airflow.providers.google.cloud.hooks.container.ContainerHook._get_client')
    def test_set_node_pool_size_without_wait_on_completion(self, mock_client, _):
        hook = container.ContainerHook()
        hook.set_node_pool_size(
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID,
            node_count=NODE_COUNT,
            wait_for_completion=False
        )
        mock_client.return_value.set_node_pool_size.assert_called_once_with(
            project_id=PROJECT_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID,
            node_count=NODE_COUNT,
            name=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )
        mock_client.return_value._wait_for_operation_complete.assert_not_called()
