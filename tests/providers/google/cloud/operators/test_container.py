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

from airflow.providers.google.cloud.operators.container import (
    ContainerCreateGKEClusterOperator, ContainerCreateNodePoolOperator, ContainerDeleteGKEClusterOperator,
    ContainerDeleteNodePoolOperator, ContainerSetNodePoolSizeOperator,
)

TASK_ID = 'task-id'
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


class TestContainerCreateGKEClusterOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.container.ContainerHook')
    def test_execute(self, mock_hook):
        operator = ContainerCreateGKEClusterOperator(
            task_id=TASK_ID,
            zone=ZONE,
            cluster=CLUSTER
        )
        operator.execute(None)
        mock_hook.return_value.create_gke_cluster.assert_called_once_with(
            project_id=None,
            zone=ZONE,
            cluster=CLUSTER,
            wait_for_completion=True,
            parent=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )


class TestContainerCreateNodePoolOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.container.ContainerHook')
    def test_execute(self, mock_hook):
        operator = ContainerCreateNodePoolOperator(
            task_id=TASK_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool=NODE_POOL
        )
        operator.execute(None)
        mock_hook.return_value.create_node_pool.assert_called_once_with(
            project_id=None,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            wait_for_completion=True,
            node_pool=NODE_POOL,
            parent=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )


class TestDeleteGKEClusterOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.container.ContainerHook')
    def test_execute(self, mock_hook):
        operator = ContainerDeleteGKEClusterOperator(
            task_id=TASK_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID
        )
        operator.execute(None)
        mock_hook.return_value.delete_gke_cluster.assert_called_once_with(
            project_id=None,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            wait_for_completion=True,
            name=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )


class TestContainerDeleteNodePoolOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.container.ContainerHook')
    def test_execute(self, mock_hook):
        operator = ContainerDeleteNodePoolOperator(
            task_id=TASK_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID
        )
        operator.execute(None)
        mock_hook.return_value.delete_node_pool.assert_called_once_with(
            project_id=None,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID,
            wait_for_completion=True,
            name=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )


class TestContainerSetNodePoolSizeOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.container.ContainerHook')
    def test_execute(self, mock_hook):
        operator = ContainerSetNodePoolSizeOperator(
            task_id=TASK_ID,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID,
            node_count=NODE_COUNT
        )
        operator.execute(None)
        mock_hook.return_value.set_node_pool_size.assert_called_once_with(
            project_id=None,
            zone=ZONE,
            cluster_id=CLUSTER_ID,
            node_pool_id=NODE_POOL_ID,
            node_count=NODE_COUNT,
            wait_for_completion=True,
            name=None,
            retry=DEFAULT,
            timeout=DEFAULT,
            metadata=None
        )
