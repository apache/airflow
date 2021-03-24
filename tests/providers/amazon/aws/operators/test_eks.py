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
from typing import List
from unittest import mock

from moto.eks.responses import DEFAULT_NEXT_TOKEN

from airflow.providers.amazon.aws.hooks.eks import EKSHook
from airflow.providers.amazon.aws.operators.eks import (
    EKSCreateClusterOperator,
    EKSCreateNodegroupOperator,
    EKSDeleteClusterOperator,
    EKSDeleteNodegroupOperator,
    EKSDescribeAllClustersOperator,
    EKSDescribeAllNodegroupsOperator,
    EKSDescribeClusterOperator,
    EKSDescribeNodegroupOperator,
    EKSListClustersOperator,
    EKSListNodegroupsOperator,
)
from tests.providers.amazon.aws.utils.eks_test_constants import (
    NODEROLE_ARN_VALUE,
    RESOURCES_VPC_CONFIG_VALUE,
    ROLE_ARN_VALUE,
    STATUS_VALUE,
    SUBNETS_VALUE,
    TASK_ID,
)
from tests.providers.amazon.aws.utils.eks_test_utils import convert_keys, random_names

DESCRIBE_CLUSTER_RESULT = f'{{"cluster": "{random_names()}"}}'
DESCRIBE_NODEGROUP_RESULT = f'{{"nodegroup": "{random_names()}"}}'
EMPTY_CLUSTER = '{"cluster": {}}'
EMPTY_NODEGROUP = '{"nodegroup": {}}'
NAME_LIST = ["foo", "bar", "baz", "qux"]


class TestEKSCreateClusterOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = random_names()
        self.nodegroup_name: str = random_names()

        self.create_cluster_params = dict(
            cluster_name=self.cluster_name,
            cluster_role_arn=ROLE_ARN_VALUE,
            resources_vpc_config=RESOURCES_VPC_CONFIG_VALUE,
        )
        # These two are added when creating both the cluster and nodegroup together.
        self.base_nodegroup_params = dict(
            nodegroup_name=self.nodegroup_name,
            nodegroup_role_arn=NODEROLE_ARN_VALUE,
        )

        # This one is used in the tests to validate method calls.
        self.create_nodegroup_params = dict(
            **self.base_nodegroup_params,
            cluster_name=self.cluster_name,
            subnets=SUBNETS_VALUE,
        )

        self.create_cluster_operator = EKSCreateClusterOperator(
            task_id=TASK_ID, **self.create_cluster_params, compute=None
        )

        self.create_cluster_operator_with_nodegroup = EKSCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            **self.base_nodegroup_params,
        )

    @mock.patch.object(EKSHook, "create_cluster")
    @mock.patch.object(EKSHook, "create_nodegroup")
    def test_execute_create_cluster(self, mock_create_nodegroup, mock_create_cluster):
        self.create_cluster_operator.execute({})

        mock_create_cluster.assert_called_once_with(**convert_keys(self.create_cluster_params))
        mock_create_nodegroup.assert_not_called()

    @mock.patch.object(EKSHook, "get_cluster_state")
    @mock.patch.object(EKSHook, "create_cluster")
    @mock.patch.object(EKSHook, "create_nodegroup")
    def test_execute_when_called_with_nodegroup_creates_both(
        self, mock_create_nodegroup, mock_create_cluster, mock_cluster_state
    ):
        mock_cluster_state.return_value = STATUS_VALUE

        self.create_cluster_operator_with_nodegroup.execute({})

        mock_create_cluster.assert_called_once_with(**convert_keys(self.create_cluster_params))
        mock_create_nodegroup.assert_called_once_with(**convert_keys(self.create_nodegroup_params))


class TestEKSCreateNodegroupOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = random_names()
        self.nodegroup_name: str = random_names()

        self.create_nodegroup_params = dict(
            cluster_name=self.cluster_name,
            nodegroup_name=self.nodegroup_name,
            nodegroup_subnets=SUBNETS_VALUE,
            nodegroup_role_arn=NODEROLE_ARN_VALUE,
        )

        self.create_nodegroup_operator = EKSCreateNodegroupOperator(
            task_id=TASK_ID, **self.create_nodegroup_params
        )

    @mock.patch.object(EKSHook, "create_nodegroup")
    def test_execute_when_nodegroup_does_not_already_exist(self, mock_create_nodegroup):
        self.create_nodegroup_operator.execute({})

        mock_create_nodegroup.assert_called_once_with(**convert_keys(self.create_nodegroup_params))


class TestEKSDeleteClusterOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = random_names()

        self.delete_cluster_operator = EKSDeleteClusterOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name
        )

    @mock.patch.object(EKSHook, "list_nodegroups")
    @mock.patch.object(EKSHook, "delete_cluster")
    def test_existing_cluster_not_in_use(self, mock_delete_cluster, mock_list_nodegroups):
        mock_list_nodegroups.return_value = dict(nodegroups=list())

        self.delete_cluster_operator.execute({})

        mock_list_nodegroups.assert_called_once
        mock_delete_cluster.assert_called_once_with(name=self.cluster_name)


class TestEKSDeleteNodegroupOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = random_names()
        self.nodegroup_name: str = random_names()

        self.delete_nodegroup_operator = EKSDeleteNodegroupOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name, nodegroup_name=self.nodegroup_name
        )

    @mock.patch.object(EKSHook, "delete_nodegroup")
    def test_existing_nodegroup(self, mock_delete_nodegroup):
        self.delete_nodegroup_operator.execute({})

        mock_delete_nodegroup.assert_called_once_with(
            clusterName=self.cluster_name, nodegroupName=self.nodegroup_name
        )


class TestEKSDescribeAllClustersOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.describe_all_clusters_operator = EKSDescribeAllClustersOperator(task_id=TASK_ID)

    @mock.patch.object(EKSHook, "list_clusters")
    @mock.patch.object(EKSHook, "describe_cluster")
    def test_clusters_exist_returns_all_cluster_details(self, mock_describe_cluster, mock_list_clusters):
        cluster_names: List[str] = NAME_LIST
        response = dict(clusters=cluster_names, nextToken=DEFAULT_NEXT_TOKEN)
        mock_describe_cluster.return_value = EMPTY_CLUSTER
        mock_list_clusters.return_value = response

        self.describe_all_clusters_operator.execute({})

        mock_list_clusters.assert_called_once()
        assert mock_describe_cluster.call_count == len(cluster_names)

    @mock.patch.object(EKSHook, "list_clusters")
    @mock.patch.object(EKSHook, "describe_cluster")
    def test_no_clusters_exist(self, mock_describe_cluster, mock_list_clusters):
        mock_list_clusters.return_value = dict(clusters=list(), token=DEFAULT_NEXT_TOKEN)

        self.describe_all_clusters_operator.execute({})

        mock_list_clusters.assert_called_once()
        mock_describe_cluster.assert_not_called()


class TestEKSDescribeAllNodegroupsOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = random_names()

        self.describe_all_nodegroups_operator = EKSDescribeAllNodegroupsOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name
        )

    @mock.patch.object(EKSHook, "list_nodegroups")
    @mock.patch.object(EKSHook, "describe_nodegroup")
    def test_nodegroups_exist_returns_all_nodegroup_details(
        self, mock_describe_nodegroup, mock_list_nodegroups
    ):
        nodegroup_names: List[str] = NAME_LIST
        cluster_name: str = random_names()
        response = dict(cluster=cluster_name, nodegroups=nodegroup_names, nextToken=DEFAULT_NEXT_TOKEN)
        mock_describe_nodegroup.return_value = EMPTY_NODEGROUP
        mock_list_nodegroups.return_value = response

        self.describe_all_nodegroups_operator.execute({})

        mock_list_nodegroups.assert_called_once()
        assert mock_describe_nodegroup.call_count == len(nodegroup_names)

    @mock.patch.object(EKSHook, "list_nodegroups")
    @mock.patch.object(EKSHook, "describe_nodegroup")
    def test_no_nodegroups_exist(self, mock_describe_nodegroup, mock_list_nodegroups):
        mock_list_nodegroups.return_value = dict(nodegroups=list(), token="")

        self.describe_all_nodegroups_operator.execute({})

        mock_list_nodegroups.assert_called_once()
        mock_describe_nodegroup.assert_not_called()


class TestEKSDescribeClusterOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = random_names()

        self.describe_cluster_operator = EKSDescribeClusterOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name
        )

    @mock.patch.object(EKSHook, "describe_cluster")
    def test_describe_cluster(self, mock_describe_cluster):
        mock_describe_cluster.return_value = DESCRIBE_CLUSTER_RESULT

        self.describe_cluster_operator.execute({})

        mock_describe_cluster.assert_called_once_with(name=self.cluster_name, verbose=False)


class TestEKSDescribeNodegroupOperator(unittest.TestCase):
    def setUp(self):
        self.cluster_name = random_names()
        self.nodegroup_name = random_names()

        self.describe_nodegroup_operator = EKSDescribeNodegroupOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name, nodegroup_name=self.nodegroup_name
        )

    @mock.patch.object(EKSHook, "describe_nodegroup")
    def test_describe_nodegroup(self, mock_describe_nodegroup):
        mock_describe_nodegroup.return_value = DESCRIBE_NODEGROUP_RESULT

        self.describe_nodegroup_operator.execute({})

        mock_describe_nodegroup.assert_called_once_with(
            clusterName=self.cluster_name, nodegroupName=self.nodegroup_name, verbose=False
        )


class TestEKSListClustersOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_names: List[str] = NAME_LIST

        self.list_clusters_operator = EKSListClustersOperator(task_id=TASK_ID)

    @mock.patch.object(EKSHook, "list_clusters")
    def test_list_clusters(self, mock_list_clusters):
        mock_list_clusters.return_value = self.cluster_names

        self.list_clusters_operator.execute({})

        mock_list_clusters.assert_called_once()


class TestEKSListNodegroupsOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = random_names()
        self.nodegroup_names: List[str] = NAME_LIST

        self.list_nodegroups_operator = EKSListNodegroupsOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name
        )

    @mock.patch.object(EKSHook, "list_nodegroups")
    def test_list_nodegroups(self, mock_list_nodegroups):
        mock_list_nodegroups.return_value = self.nodegroup_names

        self.list_nodegroups_operator.execute({})

        mock_list_nodegroups.assert_called_once()
