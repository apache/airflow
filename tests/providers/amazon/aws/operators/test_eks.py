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
from unittest import mock

from airflow.providers.amazon.aws.hooks.eks import ClusterStates, EKSHook
from airflow.providers.amazon.aws.operators.eks import (
    EKSCreateClusterOperator,
    EKSCreateNodegroupOperator,
    EKSDeleteClusterOperator,
    EKSDeleteNodegroupOperator,
)
from tests.providers.amazon.aws.utils.eks_test_constants import (
    NODEROLE_ARN,
    RESOURCES_VPC_CONFIG,
    ROLE_ARN,
    SUBNET_IDS,
    TASK_ID,
)
from tests.providers.amazon.aws.utils.eks_test_utils import convert_keys

CLUSTER_NAME = "cluster1"
NODEGROUP_NAME = "nodegroup1"
DESCRIBE_CLUSTER_RESULT = f'{{"cluster": "{CLUSTER_NAME}"}}'
DESCRIBE_NODEGROUP_RESULT = f'{{"nodegroup": "{NODEGROUP_NAME}"}}'
EMPTY_CLUSTER = '{"cluster": {}}'
EMPTY_NODEGROUP = '{"nodegroup": {}}'
NAME_LIST = ["foo", "bar", "baz", "qux"]


class TestEKSCreateClusterOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = CLUSTER_NAME
        self.nodegroup_name: str = NODEGROUP_NAME
        _, self.nodegroup_arn = NODEROLE_ARN
        _, self.resources_vpc_config = RESOURCES_VPC_CONFIG
        _, self.role_arn = ROLE_ARN

        self.create_cluster_params = dict(
            cluster_name=self.cluster_name,
            cluster_role_arn=self.role_arn,
            resources_vpc_config=self.resources_vpc_config,
        )
        # These two are added when creating both the cluster and nodegroup together.
        self.base_nodegroup_params = dict(
            nodegroup_name=self.nodegroup_name,
            nodegroup_role_arn=self.nodegroup_arn,
        )

        # This one is used in the tests to validate method calls.
        self.create_nodegroup_params = dict(
            **self.base_nodegroup_params,
            cluster_name=self.cluster_name,
            subnets=SUBNET_IDS,
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
        mock_cluster_state.return_value = ClusterStates.ACTIVE

        self.create_cluster_operator_with_nodegroup.execute({})

        mock_create_cluster.assert_called_once_with(**convert_keys(self.create_cluster_params))
        mock_create_nodegroup.assert_called_once_with(**convert_keys(self.create_nodegroup_params))


class TestEKSCreateNodegroupOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = CLUSTER_NAME
        self.nodegroup_name: str = NODEGROUP_NAME
        _, self.nodegroup_arn = NODEROLE_ARN

        self.create_nodegroup_params = dict(
            cluster_name=self.cluster_name,
            nodegroup_name=self.nodegroup_name,
            nodegroup_subnets=SUBNET_IDS,
            nodegroup_role_arn=self.nodegroup_arn,
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
        self.cluster_name: str = CLUSTER_NAME

        self.delete_cluster_operator = EKSDeleteClusterOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name
        )

    @mock.patch.object(EKSHook, "list_nodegroups")
    @mock.patch.object(EKSHook, "delete_cluster")
    def test_existing_cluster_not_in_use(self, mock_delete_cluster, mock_list_nodegroups):
        mock_list_nodegroups.return_value = []

        self.delete_cluster_operator.execute({})

        mock_list_nodegroups.assert_called_once
        mock_delete_cluster.assert_called_once_with(name=self.cluster_name)


class TestEKSDeleteNodegroupOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = CLUSTER_NAME
        self.nodegroup_name: str = NODEGROUP_NAME

        self.delete_nodegroup_operator = EKSDeleteNodegroupOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name, nodegroup_name=self.nodegroup_name
        )

    @mock.patch.object(EKSHook, "delete_nodegroup")
    def test_existing_nodegroup(self, mock_delete_nodegroup):
        self.delete_nodegroup_operator.execute({})

        mock_delete_nodegroup.assert_called_once_with(
            clusterName=self.cluster_name, nodegroupName=self.nodegroup_name
        )
