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
    EKSCreateFargateProfileOperator,
    EKSCreateNodegroupOperator,
    EKSDeleteClusterOperator,
    EKSDeleteFargateProfileOperator,
    EKSDeleteNodegroupOperator,
    EKSPodOperator,
)
from tests.providers.amazon.aws.utils.eks_test_constants import (
    NODEROLE_ARN,
    POD_EXECUTION_ROLE_ARN,
    RESOURCES_VPC_CONFIG,
    ROLE_ARN,
    SELECTORS,
    SUBNET_IDS,
    TASK_ID,
)
from tests.providers.amazon.aws.utils.eks_test_utils import convert_keys

CLUSTER_NAME = "cluster1"
NODEGROUP_NAME = "nodegroup1"
FARGATE_PROFILE_NAME = "fargate_profile1"
DESCRIBE_CLUSTER_RESULT = f'{{"cluster": "{CLUSTER_NAME}"}}'
DESCRIBE_NODEGROUP_RESULT = f'{{"nodegroup": "{NODEGROUP_NAME}"}}'
EMPTY_CLUSTER = '{"cluster": {}}'
EMPTY_NODEGROUP = '{"nodegroup": {}}'
NAME_LIST = ["foo", "bar", "baz", "qux"]


class TestEKSCreateClusterOperator(unittest.TestCase):
    def setUp(self) -> None:
        # Parameters which are needed to create a cluster.
        self.create_cluster_params = dict(
            cluster_name=CLUSTER_NAME,
            cluster_role_arn=ROLE_ARN[1],
            resources_vpc_config=RESOURCES_VPC_CONFIG[1],
        )

        self.create_cluster_operator = EKSCreateClusterOperator(
            task_id=TASK_ID, **self.create_cluster_params, compute=None
        )

        self.nodegroup_setUp()
        self.fargate_profile_setUp()

    def nodegroup_setUp(self) -> None:
        # Parameters which are added to the cluster parameters
        # when creating both the cluster and nodegroup together.
        self.base_nodegroup_params = dict(
            nodegroup_name=NODEGROUP_NAME,
            nodegroup_role_arn=NODEROLE_ARN[1],
        )

        # Parameters expected to be passed in the CreateNodegroup hook call.
        self.create_nodegroup_params = dict(
            **self.base_nodegroup_params,
            cluster_name=CLUSTER_NAME,
            subnets=SUBNET_IDS,
        )

        self.create_cluster_operator_with_nodegroup = EKSCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            **self.base_nodegroup_params,
        )

    def fargate_profile_setUp(self) -> None:
        # Parameters which are added to the cluster parameters
        # when creating both the cluster and Fargate profile together.
        self.base_fargate_profile_params = dict(
            fargate_profile_name=FARGATE_PROFILE_NAME,
            fargate_pod_execution_role_arn=POD_EXECUTION_ROLE_ARN[1],
            fargate_selectors=SELECTORS[1],
        )

        # Parameters expected to be passed in the CreateFargateProfile hook call.
        self.create_fargate_profile_params = dict(
            **self.base_fargate_profile_params,
            cluster_name=CLUSTER_NAME,
        )

        self.create_cluster_operator_with_fargate_profile = EKSCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            **self.base_fargate_profile_params,
            compute='fargate',
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

    @mock.patch.object(EKSHook, "get_cluster_state")
    @mock.patch.object(EKSHook, "create_cluster")
    @mock.patch.object(EKSHook, "create_fargate_profile")
    def test_execute_when_called_with_fargate_creates_both(
        self, mock_create_fargate_profile, mock_create_cluster, mock_cluster_state
    ):
        mock_cluster_state.return_value = ClusterStates.ACTIVE

        self.create_cluster_operator_with_fargate_profile.execute({})

        mock_create_cluster.assert_called_once_with(**convert_keys(self.create_cluster_params))
        mock_create_fargate_profile.assert_called_once_with(
            **convert_keys(self.create_fargate_profile_params)
        )


class TestEKSCreateFargateProfileOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.create_fargate_profile_params = dict(
            cluster_name=CLUSTER_NAME,
            pod_execution_role_arn=POD_EXECUTION_ROLE_ARN[1],
            selectors=SELECTORS[1],
            fargate_profile_name=FARGATE_PROFILE_NAME,
        )

        self.create_fargate_profile_operator = EKSCreateFargateProfileOperator(
            task_id=TASK_ID, **self.create_fargate_profile_params
        )

    @mock.patch.object(EKSHook, "create_fargate_profile")
    def test_execute_when_fargate_profile_does_not_already_exist(self, mock_create_fargate_profile):
        self.create_fargate_profile_operator.execute({})

        mock_create_fargate_profile.assert_called_once_with(
            **convert_keys(self.create_fargate_profile_params)
        )


class TestEKSCreateNodegroupOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.create_nodegroup_params = dict(
            cluster_name=CLUSTER_NAME,
            nodegroup_name=NODEGROUP_NAME,
            nodegroup_subnets=SUBNET_IDS,
            nodegroup_role_arn=NODEROLE_ARN[1],
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


class TestEKSDeleteFargateProfileOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster_name: str = CLUSTER_NAME
        self.fargate_profile_name: str = FARGATE_PROFILE_NAME

        self.delete_fargate_profile_operator = EKSDeleteFargateProfileOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name, fargate_profile_name=self.fargate_profile_name
        )

    @mock.patch.object(EKSHook, "delete_fargate_profile")
    def test_existing_fargate_profile(self, mock_delete_fargate_profile):
        self.delete_fargate_profile_operator.execute({})

        mock_delete_fargate_profile.assert_called_once_with(
            clusterName=self.cluster_name, fargateProfileName=self.fargate_profile_name
        )


class TestEKSPodOperator(unittest.TestCase):
    @mock.patch('airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.execute')
    @mock.patch('airflow.providers.amazon.aws.hooks.eks.EKSHook.generate_config_file')
    @mock.patch('airflow.providers.amazon.aws.hooks.eks.EKSHook.__init__', return_value=None)
    def test_existing_nodegroup(
        self, mock_eks_hook, mock_generate_config_file, mock_k8s_pod_operator_execute
    ):
        ti_context = mock.MagicMock(name="ti_context")

        op = EKSPodOperator(
            task_id="run_pod",
            pod_name="run_pod",
            cluster_name=CLUSTER_NAME,
            image="amazon/aws-cli:latest",
            cmds=["sh", "-c", "ls"],
            labels={"demo": "hello_world"},
            get_logs=True,
            # Delete the pod when it reaches its final state, or the execution is interrupted.
            is_delete_operator_pod=True,
        )
        op_return_value = op.execute(ti_context)
        mock_k8s_pod_operator_execute.assert_called_once_with(ti_context)
        mock_eks_hook.assert_called_once_with(aws_conn_id='aws_default', region_name=None)
        mock_generate_config_file.assert_called_once_with(
            eks_cluster_name=CLUSTER_NAME, pod_namespace='default'
        )
        self.assertEqual(mock_k8s_pod_operator_execute.return_value, op_return_value)
        self.assertEqual(mock_generate_config_file.return_value.__enter__.return_value, op.config_file)
