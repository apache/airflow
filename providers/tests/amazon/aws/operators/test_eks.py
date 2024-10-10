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
from __future__ import annotations

from typing import Any
from unittest import mock

import pytest
from botocore.waiter import Waiter

from airflow.exceptions import TaskDeferred
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, EksHook
from airflow.providers.amazon.aws.operators.eks import (
    EksCreateClusterOperator,
    EksCreateFargateProfileOperator,
    EksCreateNodegroupOperator,
    EksDeleteClusterOperator,
    EksDeleteFargateProfileOperator,
    EksDeleteNodegroupOperator,
    EksPodOperator,
)
from airflow.providers.amazon.aws.triggers.eks import (
    EksCreateFargateProfileTrigger,
    EksCreateNodegroupTrigger,
    EksDeleteFargateProfileTrigger,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction
from airflow.typing_compat import TypedDict

from providers.tests.amazon.aws.utils.eks_test_constants import (
    NODEROLE_ARN,
    POD_EXECUTION_ROLE_ARN,
    RESOURCES_VPC_CONFIG,
    ROLE_ARN,
    SELECTORS,
    SUBNET_IDS,
    TASK_ID,
)
from providers.tests.amazon.aws.utils.eks_test_utils import convert_keys
from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields
from providers.tests.amazon.aws.utils.test_waiter import assert_expected_waiter_type

CLUSTER_NAME = "cluster1"
NODEGROUP_NAME = "nodegroup1"
FARGATE_PROFILE_NAME = "fargate_profile1"
DESCRIBE_CLUSTER_RESULT = f'{{"cluster": "{CLUSTER_NAME}"}}'
DESCRIBE_NODEGROUP_RESULT = f'{{"nodegroup": "{NODEGROUP_NAME}"}}'
EMPTY_CLUSTER = '{"cluster": {}}'
EMPTY_NODEGROUP = '{"nodegroup": {}}'
NAME_LIST = ["foo", "bar", "baz", "qux"]
CREATE_CLUSTER_KWARGS = {"version": "1.22"}
CREATE_FARGATE_PROFILE_KWARGS = {"tags": {"hello": "world"}}
CREATE_NODEGROUP_KWARGS = {
    "capacityType": "ON_DEMAND",
    "instanceTypes": "t3.large",
}


class ClusterParams(TypedDict):
    cluster_name: str
    cluster_role_arn: str
    resources_vpc_config: dict[Any, Any]


class NodeGroupParams(TypedDict):
    nodegroup_name: str
    nodegroup_role_arn: str


class BaseFargateProfileParams(TypedDict):
    fargate_profile_name: str
    fargate_pod_execution_role_arn: str
    fargate_selectors: list[Any]


class CreateFargateProfileParams(TypedDict):
    cluster_name: str
    pod_execution_role_arn: str
    selectors: list[Any]
    fargate_profile_name: str


class CreateNodegroupParams(TypedDict):
    cluster_name: str
    nodegroup_name: str
    nodegroup_subnets: list[str]
    nodegroup_role_arn: str


class TestEksCreateClusterOperator:
    def setup_method(self) -> None:
        # Parameters which are needed to create a cluster.
        self.create_cluster_params = ClusterParams(
            cluster_name=CLUSTER_NAME,
            cluster_role_arn=ROLE_ARN[1],
            resources_vpc_config=RESOURCES_VPC_CONFIG[1],
        )
        self.nodegroup_setup()
        self.fargate_profile_setup()

    def nodegroup_setup(self) -> None:
        # Parameters which are added to the cluster parameters
        # when creating both the cluster and nodegroup together.
        self.base_nodegroup_params = NodeGroupParams(
            nodegroup_name=NODEGROUP_NAME,
            nodegroup_role_arn=NODEROLE_ARN[1],
        )

        # Parameters expected to be passed in the CreateNodegroup hook call.
        self.create_nodegroup_params = dict(
            **self.base_nodegroup_params,
            cluster_name=CLUSTER_NAME,
            subnets=SUBNET_IDS,
        )

        self.create_cluster_operator_with_nodegroup = EksCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            **self.base_nodegroup_params,
        )

    def fargate_profile_setup(self) -> None:
        # Parameters which are added to the cluster parameters
        # when creating both the cluster and Fargate profile together.
        self.base_fargate_profile_params = BaseFargateProfileParams(
            fargate_profile_name=FARGATE_PROFILE_NAME,
            fargate_pod_execution_role_arn=POD_EXECUTION_ROLE_ARN[1],
            fargate_selectors=SELECTORS[1],
        )

        # Parameters expected to be passed in the CreateFargateProfile hook call.
        self.create_fargate_profile_params = dict(
            **self.base_fargate_profile_params,
            cluster_name=CLUSTER_NAME,
        )

        self.create_cluster_operator_with_fargate_profile = EksCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            **self.base_fargate_profile_params,
            compute="fargate",
        )

    @pytest.mark.parametrize(
        "create_cluster_kwargs",
        [
            pytest.param(None, id="without cluster kwargs"),
            pytest.param(CREATE_CLUSTER_KWARGS, id="with cluster kwargs"),
        ],
    )
    @mock.patch.object(EksHook, "create_cluster")
    @mock.patch.object(EksHook, "create_nodegroup")
    def test_execute_create_cluster(self, mock_create_nodegroup, mock_create_cluster, create_cluster_kwargs):
        op_kwargs = {**self.create_cluster_params, "compute": None}
        if create_cluster_kwargs:
            op_kwargs["create_cluster_kwargs"] = create_cluster_kwargs
            parameters = {**self.create_cluster_params, **create_cluster_kwargs}
        else:
            assert "create_cluster_kwargs" not in op_kwargs
            parameters = self.create_cluster_params

        operator = EksCreateClusterOperator(task_id=TASK_ID, **op_kwargs)
        operator.execute({})
        mock_create_cluster.assert_called_with(**convert_keys(parameters))
        mock_create_nodegroup.assert_not_called()

    @pytest.mark.parametrize(
        "create_cluster_kwargs",
        [
            pytest.param(None, id="without cluster kwargs"),
            pytest.param(CREATE_CLUSTER_KWARGS, id="with cluster kwargs"),
        ],
    )
    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "create_cluster")
    @mock.patch.object(EksHook, "create_nodegroup")
    def test_execute_create_cluster_with_wait(
        self, mock_create_nodegroup, mock_create_cluster, mock_waiter, create_cluster_kwargs
    ):
        op_kwargs = {**self.create_cluster_params, "compute": None}
        if create_cluster_kwargs:
            op_kwargs["create_cluster_kwargs"] = create_cluster_kwargs
            parameters = {**self.create_cluster_params, **create_cluster_kwargs}
        else:
            assert "create_cluster_kwargs" not in op_kwargs
            parameters = self.create_cluster_params

        operator = EksCreateClusterOperator(task_id=TASK_ID, **op_kwargs, wait_for_completion=True)
        operator.execute({})
        mock_create_cluster.assert_called_with(**convert_keys(parameters))
        mock_create_nodegroup.assert_not_called()
        mock_waiter.assert_called_once_with(
            mock.ANY,
            name=CLUSTER_NAME,
            WaiterConfig={"Delay": mock.ANY, "MaxAttempts": mock.ANY},
        )
        assert_expected_waiter_type(mock_waiter, "ClusterActive")

    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "get_cluster_state")
    @mock.patch.object(EksHook, "create_cluster")
    @mock.patch.object(EksHook, "create_nodegroup")
    def test_execute_when_called_with_nodegroup_creates_both(
        self, mock_create_nodegroup, mock_create_cluster, mock_cluster_state, mock_waiter
    ):
        mock_cluster_state.return_value = ClusterStates.ACTIVE

        self.create_cluster_operator_with_nodegroup.execute({})

        mock_create_cluster.assert_called_once_with(**convert_keys(self.create_cluster_params))
        mock_create_nodegroup.assert_called_once_with(**convert_keys(self.create_nodegroup_params))
        mock_waiter.assert_called_once_with(
            mock.ANY,
            name=CLUSTER_NAME,
            WaiterConfig={"Delay": mock.ANY, "MaxAttempts": mock.ANY},
        )
        assert_expected_waiter_type(mock_waiter, "ClusterActive")

    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "get_cluster_state")
    @mock.patch.object(EksHook, "create_cluster")
    @mock.patch.object(EksHook, "create_nodegroup")
    def test_execute_with_wait_when_called_with_nodegroup_creates_both(
        self, mock_create_nodegroup, mock_create_cluster, mock_cluster_state, mock_waiter
    ):
        mock_cluster_state.return_value = ClusterStates.ACTIVE
        self.create_cluster_operator_with_nodegroup.wait_for_completion = True

        self.create_cluster_operator_with_nodegroup.execute({})

        mock_create_cluster.assert_called_once_with(**convert_keys(self.create_cluster_params))
        mock_create_nodegroup.assert_called_once_with(**convert_keys(self.create_nodegroup_params))
        # Calls waiter once for the cluster and once for the nodegroup.
        assert mock_waiter.call_count == 2
        mock_waiter.assert_called_with(
            mock.ANY,
            clusterName=CLUSTER_NAME,
            nodegroupName=NODEGROUP_NAME,
            WaiterConfig={"MaxAttempts": mock.ANY},
        )
        assert_expected_waiter_type(mock_waiter, "NodegroupActive")

    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "get_cluster_state")
    @mock.patch.object(EksHook, "create_cluster")
    @mock.patch.object(EksHook, "create_fargate_profile")
    def test_execute_when_called_with_fargate_creates_both(
        self, mock_create_fargate_profile, mock_create_cluster, mock_cluster_state, mock_waiter
    ):
        mock_cluster_state.return_value = ClusterStates.ACTIVE

        self.create_cluster_operator_with_fargate_profile.execute({})

        mock_create_cluster.assert_called_once_with(**convert_keys(self.create_cluster_params))
        mock_create_fargate_profile.assert_called_once_with(
            **convert_keys(self.create_fargate_profile_params)
        )
        mock_waiter.assert_called_once_with(
            mock.ANY,
            name=CLUSTER_NAME,
            WaiterConfig={"Delay": mock.ANY, "MaxAttempts": mock.ANY},
        )
        assert_expected_waiter_type(mock_waiter, "ClusterActive")

    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "get_cluster_state")
    @mock.patch.object(EksHook, "create_cluster")
    @mock.patch.object(EksHook, "create_fargate_profile")
    def test_execute_with_wait_when_called_with_fargate_creates_both(
        self, mock_create_fargate_profile, mock_create_cluster, mock_cluster_state, mock_waiter
    ):
        mock_cluster_state.return_value = ClusterStates.ACTIVE
        self.create_cluster_operator_with_fargate_profile.wait_for_completion = True

        self.create_cluster_operator_with_fargate_profile.execute({})

        mock_create_cluster.assert_called_once_with(**convert_keys(self.create_cluster_params))
        mock_create_fargate_profile.assert_called_once_with(
            **convert_keys(self.create_fargate_profile_params)
        )
        # Calls waiter once for the cluster and once for the nodegroup.
        assert mock_waiter.call_count == 2
        mock_waiter.assert_called_with(
            mock.ANY,
            clusterName=CLUSTER_NAME,
            fargateProfileName=FARGATE_PROFILE_NAME,
            WaiterConfig={"MaxAttempts": mock.ANY},
        )
        assert_expected_waiter_type(mock_waiter, "FargateProfileActive")

    def test_invalid_compute_value(self):
        invalid_compute = EksCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            compute="infinite",
        )

        with pytest.raises(ValueError, match="Provided compute type is not supported."):
            invalid_compute.execute({})

    def test_nodegroup_compute_missing_nodegroup_role_arn(self):
        missing_nodegroup_role_arn = EksCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            compute="nodegroup",
        )

        with pytest.raises(
            ValueError,
            match="Creating an Amazon EKS managed node groups requires nodegroup_role_arn to be passed in.",
        ):
            missing_nodegroup_role_arn.execute({})

    def test_fargate_compute_missing_fargate_pod_execution_role_arn(self):
        missing_fargate_pod_execution_role_arn = EksCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            compute="fargate",
        )

        with pytest.raises(
            ValueError,
            match="Creating an AWS Fargate profiles requires fargate_pod_execution_role_arn to be passed in.",
        ):
            missing_fargate_pod_execution_role_arn.execute({})

    @mock.patch.object(EksHook, "create_cluster")
    def test_eks_create_cluster_short_circuit_early(self, mock_create_cluster, caplog):
        mock_create_cluster.return_value = None
        eks_create_cluster_operator = EksCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            compute=None,
            wait_for_completion=False,
            deferrable=False,
        )
        eks_create_cluster_operator.execute({})
        assert len(caplog.records) == 0

    @mock.patch.object(EksHook, "create_cluster")
    def test_eks_create_cluster_with_deferrable(self, mock_create_cluster, caplog):
        mock_create_cluster.return_value = None

        eks_create_cluster_operator = EksCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            compute=None,
            wait_for_completion=False,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred):
            eks_create_cluster_operator.execute({})
        assert "Waiting for EKS Cluster to provision. This will take some time." in caplog.messages

    def test_template_fields(self):
        op = EksCreateClusterOperator(
            task_id=TASK_ID,
            **self.create_cluster_params,
            compute="fargate",
        )

        validate_template_fields(op)


class TestEksCreateFargateProfileOperator:
    def setup_method(self) -> None:
        self.create_fargate_profile_params = CreateFargateProfileParams(  # type: ignore
            cluster_name=CLUSTER_NAME,
            pod_execution_role_arn=POD_EXECUTION_ROLE_ARN[1],
            selectors=SELECTORS[1],
            fargate_profile_name=FARGATE_PROFILE_NAME,
        )

    @pytest.mark.parametrize(
        "create_fargate_profile_kwargs",
        [
            pytest.param(None, id="without fargate profile kwargs"),
            pytest.param(CREATE_FARGATE_PROFILE_KWARGS, id="with fargate profile kwargs"),
        ],
    )
    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "create_fargate_profile")
    def test_execute_when_fargate_profile_does_not_already_exist(
        self, mock_create_fargate_profile, mock_waiter, create_fargate_profile_kwargs
    ):
        op_kwargs = {**self.create_fargate_profile_params}
        if create_fargate_profile_kwargs:
            op_kwargs["create_fargate_profile_kwargs"] = create_fargate_profile_kwargs
            parameters = {**self.create_fargate_profile_params, **create_fargate_profile_kwargs}
        else:
            assert "create_fargate_profile_kwargs" not in op_kwargs
            parameters = self.create_fargate_profile_params

        operator = EksCreateFargateProfileOperator(task_id=TASK_ID, **op_kwargs)
        operator.execute({})
        mock_create_fargate_profile.assert_called_with(**convert_keys(parameters))
        mock_waiter.assert_not_called()

    @pytest.mark.parametrize(
        "create_fargate_profile_kwargs",
        [
            pytest.param(None, id="without fargate profile kwargs"),
            pytest.param(CREATE_FARGATE_PROFILE_KWARGS, id="with fargate profile kwargs"),
        ],
    )
    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "create_fargate_profile")
    def test_execute_with_wait_when_fargate_profile_does_not_already_exist(
        self, mock_create_fargate_profile, mock_waiter, create_fargate_profile_kwargs
    ):
        op_kwargs = {**self.create_fargate_profile_params}
        if create_fargate_profile_kwargs:
            op_kwargs["create_fargate_profile_kwargs"] = create_fargate_profile_kwargs
            parameters = {**self.create_fargate_profile_params, **create_fargate_profile_kwargs}
        else:
            assert "create_fargate_profile_kwargs" not in op_kwargs
            parameters = self.create_fargate_profile_params

        operator = EksCreateFargateProfileOperator(task_id=TASK_ID, **op_kwargs, wait_for_completion=True)
        operator.execute({})
        mock_create_fargate_profile.assert_called_with(**convert_keys(parameters))
        mock_waiter.assert_called_with(
            mock.ANY,
            clusterName=CLUSTER_NAME,
            fargateProfileName=FARGATE_PROFILE_NAME,
            WaiterConfig={"MaxAttempts": mock.ANY},
        )
        assert_expected_waiter_type(mock_waiter, "FargateProfileActive")

    @mock.patch.object(EksHook, "create_fargate_profile")
    def test_create_fargate_profile_deferrable(self, _):
        op_kwargs = {**self.create_fargate_profile_params}
        operator = EksCreateFargateProfileOperator(
            task_id=TASK_ID,
            **op_kwargs,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            operator.execute({})
        assert isinstance(
            exc.value.trigger, EksCreateFargateProfileTrigger
        ), "Trigger is not a EksCreateFargateProfileTrigger"

    def test_template_fields(self):
        op = EksCreateFargateProfileOperator(task_id=TASK_ID, **self.create_fargate_profile_params)

        validate_template_fields(op)


class TestEksCreateNodegroupOperator:
    def setup_method(self) -> None:
        self.create_nodegroup_params = CreateNodegroupParams(
            cluster_name=CLUSTER_NAME,
            nodegroup_name=NODEGROUP_NAME,
            nodegroup_subnets=SUBNET_IDS,
            nodegroup_role_arn=NODEROLE_ARN[1],
        )

    @pytest.mark.parametrize(
        "create_nodegroup_kwargs",
        [
            pytest.param(None, id="without nodegroup kwargs"),
            pytest.param(CREATE_NODEGROUP_KWARGS, id="with nodegroup kwargs"),
        ],
    )
    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "create_nodegroup")
    def test_execute_when_nodegroup_does_not_already_exist(
        self, mock_create_nodegroup, mock_waiter, create_nodegroup_kwargs
    ):
        op_kwargs = {**self.create_nodegroup_params}
        if create_nodegroup_kwargs:
            op_kwargs["create_nodegroup_kwargs"] = create_nodegroup_kwargs
            parameters = {**self.create_nodegroup_params, **create_nodegroup_kwargs}
        else:
            assert "create_nodegroup_params" not in op_kwargs
            parameters = self.create_nodegroup_params

        operator = EksCreateNodegroupOperator(task_id=TASK_ID, **op_kwargs)
        operator.execute({})
        mock_create_nodegroup.assert_called_with(**convert_keys(parameters))
        mock_waiter.assert_not_called()

        @pytest.mark.parametrize(
            "create_nodegroup_kwargs",
            [
                pytest.param(None, id="without nodegroup kwargs"),
                pytest.param(CREATE_NODEGROUP_KWARGS, id="with nodegroup kwargs"),
            ],
        )
        @mock.patch.object(Waiter, "wait")
        @mock.patch.object(EksHook, "create_nodegroup")
        def test_execute_with_wait_when_nodegroup_does_not_already_exist(
            self, mock_create_nodegroup, mock_waiter, create_nodegroup_kwargs
        ):
            op_kwargs = {**self.create_nodegroup_params}
            if create_nodegroup_kwargs:
                op_kwargs["create_nodegroup_kwargs"] = create_nodegroup_kwargs
                parameters = {**self.create_nodegroup_params, **create_nodegroup_kwargs}
            else:
                assert "create_nodegroup_params" not in op_kwargs
                parameters = self.create_nodegroup_params

            operator = EksCreateNodegroupOperator(task_id=TASK_ID, **op_kwargs, wait_for_completion=True)
            operator.execute({})
            mock_create_nodegroup.assert_called_with(**convert_keys(parameters))
            mock_waiter.assert_called_with(mock.ANY, clusterName=CLUSTER_NAME, nodegroupName=NODEGROUP_NAME)
            assert_expected_waiter_type(mock_waiter, "NodegroupActive")

    @mock.patch.object(EksHook, "create_nodegroup")
    def test_create_nodegroup_deferrable(self, mock_create_nodegroup):
        mock_create_nodegroup.return_value = True
        op_kwargs = {**self.create_nodegroup_params}
        operator = EksCreateNodegroupOperator(
            task_id=TASK_ID,
            **op_kwargs,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            operator.execute({})
        assert isinstance(exc.value.trigger, EksCreateNodegroupTrigger)

    def test_create_nodegroup_deferrable_versus_wait_for_completion(self):
        op_kwargs = {**self.create_nodegroup_params}
        operator = EksCreateNodegroupOperator(
            task_id=TASK_ID,
            **op_kwargs,
            deferrable=True,
            wait_for_completion=True,
        )
        assert operator.wait_for_completion is False
        operator = EksCreateNodegroupOperator(
            task_id=TASK_ID,
            **op_kwargs,
            deferrable=False,
            wait_for_completion=True,
        )
        assert operator.wait_for_completion is True

    def test_template_fields(self):
        op_kwargs = {**self.create_nodegroup_params}
        op = EksCreateNodegroupOperator(task_id=TASK_ID, **op_kwargs)

        validate_template_fields(op)


class TestEksDeleteClusterOperator:
    def setup_method(self) -> None:
        self.cluster_name: str = CLUSTER_NAME

        self.delete_cluster_operator = EksDeleteClusterOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name
        )

    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "list_nodegroups")
    @mock.patch.object(EksHook, "delete_cluster")
    def test_existing_cluster_not_in_use(self, mock_delete_cluster, mock_list_nodegroups, mock_waiter):
        mock_list_nodegroups.return_value = []
        self.delete_cluster_operator.execute({})
        mock_delete_cluster.assert_called_once_with(name=self.cluster_name)
        mock_waiter.assert_not_called()

    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "list_nodegroups")
    @mock.patch.object(EksHook, "delete_cluster")
    def test_existing_cluster_not_in_use_with_wait(
        self, mock_delete_cluster, mock_list_nodegroups, mock_waiter
    ):
        mock_list_nodegroups.return_value = []
        self.delete_cluster_operator.wait_for_completion = True

        self.delete_cluster_operator.execute({})

        mock_list_nodegroups.assert_not_called()
        mock_delete_cluster.assert_called_once_with(name=self.cluster_name)
        mock_waiter.assert_called_with(mock.ANY, name=CLUSTER_NAME)
        assert_expected_waiter_type(mock_waiter, "ClusterDeleted")

    def test_eks_delete_cluster_operator_with_deferrable(self):
        self.delete_cluster_operator.deferrable = True
        with pytest.raises(TaskDeferred):
            self.delete_cluster_operator.execute({})

    def test_template_fields(self):
        validate_template_fields(self.delete_cluster_operator)


class TestEksDeleteNodegroupOperator:
    def setup_method(self) -> None:
        self.cluster_name: str = CLUSTER_NAME
        self.nodegroup_name: str = NODEGROUP_NAME

        self.delete_nodegroup_operator = EksDeleteNodegroupOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name, nodegroup_name=self.nodegroup_name
        )

    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "delete_nodegroup")
    def test_existing_nodegroup(self, mock_delete_nodegroup, mock_waiter):
        self.delete_nodegroup_operator.execute({})

        mock_delete_nodegroup.assert_called_once_with(
            clusterName=self.cluster_name, nodegroupName=self.nodegroup_name
        )
        mock_waiter.assert_not_called()

    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "delete_nodegroup")
    def test_existing_nodegroup_with_wait(self, mock_delete_nodegroup, mock_waiter):
        self.delete_nodegroup_operator.wait_for_completion = True

        self.delete_nodegroup_operator.execute({})

        mock_delete_nodegroup.assert_called_once_with(
            clusterName=self.cluster_name, nodegroupName=self.nodegroup_name
        )
        mock_waiter.assert_called_with(mock.ANY, clusterName=CLUSTER_NAME, nodegroupName=NODEGROUP_NAME)
        assert_expected_waiter_type(mock_waiter, "NodegroupDeleted")

    def test_template_fields(self):
        validate_template_fields(self.delete_nodegroup_operator)


class TestEksDeleteFargateProfileOperator:
    def setup_method(self) -> None:
        self.cluster_name: str = CLUSTER_NAME
        self.fargate_profile_name: str = FARGATE_PROFILE_NAME

        self.delete_fargate_profile_operator = EksDeleteFargateProfileOperator(
            task_id=TASK_ID, cluster_name=self.cluster_name, fargate_profile_name=self.fargate_profile_name
        )

    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "delete_fargate_profile")
    def test_existing_fargate_profile(self, mock_delete_fargate_profile, mock_waiter):
        self.delete_fargate_profile_operator.execute({})

        mock_delete_fargate_profile.assert_called_once_with(
            clusterName=self.cluster_name, fargateProfileName=self.fargate_profile_name
        )
        mock_waiter.assert_not_called()

    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EksHook, "delete_fargate_profile")
    def test_existing_fargate_profile_with_wait(self, mock_delete_fargate_profile, mock_waiter):
        self.delete_fargate_profile_operator.wait_for_completion = True

        self.delete_fargate_profile_operator.execute({})

        mock_delete_fargate_profile.assert_called_once_with(
            clusterName=self.cluster_name, fargateProfileName=self.fargate_profile_name
        )
        mock_waiter.assert_called_with(
            mock.ANY,
            clusterName=CLUSTER_NAME,
            fargateProfileName=FARGATE_PROFILE_NAME,
            WaiterConfig={"Delay": 30, "MaxAttempts": 60},
        )
        assert_expected_waiter_type(mock_waiter, "FargateProfileDeleted")

    @mock.patch.object(EksHook, "delete_fargate_profile")
    def test_delete_fargate_profile_deferrable(self, _):
        self.delete_fargate_profile_operator.deferrable = True

        with pytest.raises(TaskDeferred) as exc:
            self.delete_fargate_profile_operator.execute({})
        assert isinstance(
            exc.value.trigger, EksDeleteFargateProfileTrigger
        ), "Trigger is not a EksDeleteFargateProfileTrigger"

    def test_template_fields(self):
        validate_template_fields(self.delete_fargate_profile_operator)


class TestEksPodOperator:
    @mock.patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.execute")
    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.generate_config_file")
    @mock.patch("airflow.providers.amazon.aws.hooks.eks.EksHook.__init__", return_value=None)
    def test_existing_nodegroup(
        self, mock_eks_hook, mock_generate_config_file, mock_k8s_pod_operator_execute
    ):
        ti_context = mock.MagicMock(name="ti_context")

        op = EksPodOperator(
            task_id="run_pod",
            pod_name="run_pod",
            cluster_name=CLUSTER_NAME,
            image="amazon/aws-cli:latest",
            cmds=["sh", "-c", "ls"],
            labels={"demo": "hello_world"},
            get_logs=True,
            # Delete the pod when it reaches its final state, or the execution is interrupted.
            on_finish_action="delete_pod",
        )
        op_return_value = op.execute(ti_context)
        mock_k8s_pod_operator_execute.assert_called_once_with(ti_context)
        mock_eks_hook.assert_called_once_with(aws_conn_id="aws_default", region_name=None)
        mock_generate_config_file.assert_called_once_with(
            eks_cluster_name=CLUSTER_NAME, pod_namespace="default"
        )
        assert mock_k8s_pod_operator_execute.return_value == op_return_value
        assert mock_generate_config_file.return_value.__enter__.return_value == op.config_file

    @pytest.mark.parametrize(
        "compatible_kpo, kwargs, expected_attributes",
        [
            (
                True,
                {"on_finish_action": "delete_succeeded_pod"},
                {"on_finish_action": OnFinishAction.DELETE_SUCCEEDED_POD},
            ),
            (
                # test default
                True,
                {},
                {"on_finish_action": OnFinishAction.DELETE_POD},
            ),
            (
                # test default
                False,
                {},
                {},
            ),
        ],
    )
    def test_on_finish_action_handler(self, compatible_kpo, kwargs, expected_attributes):
        kpo_init_args_mock = mock.MagicMock(**{"parameters": ["on_finish_action"] if compatible_kpo else []})

        with mock.patch("inspect.signature", return_value=kpo_init_args_mock):
            op = EksPodOperator(
                task_id="run_pod",
                pod_name="run_pod",
                cluster_name=CLUSTER_NAME,
                image="amazon/aws-cli:latest",
                cmds=["sh", "-c", "ls"],
                labels={"demo": "hello_world"},
                get_logs=True,
                **kwargs,
            )
            for expected_attr in expected_attributes:
                assert op.__getattribute__(expected_attr) == expected_attributes[expected_attr]

    def test_template_fields(self):
        op = EksPodOperator(
            task_id="run_pod",
            pod_name="run_pod",
            cluster_name=CLUSTER_NAME,
            image="amazon/aws-cli:latest",
            cmds=["sh", "-c", "ls"],
            labels={"demo": "hello_world"},
            get_logs=True,
            on_finish_action="delete_pod",
        )

        validate_template_fields(op)
