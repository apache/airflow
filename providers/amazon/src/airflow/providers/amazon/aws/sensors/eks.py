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
"""Tracking the state of Amazon EKS Clusters, Amazon EKS managed node groups, and AWS Fargate profiles."""

from __future__ import annotations

import warnings
from abc import abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.eks import (
    ClusterStates,
    EksHook,
    FargateProfileStates,
    NodegroupStates,
)
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


DEFAULT_CONN_ID = "aws_default"

CLUSTER_TERMINAL_STATES = frozenset({ClusterStates.ACTIVE, ClusterStates.FAILED, ClusterStates.NONEXISTENT})
FARGATE_TERMINAL_STATES = frozenset(
    {
        FargateProfileStates.ACTIVE,
        FargateProfileStates.CREATE_FAILED,
        FargateProfileStates.DELETE_FAILED,
        FargateProfileStates.NONEXISTENT,
    }
)
NODEGROUP_TERMINAL_STATES = frozenset(
    {
        NodegroupStates.ACTIVE,
        NodegroupStates.CREATE_FAILED,
        NodegroupStates.DELETE_FAILED,
        NodegroupStates.NONEXISTENT,
    }
)


class EksBaseSensor(AwsBaseSensor):
    """
    Base class to check various EKS states.

    Subclasses need to implement get_state and get_terminal_states methods.

    :param cluster_name: The name of the Cluster
    :param target_state: Will return successfully when that state is reached.
    :param target_state_type: The enum containing the states,
        will be used to convert the target state if it has to be converted from a string
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    """

    aws_hook_class = EksHook

    def __init__(
        self,
        *,
        cluster_name: str,
        target_state: ClusterStates | NodegroupStates | FargateProfileStates,
        target_state_type: type,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.target_state = (
            target_state
            if isinstance(target_state, target_state_type)
            else target_state_type(str(target_state).upper())
        )

    def poke(self, context: Context) -> bool:
        state = self.get_state()
        self.log.info("Current state: %s", state)
        if state in (self.get_terminal_states() - {self.target_state}):
            # If we reach a terminal state which is not the target state
            raise AirflowException(
                f"Terminal state reached. Current state: {state}, Expected state: {self.target_state}"
            )
        return state == self.target_state

    @abstractmethod
    def get_state(self) -> ClusterStates | NodegroupStates | FargateProfileStates: ...

    @abstractmethod
    def get_terminal_states(self) -> frozenset: ...


class EksClusterStateSensor(EksBaseSensor):
    """
    Check the state of an Amazon EKS Cluster until it reaches the target state or another terminal state.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EksClusterStateSensor`

    :param cluster_name: The name of the Cluster to watch. (templated)
    :param target_state: Target state of the Cluster. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    """

    template_fields: Sequence[str] = aws_template_fields("cluster_name", "target_state")
    ui_color = "#ff9900"
    ui_fgcolor = "#232F3E"

    def __init__(
        self,
        *,
        target_state: ClusterStates = ClusterStates.ACTIVE,
        region: str | None = None,
        **kwargs,
    ):
        if region is not None:
            warnings.warn(
                message="Parameter `region` is deprecated. Use the parameter `region_name` instead",
                category=AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            kwargs["region_name"] = region
        super().__init__(target_state=target_state, target_state_type=ClusterStates, **kwargs)

    def get_state(self) -> ClusterStates:
        return self.hook.get_cluster_state(clusterName=self.cluster_name)

    def get_terminal_states(self) -> frozenset:
        return CLUSTER_TERMINAL_STATES


class EksFargateProfileStateSensor(EksBaseSensor):
    """
    Check the state of an AWS Fargate profile until it reaches the target state or another terminal state.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EksFargateProfileStateSensor`

    :param cluster_name: The name of the Cluster which the AWS Fargate profile is attached to. (templated)
    :param fargate_profile_name: The name of the Fargate profile to watch. (templated)
    :param target_state: Target state of the Fargate profile. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    """

    template_fields: Sequence[str] = aws_template_fields(
        "cluster_name", "fargate_profile_name", "target_state"
    )
    ui_color = "#ff9900"
    ui_fgcolor = "#232F3E"

    def __init__(
        self,
        *,
        fargate_profile_name: str,
        region: str | None = None,
        target_state: FargateProfileStates = FargateProfileStates.ACTIVE,
        **kwargs,
    ):
        if region is not None:
            warnings.warn(
                message="Parameter `region` is deprecated. Use the parameter `region_name` instead",
                category=AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            kwargs["region_name"] = region
        super().__init__(target_state=target_state, target_state_type=FargateProfileStates, **kwargs)
        self.fargate_profile_name = fargate_profile_name

    def get_state(self) -> FargateProfileStates:
        return self.hook.get_fargate_profile_state(
            clusterName=self.cluster_name, fargateProfileName=self.fargate_profile_name
        )

    def get_terminal_states(self) -> frozenset:
        return FARGATE_TERMINAL_STATES


class EksNodegroupStateSensor(EksBaseSensor):
    """
    Check the state of an EKS managed node group until it reaches the target state or another terminal state.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EksNodegroupStateSensor`

    :param cluster_name: The name of the Cluster which the Nodegroup is attached to. (templated)
    :param nodegroup_name: The name of the Nodegroup to watch. (templated)
    :param target_state: Target state of the Nodegroup. (templated)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    """

    template_fields: Sequence[str] = aws_template_fields("cluster_name", "nodegroup_name", "target_state")
    ui_color = "#ff9900"
    ui_fgcolor = "#232F3E"

    def __init__(
        self,
        *,
        nodegroup_name: str,
        target_state: NodegroupStates = NodegroupStates.ACTIVE,
        region: str | None = None,
        **kwargs,
    ):
        if region is not None:
            warnings.warn(
                message="Parameter `region` is deprecated. Use the parameter `region_name` instead",
                category=AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            kwargs["region_name"] = region
        super().__init__(target_state=target_state, target_state_type=NodegroupStates, **kwargs)
        self.nodegroup_name = nodegroup_name

    def get_state(self) -> NodegroupStates:
        return self.hook.get_nodegroup_state(clusterName=self.cluster_name, nodegroupName=self.nodegroup_name)

    def get_terminal_states(self) -> frozenset:
        return NODEGROUP_TERMINAL_STATES
