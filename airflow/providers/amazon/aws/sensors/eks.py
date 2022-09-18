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

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.eks import (
    ClusterStates,
    EksHook,
    FargateProfileStates,
    NodegroupStates,
)
from airflow.sensors.base import BaseSensorOperator

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
UNEXPECTED_TERMINAL_STATE_MSG = (
    "Terminal state reached. Current state: {current_state}, Expected state: {target_state}"
)


class EksClusterStateSensor(BaseSensorOperator):
    """
    Check the state of an Amazon EKS Cluster until it reaches the target state or another terminal state.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EksClusterStateSensor`

    :param cluster_name: The name of the Cluster to watch. (templated)
    :param target_state: Target state of the Cluster. (templated)
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    """

    template_fields: Sequence[str] = ("cluster_name", "target_state", "aws_conn_id", "region")
    ui_color = "#ff9900"
    ui_fgcolor = "#232F3E"

    def __init__(
        self,
        *,
        cluster_name: str,
        target_state: ClusterStates = ClusterStates.ACTIVE,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs,
    ):
        self.cluster_name = cluster_name
        self.target_state = (
            target_state
            if isinstance(target_state, ClusterStates)
            else ClusterStates(str(target_state).upper())
        )
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def poke(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        cluster_state = eks_hook.get_cluster_state(clusterName=self.cluster_name)
        self.log.info("Cluster state: %s", cluster_state)
        if cluster_state in (CLUSTER_TERMINAL_STATES - {self.target_state}):
            # If we reach a terminal state which is not the target state:
            raise AirflowException(
                UNEXPECTED_TERMINAL_STATE_MSG.format(
                    current_state=cluster_state, target_state=self.target_state
                )
            )
        return cluster_state == self.target_state


class EksFargateProfileStateSensor(BaseSensorOperator):
    """
    Check the state of an AWS Fargate profile until it reaches the target state or another terminal state.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EksFargateProfileStateSensor`

    :param cluster_name: The name of the Cluster which the AWS Fargate profile is attached to. (templated)
    :param fargate_profile_name: The name of the Fargate profile to watch. (templated)
    :param target_state: Target state of the Fargate profile. (templated)
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    """

    template_fields: Sequence[str] = (
        "cluster_name",
        "fargate_profile_name",
        "target_state",
        "aws_conn_id",
        "region",
    )
    ui_color = "#ff9900"
    ui_fgcolor = "#232F3E"

    def __init__(
        self,
        *,
        cluster_name: str,
        fargate_profile_name: str,
        target_state: FargateProfileStates = FargateProfileStates.ACTIVE,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs,
    ):
        self.cluster_name = cluster_name
        self.fargate_profile_name = fargate_profile_name
        self.target_state = (
            target_state
            if isinstance(target_state, FargateProfileStates)
            else FargateProfileStates(str(target_state).upper())
        )
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def poke(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        fargate_profile_state = eks_hook.get_fargate_profile_state(
            clusterName=self.cluster_name, fargateProfileName=self.fargate_profile_name
        )
        self.log.info("Fargate profile state: %s", fargate_profile_state)
        if fargate_profile_state in (FARGATE_TERMINAL_STATES - {self.target_state}):
            # If we reach a terminal state which is not the target state:
            raise AirflowException(
                UNEXPECTED_TERMINAL_STATE_MSG.format(
                    current_state=fargate_profile_state, target_state=self.target_state
                )
            )
        return fargate_profile_state == self.target_state


class EksNodegroupStateSensor(BaseSensorOperator):
    """
    Check the state of an EKS managed node group until it reaches the target state or another terminal state.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:EksNodegroupStateSensor`

    :param cluster_name: The name of the Cluster which the Nodegroup is attached to. (templated)
    :param nodegroup_name: The name of the Nodegroup to watch. (templated)
    :param target_state: Target state of the Nodegroup. (templated)
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    """

    template_fields: Sequence[str] = (
        "cluster_name",
        "nodegroup_name",
        "target_state",
        "aws_conn_id",
        "region",
    )
    ui_color = "#ff9900"
    ui_fgcolor = "#232F3E"

    def __init__(
        self,
        *,
        cluster_name: str,
        nodegroup_name: str,
        target_state: NodegroupStates = NodegroupStates.ACTIVE,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: str | None = None,
        **kwargs,
    ):
        self.cluster_name = cluster_name
        self.nodegroup_name = nodegroup_name
        self.target_state = (
            target_state
            if isinstance(target_state, NodegroupStates)
            else NodegroupStates(str(target_state).upper())
        )
        self.aws_conn_id = aws_conn_id
        self.region = region
        super().__init__(**kwargs)

    def poke(self, context: Context):
        eks_hook = EksHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        nodegroup_state = eks_hook.get_nodegroup_state(
            clusterName=self.cluster_name, nodegroupName=self.nodegroup_name
        )
        self.log.info("Nodegroup state: %s", nodegroup_state)
        if nodegroup_state in (NODEGROUP_TERMINAL_STATES - {self.target_state}):
            # If we reach a terminal state which is not the target state:
            raise AirflowException(
                UNEXPECTED_TERMINAL_STATE_MSG.format(
                    current_state=nodegroup_state, target_state=self.target_state
                )
            )
        return nodegroup_state == self.target_state
