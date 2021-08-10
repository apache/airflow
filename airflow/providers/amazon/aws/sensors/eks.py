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
"""Tracking the state of EKS Clusters and Nodegroups."""
from typing import Optional

from airflow.providers.amazon.aws.hooks.eks import ClusterStates, EKSHook, NodegroupStates
from airflow.sensors.base import BaseSensorOperator

DEFAULT_CONN_ID = "aws_default"


class EKSClusterStateSensor(BaseSensorOperator):
    """
    Check the state of an Amazon EKS Cluster until the state of the Cluster equals the target state.

    :param cluster_name: The name of the Cluster to watch. (templated)
    :type cluster_name: str
    :param target_state: Target state of the Cluster. (templated)
    :type target_state: ClusterStates
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    """

    template_fields = ("cluster_name", "target_state", "aws_conn_id", "region")
    ui_color = "#ff9900"
    ui_fgcolor = "#232F3E"

    def __init__(
        self,
        *,
        cluster_name: str,
        target_state: ClusterStates = ClusterStates.ACTIVE,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
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

    def poke(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        cluster_state = eks_hook.get_cluster_state(clusterName=self.cluster_name)
        self.log.info("Cluster state: %s", cluster_state)
        return cluster_state == self.target_state


class EKSNodegroupStateSensor(BaseSensorOperator):
    """
    Check the state of an Amazon EKS Nodegroup until the state of the Nodegroup equals the target state.

    :param cluster_name: The name of the Cluster which the Nodegroup is attached to. (templated)
    :type cluster_name: str
    :param nodegroup_name: The name of the Nodegroup to watch. (templated)
    :type nodegroup_name: str
    :param target_state: Target state of the Nodegroup. (templated)
    :type target_state: NodegroupStates
    :param region: Which AWS region the connection should use. (templated)
        If this is None or empty then the default boto3 behaviour is used.
    :type region: str
    :param aws_conn_id: The Airflow connection used for AWS credentials. (templated)
         If this is None or empty then the default boto3 behaviour is used. If
         running Airflow in a distributed manner and aws_conn_id is None or
         empty, then the default boto3 configuration would be used (and must be
         maintained on each worker node).
    :type aws_conn_id: str
    """

    template_fields = ("cluster_name", "nodegroup_name", "target_state", "aws_conn_id", "region")
    ui_color = "#ff9900"
    ui_fgcolor = "#232F3E"

    def __init__(
        self,
        *,
        cluster_name: str,
        nodegroup_name: str,
        target_state: NodegroupStates = NodegroupStates.ACTIVE,
        aws_conn_id: str = DEFAULT_CONN_ID,
        region: Optional[str] = None,
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

    def poke(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region,
        )

        nodegroup_state = eks_hook.get_nodegroup_state(
            clusterName=self.cluster_name, nodegroupName=self.nodegroup_name
        )
        self.log.info("Nodegroup state: %s", nodegroup_state)
        return nodegroup_state == self.target_state
