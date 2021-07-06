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

from boto3 import Session

from airflow.providers.amazon.aws.hooks.eks import EKSHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

CONN_ID = "eks"
TARGET_STATE = 'ACTIVE'


class EKSClusterStateSensor(BaseSensorOperator):
    """
    Check the state of an Amazon EKS Cluster until the state of the Cluster equals the target state.

    :param cluster_name: The name of the Cluster to watch.
    :type cluster_name: str
    :param target_state: Target state of the Cluster.
    :type target_state: str
    """

    template_fields = ("target_state", "cluster_name")
    ui_color = "#ff9900"
    ui_fgcolor = "#232F3E"
    valid_states = ["CREATING", "ACTIVE", "DELETING", "FAILED", "UPDATING"]

    @apply_defaults
    def __init__(
        self,
        *,
        cluster_name: str,
        target_state: Optional[str] = TARGET_STATE,
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ):
        if target_state not in self.valid_states:
            raise ValueError(f"Invalid target_state: {target_state}")
        super().__init__(**kwargs)
        self.target_state = target_state.upper()
        self.cluster_name = cluster_name
        self.conn_id = conn_id
        self.region = region

    def poke(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        cluster_state = eks_hook.get_cluster_state(clusterName=self.cluster_name)
        self.log.info("Cluster state: %s", cluster_state)
        return cluster_state == self.target_state


class EKSNodegroupStateSensor(BaseSensorOperator):
    """
    Check the state of an Amazon EKS Nodegroup until the state of the Nodegroup equals the target state.

    :param cluster_name: The name of the Cluster which the Nodegroup is attached to.
    :type cluster_name: str
    :param nodegroup_name: The name of the Nodegroup to watch.
    :type nodegroup_name: str
    :param target_state: Target state of the Nodegroup.
    :type target_state: str
    """

    template_fields = ("target_state", "cluster_name", "nodegroup_name")
    ui_color = "#ff9900"
    ui_fgcolor = "#232F3E"
    valid_states = [
        "CREATING",
        "ACTIVE",
        "UPDATING",
        "DELETING",
        "CREATE_FAILED",
        "DELETE_FAILED",
        "DEGRADED",
    ]

    @apply_defaults
    def __init__(
        self,
        *,
        cluster_name: str,
        nodegroup_name: str,
        target_state: Optional[str] = TARGET_STATE,
        conn_id: Optional[str] = CONN_ID,
        region: Optional[str] = None,
        **kwargs,
    ):
        if target_state.upper() not in self.valid_states:
            raise ValueError(f"Invalid target_state: {target_state}")
        super().__init__(**kwargs)
        self.target_state = target_state.upper()
        self.cluster_name = cluster_name
        self.nodegroup_name = nodegroup_name
        self.conn_id = conn_id
        self.region = region

    def poke(self, context):
        eks_hook = EKSHook(
            aws_conn_id=self.conn_id,
            region_name=self.region,
        )

        nodegroup_state = eks_hook.get_nodegroup_state(
            clusterName=self.cluster_name, nodegroupName=self.nodegroup_name
        )
        self.log.info("Nodegroup state: %s", nodegroup_state)
        return nodegroup_state == self.target_state
