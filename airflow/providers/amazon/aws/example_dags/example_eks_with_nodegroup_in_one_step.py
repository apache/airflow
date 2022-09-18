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

from datetime import datetime
from os import environ

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, NodegroupStates
from airflow.providers.amazon.aws.operators.eks import (
    EksCreateClusterOperator,
    EksDeleteClusterOperator,
    EksPodOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksNodegroupStateSensor

# Ignore missing args provided by default_args
# type: ignore[call-arg]

CLUSTER_NAME = environ.get('EKS_CLUSTER_NAME', 'eks-demo')
NODEGROUP_NAME = f'{CLUSTER_NAME}-nodegroup'
ROLE_ARN = environ.get('EKS_DEMO_ROLE_ARN', 'arn:aws:iam::123456789012:role/role_name')
SUBNETS = environ.get('EKS_DEMO_SUBNETS', 'subnet-12345ab subnet-67890cd').split(' ')
VPC_CONFIG = {
    'subnetIds': SUBNETS,
    'endpointPublicAccess': True,
    'endpointPrivateAccess': False,
}


with DAG(
    dag_id='example_eks_with_nodegroup_in_one_step',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # [START howto_operator_eks_create_cluster_with_nodegroup]
    # Create an Amazon EKS cluster control plane and an EKS nodegroup compute platform in one step.
    create_cluster_and_nodegroup = EksCreateClusterOperator(
        task_id='create_eks_cluster_and_nodegroup',
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        cluster_role_arn=ROLE_ARN,
        nodegroup_role_arn=ROLE_ARN,
        # Opting to use the same ARN for the cluster and the nodegroup here,
        # but a different ARN could be configured and passed if desired.
        resources_vpc_config=VPC_CONFIG,
        # Compute defaults to 'nodegroup' but is called out here for the purposed of the example.
        compute='nodegroup',
    )
    # [END howto_operator_eks_create_cluster_with_nodegroup]

    await_create_nodegroup = EksNodegroupStateSensor(
        task_id='wait_for_create_nodegroup',
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        target_state=NodegroupStates.ACTIVE,
    )

    start_pod = EksPodOperator(
        task_id="run_pod",
        cluster_name=CLUSTER_NAME,
        pod_name="run_pod",
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "echo Test Airflow; date"],
        labels={"demo": "hello_world"},
        get_logs=True,
        # Delete the pod when it reaches its final state, or the execution is interrupted.
        is_delete_operator_pod=True,
    )

    # [START howto_operator_eks_force_delete_cluster]
    # An Amazon EKS cluster can not be deleted with attached resources such as nodegroups or Fargate profiles.
    # Setting the `force` to `True` will delete any attached resources before deleting the cluster.
    delete_all = EksDeleteClusterOperator(
        task_id='delete_nodegroup_and_cluster',
        cluster_name=CLUSTER_NAME,
        force_delete_compute=True,
    )
    # [END howto_operator_eks_force_delete_cluster]

    await_delete_cluster = EksClusterStateSensor(
        task_id='wait_for_delete_cluster',
        cluster_name=CLUSTER_NAME,
        target_state=ClusterStates.NONEXISTENT,
    )

    (
        create_cluster_and_nodegroup
        >> await_create_nodegroup
        >> start_pod
        >> delete_all
        >> await_delete_cluster
    )
