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
    EksCreateNodegroupOperator,
    EksDeleteClusterOperator,
    EksDeleteNodegroupOperator,
    EksPodOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksNodegroupStateSensor

# Ignore missing args provided by default_args
# type: ignore[call-arg]

CLUSTER_NAME = 'eks-demo'
NODEGROUP_SUFFIX = '-nodegroup'
NODEGROUP_NAME = CLUSTER_NAME + NODEGROUP_SUFFIX
ROLE_ARN = environ.get('EKS_DEMO_ROLE_ARN', 'arn:aws:iam::123456789012:role/role_name')
SUBNETS = environ.get('EKS_DEMO_SUBNETS', 'subnet-12345ab subnet-67890cd').split(' ')
VPC_CONFIG = {
    'subnetIds': SUBNETS,
    'endpointPublicAccess': True,
    'endpointPrivateAccess': False,
}


with DAG(
    dag_id='example_eks_with_nodegroups',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # [START howto_operator_eks_create_cluster]
    # Create an Amazon EKS Cluster control plane without attaching compute service.
    create_cluster = EksCreateClusterOperator(
        task_id='create_eks_cluster',
        cluster_name=CLUSTER_NAME,
        cluster_role_arn=ROLE_ARN,
        resources_vpc_config=VPC_CONFIG,
        compute=None,
    )
    # [END howto_operator_eks_create_cluster]

    # [START howto_sensor_eks_cluster]
    await_create_cluster = EksClusterStateSensor(
        task_id='wait_for_create_cluster',
        cluster_name=CLUSTER_NAME,
        target_state=ClusterStates.ACTIVE,
    )
    # [END howto_sensor_eks_cluster]

    # [START howto_operator_eks_create_nodegroup]
    create_nodegroup = EksCreateNodegroupOperator(
        task_id='create_eks_nodegroup',
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        nodegroup_subnets=SUBNETS,
        nodegroup_role_arn=ROLE_ARN,
    )
    # [END howto_operator_eks_create_nodegroup]

    # [START howto_sensor_eks_nodegroup]
    await_create_nodegroup = EksNodegroupStateSensor(
        task_id='wait_for_create_nodegroup',
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        target_state=NodegroupStates.ACTIVE,
    )
    # [END howto_sensor_eks_nodegroup]

    # [START howto_operator_eks_pod_operator]
    start_pod = EksPodOperator(
        task_id="run_pod",
        cluster_name=CLUSTER_NAME,
        pod_name="run_pod",
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "ls"],
        labels={"demo": "hello_world"},
        get_logs=True,
        # Delete the pod when it reaches its final state, or the execution is interrupted.
        is_delete_operator_pod=True,
    )
    # [END howto_operator_eks_pod_operator]

    # [START howto_operator_eks_delete_nodegroup]
    delete_nodegroup = EksDeleteNodegroupOperator(
        task_id='delete_eks_nodegroup',
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
    )
    # [END howto_operator_eks_delete_nodegroup]

    await_delete_nodegroup = EksNodegroupStateSensor(
        task_id='wait_for_delete_nodegroup',
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        target_state=NodegroupStates.NONEXISTENT,
    )

    # [START howto_operator_eks_delete_cluster]
    delete_cluster = EksDeleteClusterOperator(
        task_id='delete_eks_cluster',
        cluster_name=CLUSTER_NAME,
    )
    # [END howto_operator_eks_delete_cluster]

    await_delete_cluster = EksClusterStateSensor(
        task_id='wait_for_delete_cluster',
        cluster_name=CLUSTER_NAME,
        target_state=ClusterStates.NONEXISTENT,
    )

    (
        create_cluster
        >> await_create_cluster
        >> create_nodegroup
        >> await_create_nodegroup
        >> start_pod
        >> delete_nodegroup
        >> await_delete_nodegroup
        >> delete_cluster
        >> await_delete_cluster
    )
