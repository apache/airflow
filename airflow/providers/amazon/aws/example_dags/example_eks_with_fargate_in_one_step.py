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
from os import environ

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, FargateProfileStates
from airflow.providers.amazon.aws.operators.eks import (
    EKSCreateClusterOperator,
    EKSDeleteClusterOperator,
    EKSDeleteFargateProfileOperator,
    EKSPodOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EKSClusterStateSensor, EKSFargateProfileStateSensor
from airflow.utils.dates import days_ago

CLUSTER_NAME = 'fargate-all-in-one'
FARGATE_PROFILE_NAME = f'{CLUSTER_NAME}-profile'

ROLE_ARN = environ.get('EKS_DEMO_ROLE_ARN', 'arn:aws:iam::123456789012:role/role_name')
SUBNETS = environ.get('EKS_DEMO_SUBNETS', 'subnet-12345ab subnet-67890cd').split(' ')
VPC_CONFIG = {
    'subnetIds': SUBNETS,
    'endpointPublicAccess': True,
    'endpointPrivateAccess': False,
}


with DAG(
    dag_id='to-publish-fargate-all-in-one',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['example'],
) as dag:

    # [START howto_operator_eks_create_cluster_with_fargate_profile]
    # Create an Amazon EKS cluster control plane and an AWS Fargate compute platform in one step.
    create_cluster_and_fargate_profile = EKSCreateClusterOperator(
        task_id='create_eks_cluster_and_fargate_profile',
        cluster_name=CLUSTER_NAME,
        cluster_role_arn=ROLE_ARN,
        resources_vpc_config=VPC_CONFIG,
        compute='fargate',
        fargate_profile_name=FARGATE_PROFILE_NAME,
        # Opting to use the same ARN for the cluster and the pod here,
        # but a different ARN could be configured and passed if desired.
        fargate_pod_execution_role_arn=ROLE_ARN,
    )
    # [END howto_operator_eks_create_cluster_with_fargate_profile]

    await_create_fargate_profile = EKSFargateProfileStateSensor(
        task_id='wait_for_create_fargate_profile',
        cluster_name=CLUSTER_NAME,
        fargate_profile_name=FARGATE_PROFILE_NAME,
        target_state=FargateProfileStates.ACTIVE,
    )

    start_pod = EKSPodOperator(
        task_id="run_pod",
        cluster_name=CLUSTER_NAME,
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "echo Test Airflow; date"],
        labels={"demo": "hello_world"},
        get_logs=True,
        # Delete the pod when it reaches its final state, or the execution is interrupted.
        is_delete_operator_pod=True,
    )

    delete_fargate_profile = EKSDeleteFargateProfileOperator(
        task_id='delete_eks_fargate_profile',
        cluster_name=CLUSTER_NAME,
        fargate_profile_name=FARGATE_PROFILE_NAME,
    )

    await_delete_fargate_profile = EKSFargateProfileStateSensor(
        task_id='wait_for_delete_fargate_profile',
        cluster_name=CLUSTER_NAME,
        fargate_profile_name=FARGATE_PROFILE_NAME,
        target_state=FargateProfileStates.NONEXISTENT,
    )

    delete_cluster = EKSDeleteClusterOperator(task_id='delete_eks_cluster', cluster_name=CLUSTER_NAME)

    await_delete_cluster = EKSClusterStateSensor(
        task_id='wait_for_delete_cluster',
        cluster_name=CLUSTER_NAME,
        target_state=ClusterStates.NONEXISTENT,
    )

    (
        create_cluster_and_fargate_profile
        >> await_create_fargate_profile
        >> start_pod
        >> delete_fargate_profile
        >> await_delete_fargate_profile
        >> delete_cluster
        >> await_delete_cluster
    )
