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

# Ignore missing args provided by default_args
# type: ignore[call-arg]

from datetime import datetime
from os import environ

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.eks import ClusterStates, FargateProfileStates
from airflow.providers.amazon.aws.operators.eks import (
    EksCreateClusterOperator,
    EksCreateFargateProfileOperator,
    EksDeleteClusterOperator,
    EksDeleteFargateProfileOperator,
    EksPodOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EksClusterStateSensor, EksFargateProfileStateSensor

CLUSTER_NAME = 'fargate-demo'
FARGATE_PROFILE_NAME = f'{CLUSTER_NAME}-profile'
SELECTORS = [{'namespace': 'default'}]

ROLE_ARN = environ.get('EKS_DEMO_ROLE_ARN', 'arn:aws:iam::123456789012:role/role_name')
SUBNETS = environ.get('EKS_DEMO_SUBNETS', 'subnet-12345ab subnet-67890cd').split(' ')
VPC_CONFIG = {
    'subnetIds': SUBNETS,
    'endpointPublicAccess': True,
    'endpointPrivateAccess': False,
}


with DAG(
    dag_id='example_eks_with_fargate_profile',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:

    # Create an Amazon EKS Cluster control plane without attaching a compute service.
    create_cluster = EksCreateClusterOperator(
        task_id='create_eks_cluster',
        cluster_name=CLUSTER_NAME,
        cluster_role_arn=ROLE_ARN,
        resources_vpc_config=VPC_CONFIG,
        compute=None,
    )

    await_create_cluster = EksClusterStateSensor(
        task_id='wait_for_create_cluster',
        cluster_name=CLUSTER_NAME,
        target_state=ClusterStates.ACTIVE,
    )

    # [START howto_operator_eks_create_fargate_profile]
    create_fargate_profile = EksCreateFargateProfileOperator(
        task_id='create_eks_fargate_profile',
        cluster_name=CLUSTER_NAME,
        pod_execution_role_arn=ROLE_ARN,
        fargate_profile_name=FARGATE_PROFILE_NAME,
        selectors=SELECTORS,
    )
    # [END howto_operator_eks_create_fargate_profile]

    # [START howto_sensor_eks_fargate]
    await_create_fargate_profile = EksFargateProfileStateSensor(
        task_id='wait_for_create_fargate_profile',
        cluster_name=CLUSTER_NAME,
        fargate_profile_name=FARGATE_PROFILE_NAME,
        target_state=FargateProfileStates.ACTIVE,
    )
    # [END howto_sensor_eks_fargate]

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

    # [START howto_operator_eks_delete_fargate_profile]
    delete_fargate_profile = EksDeleteFargateProfileOperator(
        task_id='delete_eks_fargate_profile',
        cluster_name=CLUSTER_NAME,
        fargate_profile_name=FARGATE_PROFILE_NAME,
    )
    # [END howto_operator_eks_delete_fargate_profile]

    await_delete_fargate_profile = EksFargateProfileStateSensor(
        task_id='wait_for_delete_fargate_profile',
        cluster_name=CLUSTER_NAME,
        fargate_profile_name=FARGATE_PROFILE_NAME,
        target_state=FargateProfileStates.NONEXISTENT,
    )

    delete_cluster = EksDeleteClusterOperator(
        task_id='delete_eks_cluster',
        cluster_name=CLUSTER_NAME,
    )

    await_delete_cluster = EksClusterStateSensor(
        task_id='wait_for_delete_cluster',
        cluster_name=CLUSTER_NAME,
        target_state=ClusterStates.NONEXISTENT,
    )

    (
        create_cluster
        >> await_create_cluster
        >> create_fargate_profile
        >> await_create_fargate_profile
        >> start_pod
        >> delete_fargate_profile
        >> await_delete_fargate_profile
        >> delete_cluster
        >> await_delete_cluster
    )
