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
import os

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.eks import EKSCreateClusterOperator, EKSDeleteClusterOperator
from airflow.providers.amazon.aws.sensors.eks import EKSNodegroupStateSensor
from airflow.utils.dates import days_ago

CLUSTER_NAME = 'eks-demo-create-cluster-with-nodegroup'
NODEGROUP_SUFFIX = '-nodegroup'
NODEGROUP_NAME = CLUSTER_NAME + NODEGROUP_SUFFIX
ROLE_ARN = os.environ.get('ROLE_ARN', 'arn:aws:iam::123456789012:role/role_name')
VPC_CONFIG = {
    'subnetIds': ['subnet-12345ab', 'subnet-67890cd'],
    'endpointPublicAccess': True,
    'endpointPrivateAccess': False,
}

with DAG(
    dag_id='create_eks_cluster_and_nodegroup_dag',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['example'],
) as dag:

    # [START howto_operator_eks_create_cluster_with_compute]
    # Create an Amazon EKS Cluster control plane and an EKS Managed Nodegroup compute platform.
    create_cluster = EKSCreateClusterOperator(
        task_id='create_eks_cluster_and_nodegroup',
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        # Opting to use the same ARN for the cluster and the nodegroup here for simplicity,
        # but a different ARN could be configured and passed if desired.
        cluster_role_arn=ROLE_ARN,
        nodegroup_role_arn=ROLE_ARN,
        resources_vpc_config=VPC_CONFIG,
        # Compute defaults to 'nodegroup' but is called out here for the purposed of the example.
        compute='nodegroup',
    )
    # [END howto_operator_eks_create_cluster_with_compute]

    wait = EKSNodegroupStateSensor(
        task_id='wait_for_provisioning',
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        target_state='ACTIVE',
    )

    delete_cluster = EKSDeleteClusterOperator(task_id='delete_eks_cluster', cluster_name=CLUSTER_NAME)

    create_cluster >> wait >> delete_cluster
