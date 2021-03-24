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
from airflow.providers.amazon.aws.operators.eks import (
    EKSCreateClusterOperator,
    EKSDeleteClusterOperator,
    EKSDescribeClusterOperator,
    EKSListClustersOperator,
)
from airflow.providers.amazon.aws.sensors.eks import EKSClusterStateSensor
from airflow.utils.dates import days_ago

CLUSTER_NAME = 'eks-demo-create-cluster'
ROLE_ARN = os.environ.get('ROLE_ARN', 'arn:aws:iam::123456789012:role/role_name')
VPC_CONFIG = {
    'subnetIds': ['subnet-12345ab', 'subnet-67890cd'],
    'endpointPublicAccess': True,
    'endpointPrivateAccess': False,
}

with DAG(
    dag_id='create_eks_cluster_dag',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['example'],
) as dag:

    # [START howto_operator_eks_create_cluster]

    # Create an Amazon EKS Cluster control plane without attaching a compute service.
    create_cluster = EKSCreateClusterOperator(
        task_id='create_eks_cluster',
        cluster_name=CLUSTER_NAME,
        cluster_role_arn=ROLE_ARN,
        resources_vpc_config=VPC_CONFIG,
        compute=None,
    )
    # [END howto_operator_eks_create_cluster]

    # [START howto_operator_eks_list_clusters]
    list_clusters = EKSListClustersOperator(task_id='list_eks_clusters', verbose=True)
    # [END howto_operator_eks_list_clusters]

    wait = EKSClusterStateSensor(
        task_id='wait_for_provisioning', cluster_name=CLUSTER_NAME, target_state='ACTIVE'
    )

    # [START howto_operator_eks_describe_cluster]
    describe_cluster = EKSDescribeClusterOperator(task_id='describe_eks_cluster', cluster_name=CLUSTER_NAME)
    # [END howto_operator_eks_describe_cluster]

    # [START howto_operator_eks_delete_cluster]
    delete_cluster = EKSDeleteClusterOperator(task_id='delete_eks_cluster', cluster_name=CLUSTER_NAME)
    # [END howto_operator_eks_delete_cluster]

    create_cluster >> list_clusters >> wait >> describe_cluster >> delete_cluster
