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
from airflow.providers.amazon.aws.operators.eks import EKSCreateNodegroupOperator, EKSDeleteNodegroupOperator
from airflow.providers.amazon.aws.sensors.eks import EKSNodegroupStateSensor
from airflow.utils.dates import days_ago

####
# NOTE:  This example requires an existing EKS Cluster.
# see: example_eks_create_cluster.py
####

CLUSTER_NAME = 'existing-eks-cluster'
NODEGROUP_SUFFIX = '-nodegroup'
NODEGROUP_NAME = CLUSTER_NAME + NODEGROUP_SUFFIX
SUBNETS = ['subnet-12345ab', 'subnet-67890cd']
ROLE_ARN = os.environ.get('ROLE_ARN', 'arn:aws:iam::123456789012:role/role_name')

with DAG(
    dag_id='create_eks_nodegroup_dag',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['example'],
) as dag:

    # [START howto_operator_eks_create_nodegroup]
    create_nodegroup = EKSCreateNodegroupOperator(
        task_id='create_eks_nodegroup',
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        nodegroup_subnets=SUBNETS,
        nodegroup_role_arn=ROLE_ARN,
    )
    # [END howto_operator_eks_create_nodegroup]

    wait = EKSNodegroupStateSensor(
        task_id='wait_for_provisioning',
        cluster_name=CLUSTER_NAME,
        nodegroup_name=NODEGROUP_NAME,
        target_state='ACTIVE',
    )

    # [START howto_operator_eks_delete_nodegroup]
    delete_nodegroup = EKSDeleteNodegroupOperator(
        task_id='delete_eks_nodegroup', cluster_name=CLUSTER_NAME, nodegroup_name=NODEGROUP_NAME
    )
    # [END howto_operator_eks_delete_nodegroup]

    create_nodegroup >> wait >> delete_nodegroup
