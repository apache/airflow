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
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.eks import EKSPodOperator
from airflow.utils.dates import days_ago

####
# NOTE: This example requires an existing EKS Cluster with a compute backend.
# see: example_eks_create_cluster_with_nodegroup.py
####

CLUSTER_NAME = 'existing-cluster-with-nodegroup-ready-for-pod'
BUCKET_SUFFIX = datetime.now().strftime("-%Y%b%d-%H%M").lower()
ROLE_ARN = os.environ.get('ROLE_ARN', 'arn:aws:iam::123456789012:role/role_name')

with DAG(
    dag_id='eks_run_pod_dag',
    schedule_interval=None,
    start_date=days_ago(2),
    max_active_runs=1,
    tags=['example'],
) as dag:

    # [START howto_operator_eks_pod_operator]
    start_pod = EKSPodOperator(
        task_id="run_pod",
        cluster_name=CLUSTER_NAME,
        # Optional IAM Role to assume for credentials when signing the token.
        cluster_role_arn=ROLE_ARN,
        image="amazon/aws-cli:latest",
        cmds=["sh", "-c", "aws s3 mb s3://hello-world" + BUCKET_SUFFIX],
        labels={"demo": "hello_world"},
        get_logs=True,
        # Delete the pod when it reaches its final state, or the execution is interrupted.
        is_delete_operator_pod=True,
    )
    # [END howto_operator_eks_pod_operator]
