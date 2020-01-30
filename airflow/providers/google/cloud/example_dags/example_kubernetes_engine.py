# -*- coding: utf-8 -*-
#
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
"""
Example Airflow DAG for Google Kubernetes Engine.
"""

import os

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator, GKEDeleteClusterOperator, GKEStartPodOperator,
)
from airflow.utils.dates import days_ago

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCP_LOCATION = os.environ.get("GCP_GKE_LOCATION", "europe-north1-a")
CLUSTER_NAME = os.environ.get("GCP_GKE_CLUSTER_NAME", "cluster-name")

CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1}

default_args = {"start_date": days_ago(1)}

with models.DAG(
    "example_gcp_gke",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag:
    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body=CLUSTER,
    )

    pod_task = GKEStartPodOperator(
        task_id="pod_task",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        namespace="default",
        image="perl",
        name="test-pod",
    )

    pod_task_xcom = GKEStartPodOperator(
        task_id="pod_task_xcom",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        do_xcom_push=True,
        namespace="default",
        image="alpine",
        cmds=["sh", "-c", 'mkdir -p /airflow/xcom/;echo \'[1,2,3,4]\' > /airflow/xcom/return.json'],
        name="test-pod-xcom",
    )

    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('pod_task_xcom')[0] }}\"",
        task_id="pod_task_xcom_result",
    )

    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        name=CLUSTER_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
    )

    create_cluster >> pod_task >> delete_cluster
    create_cluster >> pod_task_xcom >> delete_cluster
    pod_task_xcom >> pod_task_xcom_result
