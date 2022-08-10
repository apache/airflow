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
from datetime import datetime

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "kubernetes_engine"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

GCP_LOCATION = "europe-north1-a"
CLUSTER_NAME = f"cluster-name-test-build-{ENV_ID}"

# [START howto_operator_gcp_gke_create_cluster_definition]
CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1}
# [END howto_operator_gcp_gke_create_cluster_definition]

with models.DAG(
    DAG_ID,
    schedule='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_gke_create_cluster]
    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body=CLUSTER,
    )
    # [END howto_operator_gke_create_cluster]

    pod_task = GKEStartPodOperator(
        task_id="pod_task",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        namespace="default",
        image="perl",
        name="test-pod",
        in_cluster=False,
        is_delete_operator_pod=True,
    )

    # [START howto_operator_gke_start_pod_xcom]
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
        in_cluster=False,
        is_delete_operator_pod=True,
    )
    # [END howto_operator_gke_start_pod_xcom]

    # [START howto_operator_gke_xcom_result]
    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('pod_task_xcom')[0] }}\"",
        task_id="pod_task_xcom_result",
    )
    # [END howto_operator_gke_xcom_result]

    # [START howto_operator_gke_delete_cluster]
    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        name=CLUSTER_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
    )
    # [END howto_operator_gke_delete_cluster]

    create_cluster >> pod_task >> delete_cluster
    create_cluster >> pod_task_xcom >> delete_cluster
    pod_task_xcom >> pod_task_xcom_result

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
