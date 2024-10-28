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
Example Airflow DAG for asynchronous mode of Google Kubernetes Engine.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "kubernetes_engine_async"
GCP_PROJECT_ID = (
    os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
)

GCP_LOCATION = "europe-north1-a"
CLUSTER_NAME_BASE = f"cluster-{DAG_ID}".replace("_", "-")
CLUSTER_NAME_FULL = CLUSTER_NAME_BASE + f"-{ENV_ID}".replace("_", "-")
CLUSTER_NAME = CLUSTER_NAME_BASE if len(CLUSTER_NAME_FULL) >= 33 else CLUSTER_NAME_FULL

CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1}

with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_gke_create_cluster_async]
    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body=CLUSTER,
        deferrable=True,
    )
    # [END howto_operator_gke_create_cluster_async]

    pod_task = GKEStartPodOperator(
        task_id="pod_task",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        namespace="default",
        image="perl",
        name="test-pod-async",
        in_cluster=False,
        on_finish_action="delete_pod",
        get_logs=True,
        deferrable=True,
    )

    # [START howto_operator_gke_start_pod_xcom_async]
    pod_task_xcom_async = GKEStartPodOperator(
        task_id="pod_task_xcom_async",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        namespace="default",
        image="alpine",
        cmds=[
            "sh",
            "-c",
            "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json",
        ],
        name="test-pod-xcom-async",
        in_cluster=False,
        on_finish_action="delete_pod",
        do_xcom_push=True,
        deferrable=True,
        get_logs=True,
    )
    # [END howto_operator_gke_start_pod_xcom_async]

    # [START howto_operator_gke_xcom_result_async]
    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('pod_task_xcom_async')[0] }}\"",
        task_id="pod_task_xcom_result",
    )
    # [END howto_operator_gke_xcom_result_async]

    # [START howto_operator_gke_delete_cluster_async]
    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        name=CLUSTER_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        deferrable=True,
    )
    # [END howto_operator_gke_delete_cluster_async]
    delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    create_cluster >> [pod_task, pod_task_xcom_async] >> delete_cluster
    pod_task_xcom_async >> pod_task_xcom_result

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
