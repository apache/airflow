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
Example Airflow DAG that show how to create a Dataproc cluster in Google Kubernetes Engine.
"""

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "dataproc-gke"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

REGION = "us-central1"
CLUSTER_NAME = f"cluster-test-build-in-gke{ENV_ID}"
GKE_CLUSTER_NAME = f"test-dataproc-gke-cluster-{ENV_ID}"
GKE_CLUSTER_CONFIG = {
    "name": GKE_CLUSTER_NAME,
    "workload_identity_config": {
        "workload_pool": f"{PROJECT_ID}.svc.id.goog",
    },
    "initial_node_count": 1,
}

# [START how_to_cloud_dataproc_create_cluster_in_gke_config]

VIRTUAL_CLUSTER_CONFIG = {
    "kubernetes_cluster_config": {
        "gke_cluster_config": {
            "gke_cluster_target": f"projects/{PROJECT_ID}/locations/{REGION}/clusters/{GKE_CLUSTER_NAME}",
            "node_pool_target": [
                {
                    "node_pool": f"projects/{PROJECT_ID}/locations/{REGION}/clusters/{GKE_CLUSTER_NAME}/nodePools/dp",  # noqa
                    "roles": ["DEFAULT"],
                }
            ],
        },
        "kubernetes_software_config": {"component_version": {"SPARK": b'3'}},
    },
    "staging_bucket": "test-staging-bucket",
}

# [END how_to_cloud_dataproc_create_cluster_in_gke_config]


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    create_gke_cluster = GKECreateClusterOperator(
        task_id="create_gke_cluster",
        project_id=PROJECT_ID,
        location=REGION,
        body=GKE_CLUSTER_CONFIG,
    )

    # [START how_to_cloud_dataproc_create_cluster_operator_in_gke]
    create_cluster_in_gke = DataprocCreateClusterOperator(
        task_id="create_cluster_in_gke",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        virtual_cluster_config=VIRTUAL_CLUSTER_CONFIG,
    )
    # [END how_to_cloud_dataproc_create_cluster_operator_in_gke]

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_gke_cluster = GKEDeleteClusterOperator(
        task_id="delete_gke_cluster",
        name=GKE_CLUSTER_NAME,
        project_id=PROJECT_ID,
        location=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_gke_cluster >> create_cluster_in_gke >> [delete_dataproc_cluster, delete_gke_cluster]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
