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
from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartKueueInsideClusterOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_kubernetes_engine_kueue"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

GCP_LOCATION = "europe-west3"
CLUSTER_NAME = f"cluster-name-test-kueue-{ENV_ID}".replace("_", "-")
CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1, "autopilot": {"enabled": True}}

with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "kubernetes-engine", "kueue"],
) as dag:
    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body=CLUSTER,
    )

    # [START howto_operator_gke_install_kueue]
    add_kueue_cluster = GKEStartKueueInsideClusterOperator(
        task_id="add_kueue_cluster",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        kueue_version="v0.5.1",
    )
    # [END howto_operator_gke_install_kueue]

    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        name=CLUSTER_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
    )

    create_cluster >> add_kueue_cluster >> delete_cluster

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
