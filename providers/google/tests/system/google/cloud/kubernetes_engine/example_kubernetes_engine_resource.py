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
    GKECreateCustomResourceOperator,
    GKEDeleteClusterOperator,
    GKEDeleteCustomResourceOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "kubernetes_engine_resource"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

GCP_LOCATION = "europe-north1-a"
CLUSTER_NAME = f"gke-resource-{ENV_ID}".replace("_", "-")
CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1}

PVC_NAME = "test-pvc-name"
PVC_CONF = f"""
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {PVC_NAME}
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: standard
  resources:
    requests:
      storage: 5Gi
"""


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    create_cluster = GKECreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body=CLUSTER,
    )

    # [START howto_operator_gke_create_resource]
    create_resource_task = GKECreateCustomResourceOperator(
        task_id="create_resource_task",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        yaml_conf=PVC_CONF,
    )
    # [END howto_operator_gke_create_resource]

    # [START howto_operator_gke_delete_resource]
    delete_resource_task = GKEDeleteCustomResourceOperator(
        task_id="delete_resource_task",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        yaml_conf=PVC_CONF,
    )
    # [END howto_operator_gke_delete_resource]

    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name=CLUSTER_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> create_resource_task >> delete_resource_task >> delete_cluster

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
