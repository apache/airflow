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

from kubernetes.client import models as k8s

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKECreateCustomResourceOperator,
    GKEDeleteClusterOperator,
    GKEStartKueueInsideClusterOperator,
    GKEStartKueueJobOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "kubernetes_engine_kueue"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

GCP_LOCATION = "europe-west3"
CLUSTER_NAME = f"gke-kueue-{ENV_ID}".replace("_", "-")
CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1, "autopilot": {"enabled": True}}

flavor_conf = """
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: default-flavor
"""
cluster_conf = """
apiVersion: kueue.x-k8s.io/v1beta1
kind: ClusterQueue
metadata:
  name: cluster-queue
spec:
  namespaceSelector: {}
  queueingStrategy: BestEffortFIFO
  resourceGroups:
  - coveredResources: ["cpu", "memory", "nvidia.com/gpu", "ephemeral-storage"]
    flavors:
    - name: "default-flavor"
      resources:
      - name: "cpu"
        nominalQuota: 10
      - name: "memory"
        nominalQuota: 10Gi
      - name: "nvidia.com/gpu"
        nominalQuota: 10
      - name: "ephemeral-storage"
        nominalQuota: 10Gi
"""
QUEUE_NAME = "local-queue"
local_conf = f"""
apiVersion: kueue.x-k8s.io/v1beta1
kind: LocalQueue
metadata:
  namespace: default # LocalQueue under team-a namespace
  name: {QUEUE_NAME}
spec:
  clusterQueue: cluster-queue # Point to the ClusterQueue
"""

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
        kueue_version="v0.6.2",
    )
    # [END howto_operator_gke_install_kueue]

    create_resource_flavor = GKECreateCustomResourceOperator(
        task_id="create_resource_flavor",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        yaml_conf=flavor_conf,
        custom_resource_definition=True,
        namespaced=False,
    )

    create_cluster_queue = GKECreateCustomResourceOperator(
        task_id="create_cluster_queue",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        yaml_conf=cluster_conf,
        custom_resource_definition=True,
        namespaced=False,
    )
    create_local_queue = GKECreateCustomResourceOperator(
        task_id="create_local_queue",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        yaml_conf=local_conf,
        custom_resource_definition=True,
    )

    # [START howto_operator_kueue_start_job]
    kueue_job_task = GKEStartKueueJobOperator(
        task_id="kueue_job_task",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        queue_name=QUEUE_NAME,
        namespace="default",
        parallelism=3,
        image="perl:5.34.0",
        cmds=["perl", "-Mbignum=bpi", "-wle", "print bpi(2000)"],
        name="test-pi",
        suspend=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": 1,
                "memory": "200Mi",
            },
        ),
    )

    # [END howto_operator_kueue_start_job]

    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name=CLUSTER_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_cluster
        >> add_kueue_cluster
        >> create_resource_flavor
        >> create_cluster_queue
        >> create_local_queue
        # TEST BODY
        >> kueue_job_task
        # TEST TEARDOWN
        >> delete_cluster
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
