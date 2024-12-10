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
Example Airflow DAG for Kubernetes Kueue operators.
"""

from __future__ import annotations

import os
from datetime import datetime

from kubernetes.client import models as k8s

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kueue import (
    KubernetesInstallKueueOperator,
    KubernetesStartKueueJobOperator,
)
from airflow.providers.cncf.kubernetes.operators.resource import KubernetesCreateResourceOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "example_kubernetes_kueue_operators"

flavor_conf = """
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: default-flavor
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


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "kubernetes", "kueue"],
) as dag:
    # [START howto_operator_k8s_kueue_install]
    install_kueue = KubernetesInstallKueueOperator(
        task_id="install_kueue",
        kueue_version="v0.9.1",
    )
    # [END howto_operator_k8s_kueue_install]

    create_resource_flavor = KubernetesCreateResourceOperator(
        task_id="create_resource_flavor",
        yaml_conf=flavor_conf,
        custom_resource_definition=True,
        namespaced=False,
    )

    create_cluster_queue = KubernetesCreateResourceOperator(
        task_id="create_cluster_queue",
        yaml_conf=cluster_conf,
        custom_resource_definition=True,
        namespaced=False,
    )
    create_local_queue = KubernetesCreateResourceOperator(
        task_id="create_local_queue",
        yaml_conf=local_conf,
        custom_resource_definition=True,
    )

    # [START howto_operator_k8s_install_kueue]
    start_kueue_job = KubernetesStartKueueJobOperator(
        task_id="kueue_job",
        queue_name=QUEUE_NAME,
        namespace="default",
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
        wait_until_job_complete=True,
    )
    # [END howto_operator_k8s_install_kueue]

    install_kueue >> create_resource_flavor >> create_cluster_queue >> create_local_queue >> start_kueue_job

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
