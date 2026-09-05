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

from kubernetes.client.rest import ApiException

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.utils.container import container_is_running
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase
from airflow.providers.google.cloud.hooks.kubernetes_engine import GKEHook, GKEKubernetesHook
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEClusterAuthDetails,
    GKECreateClusterOperator,
    GKECreateCustomResourceOperator,
    GKEDeleteClusterOperator,
    GKEDeleteCustomResourceOperator,
    GKEPodExecOperator,
    GKEStartPodOperator,
)
from airflow.providers.standard.operators.bash import BashOperator

try:
    from airflow.sdk import TriggerRule, task
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.decorators import task  # type: ignore[no-redef]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "kubernetes_engine"
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

GCP_LOCATION = "europe-west1"
CLUSTER_NAME_BASE = f"cluster-{DAG_ID}".replace("_", "-")
CLUSTER_NAME_FULL = CLUSTER_NAME_BASE + f"-{ENV_ID}".replace("_", "-")
CLUSTER_NAME = CLUSTER_NAME_BASE if len(CLUSTER_NAME_FULL) >= 33 else CLUSTER_NAME_FULL
EXEC_POD_NAME = "existing-pod"
EXEC_CONTAINER_NAME = "main"
EXPECTED_EXEC_OUTPUT = "command executed in existing GKE Pod"

# [START howto_operator_gcp_gke_create_cluster_definition]
CLUSTER = {"name": CLUSTER_NAME, "initial_node_count": 1, "autopilot": {"enabled": True}}
# [END howto_operator_gcp_gke_create_cluster_definition]

EXEC_POD = f"""
apiVersion: v1
kind: Pod
metadata:
  name: {EXEC_POD_NAME}
  namespace: default
spec:
  restartPolicy: Never
  containers:
    - name: {EXEC_CONTAINER_NAME}
      image: busybox:1.38.0
      command: ["sleep", "3600"]
"""


@task.sensor(poke_interval=10, timeout=300, mode="reschedule")
def wait_for_running_exec_pod() -> bool:
    cluster_hook = GKEHook(location=GCP_LOCATION)
    cluster_url, ssl_ca_cert = GKEClusterAuthDetails(
        cluster_name=CLUSTER_NAME,
        project_id=GCP_PROJECT_ID,
        use_internal_ip=False,
        use_dns_endpoint=False,
        cluster_hook=cluster_hook,
    ).fetch_cluster_info()
    hook = GKEKubernetesHook(
        gcp_conn_id="google_cloud_default",
        cluster_url=cluster_url,
        ssl_ca_cert=ssl_ca_cert,
    )
    try:
        pod = hook.get_pod(name=EXEC_POD_NAME, namespace="default")
    except ApiException as error:
        if error.status == 404:
            return False
        raise
    return bool(
        pod.status and pod.status.phase == PodPhase.RUNNING and container_is_running(pod, EXEC_CONTAINER_NAME)
    )


@task
def verify_exec_output(output: str) -> None:
    if output != EXPECTED_EXEC_OUTPUT:
        raise ValueError(f"Unexpected command output: {output!r}")


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
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
        on_finish_action="delete_pod",
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
        cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        name="test-pod-xcom",
        in_cluster=False,
        on_finish_action="delete_pod",
    )
    # [END howto_operator_gke_start_pod_xcom]

    create_exec_pod = GKECreateCustomResourceOperator(
        task_id="create_exec_pod",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        yaml_conf=EXEC_POD,
    )

    exec_pod_is_running = wait_for_running_exec_pod()

    # [START howto_operator_gke_pod_exec]
    exec_in_existing_pod = GKEPodExecOperator(
        task_id="exec_in_existing_pod",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        pod_name=EXEC_POD_NAME,
        namespace="default",
        container_name=EXEC_CONTAINER_NAME,
        command=["sh", "-c", f"printf '{EXPECTED_EXEC_OUTPUT}'"],
        do_xcom_push=True,
    )
    # [END howto_operator_gke_pod_exec]

    exec_output_is_valid = verify_exec_output(exec_in_existing_pod.output)

    delete_exec_pod = GKEDeleteCustomResourceOperator(
        task_id="delete_exec_pod",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        cluster_name=CLUSTER_NAME,
        yaml_conf=EXEC_POD,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START howto_operator_gke_xcom_result]
    pod_task_xcom_result = BashOperator(
        task_id="pod_task_xcom_result",
        bash_command="""
        {% if params.airflow_v3 %}
        echo "{{ task_instance.xcom_pull('pod_task_xcom') }}"
        {% else %}
        echo "{{ task_instance.xcom_pull('pod_task_xcom')[0] }}"
        {% endif %}
        """,
        params={"airflow_v3": AIRFLOW_V_3_0_PLUS},
    )
    # [END howto_operator_gke_xcom_result]

    # [START howto_operator_gke_delete_cluster]
    delete_cluster = GKEDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name=CLUSTER_NAME,
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
    )
    # [END howto_operator_gke_delete_cluster]
    delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    create_cluster >> [pod_task, pod_task_xcom] >> delete_cluster
    (
        create_cluster
        >> create_exec_pod
        >> exec_pod_is_running
        >> exec_in_existing_pod
        >> exec_output_is_valid
        >> delete_exec_pod
        >> delete_cluster
    )
    pod_task_xcom >> pod_task_xcom_result

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
