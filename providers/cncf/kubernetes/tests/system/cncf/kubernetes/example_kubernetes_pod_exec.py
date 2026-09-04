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
"""Example Dag for executing a command in an existing Kubernetes Pod."""

from __future__ import annotations

import os
from datetime import datetime

from kubernetes.client.rest import ApiException

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.pod_exec import KubernetesPodExecOperator
from airflow.providers.cncf.kubernetes.operators.resource import (
    KubernetesCreateResourceOperator,
    KubernetesDeleteResourceOperator,
)
from airflow.providers.cncf.kubernetes.utils.container import container_is_running
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase
from airflow.sdk import DAG, TriggerRule, task

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default").lower().replace("_", "-")
DAG_ID = "example_kubernetes_pod_exec_operator"
NAMESPACE = "default"
POD_NAME = f"airflow-pod-exec-{ENV_ID}"
CONTAINER_NAME = "worker"
EXPECTED_OUTPUT = "command executed in existing pod"

pod_conf = f"""
apiVersion: v1
kind: Pod
metadata:
  name: {POD_NAME}
  namespace: {NAMESPACE}
spec:
  restartPolicy: Never
  containers:
    - name: {CONTAINER_NAME}
      image: busybox:1.38.0
      command: ["sleep", "3600"]
"""


@task.sensor(poke_interval=2, timeout=120, mode="reschedule")
def wait_for_running_pod() -> bool:
    hook = KubernetesHook()
    try:
        pod = hook.get_pod(name=POD_NAME, namespace=NAMESPACE)
    except ApiException as error:
        if error.status == 404:
            return False
        raise
    return bool(
        pod.status and pod.status.phase == PodPhase.RUNNING and container_is_running(pod, CONTAINER_NAME)
    )


@task
def verify_output(output: str) -> None:
    if output != EXPECTED_OUTPUT:
        raise ValueError(f"Unexpected command output: {output!r}")


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "kubernetes"],
) as dag:
    create_pod = KubernetesCreateResourceOperator(
        task_id="create_pod",
        yaml_conf=pod_conf,
    )

    pod_is_running = wait_for_running_pod()

    # [START howto_operator_k8s_pod_exec]
    run_command = KubernetesPodExecOperator(
        task_id="run_command",
        pod_name=POD_NAME,
        namespace=NAMESPACE,
        container_name=CONTAINER_NAME,
        command=["sh", "-c", f"printf '{EXPECTED_OUTPUT}'"],
        do_xcom_push=True,
    )
    # [END howto_operator_k8s_pod_exec]

    output_is_valid = verify_output(run_command.output)

    delete_pod = KubernetesDeleteResourceOperator(
        task_id="delete_pod",
        yaml_conf=pod_conf,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_pod >> pod_is_running >> run_command >> output_is_valid >> delete_pod

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
