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
This is an example dag for using the KubernetesPodOperator.
"""
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

BUSYBOX_SLEEP_YAML = """
apiVersion: v1
kind: Pod
metadata:
  labels:
    run: example-yaml-2
  name: example-yaml-2
spec:
  containers:
  - args:
    - sh
    - -c
    - echo 123; sleep 10
    image: busybox
    name: example-yaml-2
  restartPolicy: Never
"""

ALPHINE_XCOM_YAML = """
apiVersion: v1
kind: Pod
metadata:
  name: example-yaml-xcom
spec:
  containers:
  - args:
    - sh
    - -c
    - mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json
    image: alpine
    name: example-yaml-xcom
  restartPolicy: Never
"""

try:
    # Kubernetes is optional, so not available in vanilla Airflow
    # pip install 'apache-airflow[kubernetes]'
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator, \
        KubernetesPodYamlOperator

    default_args = {
        'owner': 'Airflow',
        'start_date': days_ago(2)
    }

    with DAG(
        dag_id='example_kubernetes_operator',
        default_args=default_args,
        schedule_interval=None
    ) as dag:

        tolerations = [
            {
                'key': "key",
                'operator': 'Equal',
                'value': 'value'
            }
        ]

        k = KubernetesPodOperator(
            namespace='default',
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo", "10"],
            labels={"foo": "bar"},
            name="airflow-test-pod",
            in_cluster=False,
            task_id="task",
            get_logs=True,
            is_delete_operator_pod=False,
            tolerations=tolerations
        )

        start_pod = KubernetesPodYamlOperator(
            task_id="start_pod",
            yaml=BUSYBOX_SLEEP_YAML
        )

        start_pod_xcom = KubernetesPodYamlOperator(
            task_id="start_pod_xcom",
            yaml=ALPHINE_XCOM_YAML,
            do_xcom_push=True
        )

        pod_task_xcom_result = BashOperator(
            bash_command="echo \"{{ task_instance.xcom_pull('start_pod_xcom')[0] }}\"",
            task_id="start_pod_xcom_result",
        )

        start_pod_xcom >> pod_task_xcom_result

except ImportError as e:
    log.warning("Could not import KubernetesPodOperator: " + str(e))
    log.warning("Install kubernetes dependencies with: "
                "    pip install 'apache-airflow[kubernetes]'")
