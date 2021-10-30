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
This is an example dag for using the Kubernetes Executor.
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.example_dags.libs.helper import print_stuff

with DAG(
    dag_id='example_kubernetes_executor',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'example2'],
) as dag:
    # You don't have to use any special KubernetesExecutor configuration if you don't want to
    @task
    def start_task():
        print_stuff()

    # But you can if you want to
    kube_exec_config_special = {"KubernetesExecutor": {"image": "airflow/ci:latest"}}

    @task(executor_config=kube_exec_config_special)
    def one_task():
        print_stuff()

    # Use the zip binary, which is only found in this special docker image
    kube_exec_config_zip_binary = {"KubernetesExecutor": {"image": "airflow/ci_zip:latest"}}

    @task(task_id="two_task", executor_config=kube_exec_config_zip_binary)
    def assert_zip_binary():
        """
        Checks whether Zip is installed.

        :raises SystemError: if zip is not installed
        """
        return_code = os.system("zip")
        if return_code != 0:
            raise SystemError("The zip binary is not found")

    # Limit resources on this operator/task with node affinity & tolerations
    affinity = {
        'podAntiAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': [
                {
                    'topologyKey': 'kubernetes.io/hostname',
                    'labelSelector': {
                        'matchExpressions': [{'key': 'app', 'operator': 'In', 'values': ['airflow']}]
                    },
                }
            ]
        }
    }

    tolerations = [{'key': 'dedicated', 'operator': 'Equal', 'value': 'airflow'}]

    kube_exec_config_resource_limits = {
        "KubernetesExecutor": {
            "request_memory": "128Mi",
            "limit_memory": "128Mi",
            "tolerations": tolerations,
            "affinity": affinity,
        }
    }

    @task(executor_config=kube_exec_config_resource_limits)
    def three_task():
        print_stuff()

    # Add arbitrary labels to worker pods
    kube_exec_config_pod_labels = {"KubernetesExecutor": {"labels": {"foo": "bar"}}}

    @task(executor_config=kube_exec_config_pod_labels)
    def four_task():
        print_stuff()

    start_task = start_task()
    one_task = one_task()
    two_task = assert_zip_binary()
    three_task = three_task()
    four_task = four_task()

    start_task >> [one_task, two_task, three_task, four_task]
