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

from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task

with DAG(
    dag_id='example_kubernetes_decorator',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=['example'],
) as dag:

    @task.kubernetes(image='python:3.9-slim-buster', name='k8s_test', namespace='default')
    def execute_in_k8s_pod():
        import time

        print("Hello from k8s pod")
        time.sleep(100)

    @task.kubernetes()
    def print_pattern():
        line = "Kubernetes Decorator"
        pat = ""
        for i in range(0, line):
            for j in range(0, line):
                if (
                    (j == 1 and i != 0 and i != line - 1)
                    or ((i == 0 or i == line - 1) and j > 1 and j < line - 2)
                    or (i == ((line - 1) / 2) and j > line - 5 and j < line - 1)
                    or (j == line - 2 and i != 0 and i != line - 1 and i >= ((line - 1) / 2))
                ):
                    pat = pat + "*"
                else:
                    pat = pat + " "
            pat = pat + "\n"
        return pat

    execute_in_k8s_pod_instance = execute_in_k8s_pod()
    print_pattern_instance = print_pattern()
    execute_in_k8s_pod_instance >> print_pattern_instance
