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

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="example_kubernetes_decorator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example", "cncf", "kubernetes"],
    catchup=False,
) as dag:

    @task.kubernetes(
        image="python:3.8-slim-buster",
        name="k8s_test",
        namespace="default",
        in_cluster=False,
        config_file="/path/to/.kube/config",
    )
    def execute_in_k8s_pod():
        import time

        print("Hello from k8s pod")
        time.sleep(2)

    @task.kubernetes(image="python:3.8-slim-buster", namespace="default", in_cluster=False)
    def print_pattern():
        n = 5
        for i in range(0, n):
            # inner loop to handle number of columns
            # values changing acc. to outer loop
            for j in range(0, i + 1):
                # printing stars
                print("* ", end="")

            # ending line after each row
            print("\r")

    execute_in_k8s_pod_instance = execute_in_k8s_pod()
    print_pattern_instance = print_pattern()

    execute_in_k8s_pod_instance >> print_pattern_instance


from tests.system.utils import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
