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
from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG, task

with DAG(
    dag_id="example_kubernetes_cmd_decorator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    tags=["example", "cncf", "kubernetes"],
    catchup=False,
) as dag:
    # [START howto_decorator_kubernetes_cmd]
    @task
    def foo() -> str:
        return "foo"

    @task
    def bar() -> str:
        return "bar"

    @task.kubernetes_cmd(
        image="bash:5.2",
        name="full_cmd",
        in_cluster=False,
    )
    def execute_in_k8s_pod_full_cmd(foo_result: str, bar_result: str) -> list[str]:
        return ["echo", "-e", f"With full cmd:\\t{foo_result}\\t{bar_result}"]

    # The args_only parameter is used to indicate that the decorated function will
    # return a list of arguments to be passed as arguments to the container entrypoint:
    # in this case, the `bash` command
    @task.kubernetes_cmd(args_only=True, image="bash:5.2", in_cluster=False)
    def execute_in_k8s_pod_args_only(foo_result: str, bar_result: str) -> list[str]:
        return ["-c", f"echo -e 'With args only:\\t{foo_result}\\t{bar_result}'"]

    # Templating can be used in the returned command and all other templated fields in
    # the decorator parameters.
    @task.kubernetes_cmd(image="bash:5.2", name="my-pod-{{ ti.task_id }}", in_cluster=False)
    def apply_templating(message: str) -> list[str]:
        full_message = "Templated task_id: {{ ti.task_id }}, dag_id: " + message
        return ["echo", full_message]

    foo_result = foo()
    bar_result = bar()

    full_cmd_instance = execute_in_k8s_pod_full_cmd(foo_result, bar_result)
    args_instance = execute_in_k8s_pod_args_only(foo_result, bar_result)

    [full_cmd_instance, args_instance] >> apply_templating("{{ dag.dag_id }}")

    # [END howto_decorator_kubernetes_cmd]


from tests_common.test_utils.system_tests import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
