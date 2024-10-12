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
In this DAG all critical functions as integration test are contained.

The DAG should work in all standard setups without error.
"""

from __future__ import annotations

from datetime import datetime
from time import sleep

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator  # type: ignore[no-redef,attr-defined]

with DAG(
    dag_id="integration_test",
    dag_display_name="Integration Test",
    description=__doc__.partition(".")[0],
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2024, 7, 1),
    tags=["example", "params", "integration test"],
    params={
        "mapping_count": Param(
            4,
            type="integer",
            title="Mapping Count",
            description="Amount of tasks that should be mapped",
        ),
    },
) as dag:

    @task
    def my_setup():
        print("Assume this is a setup task")

    @task
    def mapping_from_params(**context) -> list[int]:
        mapping_count: int = context["params"]["mapping_count"]
        return list(range(1, mapping_count + 1))

    @task
    def add_one(x: int):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        print(f"Total was {total}")

    @task_group(prefix_group_id=False)
    def mapping_task_group():
        added_values = add_one.expand(x=mapping_from_params())
        sum_it(added_values)

    @task.branch
    def branching():
        return ["bash", "virtualenv", "variable", "connection", "classic_bash", "classic_python"]

    @task.bash
    def bash():
        return "echo hello world"

    @task.virtualenv(requirements="numpy")
    def virtualenv():
        import numpy

        print(f"Welcome to virtualenv with numpy version {numpy.__version__}.")

    @task
    def variable():
        Variable.set("integration_test_key", "value")
        assert Variable.get("integration_test_key") == "value"  # noqa: S101
        Variable.delete("integration_test_key")

    @task
    def connection():
        try:
            conn = BaseHook.get_connection("integration_test")
            print(f"Got connection {conn}")
        except AirflowNotFoundException:
            print("Connection not found... but also OK.")

    @task_group(prefix_group_id=False)
    def standard_tasks_group():
        classic_bash = BashOperator(
            task_id="classic_bash", bash_command="echo Parameter is {{ params.mapping_count }}"
        )

        empty = EmptyOperator(task_id="not_executed")

        def python_call():
            print("Hello world")

        classic_py = PythonOperator(task_id="classic_python", python_callable=python_call)

        branching() >> [bash(), virtualenv(), variable(), connection(), classic_bash, classic_py, empty]

    @task
    def long_running():
        print("This task runs for 15 minutes")
        for i in range(15):
            sleep(60)
            print(f"Running for {i + 1} minutes now.")
        print("Long running task completed.")

    @task
    def my_teardown():
        print("Assume this is a teardown task")

    (
        my_setup().as_setup()
        >> [mapping_task_group(), standard_tasks_group(), long_running()]
        >> my_teardown().as_teardown()
    )
