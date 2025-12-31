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
"""Test DAG with task groups for testing dag.test() method."""

from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task, task_group


@task
def start_task():
    """Starting task."""
    return "started"


@task_group(group_id="group_1")
def task_group_1():
    """First task group."""

    @task
    def group_1_task_1():
        return "group_1_task_1_completed"

    @task
    def group_1_task_2(value):
        print(f"Group 1 Task 2 received: {value}")
        return "group_1_task_2_completed"

    task_1 = group_1_task_1()
    task_2 = group_1_task_2(task_1)
    return task_2


@task_group(group_id="group_2")
def task_group_2():
    """Second task group."""

    @task
    def group_2_task_1():
        return "group_2_task_1_completed"

    @task
    def group_2_task_2(value):
        print(f"Group 2 Task 2 received: {value}")
        return "group_2_task_2_completed"

    task_1 = group_2_task_1()
    task_2 = group_2_task_2(task_1)
    return task_2


@task
def end_task():
    """Ending task."""
    return "completed"


with DAG(
    "test_dag_test_task_groups",
    schedule=None,
    catchup=False,
    tags=["test", "dag_test", "task_groups"],
) as dag:
    start = start_task()
    group1 = task_group_1()
    group2 = task_group_2()
    end = end_task()

    bash_task = BashOperator(
        task_id="bash_in_group_test",
        bash_command='echo "Bash task in DAG with task groups"',
    )

    start >> [group1, group2] >> end
    bash_task >> start

