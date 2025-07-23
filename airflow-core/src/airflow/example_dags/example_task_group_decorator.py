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
"""Example DAG demonstrating the usage of the @taskgroup decorator."""

from __future__ import annotations

# [START howto_task_group_decorator]
import pendulum

from airflow.sdk import DAG, task, task_group


# Creating Tasks
@task
def task_start():
    """Empty Task which is First Task of Dag"""
    return "[Task_start]"


@task
def task_1(value: int) -> str:
    """Empty Task1"""
    return f"[ Task1 {value} ]"


@task
def task_2(value: str) -> str:
    """Empty Task2"""
    return f"[ Task2 {value} ]"


@task
def task_3(value: str) -> None:
    """Empty Task3"""
    print(f"[ Task3 {value} ]")


@task
def task_end() -> None:
    """Empty Task which is Last Task of Dag"""
    print("[ Task_End  ]")


# Creating TaskGroups
@task_group
def task_group_function(value: int) -> None:
    """TaskGroup for grouping related Tasks"""
    task_3(task_2(task_1(value)))


# Executing Tasks and TaskGroups
with DAG(
    dag_id="example_task_group_decorator",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    start_task = task_start()
    end_task = task_end()
    for i in range(5):
        current_task_group = task_group_function(i)
        start_task >> current_task_group >> end_task

# [END howto_task_group_decorator]
