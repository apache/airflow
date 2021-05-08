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

from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from airflow.utils.dates import days_ago


# [START howto_task_group_decorator]
# Creating Tasks
@task
def task_start():
    """Dummy Task which is First Task of Dag"""
    return '[Task_start]'


@task
def task_1(value):
    """Dummy Task1"""
    return f'[ Task1 {value} ]'


@task
def task_2(value):
    """Dummy Task2"""
    return f'[ Task2 {value} ]'


@task
def task_3(value):
    """Dummy Task3"""
    print(f'[ Task3 {value} ]')


@task
def task_end():
    """Dummy Task which is Last Task of Dag"""
    print('[ Task_End  ]')


# Creating TaskGroups
@task_group
def task_group_function(value):
    """TaskGroup for grouping related Tasks"""
    return task_3(task_2(task_1(value)))


# Executing Tasks and TaskGroups
with DAG(dag_id="example_task_group_decorator", start_date=days_ago(2), tags=["example"]) as dag:
    start_task = task_start()
    end_task = task_end()
    for i in range(5):
        current_task_group = task_group_function(i)
        start_task >> current_task_group >> end_task

# [END howto_task_group_decorator]
