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

from airflow.models.dag import DAG
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from airflow.utils.task_group import taskgroup


# Creating Tasks
@task()
def task_start():
    """Dummy Task which is First Task of Dag """
    return '[Task_start]'


@task()
def task_end1():
    """Dummy Task which is Last Task of Dag"""
    print(f'[ Task_End 1 ]')


@task()
def task_end2():
    """Dummy Task which is Last Task of Dag"""
    print(f'[ Task_End 2 ]')


@task
def task_1(value):
    """ Dummy Task1"""
    return f'[ Task1 {value} ]'


@task
def task_2(value):
    """ Dummy Task2"""
    print( f'[ Task2 {value} ]')


@task
def task_3(value):
    """ Dummy Task3"""
    return f'[ Task3 {value} ]'


@task
def task_4(value):
    """ Dummy Task3"""
    print(f'[ Task4 {value} ]')


# Creating TaskGroups
@taskgroup(group_id='section_1')
def section_1(value):
    """ TaskGroup for grouping related Tasks"""
    return task_2(task_1(value))


@taskgroup(group_id='section_2')
def section_2(value):
    """ TaskGroup for grouping related Tasks"""
    return task_4(task_3(value))


# Executing Tasks and TaskGroups
with DAG(dag_id="example_task_group_decorator", start_date=days_ago(2), tags=["example"]) as dag:
    t1 = task_start()

    s1 = section_1(t1)
    s2 = section_2(t1)

    s1.set_downstream(task_end1())
    s2.set_downstream(task_end2())
