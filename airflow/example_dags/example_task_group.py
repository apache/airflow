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

"""Example DAG demonstrating the usage of the TaskGroup."""

from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


def create_section():
    """
    Create tasks in the outer section.
    """
    dummies = [DummyOperator(task_id=f'task-{i + 1}') for i in range(5)]

    with TaskGroup("inside_section_1") as inside_section_1:
        _ = [DummyOperator(task_id=f'task-{i + 1}',) for i in range(3)]

    with TaskGroup("inside_section_2") as inside_section_2:
        _ = [DummyOperator(task_id=f'task-{i + 1}',) for i in range(3)]

    dummies[-1] >> inside_section_1
    dummies[-2] >> inside_section_2


with DAG(dag_id="example_task_group", start_date=days_ago(2)) as dag:
    start = DummyOperator(task_id="start")

    with TaskGroup("section_1") as section_1:
        create_section()

    some_other_task = DummyOperator(task_id="some-other-task")

    with TaskGroup("section_2") as section_2:
        create_section()

    end = DummyOperator(task_id='end')

    start >> section_1 >> some_other_task >> section_2 >> end
