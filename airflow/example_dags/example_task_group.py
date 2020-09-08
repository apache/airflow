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

# [START howto_task_group]
with DAG(dag_id="example_task_group", start_date=days_ago(2)) as dag:
    start = DummyOperator(task_id="start")

    # [START howto_task_group_section_1]
    with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
        task_1 = DummyOperator(task_id="task_1")
        task_2 = DummyOperator(task_id="task_2")
        task_3 = DummyOperator(task_id="task_3")

        task_1 >> [task_2, task_3]
    # [END howto_task_group_section_1]

    # [START howto_task_group_section_2]
    with TaskGroup("section_2", tooltip="Tasks for section_2") as section_2:
        task_1 = DummyOperator(task_id="task_1")

        # [START howto_task_group_inner_section_2]
        with TaskGroup("inner_section_2", tooltip="Tasks for inner_section2") as inner_section_2:
            task_2 = DummyOperator(task_id="task_2")
            task_3 = DummyOperator(task_id="task_3")
            task_4 = DummyOperator(task_id="task_4")

            [task_2, task_3] >> task_4
        # [END howto_task_group_inner_section_2]

    # [END howto_task_group_section_2]

    end = DummyOperator(task_id='end')

    start >> section_1 >> section_2 >> end
# [END howto_task_group]
