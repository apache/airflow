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
"""Example DAG demonstrating the usage of the CustomWeightRule."""

from __future__ import annotations

import datetime

import pendulum

from airflow.example_dags.plugins.decreasing_priority_weight_strategy import DecreasingPriorityStrategy
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

# [START example_custom_weight_dag]

with DAG(
    dag_id="example_custom_weight",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example", "example2"],
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    # provide the class instance
    task_1 = BashOperator(task_id="task_1", bash_command="echo 1", weight_rule=DecreasingPriorityStrategy())

    # or provide the path of the class
    task_2 = BashOperator(
        task_id="task_2",
        bash_command="echo 1",
        weight_rule="airflow.example_dags.plugins.decreasing_priority_weight_strategy.DecreasingPriorityStrategy",
    )

    task_non_custom = BashOperator(task_id="task_non_custom", bash_command="echo 1", priority_weight=2)

    start >> [task_1, task_2, task_non_custom]
# [END example_custom_weight_dag]
