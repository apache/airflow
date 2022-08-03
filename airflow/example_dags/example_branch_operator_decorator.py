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

"""Example DAG demonstrating the usage of the ``@task.branch`` TaskFlow API decorator."""

from __future__ import annotations

import random

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id='example_branch_python_operator_decorator',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule_interval="@daily",
    tags=['example', 'example2'],
) as dag:
    run_this_first = EmptyOperator(task_id='run_this_first')

    options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']

    @task.branch(task_id="branching")
    def random_choice(choices: list[str]) -> str:
        return random.choice(choices)

    random_choice_instance = random_choice(choices=options)

    run_this_first >> random_choice_instance

    join = EmptyOperator(task_id='join', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    for option in options:
        t = EmptyOperator(task_id=option)

        empty_follow = EmptyOperator(task_id='follow_' + option)

        # Label is optional here, but it can help identify more complex branches
        random_choice_instance >> Label(option) >> t >> empty_follow >> join
