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
"""Example DAG demonstrating the usage of a custom PriorityWeightStrategy class."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.task.priority_strategy import PriorityWeightStrategy

if TYPE_CHECKING:
    from airflow.models import TaskInstance


def success_on_third_attempt(ti: TaskInstance, **context):
    if ti.try_number < 3:
        raise Exception("Not yet")


class DecreasingPriorityStrategy(PriorityWeightStrategy):
    """A priority weight strategy that decreases the priority weight with each attempt."""

    def get_weight(self, ti: TaskInstance):
        return max(3 - ti._try_number + 1, 1)


with DAG(
    dag_id="example_priority_weight_strategy",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    schedule="@daily",
    tags=["example"],
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(seconds=10),
    },
) as dag:
    fixed_weight_task = PythonOperator(
        task_id="fixed_weight_task",
        python_callable=success_on_third_attempt,
        priority_weight_strategy="downstream",
    )

    decreasing_weight_task = PythonOperator(
        task_id="decreasing_weight_task",
        python_callable=success_on_third_attempt,
        priority_weight_strategy=(
            "airflow.example_dags.example_priority_weight_strategy.DecreasingPriorityStrategy"
        ),
    )
