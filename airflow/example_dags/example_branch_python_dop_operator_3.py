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
Example DAG demonstrating the usage of ``@task.branch`` TaskFlow API decorator with depends_on_past=True,
where tasks may be run or skipped on alternating runs.
"""
from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator


@task.branch()
def should_run(**kwargs) -> str:
    """
    Determine which empty_task should be run based on if the execution date minute is even or odd.

    :param dict kwargs: Context
    :return: Id of the task to run
    """
    print(
        f"------------- exec dttm = {kwargs['execution_date']} and minute = {kwargs['execution_date'].minute}"
    )
    if kwargs['execution_date'].minute % 2 == 0:
        return "empty_task_1"
    else:
        return "empty_task_2"


with DAG(
    dag_id='example_branch_dop_operator_v3',
    schedule='*/1 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    default_args={'depends_on_past': True},
    tags=['example'],
) as dag:
    cond = should_run()

    empty_task_1 = EmptyOperator(task_id='empty_task_1')
    empty_task_2 = EmptyOperator(task_id='empty_task_2')
    cond >> [empty_task_1, empty_task_2]
