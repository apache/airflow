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
Example DAG demonstrating the usage of BranchPythonOperator with depends_on_past=True, where tasks may be run
or skipped on alternating runs.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator


def should_run(**kwargs):
    """
    Determine which dummy_task should be run based on if the execution date minute is even or odd.

    :param dict kwargs: Context
    :return: Id of the task to run
    :rtype: str
    """
    print(
        '------------- exec dttm = {} and minute = {}'.format(
            kwargs['execution_date'], kwargs['execution_date'].minute
        )
    )
    if kwargs['execution_date'].minute % 2 == 0:
        return "dummy_task_1"
    else:
        return "dummy_task_2"


with DAG(
    dag_id='example_branch_dop_operator_v3',
    schedule_interval='*/1 * * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={'depends_on_past': True},
    tags=['example'],
) as dag:
    cond = BranchPythonOperator(
        task_id='condition',
        python_callable=should_run,
    )

    dummy_task_1 = DummyOperator(task_id='dummy_task_1')
    dummy_task_2 = DummyOperator(task_id='dummy_task_2')
    cond >> [dummy_task_1, dummy_task_2]
