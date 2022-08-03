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

from datetime import datetime

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

skip_task_dag_dataset = Dataset('s3://dag_with_skip_task/output_1.txt', extra={'hi': 'bye'})
fail_task_dag_dataset = Dataset('s3://dag_with_fail_task/output_1.txt', extra={'hi': 'bye'})


def raise_skip_exc():
    raise AirflowSkipException


dag_with_skip_task = DAG(
    dag_id='dag_with_skip_task',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_interval='@daily',
    tags=['upstream-skipping'],
)
PythonOperator(
    task_id='skip_task',
    outlets=[skip_task_dag_dataset],
    python_callable=raise_skip_exc,
    dag=dag_with_skip_task,
)

with DAG(
    dag_id='dag_that_follows_dag_with_skip',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_on=[skip_task_dag_dataset],
    tags=['downstream-skipped'],
) as dag_that_follows_dag_with_skip:
    BashOperator(
        task_id='dag_that_follows_dag_with_skip_task',
        bash_command="sleep 5",
    )


def raise_fail_exc():
    raise AirflowFailException


dag_with_fail_task = DAG(
    dag_id='dag_with_fail_task',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_interval='@daily',
    tags=['upstream-skipping'],
)
PythonOperator(
    task_id='fail_task',
    outlets=[fail_task_dag_dataset],
    python_callable=raise_fail_exc,
    dag=dag_with_fail_task,
)

with DAG(
    dag_id='dag_that_follows_dag_with_fail',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_on=[fail_task_dag_dataset],
    tags=['downstream-failed'],
) as dag_that_follows_dag_with_fail:
    BashOperator(
        task_id='dag_that_follows_dag_with_fail_task',
        bash_command="sleep 5",
    )
