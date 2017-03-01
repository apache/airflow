# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""
Addresses issue [AIRFLOW-931]
DAG designed to test what happens when a DAG with concurrency > 1
kicks off with multiple non-dependent tasks
Note this will have to run with LocalExecutor to actually run into the
race condition
"""
from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
import time


DEFAULT_DATE = datetime(2016, 1, 1)
default_args = dict(
    start_date=DEFAULT_DATE,
    owner='airflow')

concurrency_dag = DAG(dag_id='test_queued_with_concurrency_dag', concurrency=2, default_args=default_args)


def pausing_callable():
    time.sleep(5)

task1 = PythonOperator(
    task_id='test_concurrency_1',
    python_callable=pausing_callable,
    dag=concurrency_dag)

task2 = PythonOperator(
    task_id='test_concurrency_2',
    python_callable=pausing_callable,
    dag=concurrency_dag)

task3 = PythonOperator(
    task_id='test_concurrency_3',
    python_callable=pausing_callable,
    dag=concurrency_dag)

task4 = PythonOperator(
    task_id='test_concurrency_4',
    python_callable=pausing_callable,
    dag=concurrency_dag)
