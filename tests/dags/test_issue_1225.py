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
DAG designed to test what happens when a DAG with pooled tasks is run
by a BackfillJob.
Addresses issue #1225.
"""
from datetime import datetime

from airflow.models import DAG
from airflow.operators import DummyOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
DEFAULT_DATE = datetime(2016, 1, 1)

def fail():
    raise ValueError('Expected failure.')

# DAG tests backfill with pooled tasks
# Previously backfill would queue the task but never run it
dag1 = DAG(dag_id='test_backfill_pooled_task_dag', start_date=DEFAULT_DATE)
dag1_task1 = DummyOperator(
    task_id='test_backfill_pooled_task',
    dag=dag1,
    pool='test_backfill_pooled_task_pool',
    owner='airflow')

# DAG tests depends_on_past dependencies
dag2 = DAG(dag_id='test_depends_on_past', start_date=DEFAULT_DATE)
dag2_task1 = DummyOperator(
    task_id='test_dop_task',
    dag=dag2,
    depends_on_past=True,
    owner='airflow')

# DAG tests that a Dag run that doesn't complete is marked failed
dag3 = DAG(dag_id='test_dagrun_states_fail', start_date=DEFAULT_DATE)
dag3_task1 = PythonOperator(
    task_id='test_dagrun_fail',
    dag=dag3,
    owner='airflow',
    python_callable=fail)
dag3_task2 = DummyOperator(
    task_id='test_dagrun_succeed',
    dag=dag3,
    owner='airflow')
dag3_task2.set_upstream(dag3_task1)

# DAG tests that a Dag run that completes but has a failure is marked success
dag4 = DAG(dag_id='test_dagrun_states_success', start_date=DEFAULT_DATE)
dag4_task1 = PythonOperator(
    task_id='test_dagrun_fail',
    dag=dag4,
    owner='airflow',
    python_callable=fail,
)
dag4_task2 = DummyOperator(
    task_id='test_dagrun_succeed',
    dag=dag4,
    owner='airflow',
    trigger_rule=TriggerRule.ALL_FAILED
)
dag4_task2.set_upstream(dag4_task1)

# DAG tests that a Dag run that completes but has a root failure is marked fail
dag5 = DAG(dag_id='test_dagrun_states_root_fail', start_date=DEFAULT_DATE)
dag5_task1 = DummyOperator(
    task_id='test_dagrun_succeed',
    dag=dag5,
    owner='airflow'
)
dag5_task2 = PythonOperator(
    task_id='test_dagrun_fail',
    dag=dag5,
    owner='airflow',
    python_callable=fail,
)

# DAG tests that a Dag run that is deadlocked with no states is failed
dag6 = DAG(dag_id='test_dagrun_states_deadlock', start_date=DEFAULT_DATE)
dag6_task1 = DummyOperator(
    task_id='test_depends_on_past',
    depends_on_past=True,
    dag=dag6,
    owner='airflow')
dag6_task2 = DummyOperator(
    task_id='test_depends_on_past_2',
    depends_on_past=True,
    dag=dag6,
    owner='airflow')
dag6_task2.set_upstream(dag6_task1)
