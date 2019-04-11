# -*- coding: utf-8 -*-
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
DAG designed to test what happens when a DAG with pooled tasks is run
by a BackfillJob.
Addresses issue #1225.
"""
from datetime import datetime, timedelta

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


def fail():
    raise ValueError('Expected failure.')


def delayed_fail():
    """
    Delayed failure to make sure that processes are running before the error
    is raised.

    TODO handle more directly (without sleeping)
    """
    time.sleep(5)
    raise ValueError('Expected failure.')


# DAG tests backfill with pooled tasks
# Previously backfill would queue the task but never run it
dag1 = DAG(dag_id='test_backfill_pooled_task_dag', default_args=default_args)
dag1_task1 = DummyOperator(
    task_id='test_backfill_pooled_task',
    dag=dag1,
    pool='test_backfill_pooled_task_pool',)

# DAG tests depends_on_past dependencies
dag2 = DAG(dag_id='test_depends_on_past', default_args=default_args)
dag2_task1 = DummyOperator(
    task_id='test_dop_task',
    dag=dag2,
    depends_on_past=True,)

# DAG tests that a Dag run that doesn't complete is marked failed
dag3 = DAG(dag_id='test_dagrun_states_fail', default_args=default_args)
dag3_task1 = PythonOperator(
    task_id='test_dagrun_fail',
    dag=dag3,
    python_callable=fail)
dag3_task2 = DummyOperator(
    task_id='test_dagrun_succeed',
    dag=dag3,)
dag3_task2.set_upstream(dag3_task1)

# DAG tests that a Dag run that completes but has a failure is marked success
dag4 = DAG(dag_id='test_dagrun_states_success', default_args=default_args)
dag4_task1 = PythonOperator(
    task_id='test_dagrun_fail',
    dag=dag4,
    python_callable=fail,
)
dag4_task2 = DummyOperator(
    task_id='test_dagrun_succeed',
    dag=dag4,
    trigger_rule=TriggerRule.ALL_FAILED
)
dag4_task2.set_upstream(dag4_task1)

# DAG tests that a Dag run that completes but has a root failure is marked fail
dag5 = DAG(dag_id='test_dagrun_states_root_fail', default_args=default_args)
dag5_task1 = DummyOperator(
    task_id='test_dagrun_succeed',
    dag=dag5,
)
dag5_task2 = PythonOperator(
    task_id='test_dagrun_fail',
    dag=dag5,
    python_callable=fail,
)

# DAG tests that a Dag run that is deadlocked with no states is failed
dag6 = DAG(dag_id='test_dagrun_states_deadlock', default_args=default_args)
dag6_task1 = DummyOperator(
    task_id='test_depends_on_past',
    depends_on_past=True,
    dag=dag6,)
dag6_task2 = DummyOperator(
    task_id='test_depends_on_past_2',
    depends_on_past=True,
    dag=dag6,)
dag6_task2.set_upstream(dag6_task1)


# DAG tests that a deadlocked subdag is properly caught
dag7 = DAG(dag_id='test_subdag_deadlock', default_args=default_args)
subdag7 = DAG(dag_id='test_subdag_deadlock.subdag', default_args=default_args)
subdag7_task1 = PythonOperator(
    task_id='test_subdag_fail',
    dag=subdag7,
    python_callable=fail)
subdag7_task2 = DummyOperator(
    task_id='test_subdag_dummy_1',
    dag=subdag7,)
subdag7_task3 = DummyOperator(
    task_id='test_subdag_dummy_2',
    dag=subdag7)
dag7_subdag1 = SubDagOperator(
    task_id='subdag',
    dag=dag7,
    subdag=subdag7)
subdag7_task1.set_downstream(subdag7_task2)
subdag7_task2.set_downstream(subdag7_task3)

# DAG tests that a Dag run that doesn't complete but has a root failure is marked running
dag8 = DAG(dag_id='test_dagrun_states_root_fail_unfinished', default_args=default_args)
dag8_task1 = DummyOperator(
    task_id='test_dagrun_unfinished',  # The test will unset the task instance state after
                                       # running this test
    dag=dag8,
)
dag8_task2 = PythonOperator(
    task_id='test_dagrun_fail',
    dag=dag8,
    python_callable=fail,
)

# DAG tests that a Dag run that completes but has a root in the future is marked as success
dag9 = DAG(dag_id='test_dagrun_states_root_future', default_args=default_args)
dag9_task1 = DummyOperator(
    task_id='current',
    dag=dag9,
)
dag8_task2 = DummyOperator(
    task_id='future',
    dag=dag9,
    start_date=DEFAULT_DATE + timedelta(days=1),
)
