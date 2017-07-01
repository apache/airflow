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

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sensors import ExternalTaskSensor
from tests.core import TEST_DAG_ID, DEFAULT_DATE
from datetime import timedelta

args = {'owner': 'airflow', 'start_date': DEFAULT_DATE,
        'depends_on_past': False, 'retries': 3}

dag_core_id = TEST_DAG_ID + '_core'
dag_core = DAG(dag_core_id, default_args=args,
    schedule_interval=timedelta(seconds=1))
task_core = DummyOperator(
    task_id='task_core',
    dag=dag_core)

dag_first_child_id = TEST_DAG_ID + '_first_child'
dag_first_child = DAG(dag_first_child_id, default_args=args,
    schedule_interval=timedelta(seconds=1))
t1_first_child = ExternalTaskSensor(
    task_id='t1_first_child',
    external_dag_id=dag_core_id,
    external_task_id='task_core',
    poke_interval=1,
    dag=dag_first_child,
    depends_on_past=True)
t2_first_child = DummyOperator(
    task_id='t2_first_child',
    dag=dag_first_child,
    depends_on_past=True)
t2_first_child.set_upstream(t1_first_child)

dag_second_child_id = TEST_DAG_ID + '_second_child'
dag_second_child = DAG(dag_second_child_id, default_args=args,
    schedule_interval=timedelta(seconds=1))
t1_second_child = ExternalTaskSensor(
    task_id='t1_second_child',
    external_dag_id=dag_first_child_id,
    external_task_id='t2_first_child',
    poke_interval=1,
    dag=dag_second_child,
    depends_on_past=True)
t2_second_child = DummyOperator(
    task_id='t2_second_child',
    dag=dag_second_child)
