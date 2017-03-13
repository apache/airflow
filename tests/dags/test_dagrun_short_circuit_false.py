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

from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.dummy_operator import DummyOperator


# DAG that has its short circuit op fail and skip multiple downstream tasks
dag = DAG(
    dag_id='test_dagrun_short_circuit_false',
    start_date=datetime(2017, 1, 1)
)
dag_task1 = ShortCircuitOperator(
    task_id='test_short_circuit_false',
    dag=dag,
    python_callable=lambda: False)
dag_task2 = DummyOperator(
    task_id='test_state_skipped1',
    dag=dag)
dag_task3 = DummyOperator(
    task_id='test_state_skipped2',
    dag=dag)
dag_task1.set_downstream(dag_task2)
dag_task2.set_downstream(dag_task3)
