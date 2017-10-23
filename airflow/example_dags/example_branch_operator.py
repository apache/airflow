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

import airflow
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
import random


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='example_branch_operator',
    default_args=args,
    schedule_interval="@daily")

cmd = 'ls -l'
run_this_first = DummyOperator(task_id='run_this_first', dag=dag)

options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda: random.choice(options),
    dag=dag)
branching.set_upstream(run_this_first)

join = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag
)

for option in options:
    t = DummyOperator(task_id=option, dag=dag)
    t.set_upstream(branching)
    dummy_follow = DummyOperator(task_id='follow_' + option, dag=dag)
    t.set_downstream(dummy_follow)
    dummy_follow.set_downstream(join)
