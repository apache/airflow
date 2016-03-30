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
DAG designed to test what happens when running a DAG fails before
a task runs -- prior to a fix, this could actually cause an Executor to report
SUCCESS. Since the task never reports any status, this can lead to an infinite
rescheduling loop.
"""
from datetime import datetime

from airflow.models import DAG
from airflow.operators import SubDagOperator
from airflow.example_dags.subdags.subdag import subdag

args = {
    'owner': 'airflow',
    'start_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='test_raise_executor_error',
    default_args=args,
    schedule_interval="@daily",
)

section_1 = SubDagOperator(
    task_id='subdag_op',
    subdag=subdag('test_raise_executor_error', 'subdag_op', args),
    default_args=args,
    dag=dag,
)

# change the subdag name -- this creates an error because the subdag
# won't be found, but it'll do it in a way that causes the executor to report
# success
section_1.subdag.dag_id = 'bad_id'
