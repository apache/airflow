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
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

DEFAULT_DATE = datetime(2016, 1, 1)

default_args = dict(
    start_date=DEFAULT_DATE,
    owner='airflow')

parent_dag = DAG(dag_id='test_subdag_parent_dag', default_args=default_args)

subdag = DAG(dag_id='test_subdag_parent_dag.subdag', default_args=default_args)

subdag_op = SubDagOperator(
    task_id='subdag',
    dag=parent_dag,
    subdag=subdag)

subdag_task = DummyOperator(
    task_id='test_subdag_task',
    dag=subdag)
