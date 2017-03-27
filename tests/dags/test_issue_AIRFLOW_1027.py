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
DAG designed to test a PythonOperator that calls a functool.partial
"""
import functools

from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

DEFAULT_DATE = datetime(2016, 1, 1)
default_args = dict(
    start_date=DEFAULT_DATE,
    owner='airflow')


def func_with_two_args(arg_1, arg_2):
    pass


partial_func = functools.partial(func_with_two_args, arg_1=1)


dag = DAG(dag_id='test_issue_AIRFLOW_1027_dag', default_args=default_args)

dag_task1 = PythonOperator(
    task_id='test_dagrun_functool_partial',
    dag=dag,
    python_callable=partial_func)
