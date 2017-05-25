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


from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

DEFAULT_DATE = datetime(2016, 1, 1)
default_args = dict(
    start_date=DEFAULT_DATE,
    owner='airflow')


def fail():
    raise ValueError('Expected failure.')


def success(ti=None, *args, **kwargs):
    if ti.execution_date != DEFAULT_DATE + timedelta(days=1):
        fail()
    return


# DAG tests that tasks ignore all dependencies

dag1 = DAG(dag_id='test_run_ignores_all_dependencies', default_args=dict(depends_on_past=True, **default_args))
dag1_task1 = PythonOperator(
    task_id='test_run_dependency_task',
    python_callable=fail,
    dag=dag1,)
dag1_task2 = PythonOperator(
    task_id='test_run_dependent_task',
    python_callable=success,
    provide_context=True,
    dag=dag1,)
dag1_task1.set_downstream(dag1_task2)
