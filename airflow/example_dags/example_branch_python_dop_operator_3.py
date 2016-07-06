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
#

from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta

two_days_ago = datetime.combine(datetime.today() - timedelta(2),
                                  datetime.min.time())
args = {
    'owner': 'airflow',
    'start_date': two_days_ago,
    'depends_on_past': True,
}

# BranchPython operator that depends on past
# and where tasks may run or be skipped on
# alternating runs
dag = DAG(dag_id='example_branch_dop_operator_v3',schedule_interval='*/1 * * * *',  default_args=args)


def should_run(ds, **kwargs):

    print("------------- exec dttm = {} and minute = {}".format(kwargs['execution_date'], kwargs['execution_date'].minute))
    if kwargs['execution_date'].minute % 2 == 0:
        return "oper_1"
    else:
        return "oper_2"


cond = BranchPythonOperator(
    task_id='condition',
    provide_context=True,
    python_callable=should_run,
    dag=dag)

oper_1 = DummyOperator(
    task_id='oper_1',
    dag=dag)
oper_1.set_upstream(cond)

oper_2 = DummyOperator(
    task_id='oper_2',
    dag=dag)
oper_2.set_upstream(cond)
