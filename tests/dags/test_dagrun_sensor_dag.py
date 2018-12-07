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
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.dagrun_sensor import DagRunSensor
from tests.contrib.sensors.test_dagrun_sensor import (
    DEFAULT_DATE, TEST_DAG_CHILD, TEST_DAG_PARENT)

args = {
    'start_date': DEFAULT_DATE,
    'owner': 'airflow',
    'depends_on_past': False
}

# Child dag with a 3 second delay
with DAG(dag_id=TEST_DAG_CHILD,
         default_args=args,
         start_date=DEFAULT_DATE,
         schedule_interval=None) as dag_child:
    t1 = BashOperator(
        task_id='task_1',
        bash_command="echo 'one'; sleep 3",
    )
    t2 = BashOperator(
        task_id='task_2',
        bash_command="echo 'two'",
    )
    t1 >> t2


# Parent dag that triggers child dag and senses it
with DAG(dag_id=TEST_DAG_PARENT,
         default_args=args,
         start_date=DEFAULT_DATE,
         schedule_interval=None) as dag_parent:
    def triggerchild(*args, **kwargs):
        dr = trigger_dag(TEST_DAG_CHILD)
        return [dr.run_id]
    t0 = PythonOperator(python_callable=triggerchild,
                        provide_context=True,
                        task_id='trigger_child')
    t1 = DagRunSensor(
        task_id='sense_child',
        trigger_task_id='trigger_child',
        timeout=15,
        poke_interval=1,
    )
    t2 = BashOperator(
        task_id='do_stuff',
        bash_command="echo 'finished'",
    )
    t0 >> t1 >> t2
