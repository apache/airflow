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

from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.dag_run_sensor import DagRunSensor
from tests.contrib.sensors.test_dag_run_sensor import (DEFAULT_DATE,
                                                       TEST_DAG_ID)

args = {
    'start_date': DEFAULT_DATE,
    'owner': 'airflow',
    'depends_on_past': False
}

with DAG(dag_id=TEST_DAG_ID + '_parent_clean',
         default_args=args,
         start_date=DEFAULT_DATE,
         schedule_interval=timedelta(seconds=1)) as dag_parent:

    t1 = DummyOperator(
        task_id='task_1',
    )
    t2 = DummyOperator(
        task_id='task_2',
    )

    t1 >> t2


# A five-secondly workflow that depends on the 5 secondly runs of the parent
# dag above.
with DAG(dag_id=TEST_DAG_ID + '_child_clean',
         default_args=args,
         start_date=DEFAULT_DATE,
         schedule_interval=timedelta(seconds=5)) as dag_child:

    t1 = DagRunSensor(
        task_id='sense_parent',
        external_dag_id=TEST_DAG_ID + '_parent_clean',
        execution_date_fn=lambda d: [d + timedelta(seconds=i) for i in range(5)],
        timeout=5,
        poke_interval=1,
    )
    t2 = DummyOperator(
        task_id='do_stuff',
    )

    t1 >> t2
