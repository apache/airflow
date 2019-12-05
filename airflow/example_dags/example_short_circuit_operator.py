# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import airflow.utils.helpers
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(dag_id='example_short_circuit_operator', default_args=args)

cond_true = ShortCircuitOperator(
    task_id='condition_is_True',
    python_callable=lambda: True,
    dag=dag,
)

cond_false = ShortCircuitOperator(
    task_id='condition_is_False',
    python_callable=lambda: False,
    dag=dag,
)

ds_true = [DummyOperator(task_id='true_' + str(i), dag=dag) for i in [1, 2]]
ds_false = [DummyOperator(task_id='false_' + str(i), dag=dag) for i in [1, 2]]

airflow.utils.helpers.chain(cond_true, *ds_true)
airflow.utils.helpers.chain(cond_false, *ds_false)
