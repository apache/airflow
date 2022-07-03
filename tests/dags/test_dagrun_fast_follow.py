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
from __future__ import annotations

from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator

DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
}


dag_id = 'test_dagrun_fast_follow'
dag = DAG(dag_id=dag_id, default_args=args)

# A -> B -> C
task_a = PythonOperator(task_id='A', dag=dag, python_callable=lambda: True)
task_b = PythonOperator(task_id='B', dag=dag, python_callable=lambda: True)
task_c = PythonOperator(task_id='C', dag=dag, python_callable=lambda: True)
task_a.set_downstream(task_b)
task_b.set_downstream(task_c)

# G -> F -> E & D -> E
task_d = PythonOperator(task_id='D', dag=dag, python_callable=lambda: True)
task_e = PythonOperator(task_id='E', dag=dag, python_callable=lambda: True)
task_f = PythonOperator(task_id='F', dag=dag, python_callable=lambda: True)
task_g = PythonOperator(task_id='G', dag=dag, python_callable=lambda: True)
task_g.set_downstream(task_f)
task_f.set_downstream(task_e)
task_d.set_downstream(task_e)

# H -> J & I -> J
task_h = PythonOperator(task_id='H', dag=dag, python_callable=lambda: True)
task_i = PythonOperator(task_id='I', dag=dag, python_callable=lambda: True)
task_j = PythonOperator(task_id='J', dag=dag, python_callable=lambda: True)
task_h.set_downstream(task_j)
task_i.set_downstream(task_j)

# wait_for_downstream test
# K -> L -> M
task_k = PythonOperator(task_id='K', dag=dag, python_callable=lambda: True, wait_for_downstream=True)
task_l = PythonOperator(task_id='L', dag=dag, python_callable=lambda: True, wait_for_downstream=True)
task_m = PythonOperator(task_id='M', dag=dag, python_callable=lambda: True, wait_for_downstream=True)
task_k.set_downstream(task_l)
task_l.set_downstream(task_m)
