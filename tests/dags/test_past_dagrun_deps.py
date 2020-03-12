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

from datetime import datetime

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

DEFAULT_DATE = datetime(2016, 1, 1)
default_args = dict(
    start_date=DEFAULT_DATE,
    owner='airflow')

# DAG tests depends_on_past dependencies
dag_dop = DAG(dag_id='test_depends_on_past', default_args=default_args)
dag_dop_task1 = DummyOperator(
    task_id='test_dop_task',
    dag=dag_dop,
    depends_on_past=True,)


dag_wfd = DAG(dag_id='test_wait_for_downstream', default_args=default_args)
upstream_task = DummyOperator(
    task_id='upstream_task',
    dag=dag_wfd,
    wait_for_downstream=True,
)
downstream_task = DummyOperator(
    task_id='downstream_task',
    dag=dag_wfd,
)
upstream_task.set_downstream(downstream_task)
