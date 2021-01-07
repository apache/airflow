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

from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

DEFAULT_DATE = datetime(2016, 1, 1)

# DAG tests backfill with pooled tasks
# Previously backfill would queue the task but never run it
dag1 = DAG(dag_id='test_start_date_scheduling', start_date=datetime.utcnow() + timedelta(days=1))
dag1_task1 = DummyOperator(task_id='dummy', dag=dag1, owner='airflow')

dag2 = DAG(dag_id='test_task_start_date_scheduling', start_date=DEFAULT_DATE)
dag2_task1 = DummyOperator(
    task_id='dummy1', dag=dag2, owner='airflow', start_date=DEFAULT_DATE + timedelta(days=3)
)
dag2_task2 = DummyOperator(task_id='dummy2', dag=dag2, owner='airflow')
