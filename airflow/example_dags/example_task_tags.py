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

""" Example dag demonstrating the usage of tags in tasks """

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_task_tags',
    default_args=args,
    schedule_interval="@daily",
    tags=['example']
)

init = DummyOperator(
    task_id='initialize',
    task_tags=['prepare'],
    dag=dag
)

extract = []
for i in range(3):
    extract.append(DummyOperator(
        task_id='extract_account_' + str(i),
        task_tags=['extract', 'etl'],
        dag=dag
    ))

transform = DummyOperator(
    task_id='transform',
    task_tags=['transform', 'etl'],
    dag=dag
)

load = DummyOperator(
    task_id='load',
    task_tags=['load', 'etl'],
    dag=dag
)

init >> extract >> transform >> load
