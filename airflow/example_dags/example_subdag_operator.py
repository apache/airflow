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

"""Example DAG demonstrating the usage of the SubDagOperator."""

# [START example_subdag_operator]
from airflow import DAG
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago

DAG_NAME = 'example_subdag_operator'

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id=DAG_NAME, default_args=args, start_date=days_ago(2), schedule_interval="@once", tags=['example']
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    section_1 = SubDagOperator(
        task_id='section-1',
        subdag=subdag(DAG_NAME, 'section-1', args),
    )

    some_other_task = DummyOperator(
        task_id='some-other-task',
    )

    section_2 = SubDagOperator(
        task_id='section-2',
        subdag=subdag(DAG_NAME, 'section-2', args),
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> section_1 >> some_other_task >> section_2 >> end
# [END example_subdag_operator]
