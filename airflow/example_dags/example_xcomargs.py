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

"""Example DAG demonstrating the usage of the XComArgs."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}


def dummy(*args, **kwargs):
    """Dummy function"""
    return "pass"


with DAG(
    dag_id='example_xcom_args',
    default_args=args,
    schedule_interval=None,
    tags=['example']
) as dag:
    task1 = PythonOperator(
        task_id='task1',
        python_callable=dummy,
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=dummy,
        op_kwargs={"dummy": task1.output},
    )
