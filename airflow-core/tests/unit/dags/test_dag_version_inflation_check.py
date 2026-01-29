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

import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def print_hello():
    print("Hello from Airflow!")
    return "Success"


def print_date():
    print(f"Current date: {datetime.now()}")
    return datetime.now()


def calculate_sum():
    result = sum(range(1, 11))
    print(f"Sum of 1 to 10: {result}")
    return result


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

random_task_id = random.randint(1, 10000)

for i in range(1, 10):
    dag_id = f"dynamic_dag_{i:03d}"

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"number {i}",
        schedule=timedelta(minutes=1),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        is_paused_upon_creation=False,
    )

    task1 = BashOperator(
        task_id=f"{str(random_task_id)}",
        bash_command=f'echo "Hello from Dag {i}!"',
        dag=dag,
    )

    task2 = PythonOperator(
        task_id="print_python_hello",
        python_callable=print_hello,
        dag=dag,
    )

    task3 = PythonOperator(
        task_id="print_current_date",
        python_callable=print_date,
        dag=dag,
    )

    task4 = PythonOperator(
        task_id="calculate_sum",
        python_callable=calculate_sum,
        dag=dag,
    )

    task5 = BashOperator(
        task_id="final_task",
        bash_command=f'echo "Dag {i} - All tasks completed!"',
        dag=dag,
    )

    task1 >> [task2, task3] >> task4 >> task5
