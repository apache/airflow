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
"""
Example Dag demonstrating a simple measurement correction workflow
using operator PythonOperator tasks.
"""

from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum


# Task 1: Return a raw measurement value
def read_measurement(**context):
    return 100


# Task 2: Validate the measurement value
def validate_measurement(**context):
    value = context["ti"].xcom_pull(task_ids="read_measurement")
    if value < 0:
        raise ValueError("Measurement must be positive")
    return value


# Task 3: Apply correction factor
def apply_correction(**context):
    value = context["ti"].xcom_pull(task_ids="validate_measurement")
    return value * 1.1


# Task 4: Log the corrected result
def store_result(**context):
    value = context["ti"].xcom_pull(task_ids="apply_correction")
    print(f"Corrected measurement: {value}")


# [START example_measurement_correction_operator]
with DAG(
    dag_id="example_measurement_correction_operator",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:

    read = PythonOperator(
        task_id="read_measurement",
        python_callable=read_measurement,
    )

    validate = PythonOperator(
        task_id="validate_measurement",
        python_callable=validate_measurement,
    )

    correct = PythonOperator(
        task_id="apply_correction",
        python_callable=apply_correction,
    )

    store = PythonOperator(
        task_id="store_result",
        python_callable=store_result,
    )

    # Define execution order
    read >> validate >> correct >> store
# [END example_measurement_correction_operator]