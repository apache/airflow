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

import uuid
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.financial_services import (
    FinancialServicesCreateInstanceOperator,
    FinancialServicesGetInstanceOperator,
)
from airflow.providers.google.cloud.sensors.financial_services import FinancialServicesOperationSensor

params = {
    "project_id": None,
    "location": None,
    "keyRing": None,
    "cryptoKey": None,
}

with DAG(
    "example_financial_services",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    # TODO requires an airflow variable containing the developer key / developer key secret resource URI

    generate_run_id_task = PythonOperator(
        task_id="generate_run_id_task", python_callable=uuid.uuid4().__str__
    )

    create_instance_task = FinancialServicesCreateInstanceOperator(
        task_id="create_instance_task",
        instance_id="instance_{{ task_instance.xcom_pull(task_ids='generate_run_id_task', key='return_value') }}",
        location_resource_uri="projects/{{ params.project_id }}/locations/{{ params.location }}",
        kms_key_uri="projects/{{ params.project_id }}/locations/{{ params.location }}/keyRings/{{ params.keyRing }}/cryptoKeys/{{ params.cryptoKey }}",
    )

    create_instance_sensor = FinancialServicesOperationSensor(
        task_id="create_instance_sensor",
        operation_resource_uri="{{ task_instance.xcom_pull(task_ids='create_instance_task', key='return_value') }}",
        poke_interval=timedelta(minutes=1),
        timeout=timedelta(days=1),
    )

    get_instance_task = FinancialServicesGetInstanceOperator(
        task_id="get_instance_task",
        instance_resource_uri="projects/{{ params.project_id }}/locations/{{ params.location }}/instances/instance_{{ task_instance.xcom_pull(task_ids='generate_run_id_task', key='return_value') }}",
    )

    # TODO Log the task info

    delete_instance_task = FinancialServicesGetInstanceOperator(
        task_id="delete_instance_task",
        instance_resource_uri="projects/{{ params.project_id }}/locations/{{ params.location }}/instances/instance_{{ task_instance.xcom_pull(task_ids='generate_run_id_task', key='return_value') }}",
    )

    delete_instance_sensor = FinancialServicesOperationSensor(
        task_id="delete_instance_sensor",
        operation_resource_uri="{{ task_instance.xcom_pull(task_ids='delete_instance_task', key='return_value') }}",
        poke_interval=timedelta(minutes=1),
        timeout=timedelta(days=1),
    )
