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
Example DAG: ERP Data Migration Pipeline

This DAG demonstrates a simplified ERP data migration workflow
including extraction, validation, transformation, loading,
and reconciliation steps.

It is a simplified illustration of how Apache Airflow can be used
to orchestrate enterprise ERP data migration processes.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_data():
    print("Extracting ERP data from legacy systems...")


def validate_data():
    print("Validating extracted ERP data...")


def transform_data():
    print("Transforming ERP data for S/4HANA compatibility...")


def load_data():
    print("Loading transformed data into target ERP system...")


def reconcile_data():
    print("Performing reconciliation and validation checks...")


with DAG(
    dag_id="example_erp_migration_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "erp", "migration"],
) as dag:
    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    reconcile = PythonOperator(
        task_id="reconcile_data",
        python_callable=reconcile_data,
    )

    extract >> validate >> transform >> load >> reconcile
