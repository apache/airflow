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
Dag declaring, as task outlets, the assets the system test Dags use only as inlets. It never runs.

Airflow 3.0 counts an asset as referenced only when a Dag schedules on it or a task produces it, so the scheduler
deactivates the assets used only as inlets shortly after the Dag processor activates them, and a task with such an
inlet then fails with AirflowInactiveAssetInInletOrOutletException (fixed in Airflow 3.1). Declaring them here keeps
them active. Add an asset here when a new system test Dag uses it only as an inlet.

This Dag is specific to the deployed e2e harness (it is not part of the provider system tests). It is copied into
the generated dags folder by prepare_dags.py, and the harness never triggers it (``__notrigger``).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.common.compat.assets import Asset
from airflow.providers.standard.operators.python import PythonOperator

INLET_ONLY_ASSET_URIS = (
    "file://host1/authored_a_in.txt",
    "file://host1/conf_excluded_in.txt",
    "file://host1/dag_excluded_declared_in.txt",
    "file://host1/dag_excluded_in.txt",
    "file://host1/dag_hook_excluded_in.txt",
    "file://host1/declared_in.txt",
    "file://host1/dir2/file2.txt",
    "file://host1/exclude_datasets_global_in.txt",
    "file://host1/hook_scoped_in.txt",
    "file://host1/keep_declared_in.txt",
    "file://host1/keep_in.txt",
    "file://host1/operator_excluded_in.txt",
    "file://host1/operator_hook_excluded_in.txt",
    "unknown_scheme://host1/dir2/file3.txt",
)

with DAG(
    dag_id="openlineage_inlet_assets__notrigger",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(
        task_id="declare_outlets",
        python_callable=lambda: None,
        outlets=[Asset(uri) for uri in INLET_ONLY_ASSET_URIS],
    )
