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
"""
Example Airflow DAG that show how to use various Looker
operators to submit PDT materialization job and manage it.
"""

from __future__ import annotations

from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.looker import LookerStartPdtBuildOperator
from airflow.providers.google.cloud.sensors.looker import LookerCheckPdtBuildSensor

with DAG(
    dag_id="example_gcp_looker",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START cloud_looker_async_start_pdt_sensor]
    start_pdt_task_async = LookerStartPdtBuildOperator(
        task_id="start_pdt_task_async",
        looker_conn_id="your_airflow_connection_for_looker",
        model="your_lookml_model",
        view="your_lookml_view",
        asynchronous=True,
    )

    check_pdt_task_async_sensor = LookerCheckPdtBuildSensor(
        task_id="check_pdt_task_async_sensor",
        looker_conn_id="your_airflow_connection_for_looker",
        materialization_id=start_pdt_task_async.output,
        poke_interval=10,
    )
    # [END cloud_looker_async_start_pdt_sensor]

    # [START how_to_cloud_looker_start_pdt_build_operator]
    build_pdt_task = LookerStartPdtBuildOperator(
        task_id="build_pdt_task",
        looker_conn_id="your_airflow_connection_for_looker",
        model="your_lookml_model",
        view="your_lookml_view",
    )
    # [END how_to_cloud_looker_start_pdt_build_operator]

    start_pdt_task_async >> check_pdt_task_async_sensor

    build_pdt_task
