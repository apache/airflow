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

from airflow import DAG
from airflow.providers.common.compat.sdk import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id="example_external_task",
    start_date=timezone.datetime(2022, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "async", "core"],
) as dag:
    start = EmptyOperator(task_id="start")

    # [START howto_external_task_async_sensor]
    external_task_sensor = ExternalTaskSensor(
        task_id="parent_task_sensor",
        external_task_id="child_task",
        external_dag_id="child_dag",
        deferrable=True,
    )
    # [END howto_external_task_async_sensor]

    trigger_child_task = TriggerDagRunOperator(
        task_id="trigger_child_task",
        trigger_dag_id="child_dag",
        allowed_states=[
            "success",
            "failed",
        ],
        logical_date="{{ logical_date }}",
        poke_interval=5,
        reset_dag_run=True,
        wait_for_completion=True,
    )

    end = EmptyOperator(task_id="end")

    start >> [trigger_child_task, external_task_sensor] >> end
