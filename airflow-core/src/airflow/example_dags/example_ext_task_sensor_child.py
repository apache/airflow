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

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.sensors.external_task import ExternalTaskSensor


def get_previous_execution_date(logical_date, **kwargs):
    """Get the previous execution date of the parent DAG."""
    return logical_date - timedelta(minutes=5)


with DAG(
    dag_id="example_ext_task_sensor_child",
    start_date=datetime(2026, 2, 11),
    schedule="42 00 * * *",
    catchup=False,
    tags=["example", "ext-sensor"],
    description="This DAG is used to demonstrate the ExternalTaskSensor. It is used in conjunction with example_ext_task_sensor_parent.py",
) as dag:
    wait_for_parent = ExternalTaskSensor(
        task_id="wait_for_parent",
        external_dag_id="example_ext_task_sensor_parent",
        execution_date_fn=get_previous_execution_date,
        poke_interval=30,
        mode="poke",
        timeout=300,
    )
