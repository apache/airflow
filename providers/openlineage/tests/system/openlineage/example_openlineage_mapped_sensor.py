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

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

from system.openlineage.operator import OpenLineageTestOperator


def my_task(task_number):
    print(os.getcwd())
    print(f"Executing task number: {task_number}")


def check_start_amount_func():
    start_sensor_key = "openlineage_sensor_mapped_tasks_dag.wait_for_10_seconds.event.start"  # type: ignore[union-attr]
    events = Variable.get(start_sensor_key, deserialize_json=True)
    if len(events) < 2:
        raise ValueError(f"Expected at least 2 events, got {len(events)}")


with DAG(
    dag_id="openlineage_sensor_mapped_tasks_dag",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    wait_for_10_seconds = TimeDeltaSensor(
        task_id="wait_for_10_seconds",
        mode="reschedule",
        poke_interval=5,
        delta=timedelta(seconds=10),
    )

    mapped_tasks = [
        PythonOperator(
            task_id=f"mapped_task_{i}",
            python_callable=my_task,
            op_args=[i],
        )
        for i in range(2)
    ]

    check_start_amount = PythonOperator(
        task_id="check_order",
        python_callable=check_start_amount_func,
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=str(Path(__file__).parent / "example_openlineage_mapped_sensor.json"),
        allow_duplicate_events=True,
    )

    wait_for_10_seconds >> mapped_tasks >> check_start_amount >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
