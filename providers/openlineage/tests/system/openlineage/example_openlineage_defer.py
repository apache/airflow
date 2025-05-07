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
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync

from system.openlineage.operator import OpenLineageTestOperator


def my_task(task_number):
    print(os.getcwd())
    print(f"Executing task number: {task_number}")


def check_start_amount_func():
    events = Variable.get(key="openlineage_basic_defer_dag.wait.event.start", deserialize_json=True)
    if len(events) < 2:
        raise ValueError(f"Expected at least 2 events, got {len(events)}")


with DAG(
    dag_id="openlineage_basic_defer_dag",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    # Timedelta is compared to the DAGRun start timestamp, which can occur long before a worker picks up the
    # task. We need to ensure the sensor gets deferred at least once, so setting 180s.
    wait = TimeDeltaSensorAsync(task_id="wait", delta=timedelta(seconds=180))

    check_start_events_amount = PythonOperator(
        task_id="check_start_events_amount", python_callable=check_start_amount_func
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=str(Path(__file__).parent / "example_openlineage_defer.json"),
        allow_duplicate_events=True,
    )

    wait >> check_start_events_amount >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
