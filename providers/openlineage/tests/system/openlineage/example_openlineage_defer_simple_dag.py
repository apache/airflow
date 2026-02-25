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
Simple DAG with deferrable operator.

It checks:
    - that at least two task START events (before and after deferral) are emitted and
      the try_num remains at 1
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


def check_events_number_func():
    events = Variable.get(key="openlineage_defer_simple_dag.wait.event.start", deserialize_json=True)
    if len(events) < 2:
        raise ValueError(f"Expected at least 2 START events for task `wait`, got {len(events)}")


DAG_ID = "openlineage_defer_simple_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    # Timedelta is compared to the DAGRun start timestamp, which can occur long before a worker picks up the
    # task. We need to ensure the sensor gets deferred at least once, so setting 180s.
    wait = TimeDeltaSensor(task_id="wait", delta=timedelta(seconds=180), deferrable=True)

    check_events_number = PythonOperator(
        task_id="check_events_number", python_callable=check_events_number_func
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        allow_duplicate_events_regex="openlineage_defer_simple_dag.wait.event.start",
    )

    wait >> check_events_number >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
