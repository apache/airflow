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
Simple DAG with mapped task.

It checks:
    - task's `mapped` attribute
    - taskInstance's `map_index` attribute
    - number of OL events emitted for mapped task
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

try:
    from airflow.sdk import task
except ImportError:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


def check_events_number_func():
    for event_type in ("start", "complete"):
        events = Variable.get(
            key=f"openlineage_mapped_simple_dag.add_one.event.{event_type}", deserialize_json=True
        )
        if len(events) != 2:
            raise ValueError(
                f"Expected exactly 2 {event_type.upper()} events for task `add_one`, got {len(events)}"
            )


DAG_ID = "openlineage_mapped_simple_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:

    @task(max_active_tis_per_dagrun=1)  # Execute sequentially to not overwrite each other OL events
    def add_one(x: int):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        print(f"Total was {total}")

    added_values = add_one.expand(x=[1, 2])

    check_events_number = PythonOperator(
        task_id="check_events_number", python_callable=check_events_number_func
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        allow_duplicate_events_regex="openlineage_mapped_simple_dag.add_one.event.(start|complete)",
    )

    sum_it(added_values) >> check_events_number >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
