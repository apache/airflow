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
Simple DAG with Short Circuit Operator.

It checks:
    - if events that should be emitted are there
    - if events for skipped tasks are not emitted
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


def do_nothing():
    pass


def check_events_number_func():
    try:
        from airflow.sdk.exceptions import AirflowRuntimeError as ExpectedError  # AF3
    except ImportError:
        ExpectedError = KeyError  # AF2

    for event_type in ("start", "complete"):
        try:
            events = Variable.get(
                key=f"openlineage_short_circuit_dag.should_be_skipped.event.{event_type}",
                deserialize_json=True,
            )
        except ExpectedError:
            pass
        else:
            raise ValueError(
                f"Expected no {event_type.upper()} events for task `should_be_skipped`, got {events}"
            )


DAG_ID = "openlineage_short_circuit_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    do_nothing_task = PythonOperator(task_id="do_nothing_task", python_callable=do_nothing)

    skip_tasks = ShortCircuitOperator(
        task_id="skip_tasks", python_callable=lambda: False, ignore_downstream_trigger_rules=False
    )

    should_be_skipped = PythonOperator(task_id="should_be_skipped", python_callable=do_nothing)

    check_events_number = PythonOperator(
        task_id="check_events_number", python_callable=check_events_number_func, trigger_rule="none_failed"
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )

    do_nothing_task >> skip_tasks >> should_be_skipped >> check_events_number >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
