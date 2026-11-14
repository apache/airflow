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
Emission policy of the events the API server emits when task and Dag run states are changed through the REST API.

The rules are in ``AIRFLOW__OPENLINEAGE__EMISSION_POLICY`` of ``airflow-e2e-tests/docker/openlineage.yml``:
    - ``emit_manual_state_change_events: false`` for both child Dags
    - ``emit_manual_state_change_events: true`` for ``sleep_kept`` of the tasks child, overriding its Dag's rule
    - ``emit_task_events: false`` for ``wait_task_events_off`` of the tasks child

It checks:
    - a task marked as success gets no COMPLETE, but the worker events of its START and of its downstream task
      are kept, as they are not manual state changes
    - the task-scoped rule re-enables the COMPLETE of a marked task
    - the API server, which has no task objects, applies the task-scoped ``emit_task_events`` rule to the FAIL of a
      task marked as failed
    - a Dag run marked as success gets no COMPLETE, and neither does its running task, while the Dag run START
      the scheduler emitted is kept
It needs a deployment with an API server (the OpenLineage e2e tests).
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

from system.openlineage.constants import CRITICAL_PATH_PRIORITY_WEIGHT, DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.manual_state import change_state, wait_for_dag_run_to_finish
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_policy_manual_state_dag"
TASKS_DAG_ID = f"{DAG_ID}_tasks__notrigger"
DAG_RUN_DAG_ID = f"{DAG_ID}_dag_run__notrigger"


def sleep_long() -> None:
    # Longer than [core] killed_task_cleanup_time (set in the e2e deployment), so the worker is killed
    # without getting to emit an event of its own after the state change.
    time.sleep(300)


def do_nothing() -> None:
    # Not an EmptyOperator, which the scheduler marks as successful without running it, so it emits no events
    pass


default_args = {"retries": 0, "priority_weight": CRITICAL_PATH_PRIORITY_WEIGHT}

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=TASKS_DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as tasks_dag:
    PythonOperator(task_id="sleep_suppressed", python_callable=sleep_long) >> PythonOperator(
        task_id="after_suppressed", python_callable=do_nothing
    )
    PythonOperator(task_id="sleep_kept", python_callable=sleep_long)
    # Deferred, as its START is suppressed too, and waiting for a running task needs its START
    TimeDeltaSensor(task_id="wait_task_events_off", delta=timedelta(minutes=30), deferrable=True)

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_RUN_DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag_run_dag:
    PythonOperator(task_id="sleep_plain", python_callable=sleep_long) >> PythonOperator(
        task_id="after_plain", python_callable=do_nothing
    )


def change_tasks(child_run_id: str) -> None:
    for task_id, state, wait_for_states in (
        ("sleep_suppressed", "success", ("running",)),
        ("sleep_kept", "success", ("running",)),
        ("wait_task_events_off", "failed", ("deferred",)),
    ):
        change_state(
            TASKS_DAG_ID,
            child_run_id,
            level="task",
            state=state,
            task_id=task_id,
            wait_for_states=wait_for_states,
            wait_until_finished=False,
        )
    wait_for_dag_run_to_finish(TASKS_DAG_ID, child_run_id)


def change_dag_run(child_run_id: str) -> None:
    change_state(DAG_RUN_DAG_ID, child_run_id, level="dag_run", state="success", task_id="sleep_plain")


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:
    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        # Their COMPLETE is suppressed by the emission policy
        unfinished_start_keys=[
            f"{TASKS_DAG_ID}.sleep_suppressed.event.start",
            f"{DAG_RUN_DAG_ID}.sleep_plain.event.start",
        ],
    )

    tasks_run_id = "tasks__{{ run_id }}"
    (
        TriggerDagRunOperator(
            task_id="trigger_tasks", trigger_dag_id=TASKS_DAG_ID, trigger_run_id=tasks_run_id
        )
        >> PythonOperator(
            task_id="change_tasks", python_callable=change_tasks, op_kwargs={"child_run_id": tasks_run_id}
        )
        >> check_events
    )
    dag_run_id = "dag_run__{{ run_id }}"
    (
        TriggerDagRunOperator(
            task_id="trigger_dag_run", trigger_dag_id=DAG_RUN_DAG_ID, trigger_run_id=dag_run_id
        )
        >> PythonOperator(
            task_id="change_dag_run", python_callable=change_dag_run, op_kwargs={"child_run_id": dag_run_id}
        )
        >> check_events
    )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
