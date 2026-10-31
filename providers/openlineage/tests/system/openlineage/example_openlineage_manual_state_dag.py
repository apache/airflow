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
Dags whose task and Dag run states this Dag changes through the REST API, like a user does in the UI.

It triggers a child Dag with a chain of tasks per scenario and changes the state of each chain on its own:
    - a running task marked as success, failed or skipped, and a running map index marked as success or failed
    - a task that hasn't started marked as failed with its upstream, and a running task marked as success with
      its downstream, so some of the marked tasks never started
    - a finished task marked with the other state, success to failed and failed to success
    - a task group marked as failed while its first task runs
    - a deferred task and a sensor waiting to be rescheduled marked as success, a task awaiting input as failed
      (Airflow 3.1+, as the human-in-the-loop operators are)

It also triggers two child Dags with a running task, a running map index and a deferred task, and marks the Dag
runs as success and as failed.
It needs a deployment with an API server (the OpenLineage e2e tests).

It checks:
    - the API server emits the COMPLETE or FAIL of every marked task, with the `runId` of the START the worker
      emitted for it, if it started (the `runId` check of OpenLineageTestOperator)
    - a marked task that never started gets a COMPLETE or FAIL and no START
    - a finished task marked with the other state gets the event of its new state, with the `runId` of its
      first outcome
    - tasks the scheduler skips or fails because of the change, and map indexes that never start, emit nothing
    - the Dag run events of the child Dags, whoever emitted them
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import TaskGroup, task

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import AIRFLOW_VERSION, get_expected_event_file_path
from system.openlineage.manual_state import change_finished_state, change_state, wait_for_task_instance
from system.openlineage.operator import OpenLineageTestOperator, sort_by_execution

DAG_ID = "openlineage_manual_state_dag"
TASKS_DAG_ID = f"{DAG_ID}_tasks__notrigger"
HAS_HITL = (AIRFLOW_VERSION.major, AIRFLOW_VERSION.minor) >= (3, 1)

if HAS_HITL:
    from airflow.providers.standard.operators.hitl import ApprovalOperator
DAG_RUN_STATES = ("success", "failed")


def get_dag_run_dag_id(state: str) -> str:
    return f"{DAG_ID}_dag_run_{state}__notrigger"


def sleep_long() -> None:
    # Longer than [core] killed_task_cleanup_time (set in the e2e deployment), so the worker is killed
    # without getting to emit an event of its own after the state change.
    time.sleep(300)


def do_nothing() -> None:
    # Not an EmptyOperator, which the scheduler marks as successful without running it, so it emits no events
    pass


def fail() -> None:
    raise ValueError("Intentional failure, marked as success afterwards")


def never_ready() -> bool:
    return False


def sleeper_then_after(name: str) -> None:
    sleeper = PythonOperator(task_id=f"sleep_{name}", python_callable=sleep_long)
    sleeper >> PythonOperator(task_id=f"after_{name}", python_callable=do_nothing)


def mapped_sleeper_then_after(name: str) -> None:
    # Run one at a time, so the second map index starts only after the first is marked
    @task(task_id=f"sleep_{name}", max_active_tis_per_dagrun=1)
    def sleeper(index):
        if index == 0:
            sleep_long()

    sleeper.expand(index=[0, 1]) >> PythonOperator(task_id=f"after_{name}", python_callable=do_nothing)


def deferred_waiter(name: str) -> None:
    TimeDeltaSensor(task_id=f"wait_{name}", delta=timedelta(minutes=30), deferrable=True)


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=TASKS_DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as tasks_dag:
    for name in ("success", "failed", "skipped", "upstream_failed", "downstream_success"):
        sleeper_then_after(name)
    for name in ("mapped_success", "mapped_failed"):
        mapped_sleeper_then_after(name)
    PythonOperator(task_id="finish_success_to_failed", python_callable=do_nothing)
    PythonOperator(task_id="finish_failed_to_success", python_callable=fail)
    with TaskGroup(group_id="group_failed"):
        sleeper_then_after("in_group")
    deferred_waiter("deferred_success")
    PythonSensor(
        task_id="wait_rescheduled_success",
        python_callable=never_ready,
        mode="reschedule",
        poke_interval=600,
        timeout=1200,
    )
    if HAS_HITL:
        ApprovalOperator(
            task_id="wait_awaiting_input_failed",
            subject="Approval the test marks as failed",
            response_timeout=timedelta(minutes=30),
        )

for state in DAG_RUN_STATES:
    with DAG(
        dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
        dag_id=get_dag_run_dag_id(state),
        start_date=datetime(2021, 1, 1),
        schedule=None,
        catchup=False,
        default_args={"retries": 0},
    ) as dag_run_dag:
        sleeper_then_after("plain")
        mapped_sleeper_then_after("mapped")
        deferred_waiter("deferred")
    globals()[dag_run_dag.dag_id] = dag_run_dag  # The Dag processor finds Dags among the module's globals

# State changes of the tasks child Dag run, besides dag_id and run_id, applied one after the other
TASK_CHANGES: list[dict] = [
    {"level": "task", "state": "success", "task_id": "sleep_success"},
    {"level": "task", "state": "failed", "task_id": "sleep_failed"},
    {"level": "task", "state": "skipped", "task_id": "sleep_skipped"},
    {
        "level": "task",
        "state": "failed",
        "task_id": "sleep_upstream_failed",
        "target_task_id": "after_upstream_failed",
        "include_upstream": True,
    },
    {"level": "task", "state": "success", "task_id": "sleep_downstream_success", "include_downstream": True},
    {"level": "task", "state": "success", "task_id": "sleep_mapped_success", "map_index": 0},
    {"level": "task", "state": "failed", "task_id": "sleep_mapped_failed", "map_index": 0},
    {
        "level": "task_group",
        "state": "failed",
        "task_id": "group_failed.sleep_in_group",
        "target_task_id": "group_failed",
    },
    {
        "level": "task",
        "state": "success",
        "task_id": "wait_deferred_success",
        "wait_for_states": ("deferred",),
    },
    {
        "level": "task",
        "state": "success",
        "task_id": "wait_rescheduled_success",
        "wait_for_states": ("up_for_reschedule",),
    },
    *(
        [
            {
                "level": "task",
                "state": "failed",
                "task_id": "wait_awaiting_input_failed",
                # Deferred while it waits before Airflow 3.3
                "wait_for_states": ("awaiting_input", "deferred"),
            }
        ]
        if HAS_HITL
        else []
    ),
]


def change_tasks(child_run_id: str) -> None:
    # One task applies every change, as a task per change would hold as many worker slots while waiting
    for change in TASK_CHANGES:
        change_state(TASKS_DAG_ID, child_run_id, wait_until_finished=False, **change)
    change_finished_state(TASKS_DAG_ID, child_run_id, task_id="finish_success_to_failed", state="failed")
    change_finished_state(TASKS_DAG_ID, child_run_id, task_id="finish_failed_to_success", state="success")


def change_dag_run(child_dag_id: str, child_run_id: str, state: str) -> None:
    # The other tasks are also running or deferred when the Dag run is marked
    wait_for_task_instance(child_dag_id, child_run_id, "sleep_mapped", map_index=0)
    wait_for_task_instance(child_dag_id, child_run_id, "wait_deferred", states=("deferred",))
    change_state(child_dag_id, child_run_id, level="dag_run", state=state, task_id="sleep_plain")


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        event_sort_fn=sort_by_execution,
        # The finished tasks marked with another state get a second COMPLETE/FAIL
        corrected_terminal_keys=[
            f"{TASKS_DAG_ID}.finish_success_to_failed.event.fail",
            f"{TASKS_DAG_ID}.finish_failed_to_success.event.complete",
        ],
        # Marked before they started
        unstarted_terminal_keys=[
            f"{TASKS_DAG_ID}.after_upstream_failed.event.fail",
            f"{TASKS_DAG_ID}.after_downstream_success.event.complete",
            f"{TASKS_DAG_ID}.group_failed.after_in_group.event.fail",
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
    for state in DAG_RUN_STATES:
        dag_run_dag_id = get_dag_run_dag_id(state)
        dag_run_id = f"dag_run_{state}__{{{{ run_id }}}}"
        (
            TriggerDagRunOperator(
                task_id=f"trigger_dag_run_{state}", trigger_dag_id=dag_run_dag_id, trigger_run_id=dag_run_id
            )
            >> PythonOperator(
                task_id=f"change_dag_run_{state}",
                python_callable=change_dag_run,
                op_kwargs={"child_dag_id": dag_run_dag_id, "child_run_id": dag_run_id, "state": state},
            )
            >> check_events
        )


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
