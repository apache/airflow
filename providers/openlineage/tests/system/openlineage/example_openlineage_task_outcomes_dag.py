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
DAG with tasks ending in every way the task runner maps an outcome to a state, or failing in the triggerer or
with their worker (requires the CeleryExecutor).

Each task ends in its own way, and the task runner calls the listener from the outcome it maps:

    - sensors timing out, in poke and in reschedule mode: START (only one over the pokes of the reschedule
      sensor) and FAIL with `state` failed at `try_number` 1, although a retry is left
    - `execution_timeout` hit while the task can still handle the signal: START and FAIL
    - `sys.exit(1)` and `sys.exit(0)`: START and FAIL, as the task runner fails the task on any `SystemExit`
    - a deferral whose kwargs can't be serialized, a return value XCom can't serialize, and a `map_index_template`
      that fails to render after `execute()` succeeded: START and FAIL, the outcome handler itself failing
    - a templated field that fails to render: FAIL with no START, as rendering comes before the START
    - a callback raising: the listener still sends COMPLETE or FAIL
    - `TriggerDagRunOperator` for a run id that exists: COMPLETE with `state` skipped with
      `skip_when_already_exists`, FAIL without it, and FAIL for a Dag that doesn't exist, while the child Dag
      runs only once

Other tasks fail outside the task runner:
    - a task whose trigger fails, and a task whose deferral times out: START when they start and again when they
      resume only to fail, then FAIL, all with one `runId` (the deferral doesn't start a new try)
    - tasks killing the process supervising them, which with the CeleryExecutor is the Celery worker process
      running the task, like a worker running out of memory: Celery reports the task as failed while it is still
      running in the metadata database, so the scheduler fails it as a task whose state changed externally. The
      worker emits START, the scheduler FAIL with one `runId`, and the airflow run facet of the scheduler's FAIL,
      built from the stored task instance, describes the same task instance, map index and task group included,
      while the other map indexes are not affected

It needs a deployment with the CeleryExecutor (the OpenLineage e2e tests).
"""

from __future__ import annotations

import os
import signal
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk import TaskGroup, task

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator, sort_by_execution
from system.openlineage.triggers import FailingTrigger

DAG_ID = "openlineage_task_outcomes_dag"
CHILD_DAG_ID = f"{DAG_ID}_child__notrigger"


def do_nothing() -> None:
    # Not an EmptyOperator, which the scheduler marks as successful without running it, so it emits no events
    pass


def never_ready() -> bool:
    return False


def raise_in_callback(context) -> None:
    raise ValueError("Intentional failure of a callback")


class DeferWithUnserializableKwargsOperator(BaseOperator):
    """Defer with kwargs the task runner can't serialize, so handling the deferral fails."""

    def execute(self, context):
        self.defer(
            trigger=TimeDeltaTrigger(timedelta(seconds=1)),
            method_name="execute_complete",
            kwargs={"value": object()},
        )

    def execute_complete(self, context, event=None, value=None):
        pass


class DeferToFailingTriggerOperator(BaseOperator):
    def execute(self, context):
        self.defer(trigger=FailingTrigger(), method_name="execute_complete")

    def execute_complete(self, context, event=None):
        pass


class DeferPastTimeoutOperator(BaseOperator):
    def execute(self, context):
        self.defer(
            trigger=TimeDeltaTrigger(timedelta(hours=1)),
            method_name="execute_complete",
            timeout=timedelta(seconds=5),
        )

    def execute_complete(self, context, event=None):
        pass


def kill_supervisor() -> None:
    os.kill(os.getppid(), signal.SIGKILL)
    # Not left running without a supervisor
    os._exit(1)


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    sensor_timeout_poke = PythonSensor(
        task_id="sensor_timeout_poke",
        python_callable=never_ready,
        mode="poke",
        poke_interval=1,
        timeout=3,
        retries=1,
    )
    sensor_timeout_reschedule = PythonSensor(
        task_id="sensor_timeout_reschedule",
        python_callable=never_ready,
        mode="reschedule",
        # Long enough that a loaded scheduler handles each poke's end before the next poke is due; with a
        # shorter interval it may take the poke's end for an external change and fail the try for a retry
        poke_interval=10,
        timeout=25,
        retries=1,
    )

    @task(execution_timeout=timedelta(seconds=3))
    def execution_timeout():
        time.sleep(60)

    @task
    def sys_exit_nonzero():
        sys.exit(1)

    @task
    def sys_exit_zero():
        sys.exit(0)

    defer_unserializable = DeferWithUnserializableKwargsOperator(task_id="defer_unserializable")

    @task
    def return_unserializable():
        return object()

    @task(map_index_template="{{ missing.attribute }}")
    def map_index_template_fails(value):
        return value

    template_render_fails = BashOperator(
        task_id="template_render_fails", bash_command="echo {{ missing.attribute }}"
    )

    @task(on_success_callback=raise_in_callback)
    def success_callback_raises():
        pass

    @task(on_failure_callback=raise_in_callback)
    def failure_callback_raises():
        raise ValueError("Intentional failure of a task whose callback fails")

    child_run_id = f"{DAG_ID}_child__{{{{ run_id }}}}"
    trigger_child = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id=CHILD_DAG_ID,
        trigger_run_id=child_run_id,
        wait_for_completion=True,
        # Waits in the triggerer, without holding a worker slot
        deferrable=True,
        poke_interval=2,
    )
    trigger_existing_skip = TriggerDagRunOperator(
        task_id="trigger_existing_skip",
        trigger_dag_id=CHILD_DAG_ID,
        trigger_run_id=child_run_id,
        skip_when_already_exists=True,
    )
    trigger_existing_fail = TriggerDagRunOperator(
        task_id="trigger_existing_fail",
        trigger_dag_id=CHILD_DAG_ID,
        trigger_run_id=child_run_id,
    )
    trigger_missing_dag = TriggerDagRunOperator(
        task_id="trigger_missing_dag",
        trigger_dag_id=f"{DAG_ID}_missing__notrigger",
    )

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        event_sort_fn=sort_by_execution,
        # Fails on rendering its templates, before the listener is told it runs
        unstarted_terminal_keys=[f"{DAG_ID}.template_render_fails.event.fail"],
        # START when the task starts and again when it resumes, to fail or after waiting for the Dag run it triggered
        event_count_assertions={
            rf"{DAG_ID}\.(trigger_fails|deferral_times_out|trigger_child)\.event\.start": ">=2"
        },
        trigger_rule="all_done",
    )

    trigger_fails = DeferToFailingTriggerOperator(task_id="trigger_fails")
    deferral_times_out = DeferPastTimeoutOperator(task_id="deferral_times_out")

    @task
    def worker_dies():
        kill_supervisor()

    @task(max_active_tis_per_dagrun=1)  # Execute sequentially to not overwrite each other OL events
    def mapped_worker_dies(value):
        if value == 1:
            kill_supervisor()

    with TaskGroup(group_id="group") as group:

        @task
        def worker_dies_in_group():
            kill_supervisor()

        worker_dies_in_group()

    trigger_child >> [trigger_existing_skip, trigger_existing_fail]
    [
        sensor_timeout_poke,
        sensor_timeout_reschedule,
        execution_timeout(),
        sys_exit_nonzero(),
        sys_exit_zero(),
        defer_unserializable,
        return_unserializable(),
        map_index_template_fails.expand(value=[1]),
        template_render_fails,
        success_callback_raises(),
        failure_callback_raises(),
        trigger_existing_skip,
        trigger_existing_fail,
        trigger_missing_dag,
        trigger_fails,
        deferral_times_out,
        worker_dies(),
        mapped_worker_dies.expand(value=[0, 1, 2]),
        group,
    ] >> check_events


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=CHILD_DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as child_dag:
    PythonOperator(task_id="do_nothing", python_callable=do_nothing)


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
