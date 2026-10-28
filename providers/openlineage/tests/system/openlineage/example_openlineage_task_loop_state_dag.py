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
Dags with task loops whose state this Dag changes through the REST API, like a user does in the UI, or that end in
ways the loop Dag doesn't cover (requires Airflow 3.4+).

For each scenario it triggers a child Dag with a loop and changes its state, or clears it once it finished:
    - a running task of the first iteration, and the Dag run around it, marked as success and as failed
    - a task of the middle iteration cleared alone, then the loop run again from its first iteration, twice
    - a loop stopped early by `until`, run again from its first iteration
    - a task failing for good in the second iteration, next to a mapped task retrying in the first, and a
      deferrable sensor and a sensor in reschedule mode in another loop of the same Dag

It needs a deployment with an API server (the OpenLineage e2e tests).

It checks, for every execution of every task:
    - taskInstance's `try_number`, `max_tries`, `state`, `map_index`, `expanded_ti_count` and `region_index`
    - taskInstance's `loop`: `index`, `trajectory`, `forked_from_region_id` and `resumes_from_index` of every run
      of the loop, and the gate's `decision` recomputed when the loop runs again
    - `dagRun`'s `clear_number` of every clear
    - the API server's COMPLETE or FAIL of a marked task has the `runId` of the worker's START
    - a task cleared alone runs again in its own iteration and region, and nothing after it runs again
    - a loop ending on a failed task: the gate of that iteration and the iterations after it emit nothing
    - a deferral or a reschedule inside the loop: START on every resume of the deferred task, one START for the
      sensor in reschedule mode, and one COMPLETE each per iteration
    - every execution (clear, trajectory, iteration, map index, try) has its own `runId`, shared by its START and
      COMPLETE/FAIL, and the loop fields of each event agree with its `runId` and region (the `runId` check of
      OpenLineageTestOperator)
"""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.triggers.temporal import TimeDeltaTrigger
from airflow.sdk import LoopDecision, task, task_group

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.manual_state import change_state, clear_loop_task, rerun_loop
from system.openlineage.operator import OpenLineageTestOperator, sort_by_execution

DAG_ID = "openlineage_task_loop_state_dag"


def get_child_dag_id(scenario: str) -> str:
    return f"{DAG_ID}_{scenario}__notrigger"


def build_sleeping_loop() -> None:
    @task_group
    def body():
        @task
        def work(*, loop):
            if loop.index == 1:
                # Longer than [core] killed_task_cleanup_time (set in the e2e deployment), so the worker is
                # killed without getting to emit an event of its own after the state change.
                time.sleep(300)

        work()

    @task
    def after():
        pass

    body.loop(max_iterations=2) >> after()


def build_work() -> None:
    @task_group
    def body():
        @task
        def work(*, loop):
            return loop.index

        work()

    body.loop(max_iterations=3)


def build_until() -> None:
    @task_group
    def poll():
        @task
        def check(*, loop):
            return loop.index

        check()

    def ready(*, loop):
        return LoopDecision(stop=loop.result >= 2, reason=f"check returned {loop.result}")

    poll.loop(max_iterations=3, until=ready)


class DeferOnceOperator(BaseOperator):
    """Defer once on every run, so every iteration resumes once."""

    def execute(self, context):
        self.defer(trigger=TimeDeltaTrigger(timedelta(seconds=1)), method_name="execute_complete")

    def execute_complete(self, context, event=None):
        pass


def ready_on_second_poke(task_reschedule_count: int = 0, **_) -> bool:
    return task_reschedule_count >= 1


def build_endings() -> None:
    @task_group
    def failing():
        @task(retries=1, retry_delay=timedelta(seconds=1), max_active_tis_per_dagrun=1)
        def mapped(value, *, loop, ti):
            if loop.index == 1 and value == 1 and ti.try_number == 1:
                raise ValueError("Intentional failure of the first try of one map index")

        @task
        def fail_second(*, loop):
            if loop.index == 2:
                raise ValueError("Intentional failure of the second iteration")

        mapped.expand(value=[1, 2]) >> fail_second()

    @task_group
    def waiting():
        deferred = DeferOnceOperator(task_id="deferred")
        rescheduled = PythonSensor(
            task_id="rescheduled", python_callable=ready_on_second_poke, mode="reschedule", poke_interval=2
        )
        deferred >> rescheduled

    failing.loop(max_iterations=3)
    waiting.loop(max_iterations=2)


# Scenario -> (child Dag tasks, state changes and clears in order, with their kwargs besides dag_id and run_id)
SCENARIOS: dict[str, tuple[Callable[[], None], list[tuple[Callable, dict]]]] = {
    **{
        f"{level}_{state}": (
            build_sleeping_loop,
            [(change_state, {"level": level, "state": state, "task_id": "body.work"})],
        )
        for level in ("task", "dag_run")
        for state in ("success", "failed")
    },
    "clears": (
        build_work,
        [
            (clear_loop_task, {"task_id": "body.work", "iteration": 2}),
            (rerun_loop, {"gate_task_id": "body.__loop_gate", "iteration": 1}),
            (rerun_loop, {"gate_task_id": "body.__loop_gate", "iteration": 1}),
        ],
    ),
    # The gate of a loop with `until` is named after it
    "until": (build_until, [(rerun_loop, {"gate_task_id": "poll.ready", "iteration": 1})]),
    "endings": (build_endings, []),
}

for scenario, (build_tasks, _) in SCENARIOS.items():
    with DAG(
        dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
        dag_id=get_child_dag_id(scenario),
        start_date=datetime(2021, 1, 1),
        schedule=None,
        catchup=False,
        default_args={"retries": 0},
    ) as child:
        build_tasks()
    globals()[child.dag_id] = child  # The Dag processor finds Dags among the module's globals

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
        event_count_assertions={
            # The deferred task emits START when it starts and again when it resumes, in each of the two iterations
            rf"{get_child_dag_id('endings')}\.waiting\.deferred\.event\.start": ">=4",
            # START again when it resumes after waiting for the Dag run it triggered
            rf"{DAG_ID}\.trigger_endings\.event\.start": ">=2",
        },
        # Runs also when a child Dag run that fails on purpose fails the task waiting for it
        trigger_rule="all_done",
    )
    for scenario, (_build, changes) in SCENARIOS.items():
        child_run_id = f"{scenario}__{{{{ run_id }}}}"
        upstream: BaseOperator = TriggerDagRunOperator(
            task_id=f"trigger_{scenario}",
            trigger_dag_id=get_child_dag_id(scenario),
            trigger_run_id=child_run_id,
            # The scenarios without a change check the events of the run, so it must have finished
            wait_for_completion=not changes,
            # Waits in the triggerer, without holding a worker slot
            deferrable=True,
            poke_interval=2,
        )
        for i, (change, change_kwargs) in enumerate(changes, start=1):
            change_child = PythonOperator(
                task_id=f"change_{scenario}_{i}",
                python_callable=change,
                op_kwargs={"dag_id": get_child_dag_id(scenario), "run_id": child_run_id, **change_kwargs},
            )
            upstream >> change_child
            upstream = change_child
        upstream >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
