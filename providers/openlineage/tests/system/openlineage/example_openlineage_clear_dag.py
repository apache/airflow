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
Dags that run tasks of another Dag again by clearing them through the REST API, like a user does in the UI.

It triggers two child Dags, waits for each to finish and clears it, one step after the other:
    - a Dag run with a failed task and a task that retried twice before it succeeded: only its failed tasks
      cleared, then the whole Dag run
    - a Dag run with a mapped task whose second map index retried, and a task group: one map index cleared, then
      the whole expansion, then the task group

It needs a deployment with an API server (the OpenLineage e2e tests).

It checks, for every execution of every task:
    - taskInstance's `try_number`, `max_tries` (which a clear raises by the retries of the task) and `state`
    - taskInstance's `map_index`, `rendered_map_index`, `expanded_ti_count` and `region_index` of the mapped task
    - `dagRun`'s `clear_number` on task and Dag run events, and the Dag `runId` of each clear as the parent of its
      task events
    - tasks that aren't cleared don't run again
    - every execution (clear, try, map index) has its own `runId`, shared by its START and COMPLETE/FAIL, and every
      clear of the Dag run its own Dag `runId` (the `runId` check of OpenLineageTestOperator)
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import TaskGroup, get_current_context, task

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.manual_state import clear_dag_run, clear_tasks
from system.openlineage.operator import OpenLineageTestOperator, sort_by_execution

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import BaseOperator

DAG_ID = "openlineage_clear_dag"


def get_child_dag_id(scenario: str) -> str:
    return f"{DAG_ID}_{scenario}__notrigger"


@task
def first():
    pass


@task
def second():
    pass


def build_dag_run() -> None:
    @task
    def fails_first_run():
        # Fails until its Dag run is cleared, the clear runs it again as try 2
        if get_current_context()["dag_run"].clear_number == 0:
            raise ValueError("Intentional failure before the clear")

    @task(retries=2, retry_delay=timedelta(seconds=1))
    def retries_twice():
        if get_current_context()["ti"].try_number < 3:
            raise ValueError("Intentional failure of the first two tries")

    first() >> second()
    fails_first_run()
    retries_twice()


def build_tasks() -> None:
    # Executed one at a time to not overwrite each other's OpenLineage events
    @task(
        retries=1,
        retry_delay=timedelta(seconds=1),
        map_index_template="item-{{ item }}",
        max_active_tis_per_dagrun=1,
    )
    def process(value):
        context = get_current_context()
        context["item"] = value  # type: ignore[typeddict-unknown-key]
        if value == 1 and context["ti"].try_number == 1:
            raise ValueError("Intentional failure of the first try of one map index")

    process.expand(value=[0, 1, 2])
    with TaskGroup(group_id="group"):
        first() >> second()


# Scenario -> (child Dag tasks, clears in order with their kwargs besides dag_id and run_id)
SCENARIOS: dict[str, tuple[Callable[[], None], list[tuple[Callable, dict]]]] = {
    "dag_run": (build_dag_run, [(clear_dag_run, {"only_failed": True}), (clear_dag_run, {})]),
    "tasks": (
        build_tasks,
        [
            (clear_tasks, {"task_ids": [("process", 2)]}),
            (clear_tasks, {"task_ids": ["process"]}),
            (clear_tasks, {"task_group_id": "group"}),
        ],
    ),
}

for scenario, (build_child_tasks, _) in SCENARIOS.items():
    with DAG(
        dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
        dag_id=get_child_dag_id(scenario),
        start_date=datetime(2021, 1, 1),
        schedule=None,
        catchup=False,
        default_args={"retries": 0},
    ) as child:
        build_child_tasks()
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
    )
    for scenario, (_build, clears) in SCENARIOS.items():
        child_run_id = f"{scenario}__{{{{ run_id }}}}"
        upstream: BaseOperator = TriggerDagRunOperator(
            task_id=f"trigger_{scenario}",
            trigger_dag_id=get_child_dag_id(scenario),
            trigger_run_id=child_run_id,
        )
        for i, (clear, clear_kwargs) in enumerate(clears, start=1):
            clear_child = PythonOperator(
                task_id=f"clear_{scenario}_{i}",
                python_callable=clear,
                op_kwargs={"dag_id": get_child_dag_id(scenario), "run_id": child_run_id, **clear_kwargs},
            )
            upstream >> clear_child
            upstream = clear_child
        upstream >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
