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
DAG with a plain task and task loops, some of their tasks retrying (requires Airflow 3.4+).

It checks, for a plain task, tasks in a loop, a mapped task in a loop and the loop gates:
    - taskInstance's `try_number`, `map_index`, `loop_index` and `loop_max_iterations` of every event: the
      iteration is the zero-based `loop_index`, `map_index` the mapped position, and a plain task has no
      `loop_index`
    - taskInstance's `loop_group_id` of every event: the loop group of a task in a loop, absent for a plain task
    - taskInstance's `max_tries` of the tasks other than the gates, the `retries` of each task
    - taskInstance's `id` of every event, and `region_id` and `region_index` of the tasks in a loop: the
      `region_index` is the iteration for a task that isn't mapped and the map index for a mapped one, and a
      plain task has neither
    - the loop groups' `max_iterations`, `has_until`, `gate_task_id` and `terminal_task_id`, in the task's
      `task_group` and in the `taskGroups` of the Dag's job facet
    - a retried task emits START and FAIL for each failed try, then START and COMPLETE for the last one
    - every execution (iteration, map index, try) has its own `runId`, shared by its START and COMPLETE/FAIL
      (the `runId` check of OpenLineageTestOperator is enabled by default)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import task, task_group

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_task_loop_dag"


def sort_by_execution(event: dict) -> tuple[int, int, int]:
    # Dag run events have no airflow facet
    task_instance = event["run"]["facets"].get("airflow", {}).get("taskInstance", {})
    return (
        task_instance.get("loop_index", -1),
        task_instance.get("map_index") if task_instance.get("map_index") is not None else -1,
        task_instance.get("try_number", -1),
    )


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:

    @task
    def plain_task():
        return None

    @task_group
    def accumulate():
        @task(retries=2, retry_delay=timedelta(seconds=1))
        def increment(*, loop, ti):
            # The second iteration fails on its first try
            if loop.index == 1 and ti.try_number < 2:
                raise ValueError(f"Intentional failure, iteration {loop.index}, try {ti.try_number}")
            return (0 if loop.index == 0 else loop.previous) + 1

        @task
        def report(value):
            return value

        report(increment())

    @task_group
    def process_batch():
        @task
        def choose_items():
            return [1, 2]

        @task(
            max_active_tis_per_dagrun=1,  # Execute sequentially to not overwrite each other OL events
            retries=3,
            retry_delay=timedelta(seconds=1),
        )
        def process(value, *, loop, ti):
            # The mapped instance of the second item fails on its first two tries, in every iteration
            if value == 2 and ti.try_number < 3:
                raise ValueError(f"Intentional failure, iteration {loop.index}, try {ti.try_number}")
            return value + loop.index

        process.expand(value=choose_items())

    accumulate_loop = accumulate.loop(max_iterations=3)
    process_batch_loop = process_batch.loop(max_iterations=2)

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        event_sort_fn=sort_by_execution,
    )

    [plain_task(), accumulate_loop, process_batch_loop] >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
