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
DAG with a plain task and task loops, some of their tasks retrying and one stopped early by `until`
(requires Airflow 3.4+).

It checks, for a plain task, tasks, a mapped task and a mapped task group in a loop, and loop gates:
    - taskInstance's `try_number`, `map_index` and `loop` of every event: `loop.index` is the zero-based
      iteration, `loop.max_iterations` the loop's limit and `loop.group_id` its group, `map_index` the mapped
      position, and a plain task has no `loop`
    - `loop.region_id`, the loop region of the iteration, shared by the tasks of one iteration, mapped ones
      included, and `loop.trajectory` 1 and `loop.resumes_from_index` 0, as no loop is cleared
    - taskInstance's `expanded_ti_count` of the mapped tasks, the size of their expansion in the iteration
    - taskInstance's `max_tries` of the tasks other than the gates, the `retries` of each task
    - taskInstance's `id` of every event, and `region_id` and `region_index` of the tasks in a loop: the
      `region_index` is the iteration for a task that isn't mapped and the map index for a mapped one, and a
      plain task has neither
    - the gates' `loop.decision`: absent on START; for the loops without `until`, `stop` true only at the last
      iteration and no `reason`; for the loop with `until` (whose gate is named after it), the `reason` it
      returns and `stop` true at the iteration it ends the loop, before `max_iterations`; absent for the plain
      task
    - a mapped task group inside a loop: its task's `map_index` and `region_index` are the mapped position,
      its `loop.index` the iteration, and its `loop.group_id` the loop's, unlike its own `task_group`
    - the loop groups' `max_iterations`, `has_until`, `gate_task_id` and `terminal_task_id`, in the task's
      `task_group` and in the `taskGroups` of the Dag's job facet
    - a retried task emits START and FAIL for each failed try, then START and COMPLETE for the last one
    - every execution (iteration, map index, try) has its own `runId`, shared by its START and COMPLETE/FAIL
      (the `runId` check of OpenLineageTestOperator is enabled by default)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import LoopDecision, task, task_group

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator

DAG_ID = "openlineage_task_loop_dag"


def sort_by_execution(event: dict) -> tuple[int, int, int]:
    # Dag run events have no airflow facet
    task_instance = event["run"]["facets"].get("airflow", {}).get("taskInstance", {})
    return (
        task_instance.get("loop", {}).get("index", -1),
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

        @task_group
        def fetch(source):
            @task(max_active_tis_per_dagrun=1)  # Execute sequentially to not overwrite each other OL events
            def download(source, *, loop):
                return f"{source}-{loop.index}"

            download(source)

        items = choose_items()
        fetch.expand(source=["a", "b"]) >> process.expand(value=items)

    @task_group
    def poll():
        @task
        def check(*, loop):
            return loop.index

        check()

    def ready(*, loop):
        # Stops at the second of three iterations, giving a reason both when continuing and when stopping
        return LoopDecision(stop=loop.result >= 1, reason=f"check returned {loop.result}")

    accumulate_loop = accumulate.loop(max_iterations=3)
    process_batch_loop = process_batch.loop(max_iterations=2)
    poll_loop = poll.loop(max_iterations=3, until=ready)

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        event_sort_fn=sort_by_execution,
    )

    [plain_task(), accumulate_loop, process_batch_loop, poll_loop] >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
