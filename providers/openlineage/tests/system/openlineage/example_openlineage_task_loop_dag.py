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
    - taskInstance's `try_number`, `state`, `map_index` and `loop` of every event: `loop.index` is the
      iteration, counted from 1, and `map_index` the mapped position, and a plain task has no `loop`
    - task's `loop`, the loop around the task: its `group_id`, `max_iterations`, `has_until`, `gate_task_id`
      and `terminal_task_id`, also for a task of a task group inside a loop, and absent for the plain task
    - `loop.region_id` is set, without comparing it between events, `loop.trajectory` is 1, and there is no
      `loop.forked_from_region_id` nor `loop.resumes_from_index`, as no loop is cleared
    - taskInstance's `expanded_ti_count` of the mapped tasks, the size of their expansion in the iteration
    - taskInstance's `max_tries` of the tasks other than the gates, the `retries` of each task
    - taskInstance's `id` is set on every event, and `region_id` on the tasks in a loop, without comparing them
      between events, and `region_index` is the iteration for a task that isn't mapped and the map index for a
      mapped one, while a plain task has neither
    - the gates' `loop.decision`: absent on START; for the loops without `until`, `stop` true only at the last
      iteration and no `reason`; for the loop with `until` (whose gate is named after it), the `reason` it
      returns and `stop` true at the iteration it ends the loop, before `max_iterations`; absent for the plain
      task
    - a mapped task group inside a loop: its task's `map_index` and `region_index` are the mapped position,
      its `loop.index` the iteration, and its task's `loop.group_id` the loop's, unlike its own `task_group`,
      whose `unique_group_id` is the mapped group's, while for a task directly in a loop it is the loop's
    - the loop groups' `max_iterations`, `has_until`, `gate_task_id` and `terminal_task_id` in the
      `taskGroups` of the Dag's job facet, and never in the task's `task_group`: in task events, only task's
      `loop` describes the loop
    - the job facet's `is_mapped`, `is_loop` and `parse_time_mapped_count` of each group, the `retries` of each
      task but the gates, and the `task_group`, `loop_group`, `is_mapped` and `parse_time_mapped_count` (2 for
      the mapped task group expanded over a literal list, unknown for the task expanded over an upstream result)
      of `plain_task`, `accumulate.increment`, `process_batch.process` and `process_batch.fetch.download`
    - taskInstance's `dag_version_id` of every event
    - a retried task emits START and FAIL for each failed try, then START and COMPLETE for the last one
    - every execution (iteration, map index, try) has its own `runId`, shared by its START and COMPLETE/FAIL
      (the `runId` check of OpenLineageTestOperator is enabled by default)
    - every task event's identity, as `system.openlineage.expected_events` describes it, and each task in the Dag's job facet
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import LoopDecision, task, task_group

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator, sort_by_execution

DAG_ID = "openlineage_task_loop_dag"


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
            if loop.index == 2 and ti.try_number < 2:
                raise ValueError(f"Intentional failure, iteration {loop.index}, try {ti.try_number}")
            return (0 if loop.index == 1 else loop.previous) + 1

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
        return LoopDecision(stop=loop.result >= 2, reason=f"check returned {loop.result}")

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
