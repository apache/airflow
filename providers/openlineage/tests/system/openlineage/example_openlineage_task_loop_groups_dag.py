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
DAG with task groups and a task loop in every arrangement a task can sit in (requires Airflow 3.4+).

The tasks sit:
    - in a task group and in a task group nested in it, outside a loop
    - in a mapped task group outside a loop
    - directly in a loop that is itself in a task group, so the loop's own id differs from its unique id
    - in a task group inside that loop, in a task group nested in that one, and as a mapped task in it

It checks:
    - task's `task_group`: the task's own group, by `group_id` (its own id) and `unique_group_id`, and never
      with loop fields, even when the group is the loop
    - task's `loop`: the loop around the task by its unique id, with its definition, the same for every task
      in the loop however deep it sits, and absent outside the loop
    - taskInstance's `loop.index` and `loop.trajectory`, and `map_index` and `expanded_ti_count` of the
      mapped tasks, the mapped task group's included
    - the Dag's job facet: each group's `is_mapped`, `is_loop`, `parent_group` and `parse_time_mapped_count`,
      and each task's `task_group`, `loop_group`, `is_mapped` and `parse_time_mapped_count`
    - every task event's identity, as `system.openlineage.expected_events` describes it
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.sdk import TaskGroup, task, task_group

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator, sort_by_execution

DAG_ID = "openlineage_task_loop_groups_dag"


with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:

    @task
    def noop():
        return None

    @task(max_active_tis_per_dagrun=1)  # Execute sequentially to not overwrite each other OL events
    def echo(value):
        return value

    with TaskGroup("plain") as plain:
        grouped = noop.override(task_id="grouped")()
        with TaskGroup("sub"):
            subgrouped = noop.override(task_id="subgrouped")()
        grouped >> subgrouped

    @task_group
    def fan_out(value):
        echo.override(task_id="member")(value)

    with TaskGroup("outer") as outer:

        @task_group
        def body():
            direct = noop.override(task_id="direct")()
            with TaskGroup("inner"):
                nested = noop.override(task_id="nested")()
                with TaskGroup("deeper"):
                    deepest = noop.override(task_id="deepest")()
                direct >> nested >> deepest >> echo.override(task_id="mapped").expand(value=[1, 2])

        body.loop(max_iterations=2)

    check_events = OpenLineageTestOperator(
        task_id="check_events",
        file_path=get_expected_event_file_path(DAG_ID),
        event_sort_fn=sort_by_execution,
    )

    [plain, fan_out.expand(value=["a", "b"]), outer] >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
