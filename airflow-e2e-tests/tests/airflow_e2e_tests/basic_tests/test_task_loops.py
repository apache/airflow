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
"""Run task loops in a deployment and check every iteration through the REST API."""

from __future__ import annotations

import time
from collections import Counter
from datetime import UTC, datetime
from typing import NamedTuple

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient


class ExampleLoop(NamedTuple):
    iterations: int
    # This field maps the ID of each task in the loop to that task's map indexes.
    # In the example Dags listed in EXAMPLE_LOOPS, each task's map indexes are the same in every iteration.
    # [-1] stands for a task that is not mapped.
    loop_tasks: dict[str, list[int]]
    outside_tasks: list[str]


# The keys are the IDs of the example Dags defined in example_task_loops.py.
EXAMPLE_LOOPS = {
    "refine_estimate": ExampleLoop(
        iterations=4,
        loop_tasks={"refine.improve": [-1], "refine.evaluate": [-1], "refine.accurate_enough": [-1]},
        outside_tasks=["finished"],
    ),
    "fixed_task_loop": ExampleLoop(
        iterations=3,
        loop_tasks={"accumulate.increment": [-1], "accumulate.__loop_gate": [-1]},
        outside_tasks=[],
    ),
    "mapped_task_loop": ExampleLoop(
        iterations=2,
        loop_tasks={
            "process_batch.choose_items": [-1],
            "process_batch.process": [0, 1],
            "process_batch.batch_ready": [-1],
        },
        outside_tasks=[],
    ),
    "partial_override_loop": ExampleLoop(
        iterations=3,
        loop_tasks={"accumulate_more.increment": [-1], "accumulate_more.__loop_gate": [-1]},
        outside_tasks=[],
    ),
}

LOOP_DAG_ID = "example_task_loop_test"
ADD_ONE = "accumulate.add_one"
GATE = "accumulate.__loop_gate"
MAPPED_LOOP_DAG_ID = "example_task_loop_mapped_test"
NOT_CONVERGING_DAG_ID = "example_task_loop_not_converging_test"
UNTIL_MET_EARLY_DAG_ID = "example_task_loop_until_met_early_test"
UNTIL_MET_AT_LAST_DAG_ID = "example_task_loop_until_met_at_last_test"

# Each key is the name that a test uses to find its Dag run. Each value is the ID of the Dag to trigger
# for that run.
DAG_RUNS = {
    **{dag_id: dag_id for dag_id in EXAMPLE_LOOPS},
    "read_from_outside": LOOP_DAG_ID,
    "replace_later_iterations": LOOP_DAG_ID,
    "keep_later_iterations": LOOP_DAG_ID,
    "mapped": MAPPED_LOOP_DAG_ID,
    "not_converging": NOT_CONVERGING_DAG_ID,
    "until_met_early": UNTIL_MET_EARLY_DAG_ID,
    "until_met_at_last": UNTIL_MET_AT_LAST_DAG_ID,
}

# Seconds that a Dag run gets to finish. The runs that the dag_runs fixture triggers share one deadline. If
# the loops in all of those runs hang, the tests fail after one timeout in total, not one timeout per test.
RUN_TIMEOUT = 300

# The report task returns this list after the first run of LOOP_DAG_ID. Each pair holds the value that
# add_one returned in one iteration and the trajectory that add_one ran in.
FIRST_REPORT = [[1, 1], [2, 1], [3, 1]]

# This dict gives the state and try number of each task instance of LOOP_DAG_ID after the gate of
# iteration 2 and the report task are cleared once. A clear that keeps the later iterations and a clear that
# replaces them both lead to these states and try numbers. This holds because the replacements of
# iteration 3 are new task instances, whose try numbers start from 1 again.
STATES_AFTER_RERUN = {
    (ADD_ONE, 1, -1): ("success", 1),
    (GATE, 1, -1): ("success", 1),
    (ADD_ONE, 2, -1): ("success", 1),
    (GATE, 2, -1): ("success", 2),
    (ADD_ONE, 3, -1): ("success", 1),
    (GATE, 3, -1): ("success", 1),
    ("report", None, -1): ("success", 2),
}

airflow_client = AirflowClient()


class DagRuns(NamedTuple):
    run_ids: dict[str, str]
    deadline: float


@pytest.fixture(scope="module")
def dag_runs() -> DagRuns:
    """
    Trigger one Dag run for each entry of ``DAG_RUNS`` and return the run IDs and a deadline.

    The run IDs are keyed like ``DAG_RUNS``, and all the runs share the deadline. The fixture triggers all
    the runs before any test waits for a run, so their loops run at the same time.
    """
    run_ids = {
        name: airflow_client.trigger_dag(dag_id, json={"logical_date": datetime.now(UTC).isoformat()})[
            "dag_run_id"
        ]
        for name, dag_id in DAG_RUNS.items()
    }
    return DagRuns(run_ids, time.monotonic() + RUN_TIMEOUT)


def get_task_instances_by_key(dag_id: str, run_id: str) -> dict[tuple[str, int | None, int], dict]:
    """
    Return the current task instances of a Dag run, keyed by task ID, loop iteration and map index.

    The REST API lists only the latest try of each task instance and leaves out the iterations that a
    rerun replaced. The loop iteration is None for a task outside the loop. Fail the test if two task
    instances have the same key.
    """
    task_instances = airflow_client.get_task_instances(dag_id, run_id)["task_instances"]
    keys = [
        (ti["task_id"], ti["loop_iteration"]["iteration"] if ti["in_loop"] else None, ti["map_index"])
        for ti in task_instances
    ]
    duplicates = [key for key, count in Counter(keys).items() if count > 1]
    assert not duplicates, f"More than one current task instance for {duplicates}"
    return dict(zip(keys, task_instances))


def get_states_and_tries(task_instances: dict) -> dict:
    return {key: (ti["state"], ti["try_number"]) for key, ti in task_instances.items()}


def wait_for_run(dag_id: str, run_id: str, state: str, deadline: float) -> dict:
    """
    Wait for a Dag run to finish in ``state`` and return what ``get_task_instances_by_key`` returns for it.

    If the Dag run finishes in another state, or has not finished by ``deadline``, fail the test. The failure
    message shows the state and try number of each task instance, so it points at the task that went wrong.
    """
    try:
        final_state = airflow_client.wait_for_dag_run(
            dag_id, run_id, timeout=max(deadline - time.monotonic(), 1)
        )
    except TimeoutError:
        final_state = "unfinished"
    task_instances = get_task_instances_by_key(dag_id, run_id)
    assert final_state == state, (
        f"Dag run {run_id} of {dag_id} is {final_state}, not {state}: {get_states_and_tries(task_instances)}"
    )
    return task_instances


def get_report(dag_id: str, run_id: str):
    xcom = airflow_client.get_xcom_value(dag_id=dag_id, task_id="report", run_id=run_id, key="return_value")
    return xcom["value"]


@pytest.mark.parametrize("dag_id", EXAMPLE_LOOPS)
def test_example_loop_runs_every_iteration(dag_id, dag_runs):
    example = EXAMPLE_LOOPS[dag_id]

    task_instances = wait_for_run(dag_id, dag_runs.run_ids[dag_id], "success", dag_runs.deadline)

    expected = {
        (task_id, iteration, map_index): "success"
        for task_id, map_indexes in example.loop_tasks.items()
        for iteration in range(1, example.iterations + 1)
        for map_index in map_indexes
    }
    expected.update({(task_id, None, -1): "success" for task_id in example.outside_tasks})
    assert {key: ti["state"] for key, ti in task_instances.items()} == expected


def test_task_outside_loop_reads_every_iteration(dag_runs):
    run_id = dag_runs.run_ids["read_from_outside"]

    wait_for_run(LOOP_DAG_ID, run_id, "success", dag_runs.deadline)

    assert get_report(LOOP_DAG_ID, run_id) == FIRST_REPORT


def test_mapped_task_in_loop_expands_in_every_iteration(dag_runs):
    run_id = dag_runs.run_ids["mapped"]

    task_instances = wait_for_run(MAPPED_LOOP_DAG_ID, run_id, "success", dag_runs.deadline)

    # choose_items returns one more item in each iteration, so the mapped `square` task expands to 1, 2 and
    # then 3 task instances.
    expected = {
        ("grow.square", iteration, map_index): "success"
        for iteration in (1, 2, 3)
        for map_index in range(iteration)
    }
    expected.update(
        {
            (task_id, iteration, -1): "success"
            for task_id in ("grow.choose_items", "grow.__loop_gate")
            for iteration in (1, 2, 3)
        }
    )
    expected["report", None, -1] = "success"
    assert {key: ti["state"] for key, ti in task_instances.items()} == expected
    # `report` gets the squares ordered by iteration first and by map index within an iteration.
    assert get_report(MAPPED_LOOP_DAG_ID, run_id) == [0, 0, 1, 0, 1, 4]


@pytest.mark.parametrize(
    ("run_name", "include_later_loop_iterations", "new_task_instances", "new_regions", "report"),
    [
        pytest.param(
            "replace_later_iterations",
            True,
            {(GATE, 2, -1), (ADD_ONE, 3, -1), (GATE, 3, -1), ("report", None, -1)},
            # The replacement of iteration 3 runs in a new trajectory of the loop, and each trajectory has
            # its own region. The gate of iteration 2 runs again in the region of the first trajectory.
            {(ADD_ONE, 3, -1), (GATE, 3, -1)},
            [[1, 1], [2, 1], [3, 2]],
            id="replace",
        ),
        pytest.param(
            "keep_later_iterations",
            False,
            {(GATE, 2, -1), ("report", None, -1)},
            set(),
            FIRST_REPORT,
            id="keep",
        ),
    ],
)
def test_rerun_from_gate(
    run_name, include_later_loop_iterations, new_task_instances, new_regions, report, dag_runs
):
    run_id = dag_runs.run_ids[run_name]
    before = wait_for_run(LOOP_DAG_ID, run_id, "success", dag_runs.deadline)
    assert get_states_and_tries(before) == {key: ("success", 1) for key in STATES_AFTER_RERUN}
    assert get_report(LOOP_DAG_ID, run_id) == FIRST_REPORT

    airflow_client.clear_task_instances(
        LOOP_DAG_ID,
        run_id,
        task_instance_ids=[before[GATE, 2, -1]["id"]],
        include_downstream=True,
        include_later_loop_iterations=include_later_loop_iterations,
    )

    after = wait_for_run(LOOP_DAG_ID, run_id, "success", time.monotonic() + RUN_TIMEOUT)
    assert get_states_and_tries(after) == STATES_AFTER_RERUN
    assert {key for key in after if after[key]["id"] != before[key]["id"]} == new_task_instances
    assert {
        key for key in after if after[key].get("region_id") != before[key].get("region_id")
    } == new_regions
    assert get_report(LOOP_DAG_ID, run_id) == report


def test_loop_that_never_meets_until_fails_the_run(dag_runs):
    run_id = dag_runs.run_ids["not_converging"]

    task_instances = wait_for_run(NOT_CONVERGING_DAG_ID, run_id, "failed", dag_runs.deadline)

    assert get_states_and_tries(task_instances) == {
        ("poll.check", 1, -1): ("success", 1),
        ("poll.reached_ten", 1, -1): ("success", 1),
        ("poll.check", 2, -1): ("success", 1),
        # example_task_loop_not_converging_test sets retries=1 in default_args. The gate of the last
        # iteration still fails without retrying, because `until` would not return True on a retry either.
        ("poll.reached_ten", 2, -1): ("failed", 1),
        ("after", None, -1): ("upstream_failed", 0),
    }


@pytest.mark.parametrize(
    ("run_name", "dag_id", "iterations"),
    [
        pytest.param("until_met_early", UNTIL_MET_EARLY_DAG_ID, 2, id="early"),
        pytest.param("until_met_at_last", UNTIL_MET_AT_LAST_DAG_ID, 3, id="at-last"),
    ],
)
def test_loop_stops_when_until_is_met(run_name, dag_id, iterations, dag_runs):
    task_instances = wait_for_run(dag_id, dag_runs.run_ids[run_name], "success", dag_runs.deadline)

    expected = {
        (task_id, iteration, -1): ("success", 1)
        for task_id in ("poll.check", "poll.reached_target")
        for iteration in range(1, iterations + 1)
    }
    expected["after", None, -1] = ("success", 1)
    assert get_states_and_tries(task_instances) == expected
