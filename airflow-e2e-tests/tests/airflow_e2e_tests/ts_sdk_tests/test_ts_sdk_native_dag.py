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
End-to-end test of a Dag declared entirely in TypeScript.

Run with::

    E2E_TEST_MODE=ts_sdk RUN_TS_SDK_NATIVE_DAG_TESTS=true \\
        uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/ts_sdk_tests/test_ts_sdk_native_dag.py -xvs

Unlike ``test_ts_sdk_dag.py``, no Python file declares this Dag: the Dag processor asks the
``airflow-ts-pack`` bundle to parse itself, and the bundle answers with the serialized Dag that
``ts-sdk/example/src/native.ts`` built. The graph is a graph rather than a chain -- a task group, a
named fan-in, order-only edges, a conditional, a multi-way branch, and a ``TriggerDagRunOperator`` a
Python worker runs.

Gated behind ``RUN_TS_SDK_NATIVE_DAG_TESTS`` because it needs a Dag processor that can dispatch a
parse request to a language coordinator. Until that lands, the bundle answers a request nothing
sends, and the Dag never appears.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

_RUN_NATIVE = os.environ.get("RUN_TS_SDK_NATIVE_DAG_TESTS", "").lower() in ("true", "1")

# Parsing the bundle launches node before the first task is even scheduled, so
# allow the same headroom the mixed-language suite does.
_TS_TASK_TIMEOUT = 600

_DAG_ID = "typescript_native_example"

# Read by the handlers; see ts-sdk/example/src/native.ts.
_NORTH_ROWS_VARIABLE = "typescript_native_north_rows"
_SOUTH_ROWS_VARIABLE = "typescript_native_south_rows"
_CADENCE_VARIABLE = "typescript_native_cadence"

pytestmark = pytest.mark.skipif(
    not _RUN_NATIVE,
    reason="Needs a Dag processor that dispatches parse requests to a language coordinator "
    "(RUN_TS_SDK_NATIVE_DAG_TESTS)",
)


@dataclass
class _CompletedRun:
    client: AirflowClient
    run_id: str
    state: str
    ti_states: dict[str, str]

    def xcom(self, task_id: str, key: str = "return_value"):
        return self.client.get_xcom_value(dag_id=_DAG_ID, task_id=task_id, run_id=self.run_id, key=key).get(
            "value"
        )


@pytest.fixture(scope="module")
def parsed_dag() -> AirflowClient:
    """A client that has waited for the bundle's Dag to be parsed and registered."""
    client = AirflowClient()
    # The Dag processor spawns node to parse the bundle, so the Dag appears some
    # time after the deployment is up; every read below would 404 until then.
    client.wait_for_dag(_DAG_ID, timeout=_TS_TASK_TIMEOUT)
    return client


@pytest.fixture(scope="module")
def completed_run(parsed_dag: AirflowClient) -> _CompletedRun:
    """Trigger the native Dag once, with the inputs every test below reads back."""
    client = parsed_dag
    # Both regions non-empty, so the conditional takes its `then` branch, and a
    # weekly cadence so the branch's choice is known rather than guessed.
    for key, value in (
        (_NORTH_ROWS_VARIABLE, "3"),
        (_SOUTH_ROWS_VARIABLE, "2"),
        (_CADENCE_VARIABLE, "weekly"),
    ):
        client.set_variable(key, value)

    client.un_pause_dag(_DAG_ID)
    resp = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(timezone.utc).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_TS_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    return _CompletedRun(
        client=client,
        run_id=run_id,
        state=state,
        ti_states={ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])},
    )


def test_the_dag_the_bundle_parsed_is_registered(parsed_dag: AirflowClient):
    """The Dag exists without any Python file declaring it."""
    dag = parsed_dag.get_dag(_DAG_ID)

    assert dag["dag_id"] == _DAG_ID
    # The serializer expands a cron preset, so "@daily" is recorded as its expression.
    assert dag.get("timetable_summary") == "0 0 * * *"
    # `tags` is a list of objects, each naming one tag.
    assert {tag["name"] for tag in dag.get("tags") or []} >= {"typescript", "native"}


def test_the_graph_carries_every_construct(parsed_dag: AirflowClient):
    """Group prefixes, the fan-in, the branches and the trigger all survive parsing."""
    tasks = parsed_dag.get_tasks(_DAG_ID).get("tasks", [])
    downstream = {task["task_id"]: set(task["downstream_task_ids"]) for task in tasks}

    # The task group prefixed its members.
    assert {"extract.north", "extract.south"} <= set(downstream)
    # The named fan-in put both extract tasks upstream of summarize. The API
    # reports downstream edges only, so upstream is read by inverting them.
    assert {"extract.north", "extract.south"} <= {
        task_id for task_id, down in downstream.items() if "summarize" in down
    }
    # The conditional and the branch reach their candidates.
    assert downstream["has_rows"] >= {"load_rows", "report_empty"}
    assert downstream["pick_cadence"] >= {"publish_daily", "publish_weekly"}
    # The group edge, expanded onto the tasks the group leaves from.
    assert {"extract.north", "extract.south"} <= {
        task_id for task_id, down in downstream.items() if "pick_cadence" in down
    }
    # Cleanup sits behind every branch outcome, which is what its trigger rule is for.
    assert {"load_rows", "report_empty", "publish_daily", "publish_weekly"} <= {
        task_id for task_id, down in downstream.items() if "cleanup" in down
    }
    # And the one task Python runs rather than the Node coordinator.
    by_id = {task["task_id"]: task for task in tasks}
    assert by_id["trigger_downstream"]["operator_name"] == "TriggerDagRunOperator"


def test_dag_run_succeeded(completed_run: _CompletedRun):
    assert completed_run.state == "success", (
        f"expected the run to succeed; got {completed_run.state!r}. task states: {completed_run.ti_states}"
    )


def test_the_taken_branches_ran_and_the_others_skipped(completed_run: _CompletedRun):
    """A branch is a run-time skip, so the states are what prove it worked."""
    states = completed_run.ti_states

    # Both regions had rows, so the conditional followed `then`.
    assert states.get("load_rows") == "success"
    assert states.get("report_empty") == "skipped"

    # The cadence Variable said weekly, so that is the case the branch chose.
    assert states.get("publish_weekly") == "success"
    assert states.get("publish_daily") == "skipped"


def test_every_other_task_succeeded(completed_run: _CompletedRun):
    always_run = [
        "extract.north",
        "extract.south",
        "summarize",
        "has_rows",
        "pick_cadence",
        "cleanup",
        "trigger_downstream",
    ]
    for task_id in always_run:
        assert completed_run.ti_states.get(task_id) == "success", (
            f"{task_id!r} did not succeed. all task states: {completed_run.ti_states}"
        )


def test_xcoms_flow_between_typescript_tasks(completed_run: _CompletedRun):
    """The fan-in's arguments reached the handler, and its own push came back."""
    assert completed_run.xcom("extract.north") == {"region": "north", "rows": 3}
    assert completed_run.xcom("extract.south") == {"region": "south", "rows": 2}
    assert completed_run.xcom("summarize") == {"total": 5, "regions": 2}
    # Pushed under its own key by the summarize handler, and read back by load_rows.
    assert completed_run.xcom("summarize", key="region_total") == 5
    assert completed_run.xcom("load_rows") == {"loaded": 5}


def test_the_decisions_are_recorded(completed_run: _CompletedRun):
    """A condition returns its boolean, and a branch the task id it chose."""
    assert completed_run.xcom("has_rows") is True
    assert completed_run.xcom("pick_cadence") == "publish_weekly"


def test_a_cleared_skipped_branch_stays_skipped(completed_run: _CompletedRun):
    """What `_can_skip_downstream` plus the skipmixin XCom are for."""
    assert completed_run.xcom("has_rows", key="skipmixin_key") == {"skipped": ["report_empty"]}
    assert completed_run.xcom("pick_cadence", key="skipmixin_key") == {"skipped": ["publish_daily"]}
