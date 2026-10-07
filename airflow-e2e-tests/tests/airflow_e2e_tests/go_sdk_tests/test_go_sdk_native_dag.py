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
"""E2E tests for the Go SDK native Dags of ``go-sdk/example/native``.

Run with::

    E2E_TEST_MODE=go_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/go_sdk_tests/test_go_sdk_native_dag.py -xvs

No Python file declares these Dags. ``conftest._setup_go_sdk_integration`` packs the example with
``airflow-go-pack`` into the Dags folder, where the Dag processor claims it through the
``ExecutableCoordinator`` and runs it to parse. The worker runs each task from the same binary, on
the ``golang`` queue the Dag sets.

``go_native_pipeline`` has a nested task group, a fan-in, order-only edges, an ``If``, a ``Switch``
and a trigger-rule join. ``go_native_report`` is declared in a second file, so its Code view shows
that file. ``go_native_trigger`` triggers ``go_native_report``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# The Dag processor execs the binary to parse it, so the Dags appear some time after the stack is up.
_PARSE_TIMEOUT = 600
_GO_TASK_TIMEOUT = 600

_PIPELINE_DAG_ID = "go_native_pipeline"
_REPORT_DAG_ID = "go_native_report"
_TRIGGER_DAG_ID = "go_native_trigger"

# Read by the example's tasks; see go-sdk/example/native/main.go.
_GREETING_VARIABLE = "go_native_greeting"
_CADENCE_VARIABLE = "go_native_cadence"
_GREETING = "hello from the e2e test"
# The test_http Connection that conftest._setup_go_sdk_integration defines.
_CONNECTION_HOST = "example.com"


@dataclass
class _CompletedRun:
    client: AirflowClient
    run_id: str
    state: str
    ti_states: dict[str, str]

    def xcom(self, task_id: str, key: str = "return_value"):
        return self.client.get_xcom_value(
            dag_id=_PIPELINE_DAG_ID, task_id=task_id, run_id=self.run_id, key=key
        ).get("value")


@pytest.fixture(scope="module")
def parsed_dags() -> AirflowClient:
    """A client that has waited for the Dag processor to parse and register all three Dags."""
    client = AirflowClient()
    for dag_id in (_PIPELINE_DAG_ID, _REPORT_DAG_ID, _TRIGGER_DAG_ID):
        client.wait_for_dag(dag_id, timeout=_PARSE_TIMEOUT)
    return client


@pytest.fixture(scope="module")
def completed_run(parsed_dags: AirflowClient) -> _CompletedRun:
    """Trigger the pipeline once, with the Variables its tasks read."""
    client = parsed_dags
    client.set_variable(_GREETING_VARIABLE, _GREETING)
    client.set_variable(_CADENCE_VARIABLE, "weekly")

    resp = client.trigger_dag(_PIPELINE_DAG_ID, json={"logical_date": datetime.now(UTC).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_PIPELINE_DAG_ID, run_id=run_id, timeout=_GO_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_PIPELINE_DAG_ID, run_id=run_id)
    return _CompletedRun(
        client=client,
        run_id=run_id,
        state=state,
        ti_states={ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])},
    )


def _edges(client: AirflowClient, dag_id: str) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Return the downstream and the upstream task ids of every task of *dag_id*."""
    tasks = client.get_tasks(dag_id).get("tasks", [])
    downstream = {task["task_id"]: set(task["downstream_task_ids"]) for task in tasks}
    # The API reports downstream edges only, so upstream is read by inverting them.
    upstream: dict[str, set[str]] = {task_id: set() for task_id in downstream}
    for task_id, down in downstream.items():
        for child in down:
            upstream.setdefault(child, set()).add(task_id)
    return downstream, upstream


def test_the_dags_the_binary_parsed_are_registered(parsed_dags: AirflowClient):
    """The Dags exist without any Python file declaring them."""
    for dag_id in (_PIPELINE_DAG_ID, _REPORT_DAG_ID, _TRIGGER_DAG_ID):
        dag = parsed_dags.get_dag(dag_id)
        assert dag["dag_id"] == dag_id
        assert {tag["name"] for tag in dag.get("tags") or []} >= {"go", "native"}


def test_the_graph_carries_every_construct(parsed_dags: AirflowClient):
    """Group prefixes, the group edge, the fan-in, the branches and the join survive parsing."""
    downstream, upstream = _edges(parsed_dags, _PIPELINE_DAG_ID)

    # The nested groups prefixed their tasks, and the task-to-group edge expanded onto the
    # tasks that the group starts with.
    assert {"extract.regions.north", "extract.regions.south", "extract.summarize"} <= set(downstream)
    assert downstream["seed"] >= {"extract.regions.north", "extract.regions.south"}
    # The fan-in.
    assert upstream["extract.summarize"] >= {"extract.regions.north", "extract.regions.south"}
    # The condition and the switch reach their candidates.
    assert downstream["anyRows"] >= {"loadRows", "reportEmpty"}
    assert downstream["pick"] >= {"publishDaily", "publishWeekly"}
    # Cleanup sits behind every branch outcome, which is what its trigger rule is for.
    assert upstream["cleanup"] >= {"loadRows", "reportEmpty", "publishDaily", "publishWeekly"}


def test_dag_run_succeeded(completed_run: _CompletedRun):
    assert completed_run.state == "success", (
        f"expected the run to succeed; got {completed_run.state!r}. task states: {completed_run.ti_states}"
    )


def test_the_taken_branches_ran_and_the_others_skipped(completed_run: _CompletedRun):
    """A branch is a run-time skip, so the states are what prove it worked."""
    states = completed_run.ti_states
    skipped = {"reportEmpty", "publishDaily"}
    ran = {
        "seed",
        "extract.regions.north",
        "extract.regions.south",
        "extract.summarize",
        "loadRows",
        "anyRows",
        "pick",
        "publishWeekly",
        "cleanup",
    }
    assert set(states) == ran | skipped, states
    for task_id in ran:
        assert states[task_id] == "success", f"{task_id!r} did not succeed. task states: {states}"
    for task_id in skipped:
        assert states[task_id] == "skipped", f"{task_id!r} was not skipped. task states: {states}"


def test_xcoms_flow_between_go_tasks(completed_run: _CompletedRun):
    """Each task got its literals and upstream results, and its own return came back."""
    assert completed_run.xcom("extract.regions.north") == {"region": "north", "rows": 3}
    assert completed_run.xcom("extract.regions.south") == {"region": "south", "rows": 2}
    assert completed_run.xcom("extract.summarize") == {"total": 5, "regions": 2}
    assert completed_run.xcom("loadRows") == "s3://bucket/out"


def test_the_decisions_are_recorded(completed_run: _CompletedRun):
    """A condition returns its boolean, and a switch the task id it chose."""
    assert completed_run.xcom("anyRows") is True
    assert completed_run.xcom("pick") == "publishWeekly"


def test_the_variable_and_connection_reached_the_go_task(completed_run: _CompletedRun):
    """``seed`` read the REST-set Variable and the Connection that conftest defines."""
    assert completed_run.xcom("seed") == {"greeting": _GREETING, "host": _CONNECTION_HOST}


def test_a_task_pulls_the_xcom_another_pushed_under_its_own_key(completed_run: _CompletedRun):
    note = f"seeded {_GREETING} from {_CONNECTION_HOST}"
    assert completed_run.xcom("seed", key="seed_note") == note
    assert completed_run.xcom("cleanup") == note


def test_the_dag_source_is_the_file_that_declares_the_dag(parsed_dags: AirflowClient):
    """The Code view shows the source file of each Dag, not always the entrypoint."""
    report = parsed_dags.get_dag_source(_REPORT_DAG_ID)
    assert 'airflow.Dag("go_native_report"' in report["content"]
    assert "func main(" not in report["content"]
    assert report.get("language") == "go"

    pipeline = parsed_dags.get_dag_source(_PIPELINE_DAG_ID)
    assert "func main(" in pipeline["content"]


def test_the_trigger_started_the_report_run(parsed_dags: AirflowClient):
    client = parsed_dags
    # Dags are paused at creation here, so the run the trigger starts would stay queued.
    client.un_pause_dag(_REPORT_DAG_ID)
    resp = client.trigger_dag(_TRIGGER_DAG_ID, json={"logical_date": datetime.now(UTC).isoformat()})
    state = client.wait_for_dag_run(dag_id=_TRIGGER_DAG_ID, run_id=resp["dag_run_id"], timeout=300)
    assert state == "success"

    runs = client.list_dag_runs(_REPORT_DAG_ID)["dag_runs"]
    triggered = [run for run in runs if run["run_type"] == "operator_triggered"]
    assert [run["conf"] for run in triggered] == [{"triggered_by": _TRIGGER_DAG_ID}]
