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
End-to-end test of task loops declared in TypeScript.

Run with::

    E2E_TEST_MODE=ts_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/ts_sdk_tests/test_ts_sdk_loop_dag.py -xvs

``ts-sdk/example/src/loop.ts`` declares two loops. ``refine`` runs Newton's method for the square root of
two until its gate, a TypeScript handler, says the error is small enough; ``accumulate`` only counts, three
times.
Core creates every iteration; the TypeScript runtime runs the bodies and the gates.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

_TS_TASK_TIMEOUT = 600

_DAG_ID = "typescript_loop_example"

# Newton's method from 1.0 reaches an error below 1e-6 at its fourth estimate.
_REFINE_ITERATIONS = 4
_ACCUMULATE_ITERATIONS = 3


@pytest.fixture(scope="module")
def completed_run() -> tuple[AirflowClient, str, str, list[dict]]:
    client = AirflowClient()
    client.wait_for_dag(_DAG_ID, timeout=_TS_TASK_TIMEOUT)
    client.un_pause_dag(_DAG_ID)
    run_id = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(UTC).isoformat()})["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_TS_TASK_TIMEOUT)
    tis = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id).get("task_instances", [])
    return client, run_id, state, tis


def _iterations(tis: list[dict], task_id: str) -> list[int]:
    return sorted(ti["loop_iteration"]["iteration"] for ti in tis if ti["task_id"] == task_id)


def test_the_loops_are_registered_with_their_gates(completed_run):
    client, *_ = completed_run
    downstream = {
        task["task_id"]: set(task["downstream_task_ids"]) for task in client.get_tasks(_DAG_ID)["tasks"]
    }

    # The gate is the loop's only way out: report reads evaluate, but waits for the gate.
    assert downstream["refine.evaluate"] == {"refine.accurateEnough"}
    assert downstream["refine.accurateEnough"] >= {"report", "finished", "accumulate.increment"}
    assert downstream["accumulate.increment"] == {"accumulate.__loop_gate"}


def test_dag_run_succeeded(completed_run):
    _, _, state, tis = completed_run
    assert state == "success", {ti["task_id"]: ti["state"] for ti in tis}
    assert {ti["state"] for ti in tis} == {"success"}


def test_the_gate_stopped_the_loop_when_its_handler_said_so(completed_run):
    *_, tis = completed_run
    expected = list(range(1, _REFINE_ITERATIONS + 1))
    for task_id in ("refine.improve", "refine.evaluate", "refine.accurateEnough"):
        assert _iterations(tis, task_id) == expected, task_id


def test_a_fixed_count_loop_runs_its_bound(completed_run):
    *_, tis = completed_run
    expected = list(range(1, _ACCUMULATE_ITERATIONS + 1))
    assert _iterations(tis, "accumulate.increment") == expected
    assert _iterations(tis, "accumulate.__loop_gate") == expected


def test_a_task_outside_the_loop_reads_every_iteration(completed_run):
    client, run_id, *_ = completed_run
    report = client.get_xcom_value(dag_id=_DAG_ID, task_id="report", run_id=run_id, key="return_value")[
        "value"
    ]

    assert report["iterations"] == _REFINE_ITERATIONS
    assert report["estimate"] == pytest.approx(2**0.5)
    # Each iteration improves on the previous one, which getLoop() handed the body.
    assert report["errors"] == sorted(report["errors"], reverse=True)
    assert report["errors"][-1] < 1e-6
