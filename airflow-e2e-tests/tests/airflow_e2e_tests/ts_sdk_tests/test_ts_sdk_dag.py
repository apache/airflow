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
"""End-to-end tests for coordinator-mode TypeScript tasks.

Run with::

    E2E_TEST_MODE=ts_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/ts_sdk_tests/ -xvs

The ``typescript_example`` Dag mixes a Python task with ``@task.stub``
TypeScript tasks whose handlers live in the ``airflow-ts-pack`` bundle built
by ``conftest._setup_ts_sdk_integration``. Triggered once via the
module-scoped ``completed_run`` fixture, the run confirms end-to-end that
``NodeCoordinator`` launches the bundle on the volume-provided Node runtime,
Variable/Connection reads and Python <-> TypeScript XCom round-trips work
through the Task Execution API, and coordinator-channel logs reach the
task-log store.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# Coordinator startup only needs to launch node with the prebuilt bundle;
# allow room for scheduling and the Python upstream task.
_TS_TASK_TIMEOUT = 600
# Task logs are written when the task finishes; allow a little slack for them
# to become retrievable through the API after the run reaches a terminal state.
_LOG_FETCH_TIMEOUT = 120

_DAG_ID = "typescript_example"


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

    def logs(self, task_id: str, try_number: int = 1) -> str:
        """Fetch task logs, retrying until present (log upload is async)."""
        deadline = time.monotonic() + _LOG_FETCH_TIMEOUT
        while True:
            resp = self.client.get_task_logs(
                dag_id=_DAG_ID, run_id=self.run_id, task_id=task_id, try_number=try_number
            )
            text = "\n".join(str(entry) for entry in resp.get("content", []))
            if text.strip() or time.monotonic() > deadline:
                return text
            time.sleep(3)


@pytest.fixture(scope="module")
def completed_run() -> _CompletedRun:
    """Trigger ``typescript_example`` once; every test inspects the same run."""
    client = AirflowClient()
    resp = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(timezone.utc).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_TS_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    ti_states = {ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])}
    return _CompletedRun(client=client, run_id=run_id, state=state, ti_states=ti_states)


def test_dag_run_succeeded(completed_run: _CompletedRun):
    assert completed_run.state == "success", (
        f"expected the run to succeed; got {completed_run.state!r}. task states: {completed_run.ti_states}"
    )


def test_task_states(completed_run: _CompletedRun):
    expected = {
        "python_start": "success",
        "build_message": "success",
        "read_connection": "success",
    }
    for task_id, want in expected.items():
        assert completed_run.ti_states.get(task_id) == want, (
            f"{task_id!r} expected {want!r}. all task states: {completed_run.ti_states}"
        )


def test_build_message_xcom_round_trip(completed_run: _CompletedRun):
    """``build_message`` combines ``python_start``'s XCom with the Variable,
    pushes it under ``typescript_message``, and returns it."""
    assert completed_run.xcom("python_start") == "hello from Python"

    message = "greetings from e2e; upstream=hello from Python"
    value = completed_run.xcom("build_message")
    assert value == {"message": message, "upstream": "hello from Python"}, (
        f"unexpected 'build_message' return_value: {value!r}"
    )
    assert completed_run.xcom("build_message", key="typescript_message") == message


def test_read_connection_xcom(completed_run: _CompletedRun):
    value = completed_run.xcom("read_connection")
    assert value == {
        "id": "typescript_example_http",
        "type": "http",
        "host": "example.com",
        "login": "user",
        "hasPassword": True,
    }, f"unexpected 'read_connection' return_value: {value!r}"


def test_coordinator_logs_reach_task_log_store(completed_run: _CompletedRun):
    assert "[ts-sdk.runtime] Coordinator runtime started" in completed_run.logs("build_message")
