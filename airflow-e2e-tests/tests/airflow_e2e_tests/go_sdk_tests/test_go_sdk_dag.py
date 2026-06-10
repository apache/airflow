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
"""E2E tests for the Go SDK via the compiled example bundle.

Run with::

    E2E_TEST_MODE=go_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/go_sdk_tests/ -xvs

What is verified
----------------
``conftest._setup_go_sdk_integration`` compiles ``go-sdk/example/bundle`` into a
self-contained executable bundle with the ``airflow-go-pack`` tooling and drops
it into the directory the ``ExecutableCoordinator`` scans. The ``simple_dag``
Dag (``go-sdk/dags/go_examples.py``) sandwiches the Go tasks between two native
Python tasks::

    python_task_1 >> extract >> transform >> [load, python_task_2]

* ``extract`` / ``transform`` / ``load`` are ``@task.stub(queue="golang")`` tasks
  whose implementations live in the Go bundle (``main.go``). The ``golang`` queue
  is routed to ``ExecutableCoordinator``, which locates the bundle by dag_id,
  launches the binary with ``--comm`` / ``--logs``, and drives it through the
  msgpack-over-IPC coordinator protocol.
* ``python_task_1`` (Python) pushes an XCom; ``extract`` (Go) fetches the
  ``test_http`` connection and returns ``{go_version, timestamp}``; ``transform``
  (Go) reads ``my_variable``; ``load`` (Go) returns an error on purpose;
  ``python_task_2`` (Python) pulls and re-emits the Go ``extract`` task's XCom.

The Dag is triggered exactly once by the module-scoped ``completed_run`` fixture;
each test asserts a different facet of that single run. Together they confirm,
end-to-end:

1. ``ExecutableCoordinator`` discovers the AFBNDL01 bundle by dag_id and runs the
   binary in coordinator mode for every Go task, reporting ``SucceedTask`` for
   extract/transform and a failed ``TaskState`` for load.
2. Connection / Variable reads and XCom writes work through the Task Execution
   API, XCom values keep their types (the ``timestamp`` stays an ``int``), and
   XCom crosses the Python <-> Go boundary in both directions.
3. Structured task logs emitted by the Go binary over the coordinator logs
   channel reach Airflow's task-log store.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# The Go extract task sleeps ~20 s + coordinator startup; allow plenty of room.
_GO_TASK_TIMEOUT = 600
# Task logs are written when the task finishes; allow a little slack for them to
# become retrievable through the API after the run reaches a terminal state.
_LOG_FETCH_TIMEOUT = 120

_DAG_ID = "simple_dag"


@dataclass
class _CompletedRun:
    """The single ``simple_dag`` run shared across the assertions in this module."""

    client: AirflowClient
    run_id: str
    state: str
    ti_states: dict[str, str]

    def xcom(self, task_id: str, key: str = "return_value"):
        return self.client.get_xcom_value(dag_id=_DAG_ID, task_id=task_id, run_id=self.run_id, key=key).get(
            "value"
        )

    def logs(self, task_id: str, try_number: int = 1) -> str:
        """Return the concatenated task-log records for *task_id*, retrying until present."""
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
    """Trigger ``simple_dag`` once and wait for it to finish.

    Module-scoped so the (multi-minute) Go run happens a single time; every test
    in this module inspects the resulting states, XComs, and logs.
    """
    client = AirflowClient()
    resp = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(timezone.utc).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_GO_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    ti_states = {ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])}
    return _CompletedRun(client=client, run_id=run_id, state=state, ti_states=ti_states)


def test_task_states(completed_run: _CompletedRun):
    """Every task ends in its expected state (the Go ``load`` task fails on purpose)."""
    expected = {
        "python_task_1": "success",
        "extract": "success",
        "transform": "success",
        "load": "failed",
        "python_task_2": "success",
    }
    for task_id, want in expected.items():
        assert completed_run.ti_states.get(task_id) == want, (
            f"{task_id!r} expected {want!r}. all task states: {completed_run.ti_states}"
        )


def test_dag_run_failed(completed_run: _CompletedRun):
    """The failing ``load`` leaf makes the overall run fail."""
    assert completed_run.state == "failed", (
        f"expected the run to fail because 'load' fails; got {completed_run.state!r}. "
        f"task states: {completed_run.ti_states}"
    )


def test_python_task_1_pushes_xcom(completed_run: _CompletedRun):
    """The upstream Python task's XCom is available (Python -> XCom)."""
    assert completed_run.xcom("python_task_1") == "value_from_python_task_1"


def test_extract_xcom_has_go_version_and_int_timestamp(completed_run: _CompletedRun):
    """The map returned by the Go 'extract' task round-trips through XCom (Go -> XCom)."""
    value = completed_run.xcom("extract")
    assert isinstance(value, dict), (
        f"Expected 'extract' XCom to be a mapping, got {value!r} ({type(value).__name__})"
    )
    assert str(value.get("go_version", "")).startswith("go"), (
        f"Expected go_version to look like a Go runtime version, got {value.get('go_version')!r}"
    )
    timestamp = value.get("timestamp")
    assert isinstance(timestamp, int), (
        f"Expected 'timestamp' to be an int, got {timestamp!r} ({type(timestamp).__name__})"
    )
    assert timestamp > 0, f"Expected a positive nanosecond timestamp, got {timestamp!r}"


def test_xcom_crosses_go_to_python(completed_run: _CompletedRun):
    """python_task_2 pulled the Go 'extract' XCom and re-emitted it unchanged (Go -> Python)."""
    assert completed_run.xcom("python_task_2") == completed_run.xcom("extract")


def test_extract_logs_show_beep_loop(completed_run: _CompletedRun):
    """The Go 'extract' task's structured logs reach Airflow's task log."""
    logs = completed_run.logs("extract")
    beeps = logs.count("After the beep the time will be")
    assert beeps == 10, f"expected 10 'After the beep the time will be' log lines from extract, got {beeps}"
    assert "Goodbye from task" in logs, "extract task should log 'Goodbye from task'"


def test_transform_logs_show_variable_read(completed_run: _CompletedRun):
    """The Go 'transform' task logs the variable it read."""
    assert "Obtained variable" in completed_run.logs("transform"), (
        "transform task should log 'Obtained variable'"
    )


def test_load_logs_show_failure(completed_run: _CompletedRun):
    """The Go 'load' task's error surfaces in its task log."""
    assert "Please fail" in completed_run.logs("load"), (
        "load task log should contain its failure message 'Please fail'"
    )
