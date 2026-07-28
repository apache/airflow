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
  (Go) reads ``my_variable``; ``load`` (Go) fails on its first attempt and
  succeeds on the retry (``retries=1``); ``python_task_2`` (Python) pulls and
  re-emits the Go ``extract`` task's XCom.

The Dag is triggered exactly once by the module-scoped ``completed_run`` fixture;
each test asserts a different facet of that single run. Together they confirm,
end-to-end:

1. ``ExecutableCoordinator`` discovers the AFBNDL01 bundle by dag_id and runs the
   binary in coordinator mode for every Go task, reporting ``SucceedTask`` for
   extract/transform and -- because ``load`` has ``retries=1`` -- a ``RetryTask``
   (UP_FOR_RETRY) for its first failing attempt, after which the retry succeeds.
2. Connection / Variable reads and XCom writes work through the Task Execution
   API, XCom values keep their types (the ``timestamp`` stays an ``int``), and
   XCom crosses the Python <-> Go boundary in both directions.
3. Structured task logs emitted by the Go binary over the coordinator logs
   channel reach Airflow's task-log store.
4. The runtime context surfaced to Go tasks via ``sdk.CurrentContext`` is fully
   populated on the coordinator path: the task-instance identifiers and the Dag
   run's scheduling timestamps (logical_date / data_interval_start/end).
5. A Go task that fails with retries left emits ``RetryTask`` rather than a
   terminal ``FAILED``, so the supervisor marks it UP_FOR_RETRY and re-runs it;
   ``load`` therefore ends ``success`` on its second attempt (try_number 2).
"""

from __future__ import annotations

import re
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
    ti_attrs: dict[str, dict]

    def try_number(self, task_id: str) -> int | None:
        return self.ti_attrs.get(task_id, {}).get("try_number")

    def xcom(self, task_id: str, key: str = "return_value"):
        return self.client.get_xcom_value(dag_id=_DAG_ID, task_id=task_id, run_id=self.run_id, key=key).get(
            "value"
        )

    def logs(self, task_id: str, try_number: int = 1) -> str:
        """Return the concatenated task-log records for *task_id*, retrying until present."""
        deadline = time.monotonic() + _LOG_FETCH_TIMEOUT
        while True:
            text = "\n".join(str(entry) for entry in self.log_records(task_id, try_number))
            if text.strip() or time.monotonic() > deadline:
                return text
            time.sleep(3)

    def log_records(self, task_id: str, try_number: int = 1) -> list[dict]:
        """Return the structured task-log records (parsed JSON dicts) for *task_id*."""
        resp = self.client.get_task_logs(
            dag_id=_DAG_ID, run_id=self.run_id, task_id=task_id, try_number=try_number
        )
        return [entry for entry in resp.get("content", []) if isinstance(entry, dict)]


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
    ti_attrs = {ti["task_id"]: ti for ti in ti_resp.get("task_instances", [])}
    ti_states = {task_id: ti.get("state") for task_id, ti in ti_attrs.items()}
    return _CompletedRun(client=client, run_id=run_id, state=state, ti_states=ti_states, ti_attrs=ti_attrs)


def test_task_states(completed_run: _CompletedRun):
    """Every task ends ``success`` (the Go ``load`` task succeeds on its retry)."""
    expected = {
        "python_task_1": "success",
        "extract": "success",
        "transform": "success",
        "load": "success",
        "python_task_2": "success",
    }
    for task_id, want in expected.items():
        assert completed_run.ti_states.get(task_id) == want, (
            f"{task_id!r} expected {want!r}. all task states: {completed_run.ti_states}"
        )


def test_dag_run_succeeded(completed_run: _CompletedRun):
    """The run succeeds once ``load`` recovers on its retry."""
    assert completed_run.state == "success", (
        f"expected the run to succeed because 'load' recovers on retry; got {completed_run.state!r}. "
        f"task states: {completed_run.ti_states}"
    )


def test_load_retried_then_succeeded(completed_run: _CompletedRun):
    """``load`` fails once (UP_FOR_RETRY) then succeeds on the second attempt.

    The Go coordinator must emit ``RetryTask`` (not terminal ``FAILED``) when the
    task fails with retries left, so the supervisor re-runs it. The end state is
    ``success`` reached on ``try_number`` 2, and each attempt's log reflects the
    first failure and then the recovery.
    """
    assert completed_run.ti_states.get("load") == "success", completed_run.ti_states
    assert completed_run.try_number("load") == 2, (
        f"'load' should have run twice (fail then retry); try_number="
        f"{completed_run.try_number('load')!r}, ti: {completed_run.ti_attrs.get('load')}"
    )
    assert "Please fail" in completed_run.logs("load", try_number=1), (
        "load's first attempt should log its failure message 'Please fail'"
    )
    assert "Recovered on retry" in completed_run.logs("load", try_number=2), (
        "load's retry attempt should log 'Recovered on retry'"
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


def test_extract_logs_show_runtime_context(completed_run: _CompletedRun):
    """The Go 'extract' task logs every field surfaced by ``sdk.CurrentContext``.

    ``extract`` (main.go) emits one ``task runtime context`` record whose fields
    are grouped under ``context.ti.*`` and ``context.dag_run.*``. This confirms
    the coordinator path populates the full runtime context end-to-end -- the
    task-instance identifiers and the Dag run's scheduling timestamps (the
    ti_context.dag_run.* dates the supervisor sends over msgpack).
    """
    # The fields are emitted as structured attributes on a single log record,
    # so read them from the parsed record rather than the rendered text.
    records = completed_run.log_records("extract")
    record = next((r for r in records if r.get("event") == "task runtime context"), None)
    assert record is not None, (
        f"extract should emit a 'task runtime context' record; events seen: "
        f"{[r.get('event') for r in records]}"
    )

    run_id = completed_run.run_id

    # Task-instance identifiers come straight from the task instance.
    assert record.get("context.ti.dag_id") == _DAG_ID, record
    assert record.get("context.ti.task_id") == "extract", record
    assert record.get("context.ti.run_id") == run_id, record
    assert str(record.get("context.ti.try_number")) == "1", record
    # Unmapped task -> nil *int -> logged as null.
    assert record.get("context.ti.map_index") is None, record

    # The Dag run mirrors the same ids and carries the scheduling timestamps.
    assert record.get("context.dag_run.dag_id") == _DAG_ID, record
    assert record.get("context.dag_run.run_id") == run_id, record

    iso_prefix = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")
    for ts_field in (
        "context.dag_run.logical_date",
        "context.dag_run.data_interval_start",
        "context.dag_run.data_interval_end",
    ):
        value = record.get(ts_field)
        assert value, f"{ts_field} should be present and non-empty; record: {record}"
        assert iso_prefix.match(str(value)), f"{ts_field} should be an ISO-8601 timestamp, got {value!r}"


def test_transform_logs_show_variable_read(completed_run: _CompletedRun):
    """The Go 'transform' task logs the variable it read."""
    assert "Obtained variable" in completed_run.logs("transform"), (
        "transform task should log 'Obtained variable'"
    )
