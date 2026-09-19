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

Two Dags mix Python tasks with ``@task.stub`` TypeScript tasks, both served by the single
``airflow-ts-pack`` bundle, and each is triggered once via a module-scoped fixture.

``typescript_example`` covers the runtime: Variable and Connection reads, Python <-> TypeScript XCom
round-trips, and task logs reaching the log store.

``typescript_taskflow_example`` covers TaskFlow arguments, including an upstream output pulled before
the handler runs and a ``withArgNames`` rename on its ``report`` task, and shares a ``build_message``
task ID with ``typescript_example`` so that dispatch keying on the task ID alone would run the wrong
handler.
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
_TASKFLOW_DAG_ID = "typescript_taskflow_example"


@dataclass
class _CompletedRun:
    client: AirflowClient
    dag_id: str
    run_id: str
    state: str
    ti_states: dict[str, str]

    def xcom(self, task_id: str, key: str = "return_value"):
        return self.client.get_xcom_value(
            dag_id=self.dag_id, task_id=task_id, run_id=self.run_id, key=key
        ).get("value")

    def logs(self, task_id: str, try_number: int = 1) -> str:
        """Fetch task logs, retrying until present (log upload is async)."""
        deadline = time.monotonic() + _LOG_FETCH_TIMEOUT
        while True:
            resp = self.client.get_task_logs(
                dag_id=self.dag_id, run_id=self.run_id, task_id=task_id, try_number=try_number
            )
            text = "\n".join(str(entry) for entry in resp.get("content", []))
            if text.strip() or time.monotonic() > deadline:
                return text
            time.sleep(3)


def _trigger_and_wait(dag_id: str) -> _CompletedRun:
    client = AirflowClient()
    resp = client.trigger_dag(dag_id, json={"logical_date": datetime.now(timezone.utc).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=dag_id, run_id=run_id, timeout=_TS_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=dag_id, run_id=run_id)
    ti_states = {ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])}
    return _CompletedRun(client=client, dag_id=dag_id, run_id=run_id, state=state, ti_states=ti_states)


@pytest.fixture(scope="module")
def completed_run() -> _CompletedRun:
    """Trigger ``typescript_example`` once; every test inspects the same run."""
    return _trigger_and_wait(_DAG_ID)


@pytest.fixture(scope="module")
def completed_taskflow_run() -> _CompletedRun:
    """Trigger ``typescript_taskflow_example`` once, from the same bundle."""
    return _trigger_and_wait(_TASKFLOW_DAG_ID)


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
    """``build_message`` combines ``python_start``'s XCom with the Variable, pushes it under
    ``typescript_message``, and returns it."""
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


def test_second_dag_from_the_same_bundle_succeeded(completed_taskflow_run: _CompletedRun):
    """One packed bundle provides for both Dags, so the second one also runs."""
    assert completed_taskflow_run.state == "success", (
        f"expected the run to succeed; got {completed_taskflow_run.state!r}. "
        f"task states: {completed_taskflow_run.ti_states}"
    )
    expected = {
        "make_totals": "success",
        "summarize": "success",
        "report": "success",
        "build_message": "success",
    }
    for task_id, want in expected.items():
        assert completed_taskflow_run.ti_states.get(task_id) == want, (
            f"{task_id!r} expected {want!r}. all task states: {completed_taskflow_run.ti_states}"
        )


def test_summarize_binds_its_call_arguments(completed_taskflow_run: _CompletedRun):
    """Every argument ``summarize(make_totals(), "uk", "GBP", 280.0)`` passes.

    ``region_code`` and ``dry_run`` are snake_case in the ``@task.stub``
    signature and camelCase in the handler, with nothing declared on either
    side: folding is what carries them across. ``dry_run`` is left out of the
    call, so it arrives from the stub's default. ``totals`` takes
    ``make_totals``'s output, so the runtime resolves that task's
    ``return_value`` XCom before the handler is called. ``averageOrder`` below
    is computed from it, and the handler never reads an XCom itself.

    A handler that received none of them would see ``undefined`` for each and
    return nulls and ``NaN`` here rather than failing, which is why the whole
    returned object is asserted.
    """
    assert completed_taskflow_run.xcom("make_totals") == {"orders": 12, "revenue": 3402.0}
    value = completed_taskflow_run.xcom("summarize")
    assert value == {
        "regionCode": "uk",
        "orders": 12,
        "averageOrder": 283.5,
        "currency": "GBP",
        "passed": True,
        "dryRun": False,
    }, f"unexpected 'summarize' return_value: {value!r}"
    # Written only when `dryRun` is false, so this also proves the defaulted
    # boolean arrived as `false` rather than as `undefined`.
    assert completed_taskflow_run.xcom("summarize", key="summary_line") == "uk: 12 orders"


def test_report_binds_an_explicitly_renamed_argument(completed_taskflow_run: _CompletedRun):
    """``report(summary, "nightly")`` reaches a handler that renamed one argument.

    Python names it ``run_label``; the handler destructures ``label``, a word
    the ``@task.stub`` signature never uses, so folding could not connect the
    two and the binding is stated with ``withArgNames``. The handler throws
    unless ``label`` is exactly ``"nightly"``, so a rename that did not take
    effect fails this task rather than returning a null.

    ``summary`` is not renamed: folding already covers it, which is the point
    that keeps ``withArgNames`` rare.
    """
    value = completed_taskflow_run.xcom("report")
    assert value == {"label": "nightly", "regionCode": "uk", "healthy": True}, (
        f"unexpected 'report' return_value: {value!r}"
    )


def test_same_task_id_under_two_dags_runs_its_own_handler(
    completed_run: _CompletedRun, completed_taskflow_run: _CompletedRun
):
    """Both Dags have a ``build_message``; each must reach its own handler.

    A bundle that keyed dispatch on the task ID alone would answer both from
    whichever handler was registered last, and both assertions below would
    report the same shape.
    """
    example_value = completed_run.xcom("build_message")
    taskflow_value = completed_taskflow_run.xcom("build_message")

    assert set(example_value) == {"message", "upstream"}, (
        f"unexpected 'typescript_example.build_message' return_value: {example_value!r}"
    )
    assert taskflow_value == {
        "dagId": _TASKFLOW_DAG_ID,
        "message": "uk: 12 orders averaging 283.5 GBP",
    }, f"unexpected 'typescript_taskflow_example.build_message' return_value: {taskflow_value!r}"
