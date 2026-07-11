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
"""E2E test for the Go SDK ``concurrent_xcom_dag`` example.

``pull_xcoms_concurrently`` (Go) pulls a batch of XComs sequentially then with one
goroutine per item, sharing the injected client, and fails if any result differs.
Asserts the task succeeds and the concurrent pull beats the sequential one.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# The Go task seeds and pulls the batch twice; it is fast, but allow room for
# coordinator startup.
_GO_TASK_TIMEOUT = 300

_DAG_ID = "concurrent_xcom_dag"
_TASK_ID = "pull_xcoms_concurrently"


@dataclass
class _CompletedRun:
    """The single ``concurrent_xcom_dag`` run shared across this module's tests."""

    client: AirflowClient
    run_id: str
    state: str
    ti_states: dict[str, str]

    def xcom(self, task_id: str, key: str = "return_value"):
        return self.client.get_xcom_value(dag_id=_DAG_ID, task_id=task_id, run_id=self.run_id, key=key).get(
            "value"
        )


@pytest.fixture(scope="module")
def completed_run() -> _CompletedRun:
    """Trigger ``concurrent_xcom_dag`` once and wait for it to finish."""
    client = AirflowClient()
    resp = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(timezone.utc).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_GO_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    ti_states = {ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])}
    return _CompletedRun(client=client, run_id=run_id, state=state, ti_states=ti_states)


def test_task_succeeded(completed_run: _CompletedRun):
    """Run and task succeed -- the task errors on any goroutine mismatch, so this
    proves the injected client was used safely."""
    assert completed_run.state == "success", (
        f"expected the run to succeed; got {completed_run.state!r}. task states: {completed_run.ti_states}"
    )
    assert completed_run.ti_states.get(_TASK_ID) == "success", completed_run.ti_states


def test_concurrent_faster_than_sequential(completed_run: _CompletedRun):
    """Concurrent pull-and-process beats the sequential loop."""
    value = completed_run.xcom(_TASK_ID)
    assert isinstance(value, dict), (
        f"Expected the task's XCom to be a mapping, got {value!r} ({type(value).__name__})"
    )

    sequential = value.get("sequential_ms")
    concurrent = value.get("concurrent_ms")
    assert isinstance(sequential, int), f"bad sequential_ms: {sequential!r}"
    assert isinstance(concurrent, int), f"bad concurrent_ms: {concurrent!r}"
    assert sequential > 0, f"bad sequential_ms: {sequential!r}"
    assert concurrent > 0, f"bad concurrent_ms: {concurrent!r}"
    assert concurrent < sequential, (
        f"expected concurrent ({concurrent} ms) to beat sequential ({sequential} ms)"
    )


def test_num_xcoms(completed_run: _CompletedRun):
    """The task reports the batch size it pulled."""
    assert completed_run.xcom(_TASK_ID).get("num_xcoms") == 10
