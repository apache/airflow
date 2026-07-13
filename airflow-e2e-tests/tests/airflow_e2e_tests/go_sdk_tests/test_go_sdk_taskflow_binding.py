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
"""E2E test for the Go SDK ``taskflow_binding_dag`` example.

The stub Dag's single mixed positional/keyword TaskFlow call carries literals
of every scalar type, an array literal, a defaulted ``None``, and XComs from
two upstream Go tasks (an object bound onto a strict Go struct and an array
bound onto ``[]int``). The Go ``combine`` task verifies every bound value and
errors on any mismatch, so a green run *is* the binding assertion; the tests
here check the run outcome and the summary XCom it pushes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# Three short Go tasks; allow room for coordinator startup.
_GO_TASK_TIMEOUT = 300

_DAG_ID = "taskflow_binding_dag"


@dataclass
class _CompletedRun:
    """The single ``taskflow_binding_dag`` run shared across this module's tests."""

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
    """Trigger ``taskflow_binding_dag`` once and wait for it to finish."""
    client = AirflowClient()
    resp = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(timezone.utc).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_GO_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    ti_states = {ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])}
    return _CompletedRun(client=client, run_id=run_id, state=state, ti_states=ti_states)


def test_all_tasks_succeeded(completed_run: _CompletedRun):
    """The Go ``combine`` task errors on any mis-bound argument, so success here
    proves every literal, XCom, keyword, and defaulted-None binding was correct."""
    assert completed_run.state == "success", (
        f"expected the run to succeed; got {completed_run.state!r}. task states: {completed_run.ti_states}"
    )
    for task_id in ("make_config", "make_numbers", "combine"):
        assert completed_run.ti_states.get(task_id) == "success", completed_run.ti_states


def test_upstream_xcoms_keep_their_shapes(completed_run: _CompletedRun):
    """The Go struct arrives as an object XCom and the ``[]int`` as an array."""
    assert completed_run.xcom("make_config") == {
        "environment": "production",
        "region": "eu-west-1",
        "debug": True,
    }
    assert completed_run.xcom("make_numbers") == [1, 1, 2, 3, 5, 8]


def test_combine_summary_reflects_bound_arguments(completed_run: _CompletedRun):
    """``combine`` re-emits every bound value, confirming types survived the
    Python literal / XCom -> Go parameter -> XCom round trip."""
    assert completed_run.xcom("combine") == {
        "name": "summary",
        "count": 3,
        "ratio": 2.5,
        "enabled": True,
        "tags": ["metrics", "hourly"],
        "environment": "production",
        "debug": True,
        "sum": 20,
        "note_was_null": True,
    }
