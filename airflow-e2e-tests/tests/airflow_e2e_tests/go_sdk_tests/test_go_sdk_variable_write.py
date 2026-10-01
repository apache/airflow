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
"""E2E test for the Go SDK ``variable_write_dag`` example.

``write_and_delete_variable`` (Go, ``go-sdk/example/bundle/variablewrite``) stores
its run id in ``go_e2e_variable`` and deletes a scratch Variable it has just
written, so both paths go through the supervisor and the Task Execution API.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus

import pytest
import requests

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

_GO_TASK_TIMEOUT = 300

_DAG_ID = "variable_write_dag"
_TASK_ID = "write_and_delete_variable"


@dataclass
class _CompletedRun:
    """The single ``variable_write_dag`` run shared across this module's tests."""

    client: AirflowClient
    run_id: str
    state: str
    ti_states: dict[str, str]


@pytest.fixture(scope="module")
def completed_run() -> _CompletedRun:
    """Trigger ``variable_write_dag`` once and wait for it to finish."""
    client = AirflowClient()
    resp = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(timezone.utc).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_GO_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    ti_states = {ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])}
    return _CompletedRun(client=client, run_id=run_id, state=state, ti_states=ti_states)


def test_task_succeeded(completed_run: _CompletedRun):
    assert completed_run.state == "success", (
        f"expected the run to succeed; got {completed_run.state!r}. task states: {completed_run.ti_states}"
    )
    assert completed_run.ti_states.get(_TASK_ID) == "success", completed_run.ti_states


def test_variable_written_by_go_task_is_readable(completed_run: _CompletedRun):
    variable = completed_run.client.get_variable("go_e2e_variable")
    assert variable.get("value") == completed_run.run_id, (
        f"go_e2e_variable should hold this run's id {completed_run.run_id!r}, got {variable!r}"
    )
    assert variable.get("description") == "written by the Go SDK e2e test", variable


def test_scratch_variable_deleted_by_go_task_is_gone(completed_run: _CompletedRun):
    with pytest.raises(requests.HTTPError) as excinfo:
        completed_run.client.get_variable("go_e2e_scratch")
    assert excinfo.value.response.status_code == HTTPStatus.NOT_FOUND
