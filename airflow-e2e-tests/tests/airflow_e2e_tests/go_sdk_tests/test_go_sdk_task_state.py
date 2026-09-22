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
"""E2E test for the Go SDK ``task_state_dag`` example (``go-sdk/example/bundle/taskstate``).

Assertions read the API-visible store rather than the task's XCom summary, so the data is
proven to have reached the database.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http import HTTPStatus

import pytest
import requests

from airflow_e2e_tests.constants import GO_SDK_STATE_STORE_RETENTION_DAYS
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

_GO_TASK_TIMEOUT = 300

_DAG_ID = "task_state_dag"
_TASK_ID = "roundtrip_task_state"


@dataclass(frozen=True)
class _CompletedRun:
    """The single ``task_state_dag`` run shared across this module's tests."""

    client: AirflowClient
    run_id: str
    state: str
    ti_states: dict[str, str]

    def state_store(self, key: str):
        return self.client.get_task_state_store(dag_id=_DAG_ID, run_id=self.run_id, task_id=_TASK_ID, key=key)


@pytest.fixture(scope="module")
def completed_run() -> _CompletedRun:
    client = AirflowClient()
    resp = client.trigger_dag(_DAG_ID, json={"logical_date": datetime.now(timezone.utc).isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_GO_TASK_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    ti_states = {ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])}
    return _CompletedRun(client=client, run_id=run_id, state=state, ti_states=ti_states)


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def test_task_succeeded(completed_run: _CompletedRun):
    assert completed_run.state == "success", (
        f"expected the run to succeed; got {completed_run.state!r}. task states: {completed_run.ti_states}"
    )
    assert completed_run.ti_states.get(_TASK_ID) == "success", completed_run.ti_states


def test_value_written_by_go_task_is_readable(completed_run: _CompletedRun):
    entry = completed_run.state_store("go_e2e_run_id")
    assert entry.get("value") == completed_run.run_id, (
        f"go_e2e_run_id should hold this run's id {completed_run.run_id!r}, got {entry!r}"
    )


def test_never_expire_key_has_null_expiry(completed_run: _CompletedRun):
    entry = completed_run.state_store("go_e2e_run_id")
    # A null expiry on the wire is the end-to-end proof that sdk.NeverExpire reached the database.
    assert entry.get("expires_at") is None, entry


def test_structured_value_roundtrips(completed_run: _CompletedRun):
    entry = completed_run.state_store("go_e2e_counter")
    value = entry.get("value")
    assert isinstance(value, dict), entry
    assert value.get("processed") == 3, entry
    assert isinstance(value.get("processed"), int), entry
    assert value.get("cursor") == "abc-123", entry
    assert isinstance(value.get("cursor"), str), entry


def test_deleted_key_is_gone(completed_run: _CompletedRun):
    with pytest.raises(requests.HTTPError) as excinfo:
        completed_run.state_store("go_e2e_scratch")
    assert excinfo.value.response.status_code == HTTPStatus.NOT_FOUND


def test_default_retention_applied(completed_run: _CompletedRun):
    entry = completed_run.state_store("go_e2e_retained")
    assert entry.get("expires_at") is not None, entry
    gap = _parse_timestamp(entry["expires_at"]) - _parse_timestamp(entry["updated_at"])
    expected = timedelta(days=GO_SDK_STATE_STORE_RETENTION_DAYS)
    assert abs(gap - expected) <= timedelta(minutes=5), (
        f"expected ~{GO_SDK_STATE_STORE_RETENTION_DAYS} days, got {gap / timedelta(days=1):.1f} days; "
        "if ~30, the Go runtime used its fallback, so the supervisor did not propagate "
        f"[state_store] default_retention_days. entry: {entry!r}"
    )
