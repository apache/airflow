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
E2E tests for the Go stub tasks that run the artifact they are bound to, in the Go test bundles.

Run with::

    E2E_TEST_MODE=go_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/go_sdk_tests/test_go_sdk_task_handler_runs.py -xvs

The Dags are in ``go-test-bundle/dags``. A task returns the file that ran it, so each run shows which
artifact the worker started, and the task log names it too.

* Two Dags are generated when ``go_test_dags.py`` is parsed, with the Dag ids in ``E2E_GO_DYNAMIC_DAG_IDS``.
* ``go_split_artifacts`` has its two tasks in two different artifacts of one Dag bundle.
* ``go_unbound_stub`` is on a queue the scheduler and the worker route to a Lang-SDK coordinator but the Dag
  processor does not, so its stub task is never bound and reaches the worker without an artifact.

All the Dags are triggered at once by the module-scoped ``runs`` fixture.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus

import pytest
import requests

from airflow_e2e_tests.constants import GO_TEST_TASK_HANDLER_BUNDLE
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient
from airflow_e2e_tests.e2e_test_utils.lang_sdk import (
    ArtifactRef,
    get_running_artifact_record,
    get_task_handler_bindings,
)

# Coordinator startup of a Go task takes seconds, and all the runs share the stack.
_GO_TASK_TIMEOUT = 300
# Task logs are written when the task finishes; allow a little slack for them to become retrievable.
_LOG_FETCH_TIMEOUT = 60

_SPLIT_DAG_ID = "go_split_artifacts"
_UNBOUND_DAG_ID = "go_unbound_stub"

# What a stub task that was queued without an artifact fails with: the task, its Dag file and its queue.
_UNBOUND_REASON = (
    "Task 'unbound' of Dag 'go_unbound_stub' has no task handler artifact, and its Dag file "
    "'go_unbound_stub.py' is not an artifact that ExecutableCoordinator runs. "
    "Queue 'golang-unbound' routes it to a Lang-SDK coordinator"
)
_CANNOT_RUN_EVENT = "Cannot run the task's Lang-SDK artifact"


@dataclass
class _Run:
    client: AirflowClient
    dag_id: str
    run_id: str
    state: str
    ti_attrs: dict[str, dict]

    def xcom(self, task_id: str):
        return self.client.get_xcom_value(
            dag_id=self.dag_id, task_id=task_id, run_id=self.run_id, key="return_value"
        ).get("value")

    def log_records(self, task_id: str, try_number: int = 1) -> list[dict]:
        """Return the structured task-log records of a try, retrying until there are some."""
        deadline = time.monotonic() + _LOG_FETCH_TIMEOUT
        while True:
            resp = self.client.get_task_logs(
                dag_id=self.dag_id, run_id=self.run_id, task_id=task_id, try_number=try_number
            )
            records = [entry for entry in resp.get("content", []) if isinstance(entry, dict)]
            if records or time.monotonic() > deadline:
                return records
            time.sleep(3)


@pytest.fixture(scope="module")
def runs(go_dynamic_dag_ids) -> dict[str, _Run]:
    """Trigger every Dag of this module at once and wait for each run, so the waits overlap."""
    client = AirflowClient()
    logical_date = datetime.now(timezone.utc).isoformat()
    run_ids = {
        dag_id: client.trigger_dag(dag_id, json={"logical_date": logical_date})["dag_run_id"]
        for dag_id in [*go_dynamic_dag_ids, _SPLIT_DAG_ID, _UNBOUND_DAG_ID]
    }
    runs = {}
    for dag_id, run_id in run_ids.items():
        state = client.wait_for_dag_run(dag_id=dag_id, run_id=run_id, timeout=_GO_TASK_TIMEOUT)
        task_instances = client.get_task_instances(dag_id=dag_id, run_id=run_id)["task_instances"]
        runs[dag_id] = _Run(
            client=client,
            dag_id=dag_id,
            run_id=run_id,
            state=state,
            ti_attrs={ti["task_id"]: ti for ti in task_instances},
        )
    return runs


def test_dynamically_generated_dags_run_their_handler(runs, go_dynamic_dag_ids, compose_instance):
    """
    A Dag that its file generates when it is parsed binds, and its stub task runs.

    ``handlers_a`` registers the handler for the ids it reads from the environment when it starts, which are
    the ids the Dag file generates. The task returns its Dag id and the artifact that ran it.
    """
    bindings = get_task_handler_bindings(compose_instance)
    for dag_id in go_dynamic_dag_ids:
        run = runs[dag_id]
        assert run.state == "success", f"{dag_id} ended {run.state!r}; tasks: {run.ti_attrs}"
        assert run.xcom("greet") == {"dag_id": dag_id, "artifact": "handlers_a"}
        assert bindings.get((dag_id, "greet")) == ArtifactRef(GO_TEST_TASK_HANDLER_BUNDLE, "handlers_a")


def test_each_stub_task_runs_the_artifact_it_is_bound_to(runs):
    """
    The two tasks of one Dag run two different files of the Dag bundle, each the one its task is bound to.

    ``from_a`` is registered by ``handlers_a`` only and ``from_b`` by ``handlers_b`` only, so a task that ran
    another artifact than its own would fail for want of its handler. Each task returns the file that ran it,
    and the worker names that file in the task log before it starts it.
    """
    run = runs[_SPLIT_DAG_ID]
    assert run.state == "success", f"{_SPLIT_DAG_ID} ended {run.state!r}; tasks: {run.ti_attrs}"
    for task_id, artifact in (("from_a", "handlers_a"), ("from_b", "handlers_b")):
        assert run.xcom(task_id) == {"artifact": artifact}
        record = get_running_artifact_record(run.log_records(task_id))
        assert (record["bundle_name"], record["path"]) == (GO_TEST_TASK_HANDLER_BUNDLE, artifact), record


def test_an_unbound_stub_task_fails_with_the_reason_in_its_task_log(runs):
    """
    A stub task queued without an artifact fails in the task runtime, and each try says why in its task log.

    The Dag processor does not route the task's queue, so it never bound the task, although ``handlers_a``
    registers a handler for it. Nothing searches for that handler: the worker starts no artifact, and the
    first try is retried once. The reason is the try's state reason as well.
    """
    run = runs[_UNBOUND_DAG_ID]
    assert run.state == "failed", f"{_UNBOUND_DAG_ID} ended {run.state!r}; tasks: {run.ti_attrs}"
    task_instance = run.ti_attrs["unbound"]
    assert task_instance["state"] == "failed", task_instance
    assert task_instance["try_number"] == 2, task_instance
    assert str(task_instance["state_reason"]).startswith(_UNBOUND_REASON), task_instance

    for try_number in (1, 2):
        records = run.log_records("unbound", try_number)
        errors = [record for record in records if record.get("event") == _CANNOT_RUN_EVENT]
        assert len(errors) == 1, (
            f"Try {try_number} logged {len(errors)} errors; events: {[r.get('event') for r in records]}"
        )
        assert errors[0].get("level") == "error", errors[0]
        assert str(errors[0].get("reason")).startswith(_UNBOUND_REASON), errors[0]
        assert "Running a Lang-SDK artifact" not in {record.get("event") for record in records}

    with pytest.raises(requests.HTTPError) as excinfo:
        run.xcom("unbound")
    assert excinfo.value.response.status_code == HTTPStatus.NOT_FOUND
