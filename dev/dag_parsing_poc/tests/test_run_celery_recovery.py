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
# ruff: noqa: S101
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from unittest import mock
from uuid import uuid4

import pytest
from celery.backends.redis import RedisBackend
from redis import Redis

from airflow.executors.workloads import BundleInfo
from airflow.executors.workloads.parsing import DagDefinitionAttempt, ParseDagDefinitions

from dev.dag_parsing_poc.recovery_checkpoint import check_retired_execution, validate_termination
from dev.dag_parsing_poc.run_celery_recovery import (
    delete_running_backend_record,
    matches_worker_event,
    read_import_events,
    validate_replacement,
)


@pytest.mark.parametrize("response_status", [409, 200])
def test_retirement_probe_contacts_local_api_despite_environment_proxy(monkeypatch, response_status):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            requests.append((self.path, json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
            self.send_response(response_status)
            self.end_headers()

        def log_message(self, *args):
            pass

    monkeypatch.setenv("HTTP_PROXY", "unsupported://proxy.invalid")
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")
    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deadline = datetime(2026, 9, 25, tzinfo=timezone.utc)
    workload = ParseDagDefinitions(
        workload_id=uuid4(),
        bundle_info=BundleInfo(name="poc", version="v1"),
        definitions=tuple(
            DagDefinitionAttempt(
                attempt_id=uuid4(), relative_path=f"{index}.py", source_revision="v1", timeout_seconds=5
            )
            for index in range(2)
        ),
        start_deadline=deadline,
        stop_deadline=deadline + timedelta(seconds=60),
        queue="recovery-only",
        token="fixture",
    )
    execution_id = str(uuid4())
    url = f"http://127.0.0.1:{server.server_port}"
    try:
        if response_status == 409:
            assert check_retired_execution(workload, execution_id, url=url) == [409, 409]
        else:
            with pytest.raises(RuntimeError, match="not fenced"):
                check_retired_execution(workload, execution_id, url=url)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
    route = f"/execution/poc/parsing/workloads/{workload.workload_id}/attempts/{workload.definitions[1].attempt_id}"
    assert [path for path, _ in requests] == [f"{route}/claim", f"{route}/result"]
    assert [body["execution_id"] for _, body in requests] == [execution_id, execution_id]


@pytest.fixture
def evidence():
    run_id = str(uuid4())
    workers = {
        role: {
            "stage": "worker_ready",
            "run_id": run_id,
            "worker_id": str(uuid4()),
            "container_hostname": digit * 12,
            "queue": "poc-recovery",
        }
        for role, digit in (("original", "a"), ("replacement", "b"))
    }
    control = {
        "run_id": run_id,
        **{
            f"{role}_container": {
                "Id": worker["container_hostname"] + worker["container_hostname"][0] * 52,
                "Config": {"Hostname": worker["container_hostname"]},
                "State": {
                    "Running": role == "replacement",
                    "Pid": 123 if role == "replacement" else 0,
                    "ExitCode": 0 if role == "replacement" else 137,
                },
            }
            for role, worker in workers.items()
        },
    }
    return {"control": control, **workers}, run_id


def test_replacement_correlates_readiness_with_container_inspection(evidence):
    values, run_id = evidence
    validate_replacement(**values, run_id=run_id, queue="poc-recovery")


@pytest.mark.parametrize(
    ("keys", "value", "message"),
    [
        (("control", "run_id"), "old-run", "different recovery run"),
        (("original", "run_id"), "old-run", "readiness does not match"),
        (("replacement", "run_id"), "old-run", "readiness does not match"),
        (("replacement", "stage"), "configuration_validated", "readiness does not match"),
        (("replacement", "queue"), "other-queue", "readiness does not match"),
        (("replacement", "worker_id"), "not-a-uuid", "startup identity"),
        (("original", "container_hostname"), "wrong-worker", "original worker's container"),
        (("control", "original_container", "Id"), "c" * 64, "original worker's container"),
        (("control", "replacement_container", "Id"), "c" * 64, "replacement worker's container"),
        (
            ("control", "replacement_container", "Config", "Hostname"),
            "copied-hostname",
            "replacement worker's container",
        ),
        (("control", "original_container", "State", "Running"), True, "kill was not confirmed"),
        (("control", "original_container", "State", "Pid"), 123, "kill was not confirmed"),
        (("control", "original_container", "State", "ExitCode"), 0, "kill was not confirmed"),
        (("control", "replacement_container", "State", "Running"), False, "is not running"),
        (("control", "replacement_container", "State", "Pid"), 0, "is not running"),
    ],
)
def test_unrelated_or_incomplete_evidence_is_rejected(evidence, keys, value, message):
    values, run_id = evidence
    target = values
    for key in keys[:-1]:
        target = target[key]
    target[keys[-1]] = value
    with pytest.raises(RuntimeError, match=message):
        validate_replacement(**values, run_id=run_id, queue="poc-recovery")


@pytest.mark.parametrize("reused", ["worker_id", "container"])
def test_replacement_requires_fresh_worker_and_container(evidence, reused):
    values, run_id = evidence
    if reused == "worker_id":
        values["replacement"]["worker_id"] = values["original"]["worker_id"]
    else:
        values["replacement"]["container_hostname"] = values["original"]["container_hostname"]
        values["control"]["replacement_container"] = values["control"]["original_container"]
    with pytest.raises(RuntimeError, match="different worker and container"):
        validate_replacement(**values, run_id=run_id, queue="poc-recovery")


@pytest.mark.parametrize("state", ["STARTED", "IGNORED", "IMPORT_STARTED"])
@pytest.mark.parametrize("wrong_key", [None, "run_id", "worker_id", "container_hostname", "task_id", "state"])
def test_events_must_identify_run_worker_container_and_delivery(evidence, tmp_path, state, wrong_key):
    values, _ = evidence
    worker = values["original"]
    event = {**worker, "task_id": str(uuid4()), "state": state}
    task_id = event["task_id"]
    if wrong_key:
        event[wrong_key] = "unrelated"
    path = tmp_path / "definition.imports"
    path.write_text(json.dumps(event) + '\n{"incomplete":')
    events = read_import_events(path)
    assert events == [event]
    assert matches_worker_event(events[0], worker, task_id=task_id, state=state) is (wrong_key is None)


@pytest.fixture
def backend():
    backend = mock.create_autospec(RedisBackend, instance=True)
    backend.client = mock.create_autospec(Redis, instance=True)
    backend.get_task_meta.return_value = {"status": "STARTED"}
    backend.get_key_for_task.return_value = b"celery-task-meta-this-task"
    backend.client.delete.return_value = 1
    backend.client.exists.return_value = 0
    return backend


def test_deletion_removes_exact_running_submission(backend):
    assert delete_running_backend_record(backend, "this-task") == 1
    backend.get_task_meta.assert_called_once_with("this-task", cache=False)
    backend.client.delete.assert_called_once_with(b"celery-task-meta-this-task")


@pytest.mark.parametrize("state", ["PENDING", "SUCCESS", "FAILURE"])
def test_deletion_rejects_missing_or_terminal_state(backend, state):
    backend.get_task_meta.return_value = {"status": state}
    with pytest.raises(RuntimeError, match="existing STARTED result"):
        delete_running_backend_record(backend, "this-task")
    backend.client.delete.assert_not_called()


def test_deletion_rejects_key_lost_after_state_lookup(backend):
    backend.client.delete.return_value = 0
    with pytest.raises(RuntimeError, match="exactly one Redis result key"):
        delete_running_backend_record(backend, "this-task")


def test_deletion_rejects_key_recreated_by_a_delivery(backend):
    backend.client.exists.return_value = 1
    with pytest.raises(RuntimeError, match="still exists"):
        delete_running_backend_record(backend, "this-task")


@pytest.mark.parametrize("wrong", [None, "task_id", "worker_id", "run_id", "container", "running"])
def test_checkpoint_termination_correlates_stopped_container_and_workload(evidence, wrong):
    values, run_id = evidence
    worker = values["original"]
    workload_id = str(uuid4())
    event = {**worker, "task_id": workload_id, "state": "STARTED"}
    container = values["control"]["original_container"]
    if wrong in {"task_id", "worker_id", "run_id"}:
        event[wrong] = "unrelated"
    elif wrong == "container":
        container["Id"] = "c" * 64
    elif wrong == "running":
        container["State"]["Running"] = True
    termination = {
        "workload_id": workload_id,
        "evidence": {
            "worker": worker,
            "container": container,
            "run_id": run_id,
            "queue": worker["queue"],
            "events": [event],
        },
    }
    if wrong:
        with pytest.raises(ValueError, match="stopped workload worker"):
            validate_termination(termination)
    else:
        validate_termination(termination)
