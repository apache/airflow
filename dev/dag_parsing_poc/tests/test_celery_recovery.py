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

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock
from uuid import UUID, uuid4

import httpx
import pytest
import time_machine
from celery import Celery, states
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from fastapi.testclient import TestClient

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.dag_processing import executor_worker
from airflow.executors.workloads.base import BundleInfo, WorkloadType
from airflow.executors.workloads.parsing import (
    DagDefinitionAttempt,
    DagDefinitionResult,
    ParseDagDefinitions,
    ParseDagDefinitionsState,
)
from airflow.providers.celery.executors import celery_executor_utils
from airflow.providers.celery.executors.celery_executor import CeleryExecutor

from dev.dag_parsing_poc.api import create_app
from dev.dag_parsing_poc.store import ReceiptConflictError, ReceiptStore

NOW = datetime(2026, 9, 25, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clock():
    with time_machine.travel(NOW, tick=False) as traveller:
        yield traveller


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    workload = ParseDagDefinitions(
        workload_id=uuid4(),
        bundle_info=BundleInfo(name="recovery", version="v1"),
        definitions=tuple(
            DagDefinitionAttempt(
                attempt_id=uuid4(),
                relative_path=f"definition_{index}.py",
                source_revision=f"revision-{index}",
                timeout_seconds=5,
            )
            for index in range(2)
        ),
        start_deadline=NOW + timedelta(seconds=30),
        stop_deadline=NOW + timedelta(seconds=60),
        queue="parsing-only",
        token="replace-before-dispatch",
    )
    key = Ed25519PrivateKey.generate()
    workload.token = JWTGenerator(
        private_key=key,
        kid="dag-parsing-poc",
        issuer="dag-parsing-poc",
        audience="dag-parsing-poc",
        algorithm="EdDSA",
        valid_for=120,
    ).generate(
        {
            "sub": str(workload.workload_id),
            "scope": "dag-parsing-poc",
            "attempt_ids": [str(definition.attempt_id) for definition in workload.definitions],
        }
    )
    public_key_path = tmp_path / "public.pem"
    public_key_path.write_bytes(
        key.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo)
    )
    store = ReceiptStore(tmp_path / "receipts.sqlite")
    store.register_workload(workload)
    real_client = executor_worker.ParsingAPIClient
    celery_app = Celery(
        f"parsing-recovery-{uuid4()}", broker="memory://", backend="cache+memory://", set_as_current=False
    )
    celery_app.conf.update(task_store_eager_result=True, task_eager_propagates=False)
    task = celery_app.task(name=f"parse-recovery-{uuid4()}")(
        celery_executor_utils.execute_workload.__wrapped__
    )
    monkeypatch.delenv("AIRFLOW_DAG_PARSING_POC_REMOTE", raising=False)
    with TestClient(create_app(store.path, public_key_path)) as client:

        def send(request):
            response = client.request(
                request.method, request.url.path, content=request.content, headers=request.headers
            )
            return httpx.Response(
                response.status_code, content=response.content, headers=response.headers, request=request
            )

        def open_client(**kwargs):
            return real_client(**kwargs, transport=httpx.MockTransport(send))

        monkeypatch.setattr(executor_worker, "ParsingAPIClient", open_client)
        yield SimpleNamespace(
            workload=workload,
            store=store,
            app=celery_app,
            task=task,
            root=tmp_path,
            celery_id=str(workload.workload_id),
        )
    celery_app.close()


def _build_result(workload, definition, **kwargs):
    return DagDefinitionResult(
        attempt_id=definition.attempt_id,
        relative_path=definition.relative_path,
        source_revision=definition.source_revision,
        outcome="success",
        serialized_dags=[{"dag": {"dag_id": definition.relative_path}}],
        duration_seconds=0.1,
    )


def _deliver(recovery):
    return recovery.task.apply(
        args=(recovery.workload.model_dump_json(),), task_id=recovery.celery_id, throw=False
    )


@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("replay_after", [None, "start_deadline", "stop_deadline"])
def test_accepted_batch_redelivery_does_not_reimport(parse, get_root, recovery, clock, replay_after):
    get_root.return_value = recovery.root
    parse.side_effect = _build_result
    first = _deliver(recovery)
    assert first.state == states.SUCCESS
    receipts = recovery.store.get_results(recovery.workload.workload_id)
    assert len(receipts) == 2
    assert parse.call_count == 2

    if replay_after is not None:
        clock.move_to(getattr(recovery.workload, replay_after) + timedelta(seconds=1))
    replay = _deliver(recovery)
    assert replay.state == states.SUCCESS
    assert parse.call_count == 2
    assert recovery.store.get_results(recovery.workload.workload_id) == receipts


@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("after_start_deadline", [False, True])
def test_active_claim_duplicate_does_not_poison_celery_result(
    parse, get_root, recovery, clock, after_start_deadline
):
    workload = recovery.workload
    original_execution_id = uuid4()
    definition = workload.definitions[0]
    recovery.store.claim(workload.workload_id, definition.attempt_id, original_execution_id)
    recovery.app.backend.store_result(recovery.celery_id, {"worker": "original"}, states.STARTED)
    if after_start_deadline:
        clock.move_to(workload.start_deadline + timedelta(seconds=1))

    duplicate = _deliver(recovery)
    assert duplicate.state == states.IGNORED
    assert recovery.app.backend.get_task_meta(recovery.celery_id)["status"] == states.STARTED
    parse.assert_not_called()
    get_root.assert_not_called()
    assert recovery.store.get_attempts(workload.workload_id)[0]["execution_id"] == str(original_execution_id)

    recovery.store.accept_result(
        workload.workload_id,
        definition.attempt_id,
        original_execution_id,
        _build_result(workload, definition),
    )
    assert len(recovery.store.get_results(workload.workload_id)) == 1


@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("claim_after", ["start_deadline", "stop_deadline"])
def test_new_late_claim_fails_without_importing(parse, get_root, recovery, clock, claim_after):
    workload = recovery.workload
    clock.move_to(getattr(workload, claim_after) + timedelta(seconds=1))
    delivery = _deliver(recovery)
    assert delivery.state == states.FAILURE
    parse.assert_not_called()
    get_root.assert_not_called()
    assert [attempt["status"] for attempt in recovery.store.get_attempts(workload.workload_id)] == [
        "pending",
        "pending",
    ]
    assert recovery.store.get_results(workload.workload_id) == []


@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@mock.patch("airflow.providers.celery.executors.celery_executor_utils.create_celery_app", autospec=True)
def test_worker_loss_keeps_partial_receipts_and_requires_recovery(
    create_app, parse, get_root, recovery, clock
):
    workload = recovery.workload
    get_root.return_value = recovery.root
    parse.side_effect = [_build_result(workload, workload.definitions[0]), SystemExit("worker lost")]
    recovery.app.backend.store_result(recovery.celery_id, {"worker": "original"}, states.STARTED)
    with pytest.raises(SystemExit, match="worker lost"):
        _deliver(recovery)
    receipts = recovery.store.get_results(workload.workload_id)
    assert len(receipts) == 1
    attempts = recovery.store.get_attempts(workload.workload_id)
    assert [attempt["status"] for attempt in attempts] == ["accepted", "claimed"]
    assert UUID(attempts[1]["execution_id"]) != workload.workload_id

    create_app.return_value = recovery.app
    executor = CeleryExecutor(parallelism=1)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    executor.running.add(workload.key)
    executor.workloads[workload.key] = recovery.app.AsyncResult(recovery.celery_id)
    executor.update_task_state(workload.key, states.PENDING, None)
    assert executor.slots_available == 0
    executor.update_task_state(workload.key, states.FAILURE, "worker lost")
    assert executor.slots_available == 1
    assert executor.get_event_buffer()[workload.key] == (ParseDagDefinitionsState.FAILED, "worker lost")
    assert recovery.store.get_results(workload.workload_id) == receipts

    redelivery = _deliver(recovery)
    assert redelivery.state == states.IGNORED
    assert parse.call_count == 2
    assert recovery.store.get_attempts(workload.workload_id) == attempts
    clock.move_to(workload.stop_deadline + timedelta(seconds=1))
    with pytest.raises(ReceiptConflictError):
        recovery.store.claim(workload.workload_id, workload.definitions[1].attempt_id, uuid4())
    assert recovery.store.get_results(workload.workload_id) == receipts
