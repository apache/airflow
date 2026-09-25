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

import pickle
from types import SimpleNamespace
from unittest import mock
from uuid import uuid4

import httpx
import pytest
from celery import states

from airflow.dag_processing import executor_worker
from airflow.providers.celery.executors.celery_executor import CeleryExecutor

from dev.dag_parsing_poc.tests.test_celery_recovery import (
    _build_result,
    _deliver,
    clock as clock,
    recovery as recovery,
)


def test_unknown_claim_signal_survives_executor_exception_transport():
    error = executor_worker.ParsingClaimDispositionUnknownError("Parsing API request failed: HTTP 503")
    received = pickle.loads(pickle.dumps(error))
    assert type(received) is executor_worker.ParsingClaimDispositionUnknownError
    assert str(received) == str(error)


def _build_tracking_executor(recovery, create_app):
    create_app.return_value = recovery.app
    executor = CeleryExecutor(parallelism=1)
    executor.running.add(recovery.workload.key)
    executor.workloads[recovery.workload.key] = recovery.app.AsyncResult(recovery.celery_id)
    recovery.app.backend.store_result(recovery.celery_id, {"worker": "original"}, states.STARTED)
    return executor


def _assert_nonterminal_delivery(recovery, executor, delivery):
    assert delivery.state == states.IGNORED
    backend_state = recovery.app.backend.get_task_meta(recovery.celery_id, cache=False)["status"]
    assert backend_state == states.STARTED
    executor.update_task_state(recovery.workload.key, backend_state, None)
    assert executor.running == {recovery.workload.key}
    assert executor.slots_available == 0
    assert not executor.get_event_buffer()


def _open_intercepted_client(kwargs, intercept):
    return httpx.Client(
        base_url=kwargs["base_url"],
        headers={"Authorization": f"Bearer {kwargs['token']}"},
        transport=httpx.MockTransport(intercept),
        trust_env=False,
    )


@mock.patch("airflow.providers.celery.executors.celery_executor_utils.create_celery_app", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("definition_index", [0, 1])
@pytest.mark.parametrize("claimed_elsewhere", [False, True])
@pytest.mark.parametrize("failure", ["transport", "503"])
def test_unknown_claim_keeps_capacity_and_does_not_invalidate_original(
    parse, get_root, create_app, recovery, monkeypatch, definition_index, claimed_elsewhere, failure
):
    workload = recovery.workload
    original_execution = uuid4()
    for completed in workload.definitions[:definition_index]:
        recovery.store.claim(workload.workload_id, completed.attempt_id, original_execution)
        recovery.store.accept_result(
            workload.workload_id,
            completed.attempt_id,
            original_execution,
            _build_result(workload, completed),
        )
    definition = workload.definitions[definition_index]
    if claimed_elsewhere:
        recovery.store.claim(workload.workload_id, definition.attempt_id, original_execution)
    attempts = recovery.store.get_attempts(workload.workload_id)
    executor = _build_tracking_executor(recovery, create_app)
    requests = []

    with executor_worker.ParsingAPIClient(
        base_url="http://testserver/execution/", token=workload.token
    ) as api_client:

        def intercept(request):
            if request.url.path.endswith(f"/{definition.attempt_id}/claim"):
                requests.append(request)
                if failure == "transport":
                    raise httpx.ReadTimeout("Claim acknowledgment unavailable", request=request)
                return httpx.Response(503, json={"detail": "temporarily unavailable"})
            return api_client.send(request)

        monkeypatch.setattr(
            executor_worker, "ParsingAPIClient", lambda **kwargs: _open_intercepted_client(kwargs, intercept)
        )
        delivery = _deliver(recovery)

    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    parse.assert_not_called()
    get_root.assert_not_called()
    assert recovery.store.get_attempts(workload.workload_id) == attempts
    _assert_nonterminal_delivery(recovery, executor, delivery)
    if claimed_elsewhere:
        recovery.store.accept_result(
            workload.workload_id,
            definition.attempt_id,
            original_execution,
            _build_result(workload, definition),
        )
        assert len(recovery.store.get_results(workload.workload_id)) == definition_index + 1


@mock.patch("airflow.providers.celery.executors.celery_executor_utils.create_celery_app", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("definition_index", [0, 1])
def test_lost_claim_acknowledgment_keeps_partial_batch_nonterminal(
    parse, get_root, create_app, recovery, monkeypatch, definition_index
):
    workload = recovery.workload
    definition = workload.definitions[definition_index]
    get_root.return_value = recovery.root
    parse.side_effect = _build_result
    executor = _build_tracking_executor(recovery, create_app)
    requests = []

    with executor_worker.ParsingAPIClient(
        base_url="http://testserver/execution/", token=workload.token
    ) as api_client:

        def intercept(request):
            if request.url.path.endswith(f"/{definition.attempt_id}/claim"):
                requests.append(request)
                if len(requests) == 1:
                    assert api_client.send(request).status_code == 200
                    raise httpx.ReadTimeout("Committed claim acknowledgment lost", request=request)
                return httpx.Response(503, json={"detail": "temporarily unavailable"})
            return api_client.send(request)

        monkeypatch.setattr(
            executor_worker, "ParsingAPIClient", lambda **kwargs: _open_intercepted_client(kwargs, intercept)
        )
        delivery = _deliver(recovery)

    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    assert parse.call_count == definition_index
    assert get_root.call_count == definition_index
    assert [attempt["status"] for attempt in recovery.store.get_attempts(workload.workload_id)] == (
        ["accepted"] * definition_index + ["claimed"] + ["pending"] * (1 - definition_index)
    )
    assert len(recovery.store.get_results(workload.workload_id)) == definition_index
    _assert_nonterminal_delivery(recovery, executor, delivery)


@pytest.mark.parametrize("operation", ["claim", "result"])
def test_expired_ack_recovery_budget_distinguishes_unknown_claim_from_result_failure(
    recovery, monkeypatch, operation
):
    requests = []
    monkeypatch.setattr(
        executor_worker,
        "time",
        SimpleNamespace(monotonic=mock.create_autospec(executor_worker.time.monotonic, side_effect=[10, 16])),
    )

    def unavailable(request):
        requests.append(request)
        return httpx.Response(503, text="temporarily unavailable")

    with httpx.Client(base_url="http://testserver/", transport=httpx.MockTransport(unavailable)) as client:
        with pytest.raises(executor_worker.ParsingWorkerError, match="recovery budget expired") as captured:
            executor_worker._post_with_retry(client, operation, {}, deadline=recovery.workload.start_deadline)

    assert len(requests) == 1
    assert type(captured.value) is (
        executor_worker.ParsingClaimDispositionUnknownError
        if operation == "claim"
        else executor_worker.ParsingWorkerError
    )


@pytest.mark.parametrize("status", [401, 403, 409, 410, 422])
def test_definitive_claim_http_errors_remain_worker_failures(recovery, status):
    with httpx.Client(
        base_url="http://testserver/",
        transport=httpx.MockTransport(lambda request: httpx.Response(status, json={"detail": "rejected"})),
    ) as client:
        with pytest.raises(executor_worker.ParsingWorkerError, match=f"HTTP {status}") as captured:
            executor_worker._post_with_retry(client, "claim", {}, deadline=recovery.workload.start_deadline)
    assert type(captured.value) is executor_worker.ParsingWorkerError
