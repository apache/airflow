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
from __future__ import annotations

from dataclasses import replace
from unittest import mock

import httpx2
import pytest
from uuid6 import uuid7

from airflow.api_fastapi.execution_api.datamodels.dag_parsing import ProcessorWorkItem
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.api_requested_work import ExecutionAPIWorkSource
from airflow.dag_processing.bundles.local import LocalDagBundle

from tests_common.test_utils.config import conf_vars


def _make_work(bundle_name, relative_fileloc="a.py", *, callback=None):
    return ProcessorWorkItem(
        id=str(uuid7()),
        claim_id=uuid7(),
        bundle_name=bundle_name,
        relative_fileloc=relative_fileloc,
        callback=callback.model_dump_json() if callback is not None else None,
    )


def _make_status_error(status_code):
    request = httpx2.Request("POST", "http://api.invalid/ack")
    return httpx2.HTTPStatusError(
        "acknowledgment failed", request=request, response=httpx2.Response(status_code, request=request)
    )


@pytest.fixture
def api_source(tmp_path):
    (tmp_path / "a.py").touch()
    bundle = LocalDagBundle(name="testing", path=str(tmp_path), refresh_interval=0)
    bundle.is_initialized = True
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    client.claim_work.return_value = []
    return ExecutionAPIWorkSource(client), client, bundle


def _claim_priority(source, bundle, *, capacity=2, inventory_sequence=8, active_attempts=()):
    return source.claim_priority_files(
        [bundle],
        capacity=capacity,
        unavailable_bundles=set(),
        active_attempts=active_attempts,
        inventory_sequence=inventory_sequence,
    )


def _claim_callback(source, client, bundle):
    request = DagCallbackRequest(
        filepath="a.py", bundle_name=bundle.name, bundle_version=None, dag_id="test", run_id="run"
    )
    work = _make_work(bundle.name, callback=request)
    client.claim_work.return_value = [work]
    return source.fetch_callbacks([bundle], limit=1)[0], work


def test_callback_lifecycle_preserves_equal_payload_claim_identity(api_source):
    source, client, bundle = api_source
    assert source.requires_acknowledgment
    assert not source.has_outstanding_work()
    request = DagCallbackRequest(
        filepath="a.py", bundle_name=bundle.name, bundle_version=None, dag_id="test", run_id="run"
    )
    work = [_make_work(bundle.name, callback=request) for _ in range(2)]
    client.claim_work.return_value = work
    first, second = source.fetch_callbacks([bundle], limit=2)

    assert first == second
    assert first is not second
    assert source.has_callback_claim(first)
    assert source.has_callback_claim(second)
    assert not source.has_callback_claim(request)
    assert source.has_outstanding_work()
    source.callback_queued(first)
    assert source.fetch_callbacks([bundle], limit=2) == [second]
    assert source.has_callback_claim(first)
    source.callback_queued(second)
    assert source.fetch_callbacks([bundle], limit=2) == []
    client.claim_work.assert_called_once_with("callbacks", [bundle.name], 2)

    attempt_id = uuid7()
    source.callbacks_started(attempt_id, [first, second, request])
    assert not source.has_callback_claim(first)
    assert not source.has_callback_claim(second)
    assert source.has_callback_attempt(attempt_id)
    assert source.has_outstanding_work()
    assert source.fetch_callbacks([bundle], limit=2) == []
    client.claim_work.assert_called_once()

    source.attempt_finished(uuid7(), had_callbacks=True)
    assert source.has_callback_attempt(attempt_id)
    source.attempt_finished(attempt_id, had_callbacks=True)
    source.attempt_finished(attempt_id, had_callbacks=True)
    assert not source.has_callback_attempt(attempt_id)
    assert source.has_outstanding_work()
    source.flush()

    assert client.acknowledge_work.call_args_list == [mock.call("callbacks", item) for item in work]
    assert not source.has_outstanding_work()


def test_callback_finish_does_not_acknowledge_priority_claim_for_same_attempt(api_source):
    source, client, bundle = api_source
    request, callback_work = _claim_callback(source, client, bundle)
    source.callback_queued(request)
    priority_work = _make_work(bundle.name)
    client.claim_work.return_value = [priority_work]
    file = _claim_priority(source, bundle)[0]
    attempt_id = uuid7()
    source.callbacks_started(attempt_id, [request])
    source.parse_started(file, attempt_id)

    source.attempt_finished(attempt_id, had_callbacks=True)
    source.attempt_finished(attempt_id, had_callbacks=True)
    source.flush()

    client.acknowledge_work.assert_called_once_with("callbacks", callback_work)
    assert not source.has_callback_attempt(attempt_id)
    assert source.has_priority_request(file)
    assert source.has_outstanding_work()
    source.attempt_finished(attempt_id, had_callbacks=False)
    source.flush()
    assert client.acknowledge_work.call_args_list == [
        mock.call("callbacks", callback_work),
        mock.call("priority", priority_work),
    ]
    assert not source.has_outstanding_work()


def test_priority_finish_keeps_newer_claim_for_same_file(api_source):
    source, client, bundle = api_source
    first, second = [_make_work(bundle.name) for _ in range(2)]
    client.claim_work.return_value = [first]
    file = _claim_priority(source, bundle)[0]
    assert source.has_outstanding_work()
    assert source.has_priority_request(file)
    assert not source.is_priority_ready(file, 7)
    assert source.is_priority_ready(file, 8)
    assert not source.has_priority_request(replace(file, bundle_version="v1"))
    attempt_id = uuid7()
    source.parse_started(file, attempt_id)

    client.claim_work.return_value = [second]
    assert _claim_priority(source, bundle, inventory_sequence=9, active_attempts={attempt_id}) == [file]
    source.attempt_finished(attempt_id, had_callbacks=False)
    source.attempt_finished(attempt_id, had_callbacks=False)
    source.flush()

    client.acknowledge_work.assert_called_once_with("priority", first)
    assert source.has_outstanding_work()
    assert source.has_priority_request(file)
    assert not source.is_priority_ready(file, 8)
    second_attempt = uuid7()
    source.parse_started(file, second_attempt)
    source.attempt_finished(second_attempt, had_callbacks=False)
    assert source.has_outstanding_work()
    source.flush()
    assert client.acknowledge_work.call_args_list == [mock.call("priority", item) for item in (first, second)]
    assert not source.has_outstanding_work()


@pytest.mark.parametrize("failure", [httpx2.ConnectError("unavailable"), _make_status_error(503)])
def test_flush_retries_mixed_kinds_in_fifo_order(api_source, failure):
    source, client, bundle = api_source
    request, callback_work = _claim_callback(source, client, bundle)
    source.callback_discarded(request)
    priority_work = _make_work(bundle.name)
    client.claim_work.return_value = [priority_work]
    _claim_priority(source, bundle)
    source.bundle_unusable(bundle.name, active_attempts=set())
    client.acknowledge_work.side_effect = [failure, None, None]

    source.flush()

    client.acknowledge_work.assert_called_once_with("callbacks", callback_work)
    assert source.has_outstanding_work()
    source.flush()
    assert client.acknowledge_work.call_args_list == [
        mock.call("callbacks", callback_work),
        mock.call("callbacks", callback_work),
        mock.call("priority", priority_work),
    ]
    assert not source.has_outstanding_work()


@pytest.mark.parametrize("status_code", [400, 403, 409])
def test_flush_discards_permanent_refusal_and_continues(api_source, status_code):
    source, client, bundle = api_source
    request, callback_work = _claim_callback(source, client, bundle)
    source.callback_discarded(request)
    priority_work = _make_work(bundle.name)
    client.claim_work.return_value = [priority_work]
    _claim_priority(source, bundle)
    source.bundle_unusable(bundle.name, active_attempts=set())
    client.acknowledge_work.side_effect = [_make_status_error(status_code), None]

    source.flush()

    assert client.acknowledge_work.call_args_list == [
        mock.call("callbacks", callback_work),
        mock.call("priority", priority_work),
    ]
    assert not source.has_outstanding_work()


@mock.patch("airflow.dag_processing.api_requested_work.time.monotonic", autospec=True, return_value=10)
def test_flush_preserves_pending_work_after_deadline(monotonic, api_source):
    source, client, bundle = api_source
    request, work = _claim_callback(source, client, bundle)
    source.callback_discarded(request)

    source.flush(deadline=10)

    client.acknowledge_work.assert_not_called()
    assert source.has_outstanding_work()
    source.flush(deadline=11)
    client.acknowledge_work.assert_called_once_with("callbacks", work)
    assert not source.has_outstanding_work()


@mock.patch("airflow.dag_processing.api_requested_work.time.monotonic", autospec=True, return_value=10)
@conf_vars({("dag_processor", "job_heartbeat_timeout"): "3"})
def test_callback_preparation_defers_then_expires_without_duplicate_ack(monotonic, api_source):
    source, client, bundle = api_source
    request, work = _claim_callback(source, client, bundle)
    assert source.is_callback_preparation_ready(request)

    source.callback_deferred(request)

    assert not source.is_callback_preparation_ready(request)
    assert source.has_outstanding_work()
    monotonic.return_value = 11
    assert source.is_callback_preparation_ready(request)
    source.callback_deferred(request)
    assert not source.is_callback_preparation_ready(request)
    monotonic.return_value = 13
    assert source.is_callback_preparation_ready(request)
    source.callback_deferred(request)
    source.callback_deferred(request)
    source.callback_discarded(request)
    assert not source.has_callback_claim(request)
    assert source.fetch_callbacks([bundle], limit=1) == []
    assert source.has_outstanding_work()
    source.flush()

    client.acknowledge_work.assert_called_once_with("callbacks", work)
    assert not source.has_outstanding_work()


@pytest.mark.parametrize(
    ("sequence", "files", "failed_sources", "acknowledged"),
    [
        (7, [], [], False),
        (8, ["a.py"], [], False),
        (8, [], ["a.py"], False),
        (8, [], [], True),
    ],
)
def test_inventory_reconciles_only_authoritative_missing_files(
    api_source, sequence, files, failed_sources, acknowledged
):
    source, client, bundle = api_source
    work = _make_work(bundle.name)
    client.claim_work.return_value = [work]
    file = _claim_priority(source, bundle)[0]

    source.inventory_published(bundle.name, sequence=sequence, files=files, failed_sources=failed_sources)
    source.flush()

    assert source.has_priority_request(file) is not acknowledged
    assert source.has_outstanding_work() is not acknowledged
    if acknowledged:
        client.acknowledge_work.assert_called_once_with("priority", work)
    else:
        client.acknowledge_work.assert_not_called()


def test_unusable_bundle_keeps_active_claim_until_attempt_finishes(api_source):
    source, client, bundle = api_source
    work = [_make_work(bundle.name, path) for path in ("a.py", "missing.py")]
    client.claim_work.return_value = work
    first, second = _claim_priority(source, bundle)
    attempt_id = uuid7()
    source.parse_started(first, attempt_id)

    source.bundle_unusable(bundle.name, active_attempts={attempt_id})
    source.flush()

    client.acknowledge_work.assert_called_once_with("priority", work[1])
    assert source.has_priority_request(first)
    assert not source.has_priority_request(second)
    source.attempt_finished(attempt_id, had_callbacks=False)
    source.flush()
    assert client.acknowledge_work.call_args_list == [
        mock.call("priority", work[1]),
        mock.call("priority", work[0]),
    ]
    assert not source.has_outstanding_work()


def test_priority_capacity_retains_unavailable_claims_without_blocking_healthy_bundles(api_source):
    source, client, bundle = api_source
    work = [_make_work(bundle.name, path) for path in ("a.py", "missing.py")]
    client.claim_work.return_value = work
    first, _ = _claim_priority(source, bundle)
    attempt_id = uuid7()
    source.parse_started(first, attempt_id)
    healthy = LocalDagBundle(name="healthy", path=str(bundle.path), refresh_interval=0)
    healthy.is_initialized = True
    client.claim_work.return_value = []

    source.claim_priority_files(
        [bundle, healthy],
        capacity=2,
        unavailable_bundles={bundle.name},
        active_attempts={attempt_id},
        inventory_sequence=9,
    )

    client.claim_work.assert_called_with("priority", [healthy.name], 1)
    assert source.has_priority_request(first)
    assert source.has_outstanding_work()


@pytest.mark.parametrize("kind", ["callbacks", "priority"])
def test_claim_failure_retains_outstanding_work(api_source, kind):
    source, client, bundle = api_source
    if kind == "callbacks":
        request, _ = _claim_callback(source, client, bundle)
    else:
        client.claim_work.return_value = [_make_work(bundle.name)]
        file = _claim_priority(source, bundle)[0]
    client.claim_work.side_effect = httpx2.ConnectError("unavailable")

    if kind == "callbacks":
        assert source.fetch_callbacks([bundle], limit=2) == [request]
        assert source.has_callback_claim(request)
    else:
        assert _claim_priority(source, bundle) == []
        assert source.has_priority_request(file)
    assert source.has_outstanding_work()
