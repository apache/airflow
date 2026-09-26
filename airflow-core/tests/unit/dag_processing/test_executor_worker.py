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

import json
import os
import signal
from datetime import timedelta
from multiprocessing import Queue
from queue import SimpleQueue
from types import SimpleNamespace
from unittest.mock import create_autospec, patch
from uuid import uuid4

import httpx
import pytest

from airflow._shared.timezones import timezone
from airflow.dag_processing.executor_worker import (
    ParsingAPIClient,
    ParsingAttemptAlreadyClaimedError,
    ParsingPublicationError,
    ParsingRequestTooLargeError,
    ParsingWorkerError,
    _post_with_retry,
    _publish_result,
    _set_client_environment,
    compute_source_revision,
    get_bundle_root,
    parse_definition,
    resolve_definition_path,
    supervise_dag_parse,
    validate_remote_credentials,
)
from airflow.dag_processing.processor import DagFileProcessorProcess
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.parsing import (
    DagDefinitionAttempt,
    DagDefinitionResult,
    ParseDagDefinitions,
    ParseDagDefinitionsState,
)
from airflow.sdk.api.client import Client


@pytest.fixture
def workload(tmp_path, monkeypatch):
    path = tmp_path / "example.py"
    path.write_text("from airflow.sdk import DAG\ndag = DAG('executor_parsing')\n")
    monkeypatch.setenv(
        "AIRFLOW_DAG_PARSING_POC_BUNDLE_ROOTS",
        json.dumps({"test": {"path": str(tmp_path), "version": "v1"}}),
    )
    monkeypatch.delenv("AIRFLOW_DAG_PARSING_POC_REMOTE", raising=False)
    now = timezone.utcnow()
    return ParseDagDefinitions(
        workload_id=uuid4(),
        token="test-token",
        bundle_info=BundleInfo(name="test", version="v1"),
        definitions=[
            DagDefinitionAttempt(
                attempt_id=uuid4(),
                relative_path="example.py",
                source_revision=compute_source_revision(path),
                timeout_seconds=1,
            )
        ],
        start_deadline=now + timedelta(seconds=60),
        stop_deadline=now + timedelta(seconds=120),
    )


def test_worker_resolves_its_own_root(workload, tmp_path):
    root = get_bundle_root(workload)
    assert root == tmp_path.resolve()
    assert resolve_definition_path(root, workload.definitions[0]) == root / "example.py"
    assert str(tmp_path) not in workload.model_dump_json()


def test_rejects_bundle_version_mismatch(workload):
    workload.bundle_info.version = "v2"
    with pytest.raises(ParsingWorkerError, match="version"):
        get_bundle_root(workload)


@pytest.mark.parametrize("escape", [False, True])
def test_rejects_changed_or_escaping_source(workload, tmp_path, escape):
    path = tmp_path / "example.py"
    if escape:
        external = tmp_path.parent / f"{uuid4()}.py"
        external.write_text(path.read_text())
        path.unlink()
        path.symlink_to(external)
    else:
        path.write_text("changed\n")
    with pytest.raises(ParsingWorkerError, match="escapes|revision"):
        resolve_definition_path(tmp_path, workload.definitions[0])


@patch("airflow.dag_processing.processor.DagFileProcessorProcess.start", autospec=True)
@pytest.mark.parametrize("outcome", ["success", "import_error", "worker_error"])
@pytest.mark.parametrize("include_source", [False, True])
def test_serializes_parser_result(start, workload, tmp_path, outcome, include_source, monkeypatch):
    monkeypatch.setenv("AIRFLOW_DAG_PARSING_POC_INCLUDE_SOURCE", str(int(include_source)))
    process = create_autospec(DagFileProcessorProcess, instance=True)
    start.return_value = process
    process.is_ready = True
    process._check_subprocess_exit.return_value = 0
    process.parsing_result = (
        None
        if outcome == "worker_error"
        else SimpleNamespace(
            serialized_dags=[SimpleNamespace(data={"dag": {"dag_id": "test"}})],
            import_errors={"example.py": "broken import"} if outcome == "import_error" else {},
            warnings=[{"message": "warning"}],
        )
    )
    result = parse_definition(
        workload,
        workload.definitions[0],
        bundle_root=tmp_path,
        client=create_autospec(Client, instance=True),
        log_dir=tmp_path / "logs",
        legacy=True,
    )
    assert result.outcome == outcome
    if outcome != "worker_error":
        assert result.serialized_dags == [{"dag": {"dag_id": "test"}}]
        assert result.warnings == [{"message": "warning"}]
        assert result.source_code == ((tmp_path / "example.py").read_text() if include_source else None)
    else:
        assert "without a serialized result" in result.diagnostics[0]
    assert start.call_args.kwargs["new_process_group"] is True
    assert start.call_args.kwargs["callbacks"] == []
    process.close.assert_called_once()


@patch("airflow.dag_processing.executor_worker.time.monotonic", side_effect=[0, 1, 10, 11], autospec=True)
@patch("airflow.dag_processing.processor.DagFileProcessorProcess.start", autospec=True)
def test_supervised_timeout_kills_parser(start, monotonic, workload, tmp_path):
    process = create_autospec(DagFileProcessorProcess, instance=True)
    start.return_value = process
    process.is_ready = False
    process._check_subprocess_exit.return_value = -9
    result = parse_definition(
        workload,
        workload.definitions[0],
        bundle_root=tmp_path,
        client=create_autospec(Client, instance=True),
        log_dir=tmp_path / "logs",
        legacy=True,
    )
    assert result.outcome == "timeout"
    process.kill.assert_called_once_with(signal.SIGKILL, escalation_delay=1, force=True)
    process.close.assert_called_once()


@patch("airflow.dag_processing.executor_worker.ParsingAPIClient", autospec=True)
@patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("claim_status", ["claimed", "accepted"])
def test_claim_precedes_import_and_result_publication(
    parse, client_factory, workload, tmp_path, claim_status
):
    definition = workload.definitions[0]
    calls = []

    def handle(request):
        calls.append(request.url.path.rsplit("/", 1)[-1])
        assert request.headers["authorization"] == "Bearer test-token"
        if request.url.path.endswith("/claim"):
            assert not parse.called
            return httpx.Response(200, json={"status": claim_status})
        assert parse.called
        payload = json.loads(request.content)
        assert payload["result"]["attempt_id"] == str(definition.attempt_id)
        return httpx.Response(200, json={"status": "accepted"})

    client = Client(
        base_url="http://poc.invalid/execution/", token=workload.token, transport=httpx.MockTransport(handle)
    )
    client_factory.return_value = client
    parse.return_value = DagDefinitionResult(
        attempt_id=definition.attempt_id,
        relative_path=definition.relative_path,
        source_revision=definition.source_revision,
        outcome="success",
        duration_seconds=0.1,
    )
    assert supervise_dag_parse(workload, server="http://poc.invalid/execution/") == 0
    assert calls == (["claim", "result"] if claim_status == "claimed" else ["claim"])
    assert parse.call_count == (1 if claim_status == "claimed" else 0)


@patch("airflow.dag_processing.executor_worker.ParsingAPIClient", autospec=True)
@patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
def test_rejected_claim_never_imports(parse, client_factory, workload):
    client_factory.return_value = ParsingAPIClient(
        base_url="http://poc.invalid/execution/",
        token=workload.token,
        transport=httpx.MockTransport(lambda request: httpx.Response(409, json={"detail": "claimed"})),
    )
    with pytest.raises(ParsingWorkerError, match="HTTP 409"):
        supervise_dag_parse(workload, server="http://poc.invalid/execution/")
    parse.assert_not_called()


@patch("airflow.dag_processing.executor_worker.ParsingAPIClient", autospec=True)
@patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("failure", ["transport", "503"])
def test_retryable_publication_failure_preserves_payload_and_continues_batch(
    parse, client_factory, workload, failure
):
    workload.definitions += (workload.definitions[0].model_copy(update={"attempt_id": uuid4()}),)
    published = []
    claims = []

    def handle(request):
        if request.url.path.endswith("/claim"):
            claims.append(json.loads(request.content))
            return httpx.Response(200, json={"status": "claimed"})
        published.append(request.content)
        if len(published) == 1:
            if failure == "transport":
                raise httpx.ReadError("Acknowledgment lost", request=request)
            return httpx.Response(503, json={"detail": "temporarily unavailable"})
        return httpx.Response(200, json={"status": "accepted"})

    client_factory.return_value = ParsingAPIClient(
        base_url="http://poc.invalid/execution/",
        token=workload.token,
        transport=httpx.MockTransport(handle),
    )
    parse.side_effect = [
        DagDefinitionResult(
            attempt_id=definition.attempt_id,
            relative_path=definition.relative_path,
            source_revision=definition.source_revision,
            outcome="success",
            duration_seconds=0.1,
        )
        for definition in workload.definitions
    ]
    assert supervise_dag_parse(workload, server="http://poc.invalid/execution/") == 0
    assert [call.args[1] for call in parse.call_args_list] == list(workload.definitions)
    assert len(published) == 3
    assert published[0] == published[1]
    assert json.loads(published[2])["result"]["attempt_id"] == str(workload.definitions[1].attempt_id)
    assert len(claims) == 2
    assert claims[0] == claims[1]
    assert {json.loads(body)["execution_id"] for body in published} == {claims[0]["execution_id"]}


@pytest.mark.parametrize("status", [401, 403, 409, 410, 422, 500, 502, 503, 504])
def test_http_failure_retry_policy(workload, status):
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(status, json={"detail": "failed"})

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(ParsingWorkerError, match=f"HTTP {status}"):
            _post_with_retry(
                client, "claim", {"execution_id": str(uuid4())}, deadline=workload.start_deadline
            )
    assert len(requests) == (2 if status >= 500 else 1)


@pytest.mark.parametrize(
    ("operation", "response_body", "claimed_elsewhere"),
    [
        ("claim", {"detail": "Another execution already claimed this attempt"}, True),
        ("result", {"detail": "Another execution already claimed this attempt"}, False),
        ("claim", {"detail": "Result identity differs from registration"}, False),
        ("claim", "invalid json", False),
    ],
)
def test_only_authoritative_active_claim_conflict_gets_duplicate_signal(
    workload, operation, response_body, claimed_elsewhere
):
    calls = []

    def handle(request):
        calls.append(request)
        if isinstance(response_body, dict):
            return httpx.Response(409, json=response_body)
        return httpx.Response(409, text=response_body)

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(ParsingWorkerError) as captured:
            _post_with_retry(client, operation, {}, deadline=workload.start_deadline)
    assert isinstance(captured.value, ParsingAttemptAlreadyClaimedError) is claimed_elsewhere
    assert len(calls) == 1


def test_no_first_publication_after_deadline(workload, time_machine):
    time_machine.move_to(workload.stop_deadline, tick=False)
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json={"status": "accepted"})

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(ParsingWorkerError, match="deadline expired"):
            _post_with_retry(client, "result", {}, deadline=workload.stop_deadline)
    assert not requests


def test_late_claim_reconciliation_has_a_bounded_first_request(workload, time_machine):
    time_machine.move_to(workload.stop_deadline, tick=False)
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json={"status": "accepted"})

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        response = _post_with_retry(
            client, "claim", {}, deadline=workload.start_deadline, allow_late_claim=True
        )
    assert response == {"status": "accepted"}
    assert len(requests) == 1
    assert requests[0].extensions["timeout"] == dict.fromkeys(("connect", "read", "write", "pool"), 5)


def test_late_claim_option_cannot_extend_result_publication(workload, time_machine):
    time_machine.move_to(workload.stop_deadline, tick=False)
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json={"status": "accepted"})

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(ValueError, match="only valid for claim requests"):
            _post_with_retry(client, "result", {}, deadline=workload.stop_deadline, allow_late_claim=True)
    assert not requests


@pytest.mark.parametrize("operation", ["claim", "result"])
@pytest.mark.parametrize("failure", ["transport", "503"])
def test_identical_acknowledgment_retry_can_follow_wall_deadline(workload, time_machine, operation, failure):
    deadline = workload.start_deadline if operation == "claim" else workload.stop_deadline
    time_machine.move_to(deadline - timedelta(seconds=1), tick=False)
    requests = []
    payload = {"execution_id": str(uuid4()), "value": "é"}

    def handle(request):
        requests.append(request)
        if len(requests) == 1:
            time_machine.shift(1)
            if failure == "transport":
                raise httpx.ReadError("Acknowledgment lost", request=request)
            return httpx.Response(503, text="temporarily unavailable")
        assert timezone.utcnow() >= deadline
        return httpx.Response(200, json={"status": "accepted"})

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        assert _post_with_retry(client, operation, payload, deadline=deadline) == {"status": "accepted"}
    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    assert json.loads(requests[1].content) == payload
    assert requests[0].headers["content-type"] == "application/json"
    assert requests[0].extensions["timeout"] == dict.fromkeys(("connect", "read", "write", "pool"), 1)
    assert 0 < requests[1].extensions["timeout"]["read"] <= 5


@patch("airflow.dag_processing.executor_worker.time.monotonic", side_effect=[10, 16], autospec=True)
def test_acknowledgment_retry_start_window_expires_monotonically(monotonic, workload, time_machine):
    time_machine.move_to(workload.stop_deadline - timedelta(seconds=1), tick=False)
    requests = []

    def handle(request):
        requests.append(request)
        time_machine.shift(1)
        return httpx.Response(503, text="temporarily unavailable")

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(ParsingWorkerError, match="recovery budget expired"):
            _post_with_retry(client, "result", {}, deadline=workload.stop_deadline)
    assert len(requests) == 1


@patch("airflow.dag_processing.executor_worker.ParsingAPIClient", autospec=True)
@patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
def test_late_claim_acknowledgment_cannot_start_import(
    parse, get_root, client_factory, workload, time_machine
):
    time_machine.move_to(workload.start_deadline - timedelta(seconds=1), tick=False)
    requests = []

    def handle(request):
        requests.append(request)
        if len(requests) == 1:
            time_machine.shift(1)
            raise httpx.ReadError("Acknowledgment lost", request=request)
        time_machine.move_to(workload.stop_deadline)
        return httpx.Response(200, json={"status": "already_claimed"})

    client_factory.return_value = ParsingAPIClient(
        base_url="http://poc.invalid/execution/", token=workload.token, transport=httpx.MockTransport(handle)
    )
    with pytest.raises(ParsingWorkerError, match="stop deadline expired before bundle access"):
        supervise_dag_parse(workload, server="http://poc.invalid/execution/")
    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    get_root.assert_not_called()
    parse.assert_not_called()


@pytest.mark.parametrize("failure", ["plain-text 500", "transport", "413", "uncertain 413"])
def test_http_failure_survives_executor_queue_and_releases_capacity(workload, failure):
    requests = []

    def handle(request):
        requests.append(request)
        if failure == "transport":
            raise httpx.ConnectError("private error details", request=request)
        if failure == "uncertain 413" and len(requests) == 1:
            raise httpx.ReadError("private error details", request=request)
        if failure in {"413", "uncertain 413"}:
            return httpx.Response(413, text="private error details")
        return httpx.Response(500, text="private error details")

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(ParsingWorkerError) as captured:
            _post_with_retry(client, "result", {}, deadline=workload.stop_deadline)
    assert len(requests) == (1 if failure == "413" else 2)
    assert "private error details" not in str(captured.value)
    assert workload.token not in str(captured.value)

    result_queue = Queue()
    try:
        result_queue.put((workload.key, ParseDagDefinitionsState.FAILED, captured.value))
        received = result_queue.get(timeout=5)
    finally:
        result_queue.close()
        result_queue.join_thread()

    executor = LocalExecutor(parallelism=1)
    executor.running.add(workload.key)
    executor.result_queue = SimpleQueue()
    executor.result_queue.put(received)
    executor._read_results()

    assert not executor.running
    assert executor.slots_available == 1
    state, error = executor.get_event_buffer()[workload.key]
    assert state == ParseDagDefinitionsState.FAILED
    assert isinstance(error, ParsingWorkerError)
    assert str(error) == str(captured.value)
    if failure in {"413", "uncertain 413"}:
        assert isinstance(error, ParsingRequestTooLargeError)
        assert error.uncertain is (failure == "uncertain 413")


@patch("airflow.dag_processing.executor_worker.ParsingAPIClient", autospec=True)
@patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize(
    "outcomes", [("success",), ("import_error",), ("timeout",), ("worker_error",), ("timeout", "success")]
)
def test_publication_failure_attests_only_observed_importer_exits(parse, factory, workload, outcomes):
    workload.definitions = tuple(
        workload.definitions[0].model_copy(update={"attempt_id": uuid4()}) for _ in outcomes
    )
    execution_ids = []
    publications = 0

    def handle(request):
        nonlocal publications
        if request.url.path.endswith("/claim"):
            execution_ids.append(json.loads(request.content)["execution_id"])
            return httpx.Response(200, json={"status": "claimed"})
        publications += 1
        return httpx.Response(200 if publications < len(outcomes) else 503, json={"status": "accepted"})

    factory.return_value = ParsingAPIClient(
        base_url="http://poc.invalid/execution/",
        token=workload.token,
        transport=httpx.MockTransport(handle),
    )
    parse.side_effect = [
        DagDefinitionResult(
            attempt_id=definition.attempt_id,
            relative_path=definition.relative_path,
            source_revision=definition.source_revision,
            outcome=outcome,
            duration_seconds=0,
        )
        for definition, outcome in zip(workload.definitions, outcomes)
    ]
    with pytest.raises(ParsingWorkerError) as captured:
        supervise_dag_parse(workload, server="http://poc.invalid/execution/")
    confirmed = all(outcome in {"success", "import_error"} for outcome in outcomes)
    assert isinstance(captured.value, ParsingPublicationError) is confirmed
    if confirmed:
        queue = Queue()
        try:
            queue.put(captured.value)
            received = queue.get(timeout=5)
        finally:
            queue.close()
            queue.join_thread()
        assert received.execution_id == execution_ids[0]
        assert str(received) == "Parsing API request failed: HTTP 503"


@patch("airflow.dag_processing.executor_worker.ParsingAPIClient", autospec=True)
@patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize(
    "large_field", ["serialized_dags", "import_errors", "warnings", "diagnostics", "source_code"]
)
def test_oversized_result_becomes_compact_error_and_batch_continues(
    parse, client_factory, workload, monkeypatch, large_field
):
    monkeypatch.setattr("airflow.dag_processing.executor_worker.MAX_PARSING_REQUEST_BYTES", 1024)
    workload.definitions += (workload.definitions[0].model_copy(update={"attempt_id": uuid4()}),)
    results = [
        DagDefinitionResult(
            attempt_id=definition.attempt_id,
            relative_path=definition.relative_path,
            source_revision=definition.source_revision,
            outcome="success",
            duration_seconds=0.25,
        )
        for definition in workload.definitions
    ]
    text = "é" * 400
    setattr(
        results[0],
        large_field,
        {"serialized_dags": [{"text": text}], "import_errors": {"example.py": text}, "source_code": text}.get(
            large_field, [text]
        ),
    )
    original = json.dumps(
        {"execution_id": str(uuid4()), "result": results[0].model_dump(mode="json")},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    assert len(original) < 1024 < len(original.encode("utf-8"))
    publications = []

    def handle(request):
        if request.url.path.endswith("/claim"):
            return httpx.Response(200, json={"status": "claimed"})
        assert len(request.content) <= 1024
        publications.append(json.loads(request.content))
        return httpx.Response(200, json={"status": "accepted"})

    client_factory.return_value = ParsingAPIClient(
        base_url="http://poc.invalid/execution/", token=workload.token, transport=httpx.MockTransport(handle)
    )
    parse.side_effect = results
    assert supervise_dag_parse(workload, server="http://poc.invalid/execution/") == 0
    assert len(publications) == 2
    compact, following = (publication["result"] for publication in publications)
    assert compact == {
        "attempt_id": str(workload.definitions[0].attempt_id),
        "relative_path": workload.definitions[0].relative_path,
        "source_revision": workload.definitions[0].source_revision,
        "outcome": "worker_error",
        "duration_seconds": 0.25,
        "serialized_dags": [],
        "source_code": None,
        "import_errors": {},
        "warnings": [],
        "diagnostics": ["Serialized parsing result exceeds the request size limit"],
    }
    assert following["outcome"] == "success"
    assert following["attempt_id"] == str(workload.definitions[1].attempt_id)
    assert parse.call_count == 2


@pytest.fixture
def result(workload):
    definition = workload.definitions[0]
    return DagDefinitionResult(
        attempt_id=definition.attempt_id,
        relative_path=definition.relative_path,
        source_revision=definition.source_revision,
        outcome="success",
        duration_seconds=0.25,
    )


@pytest.mark.parametrize("reject_compact", [False, True])
def test_authoritative_413_allows_one_compact_fallback(workload, result, reject_compact):
    requests = []
    execution_id = str(uuid4())

    def handle(request):
        requests.append(request)
        if len(requests) == 1 or reject_compact:
            return httpx.Response(413, text="stricter server limit")
        return httpx.Response(200, json={"status": "accepted"})

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        if reject_compact:
            with pytest.raises(ParsingRequestTooLargeError, match="HTTP 413"):
                _publish_result(
                    client,
                    "result",
                    execution_id,
                    workload.definitions[0],
                    result,
                    deadline=workload.stop_deadline,
                )
        else:
            _publish_result(
                client,
                "result",
                execution_id,
                workload.definitions[0],
                result,
                deadline=workload.stop_deadline,
            )
    assert len(requests) == 2
    original, compact = (json.loads(request.content) for request in requests)
    assert original["execution_id"] == compact["execution_id"] == execution_id
    assert original["result"]["outcome"] == "success"
    assert compact["result"]["outcome"] == "worker_error"
    assert compact["result"]["attempt_id"] == original["result"]["attempt_id"]


@pytest.mark.parametrize("failure", ["transport", "503"])
def test_uncertain_result_followed_by_413_does_not_change_payload(workload, result, failure):
    requests = []

    def handle(request):
        requests.append(request)
        if len(requests) == 1:
            if failure == "transport":
                raise httpx.ReadError("Acknowledgment lost", request=request)
            return httpx.Response(503, text="temporarily unavailable")
        return httpx.Response(413, text="stricter server limit")

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(ParsingRequestTooLargeError) as captured:
            _publish_result(
                client,
                "result",
                str(uuid4()),
                workload.definitions[0],
                result,
                deadline=workload.stop_deadline,
            )
    assert captured.value.uncertain
    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    assert json.loads(requests[1].content)["result"]["outcome"] == "success"


def test_413_cannot_authorize_new_fallback_after_stop(workload, result, time_machine):
    time_machine.move_to(workload.stop_deadline - timedelta(seconds=1), tick=False)
    requests = []

    def handle(request):
        requests.append(request)
        time_machine.shift(1)
        return httpx.Response(413, text="stricter server limit")

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(ParsingWorkerError, match="deadline expired"):
            _publish_result(
                client,
                "result",
                str(uuid4()),
                workload.definitions[0],
                result,
                deadline=workload.stop_deadline,
            )
    assert len(requests) == 1


def test_oversized_identity_fails_without_truncation_or_publication(workload, result, monkeypatch):
    monkeypatch.setattr("airflow.dag_processing.executor_worker.MAX_PARSING_REQUEST_BYTES", 512)
    definition = workload.definitions[0].model_copy(update={"source_revision": "é" * 600})
    result.source_revision = definition.source_revision
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json={"status": "accepted"})

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token=workload.token, transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(ParsingWorkerError, match="identity and compact error exceed"):
            _publish_result(
                client, "result", str(uuid4()), definition, result, deadline=workload.stop_deadline
            )
    assert definition.source_revision == result.source_revision == "é" * 600
    assert not requests


def test_remote_worker_rejects_signing_key_without_exposing_it(monkeypatch):
    monkeypatch.setenv("AIRFLOW_DAG_PARSING_POC_REMOTE", "1")
    monkeypatch.setenv("AIRFLOW__API_AUTH__JWT_SECRET", "not-for-worker")
    with pytest.raises(ParsingWorkerError) as error:
        validate_remote_credentials()
    assert "JWT_SECRET" in str(error.value)
    assert "not-for-worker" not in str(error.value)


def test_child_environment_restored(monkeypatch):
    monkeypatch.setenv("AIRFLOW__API_AUTH__JWT_SECRET", "parent-value")
    monkeypatch.setenv("_AIRFLOW_PROCESS_CONTEXT", "server")
    with _set_client_environment():
        assert "AIRFLOW__API_AUTH__JWT_SECRET" not in os.environ
        assert os.environ["_AIRFLOW_PROCESS_CONTEXT"] == "client"
        assert os.environ["AIRFLOW__DAG_PROCESSOR__PARSING_PRE_IMPORT_MODULES"] == "false"
    assert os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] == "parent-value"
    assert os.environ["_AIRFLOW_PROCESS_CONTEXT"] == "server"


def test_http_client_does_not_inherit_task_retry_schedule():
    calls = []

    def fail(request):
        calls.append(request)
        raise httpx.ConnectError("offline", request=request)

    with ParsingAPIClient(
        base_url="http://poc.invalid/", token="token", transport=httpx.MockTransport(fail)
    ) as client:
        with pytest.raises(httpx.ConnectError):
            client.get("variables/example")
    assert len(calls) == 1


@patch("airflow.dag_processing.processor.DagFileProcessorProcess.start", autospec=True)
@pytest.mark.parametrize("source_case", ["archive", "non_utf8", "changed_after_check"])
def test_source_publication_errors_discard_serialized_dags(
    start, workload, tmp_path, monkeypatch, source_case
):
    monkeypatch.setenv("AIRFLOW_DAG_PARSING_POC_INCLUDE_SOURCE", "1")
    definition = workload.definitions[0]
    path = tmp_path / definition.relative_path
    if source_case == "archive":
        path = path.rename(path.with_suffix(".zip"))
        definition.relative_path = path.name
    elif source_case == "non_utf8":
        path.write_bytes(b"\xff")
        definition.source_revision = compute_source_revision(path)
    else:
        monkeypatch.setattr(type(path), "read_bytes", lambda self: b"changed after revision check")
    process = create_autospec(DagFileProcessorProcess, instance=True)
    start.return_value = process
    process.is_ready = True
    process._check_subprocess_exit.return_value = 0
    process.parsing_result = SimpleNamespace(
        serialized_dags=[SimpleNamespace(data={"dag": {"dag_id": "test"}})], import_errors={}, warnings=[]
    )
    result = parse_definition(
        workload,
        definition,
        bundle_root=tmp_path,
        client=create_autospec(Client, instance=True),
        log_dir=tmp_path / "logs",
        legacy=True,
    )
    assert result.outcome == "worker_error"
    assert result.serialized_dags == []
    assert result.source_code is None
    assert result.diagnostics
