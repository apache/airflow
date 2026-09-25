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
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from sqlite3 import Connection, connect
from threading import Event
from unittest import mock
from uuid import uuid4

import httpx
import pytest
import time_machine
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from fastapi.testclient import TestClient

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.dag_processing import executor_worker
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.parsing import (
    MAX_PARSING_REQUEST_BYTES,
    DagDefinitionAttempt,
    DagDefinitionResult,
    ParseDagDefinitions,
)

from dev.dag_parsing_poc.api import create_app
from dev.dag_parsing_poc.store import ReceiptConflictError, ReceiptStore

NOW = datetime(2026, 9, 25, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clock():
    with time_machine.travel(NOW, tick=False) as traveller:
        yield traveller


@pytest.fixture
def workload(request):
    return ParseDagDefinitions(
        workload_id=uuid4(),
        bundle_info=BundleInfo(name="example", version="v1"),
        definitions=(
            DagDefinitionAttempt(
                attempt_id=uuid4(),
                relative_path="good.py",
                source_revision="sha256:example",
                timeout_seconds=5,
            ),
            DagDefinitionAttempt(
                attempt_id=uuid4(), relative_path="bad.py", source_revision="sha256:bad", timeout_seconds=5
            ),
        )[: getattr(request, "param", 2)],
        start_deadline=NOW + timedelta(seconds=30),
        stop_deadline=NOW + timedelta(seconds=60),
        token="not-persisted",
    )


@pytest.fixture
def result(workload):
    definition = workload.definitions[0]
    return DagDefinitionResult(
        attempt_id=definition.attempt_id,
        relative_path=definition.relative_path,
        source_revision=definition.source_revision,
        outcome="success",
        serialized_dags=[{"dag": {"dag_id": "example", "tasks": []}}],
        diagnostics=["example diagnostic"],
        warnings=[{"message": "example warning", "category": "warning"}],
        duration_seconds=0.1,
    )


@pytest.fixture
def key():
    return Ed25519PrivateKey.generate()


@pytest.fixture
def issue_token(key, workload):
    def issue(*, extras=None, private_key=None, audience="dag-parsing-poc", valid_for=120):
        generator = JWTGenerator(
            private_key=private_key or key,
            kid="dag-parsing-poc",
            issuer="dag-parsing-poc",
            audience=audience,
            algorithm="EdDSA",
            valid_for=valid_for,
        )
        return generator.generate(
            {
                "sub": str(workload.workload_id),
                "scope": "dag-parsing-poc",
                "attempt_ids": [str(attempt.attempt_id) for attempt in workload.definitions],
            }
            | (extras or {})
        )

    return issue


@pytest.fixture
def store(tmp_path, workload):
    store = ReceiptStore(tmp_path / "receipts.sqlite")
    store.register_workload(workload)
    return store


@pytest.fixture
def app(tmp_path, key, store, request):
    public_path = tmp_path / "public.pem"
    public_path.write_bytes(key.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo))
    options = {"max_request_bytes": request.param} if hasattr(request, "param") else {}
    return create_app(store.path, public_path, **options)


@pytest.fixture
def client(app, issue_token):
    with TestClient(app) as client:
        client.headers["Authorization"] = f"Bearer {issue_token()}"
        yield client


def _forward_worker_request(client, request):
    response = client.request(
        request.method, request.url.path, content=request.content, headers=request.headers
    )
    return httpx.Response(
        response.status_code, content=response.content, headers=response.headers, request=request
    )


@pytest.fixture
def install_worker_http_bridge(client, monkeypatch):
    client.headers.pop("Authorization", None)
    real_client = executor_worker.ParsingAPIClient

    def install(intercept=None):
        exchanges = []

        def send(request):
            exchange = {"request": request, "status": None}
            exchanges.append(exchange)

            def forward():
                return _forward_worker_request(client, request)

            response = intercept(request, forward) if intercept else forward()
            exchange["status"] = response.status_code
            return response

        def open_client(**kwargs):
            return real_client(**kwargs, transport=httpx.MockTransport(send))

        monkeypatch.setattr(executor_worker, "ParsingAPIClient", open_client)
        return exchanges

    return install


def _get_route(workload, definition_index=0):
    return (
        f"/execution/poc/parsing/workloads/{workload.workload_id}"
        f"/attempts/{workload.definitions[definition_index].attempt_id}"
    )


def test_claim_and_result_are_durable_and_idempotent(client, workload, result, store):
    route = _get_route(workload)
    execution_id = str(uuid4())
    claim = {"execution_id": execution_id}
    assert client.post(f"{route}/claim", json=claim).json() == claim | {"status": "claimed"}
    assert client.post(f"{route}/claim", json=claim).json() == claim | {"status": "already_claimed"}
    payload = claim | {"result": result.model_dump(mode="json")}
    first = client.post(f"{route}/result", json=payload)
    assert first.status_code == 200
    assert first.json()["status"] == "accepted"
    assert client.post(f"{route}/result", json=payload).json() == first.json()
    assert client.post(f"{route}/claim", json=claim).json() == claim | {"status": "accepted"}

    reopened = ReceiptStore(store.path)
    assert reopened.get_results(workload.workload_id) == [result.model_dump(mode="json")]
    assert [attempt["status"] for attempt in reopened.get_attempts(workload.workload_id)] == [
        "accepted",
        "pending",
    ]
    assert "not-persisted" not in Path(store.path).read_bytes().decode("latin-1")


@pytest.mark.parametrize("endpoint", ["claim", "result"])
@pytest.mark.parametrize(
    "auth_case",
    [
        "missing",
        "invalid",
        "wrong_scope",
        "wrong_subject",
        "wrong_attempt",
        "wrong_audience",
        "signature",
        "expired",
    ],
)
def test_rejects_unauthorized_requests(client, workload, result, issue_token, auth_case, endpoint):
    headers = {}
    if auth_case == "missing":
        del client.headers["Authorization"]
    else:
        arguments = {
            "invalid": None,
            "wrong_scope": {"extras": {"scope": "execution"}},
            "wrong_subject": {"extras": {"sub": str(uuid4())}},
            "wrong_attempt": {"extras": {"attempt_ids": [str(uuid4())]}},
            "wrong_audience": {"audience": "urn:airflow.apache.org:task"},
            "signature": {"private_key": Ed25519PrivateKey.generate()},
            "expired": {"valid_for": -1},
        }[auth_case]
        token = "not-a-jwt" if arguments is None else issue_token(**arguments)
        headers["Authorization"] = f"Bearer {token}"
    payload = {"execution_id": str(uuid4())}
    if endpoint == "result":
        payload["result"] = result.model_dump(mode="json")
    response = client.post(f"{_get_route(workload)}/{endpoint}", json=payload, headers=headers)
    assert response.status_code == (401 if auth_case == "missing" else 403)


def test_rejects_unregistered_attempt_with_otherwise_valid_token(client, workload, issue_token):
    attempt_id = uuid4()
    response = client.post(
        f"/execution/poc/parsing/workloads/{workload.workload_id}/attempts/{attempt_id}/claim",
        json={"execution_id": str(uuid4())},
        headers={"Authorization": f"Bearer {issue_token(extras={'attempt_ids': [str(attempt_id)]})}"},
    )
    assert response.status_code == 404


def test_rejects_competing_claim_and_unclaimed_publication(client, workload, result):
    route = _get_route(workload)
    first_execution, second_execution = str(uuid4()), str(uuid4())
    assert client.post(f"{route}/claim", json={"execution_id": first_execution}).status_code == 200
    assert client.post(f"{route}/claim", json={"execution_id": second_execution}).status_code == 409
    response = client.post(
        f"{route}/result", json={"execution_id": second_execution, "result": result.model_dump(mode="json")}
    )
    assert response.status_code == 409


@pytest.mark.parametrize("field", ["attempt_id", "relative_path", "source_revision"])
def test_rejects_result_identity_changes(client, workload, result, field):
    execution_id = str(uuid4())
    route = _get_route(workload)
    assert client.post(f"{route}/claim", json={"execution_id": execution_id}).status_code == 200
    tampered = result.model_dump(mode="json")
    tampered[field] = str(uuid4()) if field == "attempt_id" else "other.py"
    assert (
        client.post(f"{route}/result", json={"execution_id": execution_id, "result": tampered}).status_code
        == 409
    )


def test_accepted_result_is_immutable(client, workload, result, store):
    execution_id = str(uuid4())
    route = _get_route(workload)
    assert client.post(f"{route}/claim", json={"execution_id": execution_id}).status_code == 200
    payload = {"execution_id": execution_id, "result": result.model_dump(mode="json")}
    assert client.post(f"{route}/result", json=payload).status_code == 200
    payload["result"]["diagnostics"] = ["modified"]
    assert client.post(f"{route}/result", json=payload).status_code == 409
    assert store.get_results(workload.workload_id) == [result.model_dump(mode="json")]


def test_redelivery_skips_accepted_prefix_without_granting_another_claim(client, workload, result):
    route = _get_route(workload)
    first_execution, second_execution = str(uuid4()), str(uuid4())
    assert client.post(f"{route}/claim", json={"execution_id": first_execution}).status_code == 200
    payload = {"execution_id": first_execution, "result": result.model_dump(mode="json")}
    assert client.post(f"{route}/result", json=payload).status_code == 200
    assert client.post(f"{route}/claim", json={"execution_id": second_execution}).json() == {
        "execution_id": first_execution,
        "status": "accepted",
    }
    payload["execution_id"] = second_execution
    assert client.post(f"{route}/result", json=payload).status_code == 409
    assert client.post(
        f"{_get_route(workload, 1)}/claim", json={"execution_id": second_execution}
    ).json() == {"execution_id": second_execution, "status": "claimed"}


@pytest.mark.parametrize("elapsed", [30, 60])
def test_new_claim_rejected_at_registered_deadline(client, workload, clock, elapsed):
    clock.shift(timedelta(seconds=elapsed))
    assert (
        client.post(f"{_get_route(workload)}/claim", json={"execution_id": str(uuid4())}).status_code == 410
    )


def test_claim_ack_retry_does_not_extend_deadlines(client, workload, clock):
    claim = {"execution_id": str(uuid4())}
    route = _get_route(workload)
    assert client.post(f"{route}/claim", json=claim).status_code == 200
    clock.shift(timedelta(seconds=30))
    assert client.post(f"{route}/claim", json=claim).json()["status"] == "already_claimed"
    clock.shift(timedelta(seconds=30))
    assert client.post(f"{route}/claim", json=claim).status_code == 410


def test_late_new_result_rejected_but_identical_accepted_result_acknowledged(
    client, workload, result, store, clock
):
    execution_id = str(uuid4())
    for index in range(2):
        assert (
            client.post(
                f"{_get_route(workload, index)}/claim", json={"execution_id": execution_id}
            ).status_code
            == 200
        )
    payload = {"execution_id": execution_id, "result": result.model_dump(mode="json")}
    accepted = client.post(f"{_get_route(workload)}/result", json=payload)
    clock.shift(timedelta(seconds=60))
    assert client.post(f"{_get_route(workload)}/result", json=payload).json() == accepted.json()
    second = workload.definitions[1]
    late = result.model_copy(
        update={
            "attempt_id": second.attempt_id,
            "relative_path": second.relative_path,
            "source_revision": second.source_revision,
        }
    )
    assert (
        client.post(
            f"{_get_route(workload, 1)}/result",
            json={"execution_id": execution_id, "result": late.model_dump(mode="json")},
        ).status_code
        == 410
    )
    assert store.get_results(workload.workload_id) == [result.model_dump(mode="json")]


def test_registration_cannot_extend_deadlines_or_change_source(store, workload):
    store.register_workload(workload)
    changed = workload.model_copy(update={"stop_deadline": workload.stop_deadline + timedelta(seconds=60)})
    with pytest.raises(ReceiptConflictError, match="different manifest"):
        store.register_workload(changed)


@mock.patch("dev.dag_parsing_poc.store.sqlite3.connect", autospec=True)
def test_waiting_claim_reads_winner_only_after_acquiring_transaction(mock_connect, store, workload):
    contender_started = Event()
    transaction_acquired = Event()
    premature_read = Event()

    class ObservedConnection(Connection):
        def execute(self, sql, parameters=()):
            statement = sql.lstrip().upper()
            if statement.startswith("BEGIN"):
                contender_started.set()
                cursor = super().execute(sql, parameters)
                transaction_acquired.set()
                return cursor
            if statement.startswith("SELECT") and not self.in_transaction:
                # A read outside the transaction can see the old, unclaimed row while the winner holds it.
                premature_read.set()
                contender_started.set()
            return super().execute(sql, parameters)

    def open_observed_connection(*args, **kwargs):
        return connect(*args, factory=ObservedConnection, **kwargs)

    mock_connect.side_effect = open_observed_connection
    workload_id = str(workload.workload_id)
    attempt_id = str(workload.definitions[0].attempt_id)
    winning_execution = str(uuid4())
    with closing(connect(store.path, timeout=5)) as winner:
        winner.execute("BEGIN IMMEDIATE")
        with ThreadPoolExecutor(max_workers=1) as pool:
            waiting_claim = pool.submit(store.claim, workload_id, attempt_id, uuid4())
            try:
                assert contender_started.wait(timeout=3), "Claim never attempted a transaction or state read"
                assert not premature_read.is_set(), (
                    "Claim read attempt state before acquiring its transaction"
                )
                assert not transaction_acquired.is_set()
                assert not waiting_claim.done()
                winner.execute(
                    "UPDATE attempts SET execution_id = ? WHERE workload_id = ? AND attempt_id = ?",
                    (winning_execution, workload_id, attempt_id),
                )
                winner.commit()
                with pytest.raises(ReceiptConflictError, match="Another execution already claimed"):
                    waiting_claim.result(timeout=5)
            finally:
                # Release the writer before executor shutdown, including when a regression trips an assertion.
                winner.rollback()
        assert transaction_acquired.is_set()
        assert not premature_read.is_set(), "Claim read attempt state outside its transaction"
        assert winner.execute(
            "SELECT execution_id FROM attempts WHERE workload_id = ? AND attempt_id = ?",
            (workload_id, attempt_id),
        ).fetchone() == (winning_execution,)


def test_unregistered_workload_cannot_write_even_with_valid_signature(client, workload, result, issue_token):
    workload_id = uuid4()
    response = client.post(
        f"/execution/poc/parsing/workloads/{workload_id}/attempts/{result.attempt_id}/result",
        json={"execution_id": str(uuid4()), "result": result.model_dump(mode="json")},
        headers={"Authorization": f"Bearer {issue_token(extras={'sub': str(workload_id)})}"},
    )
    assert response.status_code == 404


@pytest.mark.parametrize("number", [float("nan"), float("inf"), -float("inf")])
@pytest.mark.parametrize("field", ["serialized_dags", "warnings"])
def test_nonfinite_nested_json_rejected_without_a_receipt(client, workload, result, store, number, field):
    execution_id = str(uuid4())
    route = _get_route(workload)
    assert client.post(f"{route}/claim", json={"execution_id": execution_id}).status_code == 200
    payload = {"execution_id": execution_id, "result": result.model_dump(mode="json")}
    payload["result"][field] = [{"nested": {"number": number}}]
    response = client.post(
        f"{route}/result", content=json.dumps(payload), headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422
    assert store.get_results(workload.workload_id) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("app", [256], indirect=True)
async def test_chunked_request_limit_stops_before_reading_an_unbounded_body(
    app, issue_token, workload, store
):
    sent = []

    async def generate_chunks():
        for index in range(10):
            sent.append(index)
            yield b"x" * 200

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            response = await client.post(
                f"{_get_route(workload)}/result",
                content=generate_chunks(),
                headers={"Authorization": f"Bearer {issue_token()}", "Content-Type": "application/json"},
            )
    assert response.status_code == 413
    assert sent == [0, 1]
    assert store.get_results(workload.workload_id) == []


def test_nonfinite_validation_error_does_not_echo_unencodable_input(client, workload, result):
    payload = {"execution_id": str(uuid4()), "result": result.model_dump(mode="json")}
    payload["result"]["duration_seconds"] = float("nan")
    response = client.post(
        f"{_get_route(workload)}/result",
        content=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "finite_number"
    assert "input" not in response.json()["detail"][0]


@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("workload", [1], indirect=True)
@pytest.mark.parametrize("endpoint", ["claim", "result"])
@pytest.mark.parametrize("committed", [True, False])
def test_worker_recovers_only_committed_acknowledgments_after_deadline(
    mock_parse,
    mock_root,
    workload,
    result,
    issue_token,
    store,
    clock,
    tmp_path,
    install_worker_http_bridge,
    endpoint,
    committed,
):
    mock_root.return_value = tmp_path
    mock_parse.return_value = result
    dropped = False
    acknowledgments = []

    def interrupt_acknowledgment(request, forward):
        nonlocal dropped
        if request.url.path.endswith(f"/{endpoint}") and not dropped:
            dropped = True
            if committed:
                response = forward()
                assert response.status_code == 200
                acknowledgments.append(response.json())
            deadline = workload.start_deadline if endpoint == "claim" else workload.stop_deadline
            clock.move_to(deadline + timedelta(seconds=1))
            raise httpx.ReadTimeout("Lost acknowledgment", request=request)
        response = forward()
        if request.url.path.endswith(f"/{endpoint}") and response.status_code == 200:
            acknowledgments.append(response.json())
        return response

    exchanges = install_worker_http_bridge(interrupt_acknowledgment)
    authenticated = workload.model_copy(update={"token": issue_token()})
    if committed:
        assert executor_worker.supervise_dag_parse(authenticated, server="http://receipt-api/execution/") == 0
        assert store.get_results(workload.workload_id) == [result.model_dump(mode="json")]
        mock_parse.assert_called_once()
        assert acknowledgments[1] == (
            acknowledgments[0] | {"status": "already_claimed"} if endpoint == "claim" else acknowledgments[0]
        )
    else:
        with pytest.raises(executor_worker.ParsingWorkerError, match="HTTP 410"):
            executor_worker.supervise_dag_parse(authenticated, server="http://receipt-api/execution/")
        assert store.get_results(workload.workload_id) == []
        if endpoint == "claim":
            mock_root.assert_not_called()
            mock_parse.assert_not_called()
        else:
            mock_parse.assert_called_once()
    retried = [item for item in exchanges if item["request"].url.path.endswith(f"/{endpoint}")]
    assert len(retried) == 2
    assert retried[0]["request"].content == retried[1]["request"].content
    assert retried[1]["status"] == (200 if committed else 410)
    attempts = store.get_attempts(workload.workload_id)
    assert attempts[0]["execution_id"] == (
        json.loads(retried[0]["request"].content)["execution_id"]
        if committed or endpoint == "result"
        else None
    )


@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("app", [MAX_PARSING_REQUEST_BYTES, 1024], indirect=True)
def test_worker_records_oversized_definition_and_continues_batch(
    mock_parse,
    mock_root,
    app,
    workload,
    result,
    issue_token,
    store,
    tmp_path,
    install_worker_http_bridge,
    request,
):
    api_limit = request.node.callspec.params["app"]
    text = "€" * (MAX_PARSING_REQUEST_BYTES // 3 + 1 if api_limit == MAX_PARSING_REQUEST_BYTES else 1024)
    oversized = result.model_copy(
        update={
            "serialized_dags": [{"dag": {"dag_id": "example", "payload": text}}],
            "diagnostics": ["discard this diagnostic"],
            "warnings": ["discard this warning"],
            "import_errors": {"good.py": "discard this error"},
        }
    )
    second = workload.definitions[1]
    following = result.model_copy(
        update={
            "attempt_id": second.attempt_id,
            "relative_path": second.relative_path,
            "source_revision": second.source_revision,
            "serialized_dags": [{"dag": {"dag_id": "following", "tasks": []}}],
        }
    )
    mock_root.return_value = tmp_path
    mock_parse.side_effect = [oversized, following]

    def check_rejection(request, forward):
        response = forward()
        if response.status_code == 413:
            assert store.get_results(workload.workload_id) == []
        return response

    exchanges = install_worker_http_bridge(check_rejection)
    authenticated = workload.model_copy(update={"token": issue_token()})
    assert executor_worker.supervise_dag_parse(authenticated, server="http://receipt-api/execution/") == 0

    first_posts = [item for item in exchanges if item["request"].url.path == f"{_get_route(workload)}/result"]
    assert [item["status"] for item in first_posts] == (
        [200] if api_limit == MAX_PARSING_REQUEST_BYTES else [413, 200]
    )
    assert len(first_posts[-1]["request"].content) <= api_limit
    if api_limit == MAX_PARSING_REQUEST_BYTES:
        assert len(text.encode("utf-8")) > api_limit
        assert len(text) < api_limit
    results = store.get_results(workload.workload_id)
    first = workload.definitions[0]
    assert results == [
        DagDefinitionResult(
            attempt_id=first.attempt_id,
            relative_path=first.relative_path,
            source_revision=first.source_revision,
            outcome="worker_error",
            diagnostics=["Serialized parsing result exceeds the request size limit"],
            duration_seconds=oversized.duration_seconds,
        ).model_dump(mode="json"),
        following.model_dump(mode="json"),
    ]
    assert mock_parse.call_count == 2
    assert [item["status"] for item in store.get_attempts(workload.workload_id)] == ["accepted", "accepted"]


@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("field", ["serialized_dags", "warnings"])
@pytest.mark.parametrize(
    "value",
    [float("nan"), float("inf"), float("-inf"), "\ud800", object()],
    ids=["nan", "infinity", "negative-infinity", "invalid-unicode", "unsupported-object"],
)
def test_worker_records_unencodable_definition_and_continues_batch(
    mock_parse,
    mock_root,
    workload,
    result,
    issue_token,
    store,
    tmp_path,
    install_worker_http_bridge,
    field,
    value,
):
    invalid = result.model_copy(update={field: [{"dag": {"params": {"value": value}}}]})
    second = workload.definitions[1]
    following = result.model_copy(
        update={
            "attempt_id": second.attempt_id,
            "relative_path": second.relative_path,
            "source_revision": second.source_revision,
        }
    )
    mock_root.return_value = tmp_path
    mock_parse.side_effect = [invalid, following]
    exchanges = install_worker_http_bridge()
    authenticated = workload.model_copy(update={"token": issue_token()})

    assert executor_worker.supervise_dag_parse(authenticated, server="http://receipt-api/execution/") == 0

    first = workload.definitions[0]
    results = store.get_results(workload.workload_id)
    assert results == [
        DagDefinitionResult(
            attempt_id=first.attempt_id,
            relative_path=first.relative_path,
            source_revision=first.source_revision,
            outcome="worker_error",
            diagnostics=["Parsing result cannot be encoded as JSON"],
            duration_seconds=invalid.duration_seconds,
        ).model_dump(mode="json"),
        following.model_dump(mode="json"),
    ]
    json.dumps(results, allow_nan=False)
    assert mock_parse.call_count == 2
    assert [item["status"] for item in store.get_attempts(workload.workload_id)] == ["accepted", "accepted"]
    assert len(exchanges) == 4
    assert all(item["status"] == 200 for item in exchanges)


@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("workload", [1], indirect=True)
def test_worker_does_not_replace_committed_result_after_lost_ack_then_413(
    mock_parse,
    mock_root,
    workload,
    result,
    issue_token,
    store,
    tmp_path,
    install_worker_http_bridge,
):
    mock_root.return_value = tmp_path
    mock_parse.return_value = result
    committed = False
    tighter_app = create_app(store.path, tmp_path / "public.pem", max_request_bytes=256)
    with TestClient(tighter_app) as tighter_api:

        def interrupt_acknowledgment(request, forward):
            nonlocal committed
            if not request.url.path.endswith("/result"):
                return forward()
            if committed:
                return _forward_worker_request(tighter_api, request)
            response = forward()
            assert response.status_code == 200
            committed = True
            raise httpx.ReadTimeout("Lost committed result acknowledgment", request=request)

        exchanges = install_worker_http_bridge(interrupt_acknowledgment)
        authenticated = workload.model_copy(update={"token": issue_token()})
        with pytest.raises(executor_worker.ParsingRequestTooLargeError) as error:
            executor_worker.supervise_dag_parse(authenticated, server="http://receipt-api/execution/")
    assert error.value.uncertain is True
    result_posts = [item for item in exchanges if item["request"].url.path.endswith("/result")]
    assert len(result_posts) == 2
    assert result_posts[0]["request"].content == result_posts[1]["request"].content
    assert result_posts[1]["status"] == 413
    assert store.get_results(workload.workload_id) == [result.model_dump(mode="json")]
