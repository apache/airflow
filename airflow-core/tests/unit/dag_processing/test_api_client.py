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
import runpy
from contextvars import Context
from unittest import mock

import httpx
import jwt
import pytest
from tenacity import wait_none

from airflow.api_fastapi.execution_api.datamodels.job import JobState, TerminalJobState
from airflow.dag_processing import api_client
from airflow.dag_processing.api_client import DagProcessorAPIClient, DagProcessorRegistrationRetired
from airflow.sdk.api.client import API_RETRIES, Client
from airflow.sdk.api.datamodels import _generated

SECRET = "processor-client-unit-test-signing-key"


def make_job_token(**claims):
    return jwt.encode(
        {"iat": 0, "exp": 300, "scope": "dag_processor", "job_id": 1, **claims},
        SECRET,
    )


def make_registration_response(*, job_id=1, token=None, **kwargs):
    return httpx.Response(
        201, json={"job_id": job_id, "token": token or make_job_token(job_id=job_id)}, **kwargs
    )


@pytest.fixture
def token_file(tmp_path):
    path = tmp_path / "processor.jwt"
    path.write_text("session-1\n")
    return path


@pytest.fixture(autouse=True)
def no_retry_wait(monkeypatch):
    monkeypatch.setattr(Client._request_with_retry.retry, "wait", wait_none())


@pytest.fixture
def clock():
    with mock.patch("airflow.dag_processing.api_client.monotonic", autospec=True, return_value=0) as clock:
        yield clock


@pytest.fixture
def make_client(token_file, clock):
    clients = []

    def create(*responses, **kwargs):
        pending = iter(responses)
        requests = []

        def handle(request):
            requests.append(request)
            response = next(pending)
            if isinstance(response, Exception):
                raise response
            return response(request) if callable(response) else response

        client = DagProcessorAPIClient(
            base_url="http://api/execution/",
            token_file=token_file,
            hostname="processor-1",
            transport=httpx.MockTransport(handle),
            **kwargs,
        )
        clients.append(client)
        return client, requests

    yield create
    for client in clients:
        client.close()


def test_registration_and_runtime_use_distinct_credentials(make_client):
    client, requests = make_client(
        make_registration_response(headers={"Refreshed-API-Token": "wrong-token"}),
        httpx.Response(200, json={"state": "restarting"}, headers={"Refreshed-API-Token": "wrong-token"}),
        httpx.Response(200, json={"state": "running"}),
        unixname="airflow",
        bundle_names=["bundle-a"],
    )

    assert client.register_job() == client.job_id == 1
    assert client.heartbeat() == JobState.RESTARTING
    assert client.heartbeat() == JobState.RUNNING
    assert json.loads(requests[0].content) == {
        "registration_id": str(client.registration_id),
        "hostname": "processor-1",
        "unixname": "airflow",
        "bundle_names": ["bundle-a"],
    }
    assert [request.headers["Authorization"] for request in requests] == [
        "Bearer session-1",
        f"Bearer {make_job_token()}",
        f"Bearer {make_job_token()}",
    ]
    assert requests[1].url.path == "/execution/jobs/1/heartbeat"


def test_registration_identity_survives_response_loss_but_not_a_process_restart(make_client):
    first, requests = make_client(httpx.ReadError("Lost acknowledgment"), make_registration_response())
    second, _ = make_client()

    first.register_job()

    assert first.registration_id != second.registration_id
    assert len(requests) == 2
    assert requests[0].content == requests[1].content


@pytest.mark.parametrize("succeeds", [False, True])
@pytest.mark.parametrize("status", [401, 403])
def test_registration_reloads_a_rotated_credential_after_authentication_failure(
    make_client, token_file, status, succeeds
):
    def rotate(request):
        token_file.write_text("session-2")
        return httpx.Response(status)

    client, requests = make_client(
        rotate, make_registration_response() if succeeds else httpx.Response(status)
    )

    if succeeds:
        client.register_job()
    else:
        with pytest.raises(httpx.HTTPStatusError):
            client.register_job()

    assert [request.headers["Authorization"] for request in requests] == [
        "Bearer session-1",
        "Bearer session-2",
    ]
    assert requests[0].content == requests[1].content


@pytest.mark.parametrize("status", [401, 403, 404, 409])
def test_registration_errors_do_not_start_another_job(make_client, status):
    client, requests = make_client(httpx.Response(status))
    registration_id = client.registration_id

    with pytest.raises(httpx.HTTPStatusError) as error:
        client.register_job()

    assert error.value.response.status_code == status
    assert client.job_id is None
    assert client.registration_id == registration_id
    assert len(requests) == 1


def test_rotation_is_detected_within_the_cache_interval(make_client, token_file, clock):
    client, requests = make_client(
        make_registration_response(),
        httpx.Response(204),
        make_registration_response(token=make_job_token(iat=30, exp=330)),
        httpx.Response(204),
    )
    client.register_job()
    token_file.write_text("session-2")
    clock.return_value = 29
    client.get("resource")
    clock.return_value = 30
    client.get("resource")

    assert [request.url.path for request in requests] == [
        "/execution/jobs",
        "/execution/resource",
        "/execution/jobs",
        "/execution/resource",
    ]
    assert requests[2].headers["Authorization"] == "Bearer session-2"
    assert requests[3].headers["Authorization"] == f"Bearer {make_job_token(iat=30, exp=330)}"


def test_job_token_renewal_uses_monotonic_time(make_client, clock, time_machine):
    client, requests = make_client(
        make_registration_response(), httpx.Response(204), make_registration_response(), httpx.Response(204)
    )
    client.register_job()
    time_machine.move_to("2030-01-01", tick=False)
    clock.return_value = 239
    client.get("resource")
    time_machine.move_to("2020-01-01", tick=False)
    clock.return_value = 240
    client.get("resource")

    assert [request.url.path for request in requests].count("/execution/jobs") == 2
    assert requests[0].content == requests[2].content


@pytest.mark.parametrize("status", [401, 403, 404, 409])
def test_runtime_errors_do_not_renew_credentials(make_client, status):
    client, requests = make_client(make_registration_response(), httpx.Response(status))
    client.register_job()

    with pytest.raises(httpx.HTTPStatusError) as error:
        client.get("resource")

    assert error.value.response.status_code == status
    assert len(requests) == 2


@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("failure", [httpx.ReadError("Unavailable"), httpx.Response(503)])
def test_heartbeat_does_not_retry_transport_failures(make_client, clock, expired, failure):
    client, requests = make_client(make_registration_response(), failure)
    client.register_job()
    if expired:
        clock.return_value = 300

    with pytest.raises(httpx.HTTPError):
        client.heartbeat()

    assert len(requests) == 2


@pytest.mark.parametrize("registered", [False, True])
@pytest.mark.parametrize("missing", [False, True])
def test_unreadable_credentials_fail_closed(make_client, token_file, clock, registered, missing):
    client, requests = make_client(make_registration_response())
    if registered:
        client.register_job()
    if missing:
        token_file.unlink()
    else:
        token_file.write_text(" \n")
    clock.return_value = 300

    with pytest.raises(FileNotFoundError if missing else ValueError):
        client.heartbeat() if registered else client.register_job()

    assert len(requests) == int(registered)


def test_renewal_cannot_replace_the_job(make_client):
    client, _ = make_client(make_registration_response(), make_registration_response(job_id=2))
    client.register_job()

    with pytest.raises(RuntimeError, match="different Dag processor Job"):
        client.register_job()

    assert client.job_id == 1
    assert client.auth.token == make_job_token()


@pytest.mark.parametrize(
    "token",
    [
        "not-a-jwt",
        make_job_token(scope="execution"),
        make_job_token(job_id=2),
        make_job_token(exp=0),
        make_job_token(exp="not-a-timestamp"),
        make_job_token(exp=float("inf")),
        jwt.encode({"scope": "dag_processor"}, SECRET),
    ],
)
def test_invalid_job_tokens_are_not_adopted(make_client, token):
    client, _ = make_client(make_registration_response(token=token))

    with pytest.raises(ValueError, match="invalid Dag processor Job token"):
        client.register_job()

    assert client.job_id is None
    assert client.auth.token == ""


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(lambda client: client.get("resource"), id="request"),
        pytest.param(lambda client: client.heartbeat(), id="heartbeat"),
        pytest.param(lambda client: client.complete_job(TerminalJobState.SUCCESS), id="complete"),
    ],
)
def test_runtime_operations_require_registration(make_client, operation):
    client, requests = make_client()

    with pytest.raises(RuntimeError, match="Register the Dag processor Job"):
        operation(client)

    assert requests == []


def test_completion_recovers_a_lost_acknowledgment_with_the_same_token_and_outcome(make_client):
    client, requests = make_client(
        make_registration_response(), httpx.ReadError("Lost acknowledgment"), httpx.Response(204)
    )
    client.register_job()

    client.complete_job(TerminalJobState.SUCCESS)
    client.complete_job(TerminalJobState.SUCCESS)

    assert len(requests) == 3
    assert requests[1].url.path == "/execution/jobs/1/complete"
    assert requests[1].content == requests[2].content == b'{"state":"success"}'
    assert requests[1].headers["Authorization"] == requests[2].headers["Authorization"]


def test_completion_retries_remain_possible_after_transport_retries_are_exhausted(
    make_client, token_file, clock
):
    client, requests = make_client(
        make_registration_response(),
        *[httpx.ReadError("Lost acknowledgment") for _ in range(API_RETRIES)],
        httpx.Response(204),
    )
    client.register_job()
    with pytest.raises(httpx.ReadError):
        client.complete_job(TerminalJobState.FAILED)
    token_file.unlink()
    clock.return_value = 290

    client.complete_job(TerminalJobState.FAILED)

    assert len(requests) == API_RETRIES + 2
    assert {request.content for request in requests[1:]} == {b'{"state":"failed"}'}
    assert {request.headers["Authorization"] for request in requests[1:]} == {f"Bearer {make_job_token()}"}


@pytest.mark.parametrize("status", [204, 401, 403, 404])
def test_completion_stops_normal_work_even_when_acknowledgment_is_uncertain(make_client, status):
    client, requests = make_client(make_registration_response(), httpx.Response(status))
    client.register_job()
    if status == 204:
        client.complete_job(TerminalJobState.SUCCESS)
    else:
        with pytest.raises(httpx.HTTPStatusError):
            client.complete_job(TerminalJobState.SUCCESS)

    with pytest.raises(RuntimeError, match="only completion retries"):
        client.heartbeat()
    with pytest.raises(RuntimeError, match="only completion retries"):
        client.register_job()
    with pytest.raises(ValueError, match="original outcome"):
        client.complete_job(TerminalJobState.FAILED)
    assert len(requests) == 2


@pytest.mark.parametrize("interval", [-1, float("inf"), float("nan")])
def test_invalid_reload_interval_is_rejected(make_client, interval):
    with pytest.raises(ValueError, match="token_reload_interval"):
        make_client(token_reload_interval=interval)


@pytest.mark.parametrize("operation", ["runtime", "heartbeat"])
@pytest.mark.parametrize(
    "failure", [httpx.ReadTimeout("Unavailable"), httpx.Response(503), httpx.Response(403)]
)
def test_failed_early_renewal_keeps_using_the_valid_token(make_client, clock, operation, failure):
    response_body = {"state": "running"} if operation == "heartbeat" else {"key": "key", "value": "value"}
    client, requests = make_client(
        make_registration_response(),
        failure,
        httpx.Response(200, json=response_body),
        httpx.Response(200, json=response_body),
        make_registration_response(),
        httpx.Response(204),
    )
    client.register_job()
    clock.return_value = 240

    for _ in range(2):
        if operation == "runtime":
            assert client.variables.get("key").value == "value"
        else:
            assert client.heartbeat() == JobState.RUNNING

    assert len(requests) == 4
    assert requests[1].extensions["timeout"] == {"connect": 1, "read": 1, "write": 1, "pool": 1}
    assert [request.headers["Authorization"] for request in requests[2:]] == [
        f"Bearer {make_job_token()}"
    ] * 2
    clock.return_value = 270
    client.get("resource")
    assert requests[4].url.path == "/execution/jobs"


@pytest.mark.parametrize("missing", [False, True])
def test_unreadable_rotated_file_does_not_discard_a_valid_token(make_client, token_file, clock, missing):
    client, requests = make_client(
        make_registration_response(), httpx.Response(200, json={"state": "running"})
    )
    client.register_job()
    token_file.unlink() if missing else token_file.write_text("")
    clock.return_value = 30

    assert client.heartbeat() == JobState.RUNNING
    assert len(requests) == 2


def test_renewal_that_outlasts_the_token_requires_synchronous_recovery(make_client, clock):
    def time_out(request):
        clock.return_value = 300
        raise httpx.ReadTimeout("Outlasted the token")

    client, requests = make_client(
        make_registration_response(), time_out, make_registration_response(), httpx.Response(204)
    )
    client.register_job()
    clock.return_value = 299

    client.get("resource")

    assert [request.url.path for request in requests] == ["/execution/jobs"] * 3 + ["/execution/resource"]


@pytest.mark.parametrize("complete", [False, True])
def test_retired_registration_signals_restart_without_blocking_valid_requests(make_client, clock, complete):
    client, requests = make_client(
        make_registration_response(),
        httpx.Response(409, json={"detail": {"reason": "registration_retired"}}),
        httpx.Response(204),
        httpx.Response(204),
        httpx.Response(204),
    )
    client.register_job()
    clock.return_value = 240
    client.get("resource")
    assert client.restart_required
    client.get("resource")
    if complete:
        client.complete_job(TerminalJobState.SUCCESS)
        assert requests[-1].url.path == "/execution/jobs/1/complete"
        assert requests[-1].headers["Authorization"] == f"Bearer {make_job_token()}"
    else:
        clock.return_value = 300
        with pytest.raises(DagProcessorRegistrationRetired):
            client.get("resource")
    assert len(requests) == 4 + complete


@pytest.mark.parametrize(
    "payload", [{"detail": "conflict"}, ["unexpected"], {"detail": {"reason": "registration_conflict"}}]
)
def test_other_conflicts_are_not_mistaken_for_retirement(make_client, payload):
    client, _ = make_client(httpx.Response(409, json=payload))
    with pytest.raises(httpx.HTTPStatusError):
        client.register_job()
    assert not client.restart_required


def test_closed_job_signals_restart(make_client):
    client, _ = make_client(
        make_registration_response(),
        httpx.Response(403, json={"detail": {"reason": "job_closed", "message": "replaced"}}),
    )
    client.register_job()

    with pytest.raises(DagProcessorRegistrationRetired):
        client.heartbeat()

    assert client.restart_required


@pytest.mark.parametrize(
    "payload", [{"detail": "Invalid auth token"}, {"detail": {"reason": "bundle_not_granted"}}]
)
def test_other_authorization_failures_are_not_mistaken_for_a_closed_job(make_client, payload):
    client, _ = make_client(make_registration_response(), httpx.Response(403, json=payload))
    client.register_job()

    with pytest.raises(httpx.HTTPStatusError):
        client.get("resource")

    assert not client.restart_required


def test_sdk_helpers_keep_bundle_context_for_reads_and_writes(make_client):
    client, requests = make_client(
        make_registration_response(),
        httpx.Response(200, json={"key": "key", "value": "value"}),
        httpx.Response(204),
        httpx.Response(
            200,
            json={
                "conn_id": "connection",
                "conn_type": "generic",
                "host": None,
                "schema": None,
                "login": None,
                "password": None,
                "port": None,
                "extra": None,
            },
        ),
    )
    client.register_job()
    with client.use_bundle("bundle-a"):
        assert client.variables.get("key").value == "value"
        assert client.variables.set("key", "new").ok
        assert client.connections.get("connection").conn_id == "connection"

    assert [request.headers["Airflow-Dag-Bundle"] for request in requests[1:]] == ["bundle-a"] * 3
    assert requests[2].headers["Content-Type"] == "application/json"
    assert json.loads(requests[2].content)["val"] == "new"


def test_bundle_context_preserves_headers_and_restores_context_on_error(make_client):
    client, requests = make_client(
        make_registration_response(),
        httpx.Response(204),
        httpx.ReadError("Subprocess failure"),
        *[httpx.Response(204) for _ in range(3)],
    )
    client.register_job()
    with client.use_bundle("bundle-a"):
        client.put("resource", content=b"data", headers={"content-type": "text/plain", "custom": "value"})
        with pytest.raises(httpx.ReadError), client.use_bundle("bundle-b"):
            client.request(
                "PUT",
                "resource",
                content=b"{}",
                headers={"custom": "value", "airflow-dag-bundle": "ignored"},
                retry=False,
            )
        client.get("resource")
        Context().run(client.get, "resource")
    client.get("resource")

    assert [request.headers.get("Airflow-Dag-Bundle") for request in requests[1:]] == [
        "bundle-a",
        "bundle-b",
        "bundle-a",
        None,
        None,
    ]
    assert requests[1].headers["Content-Type"] == "text/plain"
    assert requests[2].headers["Content-Type"] == "application/json"
    assert requests[1].headers["custom"] == requests[2].headers["custom"] == "value"


def test_empty_bundle_context_is_rejected(make_client):
    client, requests = make_client()
    with pytest.raises(ValueError, match="nonempty bundle"), client.use_bundle(""):
        pytest.fail("Empty bundle was accepted")
    assert not requests


@pytest.mark.parametrize("lost_acknowledgment", [False, True])
def test_completion_renews_an_expired_token_if_the_job_is_still_open(make_client, clock, lost_acknowledgment):
    failures = (
        [httpx.ReadError("Lost acknowledgment") for _ in range(API_RETRIES)] if lost_acknowledgment else []
    )
    renewed_token = make_job_token(iat=300, exp=600)
    client, requests = make_client(
        make_registration_response(),
        *failures,
        make_registration_response(token=renewed_token),
        httpx.Response(204),
    )
    client.register_job()
    if lost_acknowledgment:
        with pytest.raises(httpx.ReadError):
            client.complete_job(TerminalJobState.SUCCESS)
    clock.return_value = 300

    client.complete_job(TerminalJobState.SUCCESS)

    assert requests[0].content == requests[-2].content
    assert requests[-1].headers["Authorization"] == f"Bearer {renewed_token}"
    assert {request.content for request in requests if request.url.path.endswith("/complete")} == {
        b'{"state":"success"}'
    }


def test_completion_does_not_claim_success_when_expired_registration_is_retired(make_client, clock):
    client, requests = make_client(
        make_registration_response(), httpx.Response(409, json={"detail": {"reason": "registration_retired"}})
    )
    client.register_job()
    clock.return_value = 300

    for _ in range(2):
        with pytest.raises(DagProcessorRegistrationRetired):
            client.complete_job(TerminalJobState.SUCCESS)

    assert client.restart_required
    assert len(requests) == 2


@pytest.mark.parametrize("succeeds", [False, True])
@pytest.mark.parametrize("status", [401, 403])
def test_completion_recovers_if_the_token_expires_during_the_request(make_client, clock, status, succeeds):
    def expire(request):
        clock.return_value += 300
        return httpx.Response(status)

    client, requests = make_client(
        make_registration_response(),
        expire,
        make_registration_response(),
        httpx.Response(204) if succeeds else expire,
    )
    client.register_job()
    if succeeds:
        client.complete_job(TerminalJobState.SUCCESS)
    else:
        with pytest.raises(httpx.HTTPStatusError):
            client.complete_job(TerminalJobState.SUCCESS)

    assert requests[0].content == requests[2].content
    assert requests[1].content == requests[3].content


def test_control_contracts_do_not_require_new_sdk_models(monkeypatch, token_file):
    for name in (
        "JobRegisterBody",
        "JobRegisterResponse",
        "JobHeartbeatResponse",
        "JobCompleteBody",
        "TerminalJobState",
        "JobState",
    ):
        monkeypatch.delattr(_generated, name)
    namespace = runpy.run_path(api_client.__file__)
    with namespace["DagProcessorAPIClient"](
        base_url="http://api/", token_file=token_file, hostname="processor"
    ) as client:
        assert isinstance(client._registration, api_client.JobRegisterBody)
