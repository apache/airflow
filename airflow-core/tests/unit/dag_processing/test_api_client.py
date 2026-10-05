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
from uuid6 import uuid7

from airflow.api_fastapi.execution_api.datamodels.dag_parsing import DagParseResultBody
from airflow.api_fastapi.execution_api.datamodels.job import DagParseTokenBody, JobState, TerminalJobState
from airflow.dag_processing import api_client
from airflow.dag_processing.api_client import (
    DagParseContext,
    DagProcessorAPIClient,
    DagProcessorJobAlreadyRunning,
    DagProcessorRegistrationRetired,
    DagProcessorSecretsComms,
)
from airflow.sdk.api.client import API_RETRIES, Client
from airflow.sdk.api.datamodels import _generated
from airflow.sdk.execution_time.comms import GetVariable, GetXCom, MaskSecret, VariableResult

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


def test_registration_retry_after_a_crashed_job_keeps_its_identity(make_client):
    client, requests = make_client(
        httpx.Response(409, json={"detail": {"reason": "job_running"}}),
        make_registration_response(),
    )
    registration_id = client.registration_id

    with pytest.raises(DagProcessorJobAlreadyRunning):
        client.register_job()
    assert client.job_id is None
    assert client.register_job() == 1

    assert client.registration_id == registration_id
    assert requests[0].content == requests[1].content


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


def test_secrets_comms_answers_lookups_for_the_selected_bundle(make_client):
    client, requests = make_client(
        make_registration_response(), httpx.Response(200, json={"key": "my_key", "value": "my_value"})
    )
    client.register_job()
    comms = DagProcessorSecretsComms(client)

    with client.use_bundle("bundle-a"):
        result = comms.send(GetVariable(key="my_key"))

    assert result == VariableResult(key="my_key", value="my_value")
    assert requests[1].url.path == "/execution/variables/my_key"
    assert requests[1].headers["Airflow-Dag-Bundle"] == "bundle-a"


def test_secrets_comms_ignores_masking_and_rejects_other_messages(make_client):
    client, requests = make_client()
    comms = DagProcessorSecretsComms(client)

    assert comms.send(MaskSecret(value="secret")) is None
    with pytest.raises(TypeError, match="GetXCom"):
        comms.send(GetXCom(dag_id="dag", run_id="run", task_id="task", key="key"))
    assert requests == []


def make_parse_context(**kwargs):
    return DagParseContext(
        request=DagParseTokenBody(
            **{
                "attempt_id": "00000000-0000-0000-0000-000000000001",
                "bundle_name": "bundle-a",
                "relative_fileloc": "dag.py",
                **kwargs,
            }
        )
    )


def make_parse_token_response(**claims):
    token = jwt.encode(
        {
            "scope": "dag_parse",
            "sub": "00000000-0000-0000-0000-000000000001",
            "session_id": "00000000-0000-0000-0000-000000000002",
            "job_id": 1,
            "dag_bundles": ["bundle-a"],
            "relative_fileloc": "dag.py",
            "iat": 0,
            "exp": 60,
            **claims,
        },
        SECRET,
    )
    return httpx.Response(200, json={"token": token})


def test_parse_requests_exchange_once_and_restore_the_manager_credential(make_client):
    exchanged = make_parse_token_response()
    client, requests = make_client(
        make_registration_response(),
        exchanged,
        httpx.Response(200, json={"key": "key", "value": "value"}),
        httpx.Response(200, json={"key": "key", "value": "value"}),
        httpx.Response(200, json={"state": "running"}),
    )
    client.register_job()
    context = make_parse_context()

    with client.use_bundle("bundle-b"), client.use_parse(context):
        assert client.variables.get("key").value == "value"
        assert client.variables.get("key").value == "value"
    assert client.heartbeat() == JobState.RUNNING

    assert requests[1].url.path == "/execution/jobs/1/parse-token"
    assert json.loads(requests[1].content) == context.request.model_dump(mode="json")
    assert requests[1].headers["Airflow-API-Version"] == api_client._JOB_API_HEADERS["Airflow-API-Version"]
    assert [request.headers["Authorization"] for request in requests[1:]] == [
        f"Bearer {make_job_token()}",
        f"Bearer {exchanged.json()['token']}",
        f"Bearer {exchanged.json()['token']}",
        f"Bearer {make_job_token()}",
    ]
    assert requests[2].headers["Airflow-Dag-Bundle"] == "bundle-a"


def test_parse_exchange_recovers_a_lost_response_without_changing_attempt(make_client):
    client, requests = make_client(
        make_registration_response(),
        httpx.ReadError("exchange acknowledgment lost"),
        make_parse_token_response(),
        httpx.Response(200),
    )
    client.register_job()
    with client.use_parse(make_parse_context()):
        client.get("variables/key")

    assert requests[1].content == requests[2].content
    assert len(requests) == 4


def test_interleaved_parses_keep_distinct_file_credentials(make_client):
    first_response = make_parse_token_response()
    second_response = make_parse_token_response(
        sub="00000000-0000-0000-0000-000000000003",
        relative_fileloc="other.py",
        dag_bundles=["bundle-b"],
    )
    client, requests = make_client(
        make_registration_response(),
        first_response,
        httpx.Response(200),
        second_response,
        httpx.Response(200),
        httpx.Response(200),
    )
    client.register_job()
    second = make_parse_context(
        attempt_id="00000000-0000-0000-0000-000000000003",
        bundle_name="bundle-b",
        relative_fileloc="other.py",
    )

    with client.use_parse(make_parse_context()):
        client.get("variables/key")
        with client.use_parse(second):
            client.get("variables/key")
        client.get("variables/key")

    assert [requests[index].headers["Authorization"] for index in (2, 4, 5)] == [
        f"Bearer {first_response.json()['token']}",
        f"Bearer {second_response.json()['token']}",
        f"Bearer {first_response.json()['token']}",
    ]


@pytest.mark.parametrize("renew_at", [48, 61])
def test_parse_credential_renews_with_the_same_file_and_attempt(make_client, clock, renew_at):
    client, requests = make_client(
        make_registration_response(),
        make_parse_token_response(),
        httpx.Response(200),
        httpx.Response(200),
        make_parse_token_response(iat=renew_at, exp=renew_at + 60),
        httpx.Response(200),
    )
    client.register_job()
    with client.use_parse(make_parse_context()):
        client.get("variables/key")
        clock.return_value = 47
        client.get("variables/key")
        clock.return_value = renew_at
        client.get("variables/key")

    assert requests[1].content == requests[4].content
    assert requests[2].headers["Authorization"] == requests[3].headers["Authorization"]
    assert requests[2].headers["Authorization"] != requests[5].headers["Authorization"]


@pytest.mark.parametrize(
    "failure",
    [httpx.ReadError("unavailable"), httpx.Response(503), httpx.Response(200, json={"token": "invalid"})],
)
@pytest.mark.parametrize("recovers_at_expiry", [False, True])
def test_failed_early_parse_renewal_keeps_the_valid_token_until_expiry(
    make_client, clock, failure, recovers_at_expiry
):
    token = make_parse_token_response()
    client, requests = make_client(
        make_registration_response(),
        token,
        httpx.Response(200),
        failure,
        httpx.Response(200),
        httpx.Response(200),
        make_parse_token_response(iat=60, exp=120) if recovers_at_expiry else httpx.Response(503),
        httpx.Response(200),
    )
    client.register_job()
    with client.use_parse(make_parse_context()):
        for tick in (0, 48, 49):
            clock.return_value = tick
            client.request("GET", "variables/key", retry=False)
        clock.return_value = 60
        if recovers_at_expiry:
            client.request("GET", "variables/key", retry=False)
        else:
            with pytest.raises(httpx.HTTPStatusError):
                client.request("GET", "variables/key", retry=False)

    assert [request.url.path for request in requests[:7]] == [
        "/execution/jobs",
        "/execution/jobs/1/parse-token",
        "/execution/variables/key",
        "/execution/jobs/1/parse-token",
        "/execution/variables/key",
        "/execution/variables/key",
        "/execution/jobs/1/parse-token",
    ]
    assert all(requests[i].headers["Authorization"] == f"Bearer {token.json()['token']}" for i in (2, 4, 5))
    assert all(value <= 1 for value in requests[3].extensions["timeout"].values())
    if recovers_at_expiry:
        assert requests[7].headers["Authorization"] != requests[2].headers["Authorization"]
    else:
        assert len(requests) == 7


def test_parse_renewal_that_outlasts_the_token_requires_synchronous_recovery(make_client, clock):
    def expire(request):
        clock.return_value = 60
        return httpx.Response(503)

    client, requests = make_client(
        make_registration_response(),
        make_parse_token_response(),
        httpx.Response(200),
        expire,
        make_parse_token_response(iat=60, exp=120),
        httpx.Response(200),
    )
    client.register_job()
    with client.use_parse(make_parse_context()):
        client.get("variables/key")
        clock.return_value = 48
        client.get("variables/key")

    assert requests[1].content == requests[3].content == requests[4].content
    assert requests[2].headers["Authorization"] != requests[5].headers["Authorization"]


@pytest.mark.parametrize("reason", ["job_closed", "bundle_not_granted"])
def test_early_parse_renewal_does_not_hide_authorization_errors(make_client, clock, reason):
    client, requests = make_client(
        make_registration_response(),
        make_parse_token_response(),
        httpx.Response(200),
        httpx.Response(403, json={"detail": {"reason": reason}}),
    )
    client.register_job()
    with client.use_parse(make_parse_context()):
        client.get("variables/key")
        clock.return_value = 48
        with pytest.raises(
            DagProcessorRegistrationRetired if reason == "job_closed" else httpx.HTTPStatusError
        ):
            client.get("variables/key")

    assert client.restart_required == (reason == "job_closed")
    assert len(requests) == 4


@pytest.mark.parametrize(
    "claims",
    [
        {"scope": "execution"},
        {"sub": "another-attempt"},
        {"job_id": 2},
        {"dag_bundles": ["bundle-b"]},
        {"relative_fileloc": "other.py"},
        {"exp": 0},
        {"exp": "not-a-time"},
        {"exp": float("inf")},
    ],
)
def test_invalid_exchanged_credentials_never_reach_a_runtime_endpoint(make_client, claims):
    client, requests = make_client(make_registration_response(), make_parse_token_response(**claims))
    client.register_job()
    context = make_parse_context()

    with client.use_parse(context), pytest.raises(ValueError, match="invalid Dag parsing token"):
        client.get("variables/key")

    assert len(requests) == 2
    assert context.token is None


def test_exchange_failure_restores_context_and_signals_a_closed_job(make_client):
    client, requests = make_client(
        make_registration_response(),
        httpx.Response(403, json={"detail": {"reason": "job_closed"}}),
        httpx.Response(200, json={"state": "restarting"}),
    )
    client.register_job()
    with client.use_parse(make_parse_context()), pytest.raises(DagProcessorRegistrationRetired):
        client.get("variables/key")

    assert client.restart_required
    assert client.heartbeat() == JobState.RESTARTING
    assert requests[-1].headers["Authorization"] == f"Bearer {make_job_token()}"


@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("failure", [httpx.ReadTimeout("Lost response"), httpx.Response(503)])
def test_publication_attempt_does_not_sleep_through_heartbeats(make_client, clock, expired, failure):
    body = DagParseResultBody(
        attempt_id=uuid7(),
        dispatch_sequence=1,
        bundle_name="bundle-a",
        relative_fileloc="dag.py",
        parse_duration=0.1,
        serialized_dags=[],
        source_codes={},
    )
    responses = [make_registration_response()]
    if expired:
        responses.append(make_registration_response())
    responses.extend(
        [
            failure,
            httpx.Response(200, json={"state": "running"}),
            httpx.Response(
                200, json={"attempt_id": str(body.attempt_id), "accepted_at": "2026-10-05T12:00:00Z"}
            ),
        ]
    )
    client, requests = make_client(*responses)
    client.register_job()
    if expired:
        clock.return_value = 300
    with pytest.raises(httpx.HTTPError):
        client.publish_parse_result(body)
    assert len(requests) == (3 if expired else 2)
    assert all(value <= 1 for value in requests[-1].extensions["timeout"].values())
    if expired:
        assert all(value <= 1 for value in requests[1].extensions["timeout"].values())
    assert client.heartbeat() == JobState.RUNNING
    assert client.publish_parse_result(body).attempt_id == body.attempt_id
    assert requests[-1].content == requests[-3].content
