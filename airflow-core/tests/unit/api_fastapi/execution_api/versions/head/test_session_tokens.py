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
from datetime import UTC, datetime
from unittest import mock

import httpx2
import jwt
import pytest
import svcs
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import ValidationError
from sqlalchemy import select
from uuid6 import uuid7

from airflow.api_fastapi.auth.tokens import JWTValidator, key_to_jwk_dict
from airflow.api_fastapi.execution_api.datamodels.job import DagParseTokenBody, JobState, TerminalJobState
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.api_fastapi.execution_api.workload_identity import (
    DagProcessorIdentityVerifier,
    WorkloadIdentityConfig,
    create_dag_processor_identity_verifier,
)
from airflow.dag_processing.api_client import (
    DagParseContext,
    DagProcessorAPIClient,
    DagProcessorJobAlreadyRunning,
)
from airflow.dag_processing.credentials import WorkloadIdentitySessionTokenProvider, _SessionTokenClient
from airflow.jobs.job import Job
from airflow.models.variable import Variable

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_jobs, clear_db_variables

pytestmark = pytest.mark.db_test

URL = "/execution/dag-processor/session-token"
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC).timestamp()
SECRET = "session-exchange-runtime-signing-secret-" * 3
TRUST = {
    "issuer": "https://identity.example/",
    "jwks_url": "https://identity.example/keys",
    "audience": "airflow-dag-processor-bootstrap",
    "algorithm": "EdDSA",
    "instance_id_claim": ["kubernetes.io", "pod", "uid"],
    "grants": {"system:serviceaccount:airflow:processor": ["dags-folder"]},
}


@pytest.fixture(autouse=True)
def configure(time_machine, identity_algorithm):
    time_machine.move_to(NOW, tick=False)
    with conf_vars(
        {
            ("execution_api", "dag_processor_workload_identity"): json.dumps(
                {**TRUST, "algorithm": identity_algorithm}
            ),
            ("execution_api", "jwt_expiration_time"): "300",
            ("api_auth", "jwt_secret"): SECRET,
            ("api_auth", "jwt_leeway"): "10",
        }
    ):
        yield


@pytest.fixture
def identity_algorithm(request):
    return getattr(request, "param", "EdDSA")


@pytest.fixture
def signing_key(identity_algorithm):
    if identity_algorithm == "RS256":
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)
    if identity_algorithm == "ES256":
        return ec.generate_private_key(ec.SECP256R1())
    return Ed25519PrivateKey.generate()


@pytest.fixture
def identity_token(signing_key, identity_algorithm):
    def issue(*, remove=(), key=None, headers=None, **overrides):
        claims = {
            "iss": TRUST["issuer"],
            "aud": TRUST["audience"],
            "sub": "system:serviceaccount:airflow:processor",
            "iat": NOW,
            "exp": NOW + 600,
            "kubernetes.io": {"pod": {"uid": "pod-1"}},
            **overrides,
        }
        for name in remove:
            claims.pop(name)
        return jwt.encode(
            claims, key or signing_key, algorithm=identity_algorithm, headers=headers or {"kid": "key-1"}
        )

    return issue


@pytest.fixture
def verifier(client, exec_app, signing_key, identity_algorithm, monkeypatch):
    if identity_algorithm == "ES256":
        public_key = jwt.algorithms.ECAlgorithm.to_jwk(signing_key.public_key(), as_dict=True) | {
            "kid": "key-1"
        }
    else:
        public_key = key_to_jwk_dict(signing_key, kid="key-1")
    jwks_client = httpx2.AsyncClient(
        transport=httpx2.MockTransport(lambda request: httpx2.Response(200, json={"keys": [public_key]}))
    )
    verifier = svcs.Container(exec_app.state.svcs_registry).get(DagProcessorIdentityVerifier)
    monkeypatch.setattr(verifier.validator.jwks, "client", jwks_client)
    yield verifier
    client.portal.call(jwks_client.aclose)


def use_verifier(exec_app, verifier):
    """Serve the exchange with another verifier until this test's API server lifespan ends."""
    exec_app.state.svcs_registry.register_value(DagProcessorIdentityVerifier, verifier)


@pytest.fixture
def make_processor(client, exec_app, identity_token, verifier):
    exec_app.dependency_overrides.pop(require_auth)
    clear_db_jobs()
    processors = []

    def create(exchange_available=lambda: True):
        def forward(request):
            if request.url.path == URL and not exchange_available():
                raise httpx2.ConnectError("session exchange unavailable", request=request)
            response = client.request(
                request.method, request.url.path, headers=request.headers, content=request.content
            )
            return httpx2.Response(response.status_code, headers=response.headers, content=response.content)

        transport = httpx2.MockTransport(forward)
        provider = WorkloadIdentitySessionTokenProvider(
            base_url="https://api/execution/", identity_token=identity_token, transport=transport
        )
        processor = DagProcessorAPIClient(
            base_url="https://api/execution/",
            hostname="processor",
            token_provider=provider,
            transport=transport,
        )
        processors.append(processor)
        return processor

    yield create
    for processor in processors:
        processor.close()
    clear_db_jobs()


def exchange(client, token, **kwargs):
    return client.post(URL, headers={"Authorization": f"Bearer {token}"}, **kwargs)


def decode(response):
    assert response.status_code == 200, response.text
    return jwt.decode(response.json()["token"], SECRET, algorithms=["HS512"], options={"verify_aud": False})


@pytest.mark.parametrize(("external_lifetime", "expected"), [(60, 60), (600, 300)])
@pytest.mark.parametrize("identity_algorithm", ["RS256", "ES256", "EdDSA"], indirect=True)
def test_issues_only_configured_grants_and_caps_lifetime(
    client, verifier, identity_token, external_lifetime, expected, caplog
):
    response = exchange(
        client,
        identity_token(exp=NOW + external_lifetime, dag_bundles=["unauthorized"], scope="execution"),
        json={"bundle_names": ["unauthorized"], "session_id": str(uuid7()), "hostname": "another-pod"},
    )
    claims = decode(response)
    assert claims["scope"] == "dag_processor_session"
    assert claims["dag_bundles"] == ["dags-folder"]
    assert claims["exp"] == NOW + expected
    assert response.json()["expires_in"] == expected
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Pragma"] == "no-cache"
    assert "Refreshed-API-Token" not in response.headers
    assert "JWT reissue middleware failed to refresh token" not in caplog


@conf_vars(
    {
        ("dag_processor", "dag_bundle_config_list"): json.dumps(
            [
                {
                    "name": name,
                    "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                    "kwargs": {},
                }
                for name in ("dags-folder", "another-bundle")
            ]
        )
    }
)
def test_renewal_and_api_restart_keep_session_but_replicas_and_grants_do_not(
    client, verifier, exec_app, identity_token
):
    first = decode(exchange(client, identity_token()))
    renewed = decode(exchange(client, identity_token(exp=NOW + 900)))
    replacement = DagProcessorIdentityVerifier(verifier.config, client=verifier.validator.jwks.client)
    use_verifier(exec_app, replacement)
    restarted = decode(exchange(client, identity_token()))
    replica = decode(exchange(client, identity_token(**{"kubernetes.io": {"pod": {"uid": "pod-2"}}})))
    assert first["sub"] == renewed["sub"] == restarted["sub"]
    assert first["jti"] != renewed["jti"]
    assert replica["sub"] != first["sub"]
    changed = WorkloadIdentityConfig.model_validate(
        {**TRUST, "grants": {"system:serviceaccount:airflow:processor": ["another-bundle"]}}
    )
    replacement = DagProcessorIdentityVerifier(changed, client=verifier.validator.jwks.client)
    changed_identity = client.portal.call(replacement.authenticate, identity_token())
    assert str(changed_identity.session_id) != first["sub"]


@pytest.mark.parametrize(
    "overrides",
    [
        {"iss": "https://untrusted.example/"},
        {"aud": "urn:airflow.apache.org:task"},
        {"exp": []},
        {"exp": float("inf")},
        {"iat": NOW + 60},
        {"nbf": NOW + 60},
        {"sub": ""},
        {"kubernetes.io": {}},
        {"kubernetes.io": {"pod": {"uid": ""}}},
        {"kubernetes.io": {"pod": {"uid": 123}}},
    ],
)
def test_rejects_invalid_signed_claims(client, verifier, identity_token, overrides):
    response = exchange(client, identity_token(**overrides))
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid workload identity"}
    assert response.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.parametrize("leeway", [0, 3, 10])
@pytest.mark.parametrize("claim", ["iat", "nbf"])
def test_identity_clock_skew_uses_configured_leeway(
    client, verifier, exec_app, identity_token, leeway, claim
):
    with conf_vars({("api_auth", "jwt_leeway"): str(leeway)}):
        replacement = DagProcessorIdentityVerifier(verifier.config, client=verifier.jwks.client)
    use_verifier(exec_app, replacement)
    assert exchange(client, identity_token(**{claim: NOW + leeway})).status_code == 200
    assert exchange(client, identity_token(**{claim: NOW + leeway + 1})).status_code == 401


@pytest.mark.parametrize("remaining", [-11, -1, 0, 0.5])
def test_clock_leeway_never_issues_session_after_identity_expiry(client, verifier, identity_token, remaining):
    response = exchange(client, identity_token(exp=NOW + remaining))
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert "token" not in response.json()


@pytest.mark.parametrize("claim", ["iss", "aud", "sub", "exp", "iat", "kubernetes.io"])
def test_requires_identity_claims(client, verifier, identity_token, claim):
    assert exchange(client, identity_token(remove=[claim])).status_code == 401


@pytest.mark.parametrize("invalid", ["malformed", "wrong-key", "unknown-kid", "no-kid", "algorithm"])
def test_rejects_invalid_signatures(client, verifier, identity_token, invalid):
    token = {
        "malformed": "not-a-jwt",
        "wrong-key": identity_token(key=Ed25519PrivateKey.generate()),
        "unknown-kid": identity_token(headers={"kid": "unknown"}),
        "no-kid": identity_token(headers={"typ": "JWT"}),
        "algorithm": jwt.encode({"sub": "x"}, SECRET, algorithm="HS512", headers={"kid": "key-1"}),
    }[invalid]
    assert exchange(client, token).status_code == 401


@mock.patch("airflow.api_fastapi.auth.tokens.time.monotonic", autospec=True, return_value=100)
def test_rotated_signing_key_refreshes_after_cooldown(
    clock, client, verifier, identity_token, signing_key, monkeypatch
):
    keys = [key_to_jwk_dict(signing_key, kid="key-1")]
    requests = []

    async def get_keys(url):
        requests.append(url)
        return httpx2.Response(200, json={"keys": keys}, request=httpx2.Request("GET", url))

    monkeypatch.setattr(verifier.validator.jwks.client, "get", get_keys)
    original = decode(exchange(client, identity_token()))
    rotated_key = Ed25519PrivateKey.generate()
    keys.append(key_to_jwk_dict(rotated_key, kid="key-2"))
    rotated = identity_token(key=rotated_key, headers={"kid": "key-2"})
    assert exchange(client, rotated).status_code == 401
    assert len(requests) == 1

    clock.return_value = 111
    assert decode(exchange(client, rotated))["sub"] == original["sub"]
    assert len(requests) == 2
    assert exchange(client, identity_token(headers={"kid": "unknown"})).status_code == 401
    assert len(requests) == 2


def test_ungranted_identity_is_forbidden(client, verifier, identity_token):
    assert exchange(client, identity_token(sub="another-service-account")).status_code == 403


def test_missing_bearer_is_unauthorized(client, verifier):
    client.headers.pop("Authorization")
    assert client.post(URL).status_code == 401


def test_disabled_exchange_is_unavailable(client, exec_app, identity_token):
    use_verifier(exec_app, None)
    assert exchange(client, identity_token()).status_code == 404


@conf_vars({("execution_api", "dag_processor_workload_identity"): ""})
def test_empty_configuration_disables_verifier(client):
    async def check():
        async with create_dag_processor_identity_verifier() as verifier:
            assert verifier is None

    client.portal.call(check)


@pytest.mark.asyncio
async def test_verifier_closes_jwks_connection_pool():
    async with create_dag_processor_identity_verifier() as verifier:
        client = verifier.validator.jwks.client
        assert not client.is_closed
    assert client.is_closed


def test_identity_expiring_during_exchange_is_rejected(
    client, verifier, identity_token, monkeypatch, time_machine
):
    authenticate = verifier.authenticate

    async def expire(token):
        identity = await authenticate(token)
        time_machine.shift(600)
        return identity

    monkeypatch.setattr(verifier, "authenticate", expire)
    assert exchange(client, identity_token()).status_code == 401


@pytest.mark.parametrize("failure", ["connection", "503", "invalid-json", "json-array", "invalid-keyset"])
def test_jwks_outage_is_retryable(client, verifier, identity_token, monkeypatch, failure):
    async def unavailable(url):
        if failure == "connection":
            raise httpx2.ConnectError("offline")
        request = httpx2.Request("GET", url)
        if failure == "503":
            return httpx2.Response(503, request=request)
        if failure == "invalid-json":
            return httpx2.Response(200, content=b"unavailable", request=request)
        if failure == "json-array":
            return httpx2.Response(200, json=[{"kty": "OKP"}], request=request)
        return httpx2.Response(200, json={"keys": [None]}, request=request)

    monkeypatch.setattr(verifier.jwks.client, "get", unavailable)
    response = exchange(client, identity_token())
    assert response.status_code == 503
    assert response.json() == {"detail": "Workload identity signing keys are unavailable"}
    assert response.headers["Retry-After"] == "10"


@pytest.mark.parametrize("cached", [False, True])
@mock.patch("airflow.api_fastapi.auth.tokens.time.monotonic", autospec=True, return_value=100)
def test_empty_jwks_rejects_identities_with_or_without_cached_keys(
    clock, client, verifier, identity_token, monkeypatch, cached
):
    if cached:
        assert exchange(client, identity_token()).status_code == 200
        clock.return_value = 3701

    async def get_keys(url):
        return httpx2.Response(200, json={"keys": []}, request=httpx2.Request("GET", url))

    monkeypatch.setattr(verifier.jwks.client, "get", get_keys)
    response = exchange(client, identity_token())
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid workload identity"}
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert "Retry-After" not in response.headers


@mock.patch("airflow.api_fastapi.auth.tokens.time.monotonic", autospec=True, return_value=100)
def test_jwks_outage_preserves_cached_key_validation(clock, client, verifier, identity_token, monkeypatch):
    assert exchange(client, identity_token()).status_code == 200

    async def unavailable(url):
        raise httpx2.ConnectError("offline")

    monkeypatch.setattr(verifier.jwks.client, "get", unavailable)
    clock.return_value = 3701
    assert exchange(client, identity_token()).status_code == 200
    assert exchange(client, identity_token(headers={"kid": "new-key"})).status_code == 503
    assert exchange(client, identity_token(key=Ed25519PrivateKey.generate())).status_code == 401


@mock.patch("airflow.api_fastapi.auth.tokens.time.monotonic", autospec=True, return_value=100)
def test_registration_recovers_from_jwks_outage(clock, make_processor, verifier, signing_key, monkeypatch):
    attempts = []

    async def get_keys(url):
        attempts.append(url)
        return httpx2.Response(
            503 if len(attempts) == 1 else 200,
            json={"keys": [key_to_jwk_dict(signing_key, kid="key-1")]},
            request=httpx2.Request("GET", url),
        )

    waits = []

    def sleep(seconds):
        waits.append(seconds)
        clock.return_value += seconds

    monkeypatch.setattr(verifier.jwks.client, "get", get_keys)
    # Keep the default wait policy: its backoff alone often retries inside the server's 10s cooldown.
    monkeypatch.setattr(_SessionTokenClient._request_with_retry.retry, "sleep", sleep)
    processor = make_processor()
    assert processor.register_job() == processor.job_id
    assert processor.heartbeat() == JobState.RUNNING
    assert waits == [10]
    assert len(attempts) == 2


@conf_vars({("dag_processor", "health_check_threshold"): "30"})
def test_container_restart_waits_for_previous_job_to_become_stale(make_processor, time_machine, session):
    previous = make_processor()
    previous_job = previous.register_job()
    replacement = make_processor()
    with pytest.raises(DagProcessorJobAlreadyRunning):
        replacement.register_job()
    time_machine.shift(31)
    replacement_job = replacement.register_job()
    assert replacement_job != previous_job
    session.expire_all()
    assert session.get(Job, previous_job).state == JobState.FAILED.value
    assert session.get(Job, previous_job).session_id is None
    assert session.get(Job, replacement_job).state == JobState.RUNNING.value


def test_unconfigured_bundle_grant_refuses_only_its_subject(
    client, verifier, exec_app, identity_token, caplog
):
    config = WorkloadIdentityConfig.model_validate(
        {**TRUST, "grants": {**TRUST["grants"], "system:serviceaccount:team-b:processor": ["team-b"]}}
    )
    use_verifier(exec_app, DagProcessorIdentityVerifier(config, client=verifier.validator.jwks.client))
    assert {
        "event": "[execution_api] dag_processor_workload_identity grants Dag bundles missing from "
        "[dag_processor] dag_bundle_config_list; these subjects cannot obtain a session",
        "log_level": "error",
        "unconfigured_grants": {"system:serviceaccount:team-b:processor": ["team-b"]},
    } in caplog

    assert exchange(client, identity_token()).status_code == 200
    response = exchange(client, identity_token(sub="system:serviceaccount:team-b:processor"))
    assert response.status_code == 403
    assert response.json() == {
        "detail": "Workload identity is granted Dag bundles this API server does not have: ['team-b']"
    }


@pytest.mark.parametrize("error", [TypeError("exp"), OverflowError("exp")])
@mock.patch.object(JWTValidator, "avalidated_claims", autospec=True)
def test_malformed_time_claims_from_older_pyjwt_are_invalid(
    avalidated_claims, client, verifier, identity_token, error, caplog
):
    """PyJWT before 2.15 raises these for list or infinite time claims instead of DecodeError."""
    avalidated_claims.side_effect = error
    response = exchange(client, identity_token())
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid workload identity"}
    assert {
        "event": "Rejecting a workload identity with malformed time claims",
        "log_level": "warning",
    } in caplog


@mock.patch(
    "airflow.api_fastapi.execution_api.workload_identity.uuid5", autospec=True, side_effect=TypeError("bug")
)
def test_verifier_bugs_are_not_reported_as_invalid_identity(uuid5, client, verifier, identity_token):
    with pytest.raises(TypeError, match="bug"):
        exchange(client, identity_token())


def test_older_version_has_no_exchange(client, verifier, identity_token):
    client.headers["Airflow-API-Version"] = "2026-10-30"
    response = exchange(client, identity_token())
    assert response.status_code == 404
    assert response.json() == {"detail": "Not Found"}


@pytest.mark.parametrize(
    "url", ["http://identity/keys", "https://user:secret@identity/keys", "https://identity/keys#fragment"]
)
def test_jwks_url_requires_https_without_embedded_credentials(url):
    with pytest.raises(ValidationError):
        WorkloadIdentityConfig.model_validate({**TRUST, "jwks_url": url})


def test_identity_audience_must_differ_from_runtime(client, verifier):
    config = WorkloadIdentityConfig.model_validate({**TRUST, "audience": "urn:airflow.apache.org:task"})
    with pytest.raises(ValueError, match="dedicated audience"):
        DagProcessorIdentityVerifier(config, client=verifier.validator.jwks.client)


def test_identity_and_runtime_tokens_are_not_interchangeable(client, verifier, exec_app, identity_token):
    exec_app.dependency_overrides.pop(require_auth)
    identity = identity_token()
    session_token = decode(exchange(client, identity))
    assert session_token["scope"] == "dag_processor_session"
    issued_token = exchange(client, identity).json()["token"]
    assert exchange(client, issued_token).status_code == 401
    response = client.post(
        "/execution/jobs",
        headers={"Authorization": f"Bearer {identity}"},
        json={"registration_id": str(uuid7()), "hostname": "processor"},
    )
    assert response.status_code == 403


@mock.patch("airflow.dag_processing.api_client.monotonic", autospec=True, return_value=0)
@mock.patch("airflow.dag_processing.credentials.monotonic", autospec=True, return_value=0)
def test_session_renewal_and_job_lifecycle(
    credential_clock, job_clock, make_processor, session, time_machine
):
    clear_db_variables()
    exchange_available = True
    processor = make_processor(exchange_available=lambda: exchange_available)
    provider = processor._token_provider
    try:
        Variable.set("identity-test-key", "value")
        job_id = processor.register_job()
        first_session = provider.get_token()
        assert processor.heartbeat() == JobState.RUNNING
        context = DagParseContext(
            request=DagParseTokenBody(
                attempt_id=uuid7(), bundle_name="dags-folder", relative_fileloc="dag.py"
            )
        )
        with processor.use_parse(context):
            assert processor.variables.get("identity-test-key").value == "value"
        time_machine.shift(240)
        credential_clock.return_value = job_clock.return_value = 240
        assert processor.heartbeat() == JobState.RUNNING
        assert provider.get_token() != first_session
        assert processor.job_id == job_id
        assert not processor.restart_required
        assert session.scalars(select(Job.id)).all() == [job_id]

        exchange_available = False
        time_machine.shift(240)
        credential_clock.return_value = job_clock.return_value = 480
        assert processor.heartbeat() == JobState.RUNNING
        time_machine.shift(61)
        credential_clock.return_value = job_clock.return_value = 541
        with pytest.raises(httpx2.ConnectError):
            processor.heartbeat()

        exchange_available = True
        assert processor.heartbeat() == JobState.RUNNING
        assert processor.job_id == job_id
        processor.complete_job(TerminalJobState.SUCCESS)
        assert session.get(Job, job_id).state == JobState.SUCCESS.value
    finally:
        clear_db_variables()
