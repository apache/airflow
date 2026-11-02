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

from contextlib import nullcontext
from unittest import mock

import httpx2
import pytest
from pydantic import ValidationError
from tenacity import wait_none, wait_random_exponential

from airflow.api_fastapi.execution_api.datamodels.job import JobState
from airflow.api_fastapi.execution_api.versions import bundle
from airflow.dag_processing import credentials
from airflow.dag_processing.api_client import DagProcessorAPIClient
from airflow.dag_processing.credentials import (
    SessionTokenProvider,
    WorkloadIdentitySessionTokenProvider,
    WorkloadIdentityUnavailableError,
    _SessionTokenClient,
)
from airflow.sdk.api.client import (
    API_RETRIES,
    API_RETRY_WAIT_MAX,
    API_RETRY_WAIT_MIN,
    API_TIMEOUT,
    BearerAuth,
    Client,
)


class RefreshError(Exception):
    """Stands in for google-auth's RefreshError, which subclasses Exception directly."""


@pytest.fixture(autouse=True)
def no_retry_wait(monkeypatch):
    monkeypatch.setattr(Client._request_with_retry.retry, "wait", wait_none())
    # The exchange keeps its own wait policy under test; only the sleeping is skipped.
    monkeypatch.setattr(_SessionTokenClient._request_with_retry.retry, "sleep", lambda seconds: None)


@pytest.fixture
def clock():
    with mock.patch("airflow.dag_processing.credentials.monotonic", autospec=True, return_value=0) as clock:
        yield clock


@pytest.fixture
def make_provider(clock):
    providers = []

    def create(*responses, identity=None):
        pending = iter(responses)
        requests = []

        def handle(request):
            requests.append(request)
            response = next(pending)
            if isinstance(response, Exception):
                raise response
            return response(request) if callable(response) else response

        provider = WorkloadIdentitySessionTokenProvider(
            base_url="https://api/execution/",
            identity_token=identity or mock.Mock(spec=lambda: "", return_value="identity-1\n"),
            transport=httpx2.MockTransport(handle),
        )
        providers.append(provider)
        return provider, requests

    yield create
    for provider in providers:
        provider.close()


def issued(token="session-1", expires_in=100):
    return httpx2.Response(200, json={"token": token, "expires_in": expires_in})


@mock.patch("airflow.sdk.api.client.API_SSL_CA_FILE_PATH", "/private-ca.pem")
@mock.patch("airflow.sdk.api.client.API_SSL_CERT_PATH", "/server.pem")
@mock.patch("airflow.sdk.api.client.API_CLIENT_SSL_CERT", "/client.pem")
@mock.patch("airflow.sdk.api.client.API_CLIENT_SSL_KEY", "/client.key")
@mock.patch.object(
    Client, "_get_ssl_context_cached", new_callable=mock.Mock, spec=lambda ca_file, ca_path: None
)
def test_exchange_uses_api_tls_settings(get_context, make_provider, monkeypatch):
    captured = {}
    original_init = httpx2.Client.__init__

    def capture_init(self, **kwargs):
        captured.update(kwargs)
        original_init(self, **kwargs)

    monkeypatch.setattr(httpx2.Client, "__init__", capture_init)
    provider, _ = make_provider(issued())

    get_context.assert_called_once_with("/private-ca.pem", "/server.pem")
    assert captured["verify"] is get_context.return_value
    assert captured["cert"] == ("/client.pem", "/client.key")
    assert provider._client.timeout == httpx2.Timeout(API_TIMEOUT)


def test_runtime_refresh_header_cannot_replace_external_identity(make_provider):
    response = issued()
    response.headers["Refreshed-API-Token"] = "runtime-token"
    provider, requests = make_provider(response, issued("session-2"))
    assert provider.get_token() == "session-1"
    assert provider.get_token(force_refresh=True) == "session-2"
    assert [request.headers["Authorization"] for request in requests] == ["Bearer identity-1"] * 2


def test_caches_only_in_memory_and_renews_with_current_identity(make_provider, clock):
    identity = mock.Mock(spec=lambda: "", side_effect=["identity-1\n", "identity-2"])
    provider, requests = make_provider(issued(), issued("session-2"), identity=identity)

    assert provider.get_token() == "session-1"
    clock.return_value = 79
    assert provider.get_token() == "session-1"
    clock.return_value = 80
    assert provider.get_token() == "session-2"

    assert identity.call_count == 2
    assert [r.headers["Authorization"] for r in requests] == ["Bearer identity-1", "Bearer identity-2"]
    assert all(r.url.path == "/execution/dag-processor/session-token" for r in requests)
    assert all(r.headers["Airflow-API-Version"] == bundle.version_values[0] for r in requests)
    assert all(r.content == b"" for r in requests)
    provider.close()
    assert provider._token is None
    assert provider._client.is_closed


def test_forced_refresh_and_timeout_are_forwarded(make_provider):
    provider, requests = make_provider(issued(), issued("session-2"))

    assert provider.get_token() == "session-1"
    assert provider.get_token(force_refresh=True, timeout=httpx2.Timeout(0.5)) == "session-2"
    assert requests[-1].extensions["timeout"] == httpx2.Timeout(0.5).as_dict()


@pytest.mark.parametrize(
    "failure", [httpx2.Response(401), httpx2.Response(503), httpx2.ConnectError("offline")]
)
def test_failed_renewal_does_not_return_cached_session(make_provider, clock, failure):
    provider, requests = make_provider(issued(), failure, issued("session-2"))
    assert provider.get_token() == "session-1"
    clock.return_value = 100

    with pytest.raises(httpx2.HTTPError):
        provider.get_token()

    assert provider.get_token() == "session-2"
    assert len(requests) == 3


@pytest.mark.parametrize("payload", [{"token": "", "expires_in": 10}, {"token": "x", "expires_in": 0}, {}])
def test_invalid_exchange_response_is_not_cached(make_provider, payload):
    provider, _ = make_provider(httpx2.Response(200, json=payload))
    with pytest.raises(ValidationError):
        provider.get_token()
    assert provider._token is None


def test_response_lifetime_includes_request_duration(make_provider, clock):
    def delayed(request):
        clock.return_value = 100
        return issued(expires_in=100)

    provider, _ = make_provider(delayed)
    with pytest.raises(ValueError, match="expired during the exchange"):
        provider.get_token()
    assert provider._token is None


def test_missing_identity_fails_before_request(make_provider):
    provider, requests = make_provider(identity=mock.Mock(spec=lambda: "", return_value="\n"))
    with pytest.raises(ValueError, match="credential is empty"):
        provider.get_token()
    assert not requests


def test_identity_supplier_errors_are_reported_as_unavailable(make_provider):
    error = RefreshError("metadata server unavailable")
    provider, requests = make_provider(identity=mock.Mock(spec=lambda: "", side_effect=error))
    with pytest.raises(WorkloadIdentityUnavailableError) as raised:
        provider.get_token()
    assert raised.value.__cause__ is error
    assert not requests


@pytest.mark.parametrize(
    ("retry_after", "maximum_wait", "wait"),
    [
        ("10", API_RETRY_WAIT_MAX, 10),
        ("10", 1, 10),
        ("3600", API_RETRY_WAIT_MAX, 3600),
        ("inf", API_RETRY_WAIT_MAX, API_RETRY_WAIT_MIN),
        ("Wed, 21 Oct 2026 07:28:00 GMT", API_RETRY_WAIT_MAX, API_RETRY_WAIT_MIN),
    ],
)
def test_exchange_retry_waits_for_retry_after(make_provider, monkeypatch, retry_after, maximum_wait, wait):
    waits = []
    monkeypatch.setattr(credentials, "API_RETRY_WAIT_MAX", maximum_wait)
    monkeypatch.setattr(
        credentials, "_backoff", wait_random_exponential(min=API_RETRY_WAIT_MIN, max=maximum_wait)
    )
    monkeypatch.setattr(_SessionTokenClient._request_with_retry.retry, "sleep", waits.append)
    provider, requests = make_provider(httpx2.Response(503, headers={"Retry-After": retry_after}), issued())
    assert provider.get_token(retry=True) == "session-1"
    assert waits == [wait]
    assert len(requests) == 2


def test_redirect_does_not_forward_identity(make_provider):
    provider, requests = make_provider(httpx2.Response(307, headers={"Location": "https://other/execution/"}))
    with pytest.raises(httpx2.HTTPStatusError):
        provider.get_token()
    assert len(requests) == 1


def test_exchange_requires_https():
    with pytest.raises(ValueError, match="HTTPS"):
        WorkloadIdentitySessionTokenProvider(base_url="http://api/execution/", identity_token=lambda: "x")


@pytest.mark.parametrize("sources", [{}, {"token_file": "unused", "token_provider": mock.sentinel.provider}])
def test_client_requires_exactly_one_credential_source(sources):
    with pytest.raises(ValueError, match="exactly one"):
        DagProcessorAPIClient(base_url="https://api/execution/", hostname="processor", **sources)


@pytest.mark.parametrize("status", [401, 403])
def test_client_forces_provider_refresh_after_registration_auth_failure(status):
    provider = mock.Mock(spec=SessionTokenProvider)
    provider.get_token.side_effect = ["session-1", "session-2"]
    requests = []

    def handle(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx2.Response(status)
        return httpx2.Response(201, json={"job_id": 1, "token": "job-1", "expires_in": 300})

    with DagProcessorAPIClient(
        base_url="https://api/execution/",
        hostname="processor",
        token_provider=provider,
        transport=httpx2.MockTransport(handle),
    ) as client:
        assert client.register_job() == 1
        assert provider.get_token.call_args_list == [
            mock.call(force_refresh=False, retry=True, timeout=None),
            mock.call(force_refresh=True, retry=True, timeout=None),
        ]
        assert requests[0].content == requests[1].content

    provider.close.assert_called_once_with()
    assert client._session_token is None
    assert client._registered_with is None


@pytest.mark.parametrize("retry", [False, True])
@pytest.mark.parametrize("failure", [httpx2.Response(503), httpx2.ConnectError("offline")])
def test_registration_retries_session_exchange_only_when_requested(make_provider, failure, retry):
    provider, requests = make_provider(failure, issued())
    registrations = []

    def register(request):
        registrations.append(request)
        return httpx2.Response(201, json={"job_id": 1, "token": "job-1", "expires_in": 300})

    with DagProcessorAPIClient(
        base_url="https://api/execution/",
        hostname="processor",
        token_provider=provider,
        transport=httpx2.MockTransport(register),
    ) as client:
        if retry:
            assert client.register_job(retry=True) == 1
            assert len(requests) == 2
            assert len(registrations) == 1
        else:
            with pytest.raises(httpx2.HTTPError):
                client.register_job(retry=False)
            assert len(requests) == 1
            assert not registrations


@pytest.mark.parametrize(("status", "attempts"), [(401, 1), (403, 1), (503, API_RETRIES)])
def test_exchange_retries_are_bounded_and_do_not_retry_auth_failures(make_provider, status, attempts):
    provider, requests = make_provider(*(httpx2.Response(status) for _ in range(API_RETRIES)))
    with pytest.raises(httpx2.HTTPStatusError):
        provider.get_token(retry=True)
    assert len(requests) == attempts
    assert provider._token is None


@mock.patch("airflow.dag_processing.api_client.monotonic", autospec=True)
def test_optional_renewal_attempts_exchange_once_then_uses_valid_job_token(
    client_clock, make_provider, clock
):
    client_clock.return_value = 0
    provider, exchanges = make_provider(issued(), httpx2.Response(503))
    requests = []

    def handle(request):
        requests.append(request)
        if request.url.path.endswith("/jobs"):
            return httpx2.Response(201, json={"job_id": 1, "token": "job-1", "expires_in": 300})
        return httpx2.Response(200, json={"state": "running"})

    with DagProcessorAPIClient(
        base_url="https://api/execution/",
        hostname="processor",
        token_provider=provider,
        transport=httpx2.MockTransport(handle),
    ) as client:
        client.register_job()
        client_clock.return_value = clock.return_value = 80
        client.heartbeat()

    assert len(exchanges) == 2
    assert exchanges[-1].extensions["timeout"] == httpx2.Timeout(1).as_dict()
    assert len(requests) == 2
    assert requests[-1].headers["Authorization"] == "Bearer job-1"


@mock.patch("airflow.dag_processing.api_client.monotonic", autospec=True)
def test_failed_identity_supplier_keeps_using_valid_job_token(client_clock, make_provider, clock):
    client_clock.return_value = 0
    identity = mock.Mock(spec=lambda: "", side_effect=["identity-1", RefreshError("metadata server down")])
    provider, exchanges = make_provider(issued(), identity=identity)
    requests = []

    def handle(request):
        requests.append(request)
        if request.url.path.endswith("/jobs"):
            return httpx2.Response(201, json={"job_id": 1, "token": "job-1", "expires_in": 300})
        return httpx2.Response(200, json={"state": "running"})

    with DagProcessorAPIClient(
        base_url="https://api/execution/",
        hostname="processor",
        token_provider=provider,
        transport=httpx2.MockTransport(handle),
    ) as client:
        client.register_job()
        client_clock.return_value = clock.return_value = 80
        assert client.heartbeat() == JobState.RUNNING

    assert identity.call_count == 2
    assert len(exchanges) == 1
    assert requests[-1].headers["Authorization"] == "Bearer job-1"


@pytest.mark.parametrize("use_context", [False, True])
@pytest.mark.parametrize("close_fails", [False, True])
def test_client_close_releases_credentials_and_connections(use_context, close_fails):
    provider = mock.Mock(spec=SessionTokenProvider)
    if close_fails:
        provider.close.side_effect = OSError("cleanup failed")
    client = DagProcessorAPIClient(
        base_url="https://api/execution/", hostname="processor", token_provider=provider
    )
    client._session_token = client._registered_with = "session"
    client.auth = BearerAuth("job")
    with pytest.raises(OSError, match="cleanup failed") if close_fails else nullcontext():
        if use_context:
            with client:
                pass
        else:
            client.close()
    provider.close.assert_called_once_with()
    assert client._session_token is None
    assert client._registered_with is None
    assert client.auth.token == ""
    assert client.is_closed
