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

import base64
import json
import ssl
import traceback
from functools import partial
from unittest import mock

import httpx2
import pytest

from airflow.cli.commands import dag_processor_token_kubernetes
from airflow.cli.commands.dag_processor_token_kubernetes import (
    KubernetesSecretTokenError,
    KubernetesSecretTokenStore,
)


def make_secret_response(token: str = "old-session", version: str = "10") -> httpx2.Response:
    return httpx2.Response(
        200,
        json={
            "metadata": {"resourceVersion": version},
            "data": {
                "processor.jwt": base64.b64encode(token.encode()).decode(),
                "other-key": base64.b64encode(b"preserved").decode(),
            },
        },
    )


@pytest.fixture
def service_account_directory(tmp_path, monkeypatch):
    (tmp_path / "token").write_text("service-account-1\n")
    monkeypatch.setattr(dag_processor_token_kubernetes, "_SERVICE_ACCOUNT_DIRECTORY", tmp_path)
    return tmp_path


@pytest.fixture
def make_store(service_account_directory, monkeypatch):
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", "443")
    monkeypatch.delenv("KUBERNETES_SERVICE_PORT_HTTPS", raising=False)
    client_class = httpx2.Client
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    create_context = mock.create_autospec(ssl.create_default_context, return_value=context)
    monkeypatch.setattr(dag_processor_token_kubernetes.ssl, "create_default_context", create_context)
    clients = []

    def create(*responses):
        requests = []
        pending = iter(responses)

        def handle(request):
            requests.append(request)
            result = next(pending)
            if isinstance(result, Exception):
                raise result
            return result

        def create_client(**kwargs):
            return client_class(transport=httpx2.MockTransport(handle), **kwargs)

        factory = mock.create_autospec(client_class, side_effect=create_client)
        monkeypatch.setattr(dag_processor_token_kubernetes.httpx2, "Client", factory)
        store = KubernetesSecretTokenStore("processor-session", "dag-processors")
        clients.append(store)
        return store, requests, factory, create_context

    yield create
    for store in clients:
        store.__exit__(None, None, None)


def test_reads_and_conditionally_updates_only_the_token(make_store):
    store, requests, _, _ = make_store(make_secret_response(), httpx2.Response(200, json={}))

    with store:
        assert store.read_token() == "old-session"
        store.write_token("new-session")

    assert [request.method for request in requests] == ["GET", "PATCH"]
    assert str(requests[0].url) == (
        "https://10.96.0.1/api/v1/namespaces/dag-processors/secrets/processor-session"
    )
    assert requests[1].headers["content-type"] == "application/merge-patch+json"
    assert json.loads(requests[1].content) == {
        "metadata": {"resourceVersion": "10"},
        "data": {"processor.jwt": base64.b64encode(b"new-session").decode()},
    }
    assert store._client.is_closed


def test_uses_cluster_ca_and_bounded_direct_requests(make_store, service_account_directory):
    store, _, factory, create_context = make_store()

    create_context.assert_called_once_with(cafile=str(service_account_directory / "ca.crt"))
    factory.assert_called_once_with(
        base_url=httpx2.URL("https://10.96.0.1"),
        verify=create_context.return_value,
        timeout=10,
        follow_redirects=False,
        trust_env=False,
    )
    with pytest.raises(RuntimeError, match="stop"), store:
        raise RuntimeError("stop")
    assert store._client.is_closed


@pytest.mark.parametrize(
    ("host", "https_port", "service_port", "endpoint"),
    [
        ("10.96.0.2", None, "443", "https://10.96.0.2"),
        ("10.96.0.2", "6443", "443", "https://10.96.0.2:6443"),
        ("api.internal", "8443", None, "https://api.internal:8443"),
        ("api.internal", "", "8443", "https://api.internal:8443"),
        ("fd00::1", None, "6443", "https://[fd00::1]:6443"),
        ("fd00::1", "443", "6443", "https://[fd00::1]"),
    ],
)
def test_uses_advertised_kubernetes_endpoint(
    make_store, monkeypatch, host, https_port, service_port, endpoint
):
    for name, value in (
        ("KUBERNETES_SERVICE_HOST", host),
        ("KUBERNETES_SERVICE_PORT_HTTPS", https_port),
        ("KUBERNETES_SERVICE_PORT", service_port),
    ):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    store, requests, _, _ = make_store(make_secret_response(), httpx2.Response(200))

    store.read_token()
    store.write_token("new-session")

    assert [str(request.url) for request in requests] == [
        f"{endpoint}/api/v1/namespaces/dag-processors/secrets/processor-session"
    ] * 2


@pytest.mark.parametrize(
    ("host", "port"),
    [(None, "443"), ("", "443"), ("10.96.0.1", None), ("10.96.0.1", "")],
)
def test_requires_in_cluster_endpoint_configuration(make_store, monkeypatch, host, port):
    for name, value in (("KUBERNETES_SERVICE_HOST", host), ("KUBERNETES_SERVICE_PORT", port)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match="Kubernetes API access requires KUBERNETES_SERVICE_HOST"):
        make_store()


@pytest.mark.parametrize("port", ["not-a-port", "0", "-1", "65536"])
def test_rejects_invalid_kubernetes_service_port(make_store, monkeypatch, port):
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", port)

    with pytest.raises(ValueError, match="Kubernetes service port must be an integer between 1 and 65535"):
        make_store()


@pytest.mark.parametrize("data", [None, {}, {"processor.jwt": ""}, {"processor.jwt": "IAo="}])
def test_accepts_an_uninitialized_secret(make_store, data):
    body = {"metadata": {"resourceVersion": "1"}}
    if data is not None:
        body["data"] = data
    store, requests, _, _ = make_store(httpx2.Response(200, json=body), httpx2.Response(200))

    assert store.read_token() is None
    store.write_token("first-session")

    assert json.loads(requests[1].content)["metadata"] == {"resourceVersion": "1"}


def test_reloads_service_account_token_for_each_request(make_store, service_account_directory):
    store, requests, _, _ = make_store(make_secret_response(), httpx2.Response(200))

    store.read_token()
    (service_account_directory / "token").write_text("service-account-2\n")
    store.write_token("new-session")

    assert [request.headers["authorization"] for request in requests] == [
        "Bearer service-account-1",
        "Bearer service-account-2",
    ]


@pytest.mark.parametrize("missing", [False, True])
def test_rejects_empty_or_unreadable_service_account_token(make_store, service_account_directory, missing):
    store, requests, _, _ = make_store()
    token_file = service_account_directory / "token"
    if missing:
        token_file.unlink()
    else:
        token_file.write_text(" \n")

    with pytest.raises(KubernetesSecretTokenError, match="service account token"):
        store.read_token()

    assert not requests


@pytest.mark.parametrize("method", ["GET", "PATCH"])
@pytest.mark.parametrize("status", [302, 403, 404, 409, 422, 503])
def test_reports_http_failures_without_secret_contents(make_store, method, status):
    failure = httpx2.Response(
        status,
        json={"message": "sensitive-token"},
        headers={"Location": "https://another-service.invalid/"},
    )
    responses = [failure] if method == "GET" else [make_secret_response(), failure]
    store, requests, _, _ = make_store(*responses)
    if method == "PATCH":
        store.read_token()
    operation = store.read_token if method == "GET" else partial(store.write_token, "new-session")

    with pytest.raises(KubernetesSecretTokenError, match=f"{method} returned HTTP {status}") as raised:
        operation()

    assert raised.value.status_code == status
    assert "sensitive-token" not in str(raised.value)
    assert len(requests) == len(responses)


@pytest.mark.parametrize("method", ["GET", "PATCH"])
@pytest.mark.parametrize("error_type", [httpx2.ConnectError, httpx2.ReadTimeout, httpx2.PoolTimeout])
def test_reports_transport_failures_without_secret_contents(make_store, method, error_type):
    failure = error_type("sensitive-token")
    responses = [failure] if method == "GET" else [make_secret_response(), failure]
    store, _, _, _ = make_store(*responses)
    if method == "PATCH":
        store.read_token()
    operation = store.read_token if method == "GET" else partial(store.write_token, "new-session")

    with pytest.raises(
        KubernetesSecretTokenError, match=rf"{method} request failed \({error_type.__name__}\)"
    ) as raised:
        operation()

    assert raised.value.status_code is None
    assert "sensitive-token" not in "".join(traceback.format_exception(raised.value))


@pytest.mark.parametrize(
    "body",
    [
        None,
        [],
        {},
        {"metadata": None},
        {"metadata": {"resourceVersion": ""}},
        {"metadata": {"resourceVersion": 10}},
        {"metadata": {"resourceVersion": "10"}, "data": []},
        {"metadata": {"resourceVersion": "10"}, "data": {"processor.jwt": None}},
        {"metadata": {"resourceVersion": "10"}, "data": {"processor.jwt": "invalid!"}},
        {"metadata": {"resourceVersion": "10"}, "data": {"processor.jwt": "/w=="}},
    ],
)
def test_rejects_malformed_secret_without_allowing_a_write(make_store, body):
    store, requests, _, _ = make_store(httpx2.Response(200, json=body))

    with pytest.raises(KubernetesSecretTokenError, match="invalid token data or metadata"):
        store.read_token()
    with pytest.raises(KubernetesSecretTokenError, match="Read the Kubernetes Secret successfully"):
        store.write_token("new-session")

    assert len(requests) == 1


def test_failed_read_invalidates_the_previous_version(make_store):
    store, requests, _, _ = make_store(make_secret_response(), httpx2.Response(503))
    store.read_token()

    with pytest.raises(KubernetesSecretTokenError, match="HTTP 503"):
        store.read_token()
    with pytest.raises(KubernetesSecretTokenError, match="Read the Kubernetes Secret successfully"):
        store.write_token("new-session")

    assert [request.method for request in requests] == ["GET", "GET"]


@pytest.mark.parametrize(
    "write_result",
    [
        httpx2.Response(200),
        httpx2.Response(409),
        httpx2.ReadTimeout("The response was lost after persistence"),
    ],
)
def test_each_write_requires_a_new_read_even_after_an_uncertain_response(make_store, write_result):
    store, requests, _, _ = make_store(
        make_secret_response(),
        write_result,
        make_secret_response("persisted-session", "11"),
        httpx2.Response(200),
    )
    store.read_token()
    if isinstance(write_result, Exception) or write_result.status_code == 409:
        with pytest.raises(KubernetesSecretTokenError):
            store.write_token("first-attempt")
    else:
        store.write_token("first-attempt")
    with pytest.raises(KubernetesSecretTokenError, match="Read the Kubernetes Secret successfully"):
        store.write_token("must-not-overwrite")

    assert store.read_token() == "persisted-session"
    store.write_token("second-attempt")

    assert [request.method for request in requests] == ["GET", "PATCH", "GET", "PATCH"]
    assert json.loads(requests[3].content)["metadata"] == {"resourceVersion": "11"}


def test_rejects_empty_session_token(make_store):
    store, requests, _, _ = make_store(make_secret_response())
    store.read_token()

    with pytest.raises(ValueError, match="must not be empty"):
        store.write_token(" \n")

    assert len(requests) == 1


@pytest.mark.parametrize(
    ("secret_name", "namespace"),
    [
        ("", "processors"),
        ("../other-secret", "processors"),
        ("secret/another", "processors"),
        ("secret..name", "processors"),
        ("Secret", "processors"),
        ("s" * 254, "processors"),
        ("secret", ""),
        ("secret", "another/namespace"),
        ("secret", "processor.example"),
        ("secret", "n" * 64),
    ],
)
def test_rejects_invalid_secret_or_namespace_names(secret_name, namespace):
    with pytest.raises(ValueError, match="Kubernetes"):
        KubernetesSecretTokenStore(secret_name, namespace)
