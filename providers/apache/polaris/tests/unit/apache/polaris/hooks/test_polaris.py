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
import contextlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from typing import ClassVar
from unittest import mock

import pytest
import requests
import urllib3
from apache_polaris.sdk.management import Catalog

from airflow.models import Connection
from airflow.providers.apache.polaris.exceptions import (
    PolarisApiError,
    PolarisAuthenticationError,
    PolarisConflictError,
    PolarisConnectionError,
    PolarisError,
    PolarisForbiddenError,
    PolarisInvalidArgumentError,
    PolarisNotFoundError,
    PolarisRequestError,
)
from airflow.providers.apache.polaris.hooks.polaris import (
    DEFAULT_SCOPE,
    UPDATE_ATTEMPTS,
    UPDATE_RETRY_PAUSE,
    PolarisCatalogHook,
    PolarisConnectionConfig,
    PolarisHook,
    PolarisManagementHook,
    _pause_before_retry,
    encode_namespace,
)

from unit.apache.polaris.fake_polaris import TOKEN_PATH

HOOK_MODULE = "airflow.providers.apache.polaris.hooks.polaris"
MGMT = "/api/management/v1"
CAT = "/api/catalog"


def _config(**kwargs) -> PolarisConnectionConfig:
    extra = kwargs.pop("extra", {})
    conn = Connection(conn_id="c", conn_type="polaris", extra=json.dumps(extra), **kwargs)
    return PolarisConnectionConfig.from_connection(conn)


def _basic(client_id: str, secret: str) -> str:
    return "Basic " + base64.b64encode(f"{client_id}:{secret}".encode()).decode()


def _jwt(claims: dict) -> str:
    def enc(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{enc({'alg': 'none'})}.{enc(claims)}.sig"


@pytest.fixture
def retry_pause():
    """Replace the random pause before an update retry, so retry tests run instantly."""
    with mock.patch(f"{HOOK_MODULE}._pause_before_retry", autospec=True) as pause:
        yield pause


def _error(status: int, message: str, error_type: str):
    return status, {"error": {"message": message, "type": error_type, "code": status}}


class TestConnectionConfig:
    def test_internal_defaults(self):
        config = _config(host="https://polaris.example.com/", login="id", password="secret")
        assert config.auth_type == "internal"
        assert config.catalog_url == "https://polaris.example.com/api/catalog"
        assert config.management_url == "https://polaris.example.com/api/management/v1"
        assert config.token_url == "https://polaris.example.com/api/catalog/v1/oauth/tokens"
        assert config.scope == DEFAULT_SCOPE
        assert config.realm_header == "Polaris-Realm"
        assert config.client_auth_method == "client_secret_basic"
        assert config.verify is True
        assert config.timeout == 30.0

    def test_host_without_scheme_gets_https_and_port_and_path_prefix(self):
        config = _config(host="gw.example.com/polaris", port=8443, login="id", password="secret")
        assert config.catalog_url == "https://gw.example.com:8443/polaris/api/catalog"

    def test_uri_form_path_becomes_the_path_prefix(self):
        conn = Connection(conn_id="c", uri="polaris://id:secret@gw.example.com:8443/polaris?realm=PROD")
        config = PolarisConnectionConfig.from_connection(conn)
        assert config.catalog_url == "https://gw.example.com:8443/polaris/api/catalog"
        assert config.management_url == "https://gw.example.com:8443/polaris/api/management/v1"

    def test_custom_paths_and_token_url(self):
        config = _config(
            host="http://h",
            login="id",
            password="secret",
            extra={"catalog_path": "cat/", "management_path": "/mgmt", "oauth2_server_uri": "http://h/tok"},
        )
        assert config.catalog_url == "http://h/cat"
        assert config.management_url == "http://h/mgmt"
        assert config.token_url == "http://h/tok"

    def test_secrets_not_in_repr(self):
        config = _config(host="h", login="id", password="very-secret")
        assert "very-secret" not in repr(config)
        token_config = _config(host="h", password="tok-secret", extra={"auth_type": "token"})
        assert "tok-secret" not in repr(token_config)

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (None, True),
            (True, True),
            (False, False),
            ("false", False),
            ("True", True),
            ("/ca.pem", "/ca.pem"),
        ],
    )
    def test_verify(self, value, expected):
        assert _config(host="h", login="i", password="p", extra={"verify": value}).verify == expected

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"host": ""}, "Host"),
            ({"host": "h", "extra": {"auth_type": "kerberos"}}, "unknown auth_type"),
            ({"host": "h", "extra": {"client_auth_method": "jwt"}}, "unknown client_auth_method"),
            ({"host": "h", "password": "p"}, "Client ID"),
            ({"host": "h", "login": "i"}, "Client Secret"),
            ({"host": "h", "extra": {"auth_type": "token"}}, "bearer token in Password"),
            (
                {"host": "h", "login": "i", "password": "p", "extra": {"auth_type": "oidc"}},
                "oauth2_server_uri",
            ),
            ({"host": "h", "login": "i", "password": "p", "extra": {"scope": "catalog"}}, "PRINCIPAL_ROLE"),
            (
                {"host": "h", "login": "i", "password": "p", "extra": {"scope": "PRINCIPAL_ROLE:"}},
                "PRINCIPAL_ROLE",
            ),
            ({"host": "h", "login": "i", "password": "p", "extra": {"timeout": "soon"}}, "timeout"),
        ],
    )
    def test_invalid(self, kwargs, match):
        with pytest.raises(PolarisConnectionError, match=match):
            _config(**kwargs)

    def test_oidc_keeps_scope_audience_resource(self):
        config = _config(
            host="h",
            login="i",
            password="p",
            extra={
                "auth_type": "oidc",
                "oauth2_server_uri": "https://idp/token",
                "scope": "api://polaris/.default",
                "audience": "polaris",
                "resource": "urn:polaris",
            },
        )
        assert (config.scope, config.audience, config.resource) == (
            "api://polaris/.default",
            "polaris",
            "urn:polaris",
        )

    def test_token_mode_ignores_scope(self):
        config = _config(host="h", password="tok", extra={"auth_type": "token", "scope": "x"})
        assert config.token == "tok"
        assert config.scope is None
        assert config.token_url is None


class TestTokens:
    @mock.patch(f"{HOOK_MODULE}.mask_secret", autospec=True)
    def test_internal_token_request(self, mask, fake_polaris, polaris_conn):
        hook = PolarisHook(polaris_conn(realm="PROD", scope="PRINCIPAL_ROLE:etl"))
        assert hook.get_token() == "tok-1"
        mask.assert_called_once_with("tok-1")
        (request,) = fake_polaris.calls("POST", TOKEN_PATH)
        assert request.headers["authorization"] == _basic("client-id", "s3cr3t")
        assert request.headers["polaris-realm"] == "PROD"
        assert request.form == {"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:etl"}

    def test_client_secret_post(self, fake_polaris, polaris_conn):
        PolarisHook(polaris_conn(client_auth_method="client_secret_post")).get_token()
        (request,) = fake_polaris.calls("POST", TOKEN_PATH)
        assert "authorization" not in request.headers
        assert request.form["client_id"] == "client-id"
        assert request.form["client_secret"] == "s3cr3t"

    def test_token_is_cached_then_refreshed_before_expiry(self, fake_polaris, polaris_conn):
        hook = PolarisHook(polaris_conn())
        now = time.monotonic()
        with mock.patch(f"{HOOK_MODULE}.time.monotonic", return_value=now):
            assert hook.get_token() == "tok-1"
            assert hook.get_token() == "tok-1"
        with mock.patch(f"{HOOK_MODULE}.time.monotonic", return_value=now + 3600 - 59):
            assert hook.get_token() == "tok-2"  # inside the 60 s refresh margin
        assert hook.get_token(force_refresh=True) == "tok-3"

    def test_expiry_from_jwt_when_expires_in_missing(self, fake_polaris, polaris_conn):
        token = _jwt({"exp": time.time() + 1000})
        fake_polaris.route("POST", TOKEN_PATH, (200, {"access_token": token}))
        hook = PolarisHook(polaris_conn())
        hook.get_token()
        remaining = hook._token_expires_at - time.monotonic()
        assert 900 < remaining <= 940

    def test_expiry_from_jwt_when_expires_in_is_not_a_number(self, fake_polaris, polaris_conn):
        token = _jwt({"exp": time.time() + 1000})
        fake_polaris.route("POST", TOKEN_PATH, (200, {"access_token": token, "expires_in": "soon"}))
        hook = PolarisHook(polaris_conn())
        hook.get_token()
        assert 900 < hook._token_expires_at - time.monotonic() <= 940

    def test_fallback_ttl_for_opaque_token(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", TOKEN_PATH, (200, {"access_token": "opaque"}))
        hook = PolarisHook(polaris_conn())
        hook.get_token()
        assert 200 < hook._token_expires_at - time.monotonic() <= 240

    def test_oidc_token_request_goes_to_idp_without_realm_header(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", "/idp/token", fake_polaris.token_responder)
        hook = PolarisHook(
            polaris_conn(
                auth_type="oidc",
                oauth2_server_uri=f"{fake_polaris.url}/idp/token",
                realm="PROD",
                scope="polaris",
                audience="aud",
                resource="res",
            )
        )
        hook.get_token()
        (request,) = fake_polaris.calls("POST", "/idp/token")
        assert "polaris-realm" not in request.headers
        assert request.form == {
            "grant_type": "client_credentials",
            "scope": "polaris",
            "audience": "aud",
            "resource": "res",
        }
        assert not fake_polaris.calls("POST", TOKEN_PATH)

    def test_static_token_makes_no_request(self, fake_polaris, polaris_conn):
        hook = PolarisHook(polaris_conn(login=None, password="static-tok", auth_type="token"))
        assert hook.get_token() == "static-tok"
        assert fake_polaris.requests == []

    @pytest.mark.parametrize(
        "use",
        [
            lambda hook: hook.get_token(),
            lambda hook: hook.get_iceberg_rest_properties(catalog="c"),
            lambda hook: PolarisCatalogHook(hook.polaris_conn_id).get_iceberg_catalog_properties("c"),
        ],
        ids=["get_token", "engine_properties", "pyiceberg_properties"],
    )
    @mock.patch(f"{HOOK_MODULE}.mask_secret", autospec=True)
    def test_static_token_is_masked(self, mask, polaris_conn, use):
        hook = PolarisHook(polaris_conn(login=None, password="static-tok", auth_type="token"))
        use(hook)
        mask.assert_any_call("static-tok")

    @mock.patch(f"{HOOK_MODULE}.mask_secret", autospec=True)
    def test_client_secret_connections_mask_only_fetched_tokens(self, mask, fake_polaris, polaris_conn):
        # The client secret is the connection's Password, which Airflow masks itself.
        PolarisHook(polaris_conn()).get_token()
        mask.assert_called_once_with("tok-1")

    @pytest.mark.parametrize(
        ("response", "match"),
        [
            ((400, {"error": "invalid_scope", "error_description": "bad"}), "scope extra"),
            ((401, {"error": "unauthorized_client"}), "Client Secret"),
            ((400, {"error": "invalid_client"}), "client_auth_method"),
            ((501, None), "auth_type 'oidc'"),
            (_error(404, "Missing or invalid realm", "MissingOrInvalidRealm"), "realm 'NOPE'"),
            ((200, {"token_type": "bearer"}), "without an access_token"),
        ],
    )
    def test_token_errors(self, fake_polaris, polaris_conn, response, match):
        fake_polaris.route("POST", TOKEN_PATH, response)
        with pytest.raises(PolarisAuthenticationError, match=match):
            PolarisHook(polaris_conn(realm="NOPE")).get_token()

    def test_unreachable_token_endpoint(self, polaris_conn):
        hook = PolarisHook(polaris_conn(oauth2_server_uri="http://127.0.0.1:9/tok", timeout=2))
        with pytest.raises(
            PolarisAuthenticationError, match="Token request to http://127.0.0.1:9/tok failed"
        ) as info:
            hook.get_token()
        assert info.value.retryable  # the endpoint may come back

    def test_invalid_token_url_is_not_retryable(self, polaris_conn):
        hook = PolarisHook(polaris_conn(oauth2_server_uri="http://"))
        with pytest.raises(PolarisAuthenticationError, match="Token request to http:// failed") as info:
            hook.get_token()
        assert info.value.retryable is False

    @pytest.mark.parametrize(
        ("response", "retryable"),
        [
            ((400, {"error": "invalid_client"}), False),
            ((401, {"error": "unauthorized_client"}), False),
            (_error(404, "Missing or invalid realm", "MissingOrInvalidRealm"), False),
            ((501, None), False),
            ((503, None), True),
            ((429, None), True),
        ],
    )
    def test_token_error_retryable(self, fake_polaris, polaris_conn, response, retryable):
        fake_polaris.route("POST", TOKEN_PATH, response)
        with pytest.raises(PolarisAuthenticationError) as info:
            PolarisHook(polaris_conn()).get_token()
        assert info.value.retryable is retryable


class TestCall:
    def test_bearer_and_realm_headers(self, fake_polaris, polaris_conn):
        fake_polaris.route("GET", f"{MGMT}/catalogs", (200, {"catalogs": []}))
        hook = PolarisManagementHook(polaris_conn(realm="PROD"))
        assert hook.list_catalogs() == []
        (request,) = fake_polaris.calls("GET", f"{MGMT}/catalogs")
        assert request.headers["authorization"] == "Bearer tok-1"
        assert request.headers["polaris-realm"] == "PROD"

    def test_custom_realm_header(self, fake_polaris, polaris_conn):
        fake_polaris.route("GET", f"{MGMT}/catalogs", (200, {"catalogs": []}))
        PolarisManagementHook(polaris_conn(realm="PROD", realm_header="X-Tenant")).list_catalogs()
        assert fake_polaris.calls("POST", TOKEN_PATH)[0].headers["x-tenant"] == "PROD"
        assert fake_polaris.calls("GET", f"{MGMT}/catalogs")[0].headers["x-tenant"] == "PROD"

    def test_no_realm_header_when_unset(self, fake_polaris, polaris_conn):
        fake_polaris.route("GET", f"{MGMT}/catalogs", (200, {"catalogs": []}))
        PolarisManagementHook(polaris_conn()).list_catalogs()
        assert "polaris-realm" not in fake_polaris.calls("GET", f"{MGMT}/catalogs")[0].headers

    def test_401_refreshes_token_and_retries_once(self, fake_polaris, polaris_conn):
        def catalogs(request):
            if request.headers["authorization"] == "Bearer tok-1":
                return _error(401, "expired", "NotAuthorizedException")
            return 200, {"catalogs": []}

        fake_polaris.route("GET", f"{MGMT}/catalogs", catalogs)
        assert PolarisManagementHook(polaris_conn()).list_catalogs() == []
        assert len(fake_polaris.calls("POST", TOKEN_PATH)) == 2
        assert len(fake_polaris.calls("GET", f"{MGMT}/catalogs")) == 2

    def test_persistent_401_raises_after_one_retry(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "GET", f"{MGMT}/catalogs", _error(401, "Unable to authenticate", "NotAuthorizedException")
        )
        with pytest.raises(PolarisAuthenticationError, match="holds every role in scope"):
            PolarisManagementHook(polaris_conn()).list_catalogs()
        assert len(fake_polaris.calls("GET", f"{MGMT}/catalogs")) == 2

    def test_static_token_401_is_not_retried(self, fake_polaris, polaris_conn):
        fake_polaris.route("GET", f"{MGMT}/catalogs", _error(401, "expired", "NotAuthorizedException"))
        hook = PolarisManagementHook(polaris_conn(login=None, password="t", auth_type="token"))
        with pytest.raises(PolarisAuthenticationError, match="cannot refresh"):
            hook.list_catalogs()
        assert len(fake_polaris.calls("GET", f"{MGMT}/catalogs")) == 1

    @pytest.mark.parametrize(
        ("response", "error_cls", "match"),
        [
            (
                _error(404, "Catalog not found", "NotFoundException"),
                PolarisNotFoundError,
                "Catalog not found",
            ),
            (_error(409, "exists", "AlreadyExistsException"), PolarisConflictError, "exists"),
            (_error(403, "not authorized", "ForbiddenException"), PolarisForbiddenError, "not authorized"),
            (_error(500, "boom", "ServiceFailureException"), PolarisApiError, "HTTP 500"),
            (
                _error(404, "Missing or invalid realm", "MissingOrInvalidRealm"),
                PolarisAuthenticationError,
                "realm",
            ),
            (
                (
                    403,
                    {
                        "error": {
                            "message": "Principal 'p' is not authorized because it must rotate "
                            "credentials first"
                        }
                    },
                ),
                PolarisAuthenticationError,
                "credentialRotationRequired",
            ),
        ],
    )
    def test_error_translation(self, fake_polaris, polaris_conn, response, error_cls, match):
        fake_polaris.route("GET", f"{MGMT}/catalogs/c1", response)
        with pytest.raises(error_cls, match=match) as info:
            PolarisManagementHook(polaris_conn()).get_catalog("c1")
        assert type(info.value) is error_cls

    @pytest.mark.parametrize(
        ("response", "retryable"),
        [
            (_error(400, "bad", "BadRequestException"), False),
            (_error(401, "expired", "NotAuthorizedException"), False),
            (_error(403, "not authorized", "ForbiddenException"), False),
            (_error(404, "Catalog not found", "NotFoundException"), False),
            (_error(409, "changed", "CommitFailedException"), True),
            (_error(409, "exists", "AlreadyExistsException"), False),
            (_error(500, "boom", "ServiceFailureException"), True),
            (_error(503, "busy", "ServiceUnavailableException"), True),
        ],
    )
    def test_error_retryable(self, fake_polaris, polaris_conn, response, retryable):
        fake_polaris.route("GET", f"{MGMT}/catalogs/c1", response)
        with pytest.raises(PolarisError) as info:
            PolarisManagementHook(polaris_conn()).get_catalog("c1")
        assert info.value.retryable is retryable

    def test_network_errors_are_translated_and_retryable(self, polaris_conn):
        def unreachable(**kwargs):
            raise urllib3.exceptions.ProtocolError("Connection aborted.")

        with pytest.raises(PolarisRequestError, match="Connection aborted") as info:
            PolarisHook(polaris_conn()).call(unreachable)
        assert info.value.retryable

    @pytest.mark.parametrize(
        ("body", "message"),
        [
            (b"<html>502 Bad Gateway</html>", "<html>502 Bad Gateway</html>"),  # from a proxy
            (b"[]", "Bad Gateway"),  # JSON, but not an error object: falls back to the reason
            ({"message": "flat error", "type": "T"}, "flat error"),
        ],
    )
    def test_error_bodies_that_are_not_iceberg_errors(self, fake_polaris, polaris_conn, body, message):
        fake_polaris.route("GET", f"{MGMT}/catalogs/c1", (502, body))
        with pytest.raises(PolarisApiError, match="HTTP 502") as info:
            PolarisManagementHook(polaris_conn()).get_catalog("c1")
        assert message in str(info.value)
        assert info.value.retryable is True

    def test_slow_api_call_times_out_and_is_retryable(self, fake_polaris, polaris_conn):
        def slow(_request):
            time.sleep(0.5)
            return 200, {"catalogs": []}

        fake_polaris.route("GET", f"{MGMT}/catalogs", slow)
        with pytest.raises(PolarisRequestError) as info:
            PolarisManagementHook(polaris_conn(timeout=0.1)).list_catalogs()
        assert info.value.retryable is True

    def test_slow_token_endpoint_times_out_and_is_retryable(self, fake_polaris, polaris_conn):
        def slow(request):
            time.sleep(0.5)
            return fake_polaris.token_responder(request)

        fake_polaris.route("POST", TOKEN_PATH, slow)
        with pytest.raises(PolarisAuthenticationError, match="failed") as info:
            PolarisHook(polaris_conn(timeout=0.1)).get_token()
        assert info.value.retryable is True

    def test_concurrent_get_token_fetches_once(self, fake_polaris, polaris_conn):
        def slow(request):
            time.sleep(0.2)
            return fake_polaris.token_responder(request)

        fake_polaris.route("POST", TOKEN_PATH, slow)
        hook = PolarisHook(polaris_conn())
        hook.config  # resolve the connection before the threads start
        with ThreadPoolExecutor(max_workers=5) as pool:
            tokens = list(pool.map(lambda _: hook.get_token(), range(5)))
        assert tokens == ["tok-1"] * 5
        assert len(fake_polaris.calls("POST", TOKEN_PATH)) == 1

    @pytest.mark.parametrize(
        ("verify", "verify_ssl", "ca_cert"),
        [("true", True, None), ("false", False, None), ("/etc/ca.pem", True, "/etc/ca.pem")],
    )
    def test_tls_settings_reach_the_sdk_clients(self, polaris_conn, verify, verify_ssl, ca_cert):
        hook = PolarisHook(polaris_conn(verify=verify))
        for client in (hook._management_client, hook._catalog_client):
            assert client.configuration.verify_ssl is verify_ssl
            assert client.configuration.ssl_ca_cert == ca_cert

    @pytest.mark.parametrize(("verify", "expected"), [("false", False), ("/etc/ca.pem", "/etc/ca.pem")])
    @mock.patch(f"{HOOK_MODULE}.requests.post", autospec=True, side_effect=requests.post)
    def test_tls_settings_and_timeout_reach_the_token_request(
        self, post, fake_polaris, polaris_conn, verify, expected
    ):
        hook = PolarisHook(polaris_conn(verify=verify, timeout=7))
        # A CA path that does not exist fails the real request; only the arguments matter here.
        with contextlib.suppress(PolarisAuthenticationError):
            hook.get_token()
        assert post.call_args.kwargs["verify"] == expected
        assert post.call_args.kwargs["timeout"] == 7.0

    def test_get_conn_returns_management_api(self, polaris_conn):
        hook = PolarisHook(polaris_conn())
        assert hook.get_conn() is hook.management_api


class TestTestConnection:
    def test_with_default_catalog_reads_config(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "GET", f"{CAT}/v1/config", (200, {"defaults": {}, "overrides": {"prefix": "analytics"}})
        )
        ok, message = PolarisHook(polaris_conn(default_catalog="analytics")).test_connection()
        assert ok, message
        assert fake_polaris.calls("GET", f"{CAT}/v1/config")[0].query == {"warehouse": ["analytics"]}

    def test_without_catalog_lists_catalogs(self, fake_polaris, polaris_conn):
        fake_polaris.route("GET", f"{MGMT}/catalogs", (200, {"catalogs": []}))
        assert PolarisHook(polaris_conn()).test_connection() == (True, "Authenticated and listed catalogs.")

    def test_403_on_list_catalogs_counts_as_authenticated(self, fake_polaris, polaris_conn):
        fake_polaris.route("GET", f"{MGMT}/catalogs", _error(403, "no LIST_CATALOGS", "ForbiddenException"))
        ok, message = PolarisHook(polaris_conn()).test_connection()
        assert ok
        assert "lacks the privilege to list catalogs" in message

    def test_failure_is_reported_not_raised(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", TOKEN_PATH, (401, {"error": "unauthorized_client"}))
        ok, message = PolarisHook(polaris_conn()).test_connection()
        assert not ok
        assert "HTTP 401" in message

    def test_misconfiguration_is_reported(self, polaris_conn):
        ok, message = PolarisHook(polaris_conn(auth_type="oidc")).test_connection()
        assert not ok
        assert "oauth2_server_uri" in message


class TestEngineProperties:
    def test_spark_internal(self, fake_polaris, polaris_conn):
        props = PolarisHook(
            polaris_conn(realm="PROD", default_catalog="analytics")
        ).get_iceberg_rest_properties()
        assert props == {
            "uri": f"{fake_polaris.url}/api/catalog",
            "warehouse": "analytics",
            "token": "tok-1",
            "oauth2-server-uri": f"{fake_polaris.url}{TOKEN_PATH}",
            "scope": "PRINCIPAL_ROLE:ALL",  # used for token exchange; Iceberg Java's default is rejected
            "rest.auth.type": "oauth2",
            "header.Polaris-Realm": "PROD",
        }
        assert "s3cr3t" not in json.dumps(props)

    def test_spark_internal_uses_connection_scope(self, polaris_conn):
        props = PolarisHook(polaris_conn(scope="PRINCIPAL_ROLE:etl")).get_iceberg_rest_properties(catalog="c")
        assert props["scope"] == "PRINCIPAL_ROLE:etl"

    def test_spark_oidc_names_the_idp_endpoint_and_turns_refresh_off(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", "/idp", fake_polaris.token_responder)
        hook = PolarisHook(polaris_conn(auth_type="oidc", oauth2_server_uri=f"{fake_polaris.url}/idp"))
        assert hook.get_iceberg_rest_properties(catalog="c") == {
            "uri": f"{fake_polaris.url}/api/catalog",
            "warehouse": "c",
            "token": "tok-1",
            "rest.auth.type": "oauth2",
            "oauth2-server-uri": f"{fake_polaris.url}/idp",  # not Polaris's endpoint
            "token-refresh-enabled": "false",  # Polaris cannot exchange an IdP's token
        }

    def test_spark_static_token_turns_refresh_off(self, fake_polaris, polaris_conn):
        hook = PolarisHook(polaris_conn(login=None, password="static-tok", auth_type="token"))
        assert hook.get_iceberg_rest_properties(catalog="c") == {
            "uri": f"{fake_polaris.url}/api/catalog",
            "warehouse": "c",
            "token": "static-tok",
            "rest.auth.type": "oauth2",
            "oauth2-server-uri": f"{fake_polaris.url}{TOKEN_PATH}",
            "token-refresh-enabled": "false",  # a refresh would ask for scope "catalog", rejected
        }

    def test_trino_gets_a_token_without_scope(self, fake_polaris, polaris_conn):
        # Trino rejects a catalog that sets oauth2.scope together with oauth2.token.
        props = PolarisHook(polaris_conn(realm="PROD")).get_iceberg_rest_properties(
            catalog="c", engine="trino"
        )
        assert props == {
            "iceberg.rest-catalog.uri": f"{fake_polaris.url}/api/catalog",
            "iceberg.rest-catalog.warehouse": "c",
            "iceberg.rest-catalog.security": "OAUTH2",
            "iceberg.rest-catalog.oauth2.token": "tok-1",
            "iceberg.rest-catalog.oauth2.token-refresh-enabled": "false",
            "iceberg.rest-catalog.http-headers": "Polaris-Realm: PROD",
        }

    def test_trino_oidc_gets_the_token_alone(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", "/idp", fake_polaris.token_responder)
        hook = PolarisHook(polaris_conn(auth_type="oidc", oauth2_server_uri=f"{fake_polaris.url}/idp"))
        assert hook.get_iceberg_rest_properties(catalog="c", engine="trino") == {
            "iceberg.rest-catalog.uri": f"{fake_polaris.url}/api/catalog",
            "iceberg.rest-catalog.warehouse": "c",
            "iceberg.rest-catalog.security": "OAUTH2",
            "iceberg.rest-catalog.oauth2.token": "tok-1",
            "iceberg.rest-catalog.oauth2.token-refresh-enabled": "false",
        }

    def test_trino_without_realm_sends_no_headers(self, polaris_conn):
        props = PolarisHook(polaris_conn()).get_iceberg_rest_properties(catalog="c", engine="trino")
        assert "iceberg.rest-catalog.http-headers" not in props

    def test_unknown_engine(self, polaris_conn):
        with pytest.raises(PolarisInvalidArgumentError, match="engine"):
            PolarisHook(
                polaris_conn(login=None, password="t", auth_type="token")
            ).get_iceberg_rest_properties(
                catalog="c",
                engine="flink",  # type: ignore[arg-type]
            )

    def test_missing_catalog(self, polaris_conn):
        with pytest.raises(PolarisConnectionError, match="default_catalog"):
            PolarisHook(polaris_conn()).get_iceberg_rest_properties()


class TestManagementHook:
    CATALOG: ClassVar[dict] = {
        "type": "INTERNAL",
        "name": "c1",
        "properties": {"default-base-location": "file:///tmp/c1"},
        "storageConfigInfo": {"storageType": "FILE", "allowedLocations": ["file:///tmp"]},
    }

    def test_create_catalog_from_dict(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "POST", f"{MGMT}/catalogs", lambda r: (201, {**r.json["catalog"], "entityVersion": 1})
        )
        created = PolarisManagementHook(polaris_conn()).create_catalog(self.CATALOG)
        assert created.name == "c1"
        assert fake_polaris.calls("POST", f"{MGMT}/catalogs")[0].json == {"catalog": self.CATALOG}

    def test_create_federated_catalog_sends_the_connection_config_as_given(self, fake_polaris, polaris_conn):
        # The SDK's models would drop remoteCatalogName and the OAuth parameters.
        catalog = {
            "type": "EXTERNAL",
            "name": "fed",
            "properties": {"default-base-location": "s3://lake/fed"},
            "storageConfigInfo": {"storageType": "S3", "roleArn": "arn:aws:iam::1:role/r"},
            "connectionConfigInfo": {
                "connectionType": "ICEBERG_REST",
                "uri": "https://remote/api/catalog",
                "remoteCatalogName": "upstream",
                "authenticationParameters": {
                    "authenticationType": "OAUTH",
                    "tokenUri": "https://remote/token",
                    "clientId": "c",
                    "clientSecret": "s",
                    "scopes": ["PRINCIPAL_ROLE:ALL"],
                },
            },
        }
        fake_polaris.route(
            "POST", f"{MGMT}/catalogs", lambda r: (201, {**r.json["catalog"], "entityVersion": 1})
        )
        assert PolarisManagementHook(polaris_conn()).create_catalog(catalog).name == "fed"
        assert fake_polaris.calls("POST", f"{MGMT}/catalogs")[0].json == {"catalog": catalog}

    def test_create_catalog_accepts_a_model(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "POST", f"{MGMT}/catalogs", lambda r: (201, {**r.json["catalog"], "entityVersion": 1})
        )
        PolarisManagementHook(polaris_conn()).create_catalog(Catalog.from_dict(self.CATALOG))
        assert fake_polaris.calls("POST", f"{MGMT}/catalogs")[0].json == {"catalog": self.CATALOG}

    @pytest.mark.parametrize(
        ("catalog", "match"),
        [
            ("{'name': 'c1'}", "catalog must be a dict.*render_template_as_native_obj"),
            ({"type": "INTERNAL"}, "catalog needs a name"),
        ],
    )
    def test_create_catalog_rejects_invalid_definitions(self, fake_polaris, polaris_conn, catalog, match):
        with pytest.raises(PolarisInvalidArgumentError, match=match):
            PolarisManagementHook(polaris_conn()).create_catalog(catalog)
        assert not fake_polaris.calls("POST", f"{MGMT}/catalogs")

    def test_update_catalog_sends_the_storage_config_as_given(self, fake_polaris, polaris_conn):
        fake_polaris.route("GET", f"{MGMT}/catalogs/c1", (200, {**self.CATALOG, "entityVersion": 4}))
        fake_polaris.route("PUT", f"{MGMT}/catalogs/c1", (200, {**self.CATALOG, "entityVersion": 5}))
        storage = {"storageType": "S3", "roleArn": "arn:aws:iam::1:role/r", "stsUnavailable": True}
        PolarisManagementHook(polaris_conn()).update_catalog("c1", storage_config_info=storage)
        assert fake_polaris.calls("PUT", f"{MGMT}/catalogs/c1")[0].json == {
            "currentEntityVersion": 4,
            "storageConfigInfo": storage,
        }

    def test_create_catalog_if_exists_skip(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", f"{MGMT}/catalogs", _error(409, "exists", "AlreadyExistsException"))
        fake_polaris.route("GET", f"{MGMT}/catalogs/c1", (200, {**self.CATALOG, "entityVersion": 3}))
        hook = PolarisManagementHook(polaris_conn())
        assert hook.create_catalog(self.CATALOG, if_exists="skip").entity_version == 3
        with pytest.raises(PolarisConflictError):
            hook.create_catalog(self.CATALOG)

    @pytest.mark.parametrize("error_type", ["CommitConflictException", None])
    def test_create_if_exists_skip_raises_conflicts_other_than_already_exists(
        self, fake_polaris, polaris_conn, error_type
    ):
        # Only "already exists" is skipped; a concurrent change (or an untyped 409) is raised.
        fake_polaris.route("POST", f"{MGMT}/catalogs", _error(409, "changed", error_type))
        with pytest.raises(PolarisConflictError) as info:
            PolarisManagementHook(polaris_conn()).create_catalog(self.CATALOG, if_exists="skip")
        assert info.value.retryable is True
        assert not fake_polaris.calls("GET", f"{MGMT}/catalogs/c1")

    @pytest.mark.parametrize(
        ("assign", "path"),
        [
            (lambda hook: hook.assign_principal_role("etl", "etl_role"), "/principals/etl/principal-roles"),
            (
                lambda hook: hook.assign_catalog_role("etl_role", "c1", "writer"),
                "/principal-roles/etl_role/catalog-roles/c1",
            ),
        ],
        ids=["principal_role", "catalog_role"],
    )
    def test_role_assignment_conflict_is_raised(self, fake_polaris, polaris_conn, assign, path):
        # Polaris accepts repeated assignments, so a 409 here is a real conflict.
        fake_polaris.route("PUT", f"{MGMT}{path}", _error(409, "changed", "CommitConflictException"))
        with pytest.raises(PolarisConflictError):
            assign(PolarisManagementHook(polaris_conn()))

    def test_update_catalog_retries_from_the_latest_version(self, fake_polaris, polaris_conn, retry_pause):
        # Between our read and our write another client set "c"; the retry must keep it.
        base = {"default-base-location": "file:///tmp/c1"}
        reads = iter([({**base, "a": "1"}, 7), ({**base, "a": "1", "c": "3"}, 8)])

        def get(_request):
            properties, version = next(reads)
            return 200, {**self.CATALOG, "properties": properties, "entityVersion": version}

        def put(request):
            if request.json["currentEntityVersion"] == 7:
                return _error(409, "The entity version doesn't match", "CommitConflictException")
            return 200, {**self.CATALOG, "entityVersion": 9}

        fake_polaris.route("GET", f"{MGMT}/catalogs/c1", get)
        fake_polaris.route("PUT", f"{MGMT}/catalogs/c1", put)
        updated = PolarisManagementHook(polaris_conn()).update_catalog("c1", properties={"b": "2"})
        assert updated.entity_version == 9
        puts = [r.json for r in fake_polaris.calls("PUT", f"{MGMT}/catalogs/c1")]
        assert [put["currentEntityVersion"] for put in puts] == [7, 8]
        assert puts[1]["properties"] == {**base, "a": "1", "c": "3", "b": "2"}
        retry_pause.assert_called_once_with()

    def test_update_catalog_gives_up_after_the_last_attempt(self, fake_polaris, polaris_conn, retry_pause):
        fake_polaris.route("GET", f"{MGMT}/catalogs/c1", (200, {**self.CATALOG, "entityVersion": 7}))
        fake_polaris.route("PUT", f"{MGMT}/catalogs/c1", _error(409, "changed", "CommitConflictException"))
        with pytest.raises(PolarisConflictError) as info:
            PolarisManagementHook(polaris_conn()).update_catalog("c1", properties={"b": "2"})
        assert info.value.retryable is True  # the task's retries can still help
        assert len(fake_polaris.calls("PUT", f"{MGMT}/catalogs/c1")) == UPDATE_ATTEMPTS
        assert retry_pause.call_count == UPDATE_ATTEMPTS - 1  # before each retry, not after the last

    @pytest.mark.parametrize("error_type", ["AlreadyExistsException", None])
    def test_update_catalog_does_not_retry_other_conflicts(
        self, fake_polaris, polaris_conn, retry_pause, error_type
    ):
        fake_polaris.route("GET", f"{MGMT}/catalogs/c1", (200, {**self.CATALOG, "entityVersion": 7}))
        fake_polaris.route("PUT", f"{MGMT}/catalogs/c1", _error(409, "conflict", error_type))
        with pytest.raises(PolarisConflictError):
            PolarisManagementHook(polaris_conn()).update_catalog("c1", properties={"b": "2"})
        assert len(fake_polaris.calls("PUT", f"{MGMT}/catalogs/c1")) == 1
        retry_pause.assert_not_called()

    def test_pause_before_retry_waits_a_random_short_time(self):
        with (
            mock.patch(f"{HOOK_MODULE}.random.uniform", autospec=True, return_value=0.42) as uniform,
            mock.patch(f"{HOOK_MODULE}.time.sleep", autospec=True) as sleep,
        ):
            _pause_before_retry()
        uniform.assert_called_once_with(*UPDATE_RETRY_PAUSE)
        sleep.assert_called_once_with(0.42)
        assert 0 < UPDATE_RETRY_PAUSE[0] < UPDATE_RETRY_PAUSE[1] <= 1

    def test_update_catalog_validates_before_reading(self, fake_polaris, polaris_conn):
        with pytest.raises(PolarisInvalidArgumentError, match="properties values must be strings"):
            PolarisManagementHook(polaris_conn()).update_catalog("c1", properties={"b": 2})
        assert not fake_polaris.calls("GET", f"{MGMT}/catalogs/c1")

    def test_update_catalog_merges_properties_with_current_version(self, fake_polaris, polaris_conn):
        current = {
            **self.CATALOG,
            "properties": {"default-base-location": "file:///tmp/c1", "a": "1"},
            "entityVersion": 7,
        }
        fake_polaris.route("GET", f"{MGMT}/catalogs/c1", (200, current))
        fake_polaris.route("PUT", f"{MGMT}/catalogs/c1", (200, current))
        PolarisManagementHook(polaris_conn()).update_catalog("c1", properties={"b": "2"})
        body = fake_polaris.calls("PUT", f"{MGMT}/catalogs/c1")[0].json
        assert body == {
            "currentEntityVersion": 7,
            "properties": {"default-base-location": "file:///tmp/c1", "a": "1", "b": "2"},
        }

    def test_update_catalog_replace_properties(self, fake_polaris, polaris_conn):
        fake_polaris.route("GET", f"{MGMT}/catalogs/c1", (200, {**self.CATALOG, "entityVersion": 1}))
        fake_polaris.route("PUT", f"{MGMT}/catalogs/c1", (200, {**self.CATALOG, "entityVersion": 2}))
        PolarisManagementHook(polaris_conn()).update_catalog(
            "c1", properties={"default-base-location": "file:///x"}, replace_properties=True
        )
        assert fake_polaris.calls("PUT", f"{MGMT}/catalogs/c1")[0].json["properties"] == {
            "default-base-location": "file:///x"
        }

    def test_delete_if_missing(self, fake_polaris, polaris_conn):
        fake_polaris.route("DELETE", f"{MGMT}/catalogs/c1", _error(404, "nope", "NotFoundException"))
        hook = PolarisManagementHook(polaris_conn())
        assert hook.delete_catalog("c1", if_missing="skip") is False
        with pytest.raises(PolarisNotFoundError):
            hook.delete_catalog("c1")
        fake_polaris.route("DELETE", f"{MGMT}/catalogs/c2", (204, None))
        assert hook.delete_catalog("c2") is True

    @mock.patch(f"{HOOK_MODULE}.mask_secret", autospec=True)
    def test_create_principal_discards_and_masks_secret(self, mask, fake_polaris, polaris_conn):
        fake_polaris.route(
            "POST",
            f"{MGMT}/principals",
            (
                201,
                {
                    "principal": {"name": "etl", "clientId": "cid"},
                    "credentials": {"clientId": "cid", "clientSecret": "NEW-SECRET"},
                },
            ),
        )
        principal = PolarisManagementHook(polaris_conn()).create_principal(
            "etl", credential_rotation_required=True
        )
        assert principal.to_dict() == {"name": "etl", "clientId": "cid"}
        mask.assert_any_call("NEW-SECRET")
        assert fake_polaris.calls("POST", f"{MGMT}/principals")[0].json == {
            "principal": {"name": "etl"},
            "credentialRotationRequired": True,
        }

    def test_create_principal_if_exists_skip(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", f"{MGMT}/principals", _error(409, "exists", "AlreadyExistsException"))
        fake_polaris.route("GET", f"{MGMT}/principals/etl", (200, {"name": "etl", "clientId": "cid"}))
        assert (
            PolarisManagementHook(polaris_conn()).create_principal("etl", if_exists="skip").client_id == "cid"
        )

    def test_roles_and_assignments(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", f"{MGMT}/principal-roles", (201, {"name": "etl_role"}))
        fake_polaris.route("POST", f"{MGMT}/catalogs/c1/catalog-roles", (201, {"name": "writer"}))
        fake_polaris.route("PUT", f"{MGMT}/principals/etl/principal-roles", (201, None))
        fake_polaris.route("PUT", f"{MGMT}/principal-roles/etl_role/catalog-roles/c1", (201, None))
        hook = PolarisManagementHook(polaris_conn())
        hook.create_principal_role("etl_role", properties={"team": "data"})
        hook.create_catalog_role("c1", "writer")
        hook.assign_principal_role("etl", "etl_role")
        hook.assign_catalog_role("etl_role", "c1", "writer")
        assert fake_polaris.calls("POST", f"{MGMT}/principal-roles")[0].json == {
            "principalRole": {"name": "etl_role", "federated": False, "properties": {"team": "data"}}
        }
        assert fake_polaris.calls("PUT", f"{MGMT}/principal-roles/etl_role/catalog-roles/c1")[0].json == {
            "catalogRole": {"name": "writer"}
        }

    def test_create_catalog_role_if_exists_skip(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "POST", f"{MGMT}/catalogs/c1/catalog-roles", _error(409, "exists", "AlreadyExistsException")
        )
        fake_polaris.route(
            "GET", f"{MGMT}/catalogs/c1/catalog-roles/writer", (200, {"name": "writer", "entityVersion": 2})
        )
        assert (
            PolarisManagementHook(polaris_conn())
            .create_catalog_role("c1", "writer", if_exists="skip")
            .entity_version
            == 2
        )

    def test_grants(self, fake_polaris, polaris_conn):
        grants = f"{MGMT}/catalogs/c1/catalog-roles/writer/grants"
        fake_polaris.route("PUT", grants, (201, None))
        fake_polaris.route("POST", grants, (201, None))
        fake_polaris.route(
            "GET", grants, (200, {"grants": [{"type": "catalog", "privilege": "TABLE_READ_DATA"}]})
        )
        hook = PolarisManagementHook(polaris_conn())
        grant = {
            "type": "table",
            "namespace": ["sales"],
            "tableName": "orders",
            "privilege": "TABLE_WRITE_DATA",
        }
        hook.add_grant("c1", "writer", grant)
        hook.revoke_grant("c1", "writer", grant, cascade=True)
        assert [g.to_dict() for g in hook.list_grants("c1", "writer")] == [
            {"type": "catalog", "privilege": "TABLE_READ_DATA"}
        ]
        assert fake_polaris.calls("PUT", grants)[0].json == {"grant": grant}
        revoke = fake_polaris.calls("POST", grants)[0]
        assert revoke.json == {"grant": grant}
        assert revoke.query == {"cascade": ["true"]}

    @pytest.mark.parametrize(
        ("call", "method", "path", "response", "expected"),
        [
            (lambda h: h.list_principals(), "GET", "/principals", {"principals": []}, []),
            (lambda h: h.get_principal("etl"), "GET", "/principals/etl", {"name": "etl"}, "etl"),
            (lambda h: h.delete_principal("etl"), "DELETE", "/principals/etl", None, True),
            (lambda h: h.list_principal_roles(), "GET", "/principal-roles", {"roles": []}, []),
            (lambda h: h.delete_principal_role("r"), "DELETE", "/principal-roles/r", None, True),
            (lambda h: h.list_catalog_roles("c1"), "GET", "/catalogs/c1/catalog-roles", {"roles": []}, []),
            (
                lambda h: h.delete_catalog_role("c1", "r"),
                "DELETE",
                "/catalogs/c1/catalog-roles/r",
                None,
                True,
            ),
            (lambda h: h.list_catalogs(), "GET", "/catalogs", {"catalogs": []}, []),
        ],
    )
    def test_management_wrappers_call_the_right_endpoint(
        self, fake_polaris, polaris_conn, call, method, path, response, expected
    ):
        fake_polaris.route(method, f"{MGMT}{path}", (200 if response else 204, response))
        result = call(PolarisManagementHook(polaris_conn()))
        assert (result.name if expected == "etl" else result) == expected
        assert len(fake_polaris.calls(method, f"{MGMT}{path}")) == 1

    @pytest.mark.parametrize(
        ("grant", "match"),
        [
            ({"type": "catalog", "privilege": "NOT_A_PRIVILEGE"}, "Invalid GrantResource"),
            ({"privilege": "TABLE_READ_DATA"}, "Invalid GrantResource"),
            ('{"type": "catalog"}', "Expected a GrantResource or a dict.*render_template_as_native_obj"),
        ],
    )
    def test_invalid_grants_are_invalid_arguments(self, fake_polaris, polaris_conn, grant, match):
        with pytest.raises(PolarisInvalidArgumentError, match=match) as info:
            PolarisManagementHook(polaris_conn()).add_grant("c1", "writer", grant)
        assert info.value.retryable is False

    @pytest.mark.parametrize(
        "create",
        [
            lambda hook: hook.create_principal("etl", properties={"team": 1}),
            lambda hook: hook.create_principal_role("r", properties={"team": 1}),
            lambda hook: hook.create_catalog_role("c1", "r", properties={"team": 1}),
        ],
    )
    def test_entity_properties_must_be_strings(self, fake_polaris, polaris_conn, create):
        with pytest.raises(PolarisInvalidArgumentError, match="properties values must be strings"):
            create(PolarisManagementHook(polaris_conn()))

    def test_revoke_role_if_missing(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "DELETE", f"{MGMT}/principals/etl/principal-roles/r", _error(404, "nope", "NotFoundException")
        )
        assert (
            PolarisManagementHook(polaris_conn()).revoke_principal_role("etl", "r", if_missing="skip")
            is False
        )

    GRANT_NOT_FOUND = _error(400, "Operation failed: GRANT_NOT_FOUND", "GRANT_NOT_FOUND")
    REVOKES: ClassVar[list] = [
        pytest.param(
            "DELETE",
            f"{MGMT}/principals/etl/principal-roles/r",
            "revoke_principal_role",
            ("etl", "r"),
            id="principal-role",
        ),
        pytest.param(
            "DELETE",
            f"{MGMT}/principal-roles/pr/catalog-roles/c1/cr",
            "revoke_catalog_role",
            ("pr", "c1", "cr"),
            id="catalog-role",
        ),
        pytest.param(
            "POST",
            f"{MGMT}/catalogs/c1/catalog-roles/cr/grants",
            "revoke_grant",
            ("c1", "cr", {"type": "catalog", "privilege": "TABLE_READ_DATA"}),
            id="grant",
        ),
    ]

    @pytest.mark.parametrize(("method", "path", "revoke", "args"), REVOKES)
    def test_revoking_what_is_not_granted(self, fake_polaris, polaris_conn, method, path, revoke, args):
        # Polaris answers HTTP 400 GRANT_NOT_FOUND, not 404, for an absent assignment or grant.
        fake_polaris.route(method, path, self.GRANT_NOT_FOUND)
        hook = PolarisManagementHook(polaris_conn())
        assert getattr(hook, revoke)(*args, if_missing="skip") is False
        with pytest.raises(PolarisApiError, match="GRANT_NOT_FOUND") as info:
            getattr(hook, revoke)(*args)
        assert not info.value.retryable

    def test_other_bad_requests_are_not_treated_as_missing(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "DELETE", f"{MGMT}/principals/etl/principal-roles/r", _error(400, "bad", "BadRequestException")
        )
        with pytest.raises(PolarisApiError, match="bad"):
            PolarisManagementHook(polaris_conn()).revoke_principal_role("etl", "r", if_missing="skip")


class TestCatalogHook:
    def test_encode_namespace(self):
        assert encode_namespace("a.b") == "a\x1fb"
        assert encode_namespace(["a.b", "c"]) == "a.b\x1fc"
        with pytest.raises(PolarisInvalidArgumentError, match="Invalid namespace"):
            encode_namespace("a..b")

    def test_list_tables_follows_pages_and_encodes_namespace(self, fake_polaris, polaris_conn):
        def tables(request):
            if request.query.get("pageToken") == ["p2"]:
                return 200, {"identifiers": [{"namespace": ["sales", "eu"], "name": "t2"}]}
            return 200, {
                "identifiers": [{"namespace": ["sales", "eu"], "name": "t1"}],
                "next-page-token": "p2",
            }

        fake_polaris.route("GET", f"{CAT}/v1/c1/namespaces/sales\x1feu/tables", tables)
        result = PolarisCatalogHook(polaris_conn()).list_tables("sales.eu", catalog="c1")
        assert [t.name for t in result] == ["t1", "t2"]

    def test_list_namespaces_uses_default_catalog(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "GET", f"{CAT}/v1/analytics/namespaces", (200, {"namespaces": [["sales"], ["hr"]]})
        )
        assert PolarisCatalogHook(polaris_conn(default_catalog="analytics")).list_namespaces() == [
            ["sales"],
            ["hr"],
        ]

    def test_get_table_location(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "GET",
            f"{CAT}/v1/c1/namespaces/sales/tables/orders",
            (
                200,
                {"metadata": {"format-version": 2, "table-uuid": "u", "location": "s3://lake/sales/orders"}},
            ),
        )
        location = PolarisCatalogHook(polaris_conn()).get_table_location("sales", "orders", catalog="c1")
        assert location == "s3://lake/sales/orders"
        assert fake_polaris.calls("GET", f"{CAT}/v1/c1/namespaces/sales/tables/orders")[0].query == {
            "snapshots": ["refs"]
        }

    POLICY: ClassVar[dict] = {
        "policy-type": "system.snapshot-expiry",
        "inheritable": True,
        "name": "expiry",
        "version": 0,
    }
    POLICIES = f"{CAT}/polaris/v1/c1/namespaces/sales/policies"

    def test_create_policy_serializes_dict_content(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "POST", self.POLICIES, lambda r: (200, {"policy": {**self.POLICY, "content": r.json["content"]}})
        )
        policy = PolarisCatalogHook(polaris_conn()).create_policy(
            "sales",
            "expiry",
            "system.snapshot-expiry",
            content={"version": "2025-02-03", "enable": True},
            catalog="c1",
        )
        assert json.loads(policy.content) == {"version": "2025-02-03", "enable": True}
        body = fake_polaris.calls("POST", self.POLICIES)[0].json
        assert body["name"] == "expiry"
        assert body["type"] == "system.snapshot-expiry"
        assert json.loads(body["content"]) == {"version": "2025-02-03", "enable": True}

    def test_create_policy_if_exists_update(self, fake_polaris, polaris_conn):
        old = {**self.POLICY, "content": json.dumps({"enable": False}), "version": 4}
        fake_polaris.route("POST", self.POLICIES, _error(409, "exists", "AlreadyExistsException"))
        fake_polaris.route("GET", f"{self.POLICIES}/expiry", (200, {"policy": old}))
        fake_polaris.route(
            "PUT", f"{self.POLICIES}/expiry", lambda r: (200, {"policy": {**old, "version": 5}})
        )
        hook = PolarisCatalogHook(polaris_conn())
        hook.create_policy(
            "sales",
            "expiry",
            "system.snapshot-expiry",
            content={"enable": True},
            catalog="c1",
            if_exists="update",
        )
        put = fake_polaris.calls("PUT", f"{self.POLICIES}/expiry")[0].json
        assert put["current-policy-version"] == 4
        assert json.loads(put["content"]) == {"enable": True}

    def test_update_policy_retries_from_the_latest_version(self, fake_polaris, polaris_conn, retry_pause):
        # Between our read and our write another client changed the description; the retry
        # updates the content only and keeps that description.
        loads = iter([(4, "old"), (5, "set by someone else")])

        def get(_request):
            version, description = next(loads)
            policy = {**self.POLICY, "content": "{}", "version": version, "description": description}
            return 200, {"policy": policy}

        def put(request):
            if request.json["current-policy-version"] == 4:
                return _error(409, "version mismatch", "PolicyVersionMismatchException")
            return 200, {"policy": {**self.POLICY, "content": request.json["content"], "version": 6}}

        fake_polaris.route("GET", f"{self.POLICIES}/expiry", get)
        fake_polaris.route("PUT", f"{self.POLICIES}/expiry", put)
        policy = PolarisCatalogHook(polaris_conn()).update_policy(
            "sales", "expiry", content={"enable": True}, catalog="c1"
        )
        assert policy.version == 6
        puts = [r.json for r in fake_polaris.calls("PUT", f"{self.POLICIES}/expiry")]
        assert [put["current-policy-version"] for put in puts] == [4, 5]
        assert puts[1]["description"] == "set by someone else"
        retry_pause.assert_called_once_with()

    @pytest.mark.parametrize("if_exists", ["skip", "update"])
    def test_create_policy_raises_conflicts_other_than_already_exists(
        self, fake_polaris, polaris_conn, if_exists
    ):
        fake_polaris.route("POST", self.POLICIES, _error(409, "changed", "CommitConflictException"))
        with pytest.raises(PolarisConflictError) as info:
            PolarisCatalogHook(polaris_conn()).create_policy(
                "sales", "expiry", "system.snapshot-expiry", catalog="c1", if_exists=if_exists
            )
        assert info.value.retryable is True
        assert not fake_polaris.calls("GET", f"{self.POLICIES}/expiry")

    @pytest.mark.parametrize(
        ("if_exists", "reads"),
        [
            pytest.param("skip", [_error(404, "gone", "NoSuchPolicyException")], id="skip"),
            pytest.param("update", [_error(404, "gone", "NoSuchPolicyException")], id="update"),
            pytest.param(
                "update",
                [
                    (200, {"policy": {**POLICY, "content": json.dumps({"enable": False}), "version": 4}}),
                    _error(404, "gone", "NoSuchPolicyException"),
                ],
                id="update_dropped_after_the_type_check",
            ),
        ],
    )
    def test_create_policy_if_exists_after_a_concurrent_drop(
        self, fake_polaris, polaris_conn, if_exists, reads
    ):
        # Another client drops the policy between Polaris's "already exists" and our read of it
        # (or, with "update", before update_policy reads it): the create runs again.
        creates = iter(
            [
                _error(409, "exists", "AlreadyExistsException"),
                (200, {"policy": {**self.POLICY, "content": json.dumps({"enable": True})}}),
            ]
        )
        reads = iter(reads)
        fake_polaris.route("POST", self.POLICIES, lambda _r: next(creates))
        fake_polaris.route("GET", f"{self.POLICIES}/expiry", lambda _r: next(reads))
        policy = PolarisCatalogHook(polaris_conn()).create_policy(
            "sales",
            "expiry",
            "system.snapshot-expiry",
            content={"enable": True},
            catalog="c1",
            if_exists=if_exists,
        )
        assert (policy.version, json.loads(policy.content)) == (0, {"enable": True})
        posts = [r.json for r in fake_polaris.calls("POST", self.POLICIES)]
        assert len(posts) == 2
        assert posts[0] == posts[1]
        assert not fake_polaris.calls("PUT", f"{self.POLICIES}/expiry")

    def test_create_policy_if_exists_raises_the_conflict_when_the_policy_stays_unreadable(
        self, fake_polaris, polaris_conn
    ):
        fake_polaris.route("POST", self.POLICIES, _error(409, "Policy exists", "AlreadyExistsException"))
        fake_polaris.route("GET", f"{self.POLICIES}/expiry", _error(404, "gone", "NoSuchPolicyException"))
        with pytest.raises(PolarisConflictError, match="Policy exists") as info:
            PolarisCatalogHook(polaris_conn()).create_policy(
                "sales", "expiry", "system.snapshot-expiry", catalog="c1", if_exists="skip"
            )
        assert info.value.already_exists
        assert len(fake_polaris.calls("POST", self.POLICIES)) == 2
        assert len(fake_polaris.calls("GET", f"{self.POLICIES}/expiry")) == 2

    def test_create_policy_if_exists_update_is_noop_when_unchanged(self, fake_polaris, polaris_conn):
        old = {**self.POLICY, "content": json.dumps({"enable": True}), "version": 4}
        fake_polaris.route("POST", self.POLICIES, _error(409, "exists", "AlreadyExistsException"))
        fake_polaris.route("GET", f"{self.POLICIES}/expiry", (200, {"policy": old}))
        hook = PolarisCatalogHook(polaris_conn())
        result = hook.create_policy(
            "sales",
            "expiry",
            "system.snapshot-expiry",
            content={"enable": True},
            catalog="c1",
            if_exists="update",
        )
        assert result.version == 4
        assert not fake_polaris.calls("PUT", f"{self.POLICIES}/expiry")

    def test_create_policy_if_exists_skip_and_fail(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", self.POLICIES, _error(409, "exists", "AlreadyExistsException"))
        fake_polaris.route("GET", f"{self.POLICIES}/expiry", (200, {"policy": self.POLICY}))
        hook = PolarisCatalogHook(polaris_conn())
        expiry = "system.snapshot-expiry"
        assert hook.create_policy("sales", "expiry", expiry, catalog="c1", if_exists="skip").name == "expiry"
        with pytest.raises(PolarisConflictError) as info:
            hook.create_policy("sales", "expiry", expiry, catalog="c1")
        assert info.value.retryable is False  # already exists: a retry cannot help

    @pytest.mark.parametrize("if_exists", ["skip", "update"])
    def test_create_policy_existing_with_other_type_fails(self, fake_polaris, polaris_conn, if_exists):
        fake_polaris.route("POST", self.POLICIES, _error(409, "exists", "AlreadyExistsException"))
        fake_polaris.route("GET", f"{self.POLICIES}/expiry", (200, {"policy": self.POLICY}))
        with pytest.raises(
            PolarisInvalidArgumentError, match="already exists with type 'system.snapshot-expiry'"
        ):
            PolarisCatalogHook(polaris_conn()).create_policy(
                "sales", "expiry", "system.data-compaction", catalog="c1", if_exists=if_exists
            )
        assert not fake_polaris.calls("PUT", f"{self.POLICIES}/expiry")

    def test_update_policy_keeps_description_when_only_content_changes(self, fake_polaris, polaris_conn):
        old = {**self.POLICY, "content": json.dumps({"enable": False}), "description": "keep me"}
        fake_polaris.route("GET", f"{self.POLICIES}/expiry", (200, {"policy": old}))
        fake_polaris.route("PUT", f"{self.POLICIES}/expiry", (200, {"policy": old}))
        PolarisCatalogHook(polaris_conn()).update_policy("sales", "expiry", {"enable": True}, catalog="c1")
        put = fake_polaris.calls("PUT", f"{self.POLICIES}/expiry")[0].json
        assert put["description"] == "keep me"  # Polaris would clear a description sent as null
        assert json.loads(put["content"]) == {"enable": True}

    def test_update_policy_keeps_content_when_only_description_changes(self, fake_polaris, polaris_conn):
        old = {**self.POLICY, "content": json.dumps({"enable": True}), "description": "old"}
        fake_polaris.route("GET", f"{self.POLICIES}/expiry", (200, {"policy": old}))
        fake_polaris.route("PUT", f"{self.POLICIES}/expiry", (200, {"policy": old}))
        PolarisCatalogHook(polaris_conn()).update_policy("sales", "expiry", description="new", catalog="c1")
        put = fake_polaris.calls("PUT", f"{self.POLICIES}/expiry")[0].json
        assert put["description"] == "new"
        assert put["content"] == old["content"]  # Polaris rejects an update without content

    def test_update_policy_rejects_content_that_is_not_json(self, fake_polaris, polaris_conn):
        old = {**self.POLICY, "content": json.dumps({"enable": True})}
        fake_polaris.route("GET", f"{self.POLICIES}/expiry", (200, {"policy": old}))
        with pytest.raises(PolarisInvalidArgumentError, match="not valid JSON") as info:
            PolarisCatalogHook(polaris_conn()).update_policy("sales", "expiry", "{oops", catalog="c1")
        assert isinstance(info.value, ValueError)
        assert not info.value.retryable

    def test_list_policies(self, fake_polaris, polaris_conn):
        def policies(request):
            if request.query.get("pageToken") == ["p2"]:
                return 200, {"identifiers": [{"namespace": ["sales"], "name": "b"}]}
            return 200, {"identifiers": [{"namespace": ["sales"], "name": "a"}], "next-page-token": "p2"}

        fake_polaris.route("GET", self.POLICIES, policies)
        result = PolarisCatalogHook(polaris_conn()).list_policies("sales", catalog="c1", policy_type="t")
        assert [p.name for p in result] == ["a", "b"]
        assert fake_polaris.calls("GET", self.POLICIES)[0].query == {"policyType": ["t"]}

    def test_attach_and_detach(self, fake_polaris, polaris_conn):
        mappings = f"{self.POLICIES}/expiry/mappings"
        fake_polaris.route("PUT", mappings, (204, None))  # Polaris accepts re-attaching the same policy
        fake_polaris.route("POST", mappings, _error(404, "not attached", "NotFoundException"))
        hook = PolarisCatalogHook(polaris_conn())
        target = {"type": "table-like", "path": ["sales", "orders"]}
        hook.attach_policy("sales", "expiry", target, parameters={"k": "v"}, catalog="c1")
        assert fake_polaris.calls("PUT", mappings)[0].json == {"target": target, "parameters": {"k": "v"}}
        assert hook.detach_policy("sales", "expiry", target, catalog="c1", if_missing="skip") is False
        with pytest.raises(PolarisNotFoundError):
            hook.detach_policy("sales", "expiry", target, catalog="c1")

    def test_attach_fails_when_another_policy_of_the_type_is_attached(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "PUT",
            f"{self.POLICIES}/expiry/mappings",
            _error(
                409, "The policy mapping of same type already exists", "PolicyMappingAlreadyExistsException"
            ),
        )
        with pytest.raises(PolarisConflictError, match="same type") as info:
            PolarisCatalogHook(polaris_conn()).attach_policy(
                "sales", "expiry", {"type": "namespace", "path": ["sales"]}, catalog="c1"
            )
        assert info.value.retryable is False

    def test_drop_policy(self, fake_polaris, polaris_conn):
        fake_polaris.route("DELETE", f"{self.POLICIES}/expiry", (204, None))
        assert PolarisCatalogHook(polaris_conn()).drop_policy(
            "sales", "expiry", catalog="c1", detach_all=True
        )
        assert fake_polaris.calls("DELETE", f"{self.POLICIES}/expiry")[0].query == {"detach-all": ["true"]}

    def test_get_applicable_policies(self, fake_polaris, polaris_conn):
        applicable = {**self.POLICY, "inherited": True, "namespace": ["sales"], "content": "{}"}
        fake_polaris.route(
            "GET", f"{CAT}/polaris/v1/c1/applicable-policies", (200, {"applicable-policies": [applicable]})
        )
        result = PolarisCatalogHook(polaris_conn()).get_applicable_policies(
            catalog="c1", namespace="sales.eu", table="orders", policy_type="system.snapshot-expiry"
        )
        assert [p.name for p in result] == ["expiry"]
        query = fake_polaris.calls("GET", f"{CAT}/polaris/v1/c1/applicable-policies")[0].query
        assert query == {
            "namespace": ["sales\x1feu"],
            "target-name": ["orders"],
            "policyType": ["system.snapshot-expiry"],
        }

    def test_get_applicable_policies_table_needs_namespace(self, polaris_conn):
        with pytest.raises(PolarisInvalidArgumentError, match="namespace"):
            PolarisCatalogHook(polaris_conn()).get_applicable_policies(catalog="c1", table="t")

    def test_send_notification(self, fake_polaris, polaris_conn):
        path = f"{CAT}/v1/c1/namespaces/sales/tables/orders/notifications"
        fake_polaris.route("POST", path, (204, None))
        notification = {
            "notification-type": "UPDATE",
            "payload": {
                "table-name": "orders",
                "timestamp": 1,
                "table-uuid": "u",
                "metadata-location": "s3://m.json",
            },
        }
        PolarisCatalogHook(polaris_conn()).send_notification("sales", "orders", notification, catalog="c1")
        assert fake_polaris.calls("POST", path)[0].json == notification


class TestNamespaces:
    NAMESPACES = f"{CAT}/v1/c1/namespaces"

    def test_create_namespace(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "POST",
            self.NAMESPACES,
            lambda r: (
                200,
                {
                    "namespace": r.json["namespace"],
                    "properties": {**r.json["properties"], "location": "s3://l"},
                },
            ),
        )
        properties = PolarisCatalogHook(polaris_conn()).create_namespace(
            "sales.eu", properties={"owner": "data-eng"}, catalog="c1"
        )
        assert properties == {"owner": "data-eng", "location": "s3://l"}
        assert fake_polaris.calls("POST", self.NAMESPACES)[0].json == {
            "namespace": ["sales", "eu"],
            "properties": {"owner": "data-eng"},
        }

    def test_create_namespace_if_exists(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", self.NAMESPACES, _error(409, "exists", "AlreadyExistsException"))
        fake_polaris.route(
            "GET", f"{self.NAMESPACES}/sales", (200, {"namespace": ["sales"], "properties": {"owner": "x"}})
        )
        hook = PolarisCatalogHook(polaris_conn())
        assert hook.create_namespace("sales", catalog="c1", if_exists="skip") == {"owner": "x"}
        with pytest.raises(PolarisConflictError) as info:
            hook.create_namespace("sales", catalog="c1")
        assert info.value.retryable is False

    @pytest.mark.parametrize(
        ("creates", "reads", "expected"),
        [
            pytest.param(
                [
                    _error(409, "exists", "AlreadyExistsException"),
                    (200, {"namespace": ["sales"], "properties": {"owner": "new"}}),
                ],
                [_error(404, "gone", "NoSuchNamespaceException")],
                {"owner": "new"},
                id="dropped",
            ),
            pytest.param(
                [
                    _error(409, "exists", "AlreadyExistsException"),
                    _error(409, "exists", "AlreadyExistsException"),
                ],
                [
                    _error(404, "gone", "NoSuchNamespaceException"),
                    (200, {"namespace": ["sales"], "properties": {"owner": "other"}}),
                ],
                {"owner": "other"},
                id="dropped_and_created_again",
            ),
        ],
    )
    def test_create_namespace_if_exists_after_a_concurrent_drop(
        self, fake_polaris, polaris_conn, creates, reads, expected
    ):
        # Another client drops the namespace between Polaris's "already exists" and our read (and
        # may create it again before our next attempt): the create runs again.
        creates, reads = iter(creates), iter(reads)
        fake_polaris.route("POST", self.NAMESPACES, lambda _r: next(creates))
        fake_polaris.route("GET", f"{self.NAMESPACES}/sales", lambda _r: next(reads))
        hook = PolarisCatalogHook(polaris_conn())
        assert hook.create_namespace("sales", catalog="c1", if_exists="skip") == expected
        assert len(fake_polaris.calls("POST", self.NAMESPACES)) == 2

    def test_update_properties_sends_updates_and_removals_separately(self, fake_polaris, polaris_conn):
        # Polaris answers 409 "Concurrent modification" to one request carrying both.
        def properties(request):
            body = request.json
            if body.get("updates") and body.get("removals"):
                return _error(409, "Concurrent modification of namespace", "CommitConflictException")
            return 200, {
                "updated": list(body.get("updates") or {}),
                "removed": [k for k in body.get("removals") or [] if k != "nope"],
                "missing": [k for k in body.get("removals") or [] if k == "nope"],
            }

        fake_polaris.route("POST", f"{self.NAMESPACES}/sales/properties", properties)
        result = PolarisCatalogHook(polaris_conn()).update_namespace_properties(
            "sales", updates={"a": "1"}, removals=["b", "nope"], catalog="c1"
        )
        assert result == {"updated": ["a"], "removed": ["b"], "missing": ["nope"]}
        bodies = [r.json for r in fake_polaris.calls("POST", f"{self.NAMESPACES}/sales/properties")]
        assert bodies == [{"updates": {"a": "1"}}, {"removals": ["b", "nope"]}]

    def test_update_properties_with_updates_only_sends_one_request(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "POST",
            f"{self.NAMESPACES}/sales/properties",
            (200, {"updated": ["a"], "removed": [], "missing": []}),
        )
        PolarisCatalogHook(polaris_conn()).update_namespace_properties(
            "sales", updates={"a": "1"}, catalog="c1"
        )
        assert len(fake_polaris.calls("POST", f"{self.NAMESPACES}/sales/properties")) == 1

    def test_drop_namespace(self, fake_polaris, polaris_conn):
        fake_polaris.route("DELETE", f"{self.NAMESPACES}/sales\x1feu", (204, None))
        assert PolarisCatalogHook(polaris_conn()).drop_namespace(["sales", "eu"], catalog="c1") is True

    def test_drop_namespace_if_missing(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "DELETE", f"{self.NAMESPACES}/sales", _error(404, "missing", "NoSuchNamespaceException")
        )
        hook = PolarisCatalogHook(polaris_conn())
        assert hook.drop_namespace("sales", catalog="c1", if_missing="skip") is False
        with pytest.raises(PolarisNotFoundError):
            hook.drop_namespace("sales", catalog="c1")

    def test_dropping_a_namespace_with_contents_is_not_retryable(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "DELETE",
            f"{self.NAMESPACES}/sales",
            _error(409, "Namespace sales is not empty", "NamespaceNotEmptyException"),
        )
        with pytest.raises(PolarisConflictError, match="not empty") as info:
            PolarisCatalogHook(polaris_conn()).drop_namespace("sales", catalog="c1", if_missing="skip")
        assert info.value.retryable is False

    @pytest.mark.parametrize(("response", "exists"), [((204, None), True), ((404, None), False)])
    def test_namespace_exists(self, fake_polaris, polaris_conn, response, exists):
        fake_polaris.route("HEAD", f"{self.NAMESPACES}/sales", response)
        assert PolarisCatalogHook(polaris_conn()).namespace_exists("sales", catalog="c1") is exists

    def test_exists_reports_a_rejected_realm_instead_of_false(self, fake_polaris, polaris_conn):
        # A HEAD response has no body, so a bad realm's 404 looks like a missing namespace.
        realm_error = _error(404, "Missing or invalid realm", "MissingOrInvalidRealm")
        fake_polaris.route("HEAD", f"{self.NAMESPACES}/sales", realm_error)
        fake_polaris.route("GET", f"{self.NAMESPACES}/sales", realm_error)
        fake_polaris.route("HEAD", f"{self.NAMESPACES}/sales/tables/orders", realm_error)
        fake_polaris.route("GET", f"{self.NAMESPACES}/sales/tables/orders", realm_error)
        hook = PolarisCatalogHook(polaris_conn(auth_type="token", login=None, password="t", realm="NOPE"))
        with pytest.raises(PolarisAuthenticationError, match="rejected the realm"):
            hook.namespace_exists("sales", catalog="c1")
        with pytest.raises(PolarisAuthenticationError, match="rejected the realm"):
            hook.table_exists("sales", "orders", catalog="c1")

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"updates": {"a": 1}}, "updates values must be strings.*not for: a"),
            ({"removals": "a"}, "removals must be a list of strings.*render_template_as_native_obj"),
            ({"removals": ["a", 1]}, "removals must be a list of strings"),
            ({"updates": "{}"}, "updates must be a dict.*render_template_as_native_obj"),
            ({"updates": {"a": "1", "b": "2"}, "removals": ["b"]}, "both updated and removed: b"),
        ],
    )
    def test_update_properties_rejects_invalid_changes(self, fake_polaris, polaris_conn, kwargs, match):
        with pytest.raises(PolarisInvalidArgumentError, match=match) as info:
            PolarisCatalogHook(polaris_conn()).update_namespace_properties("sales", catalog="c1", **kwargs)
        assert info.value.retryable is False
        assert not fake_polaris.calls("POST", f"{self.NAMESPACES}/sales/properties")

    def test_create_namespace_rejects_properties_that_are_not_strings(self, fake_polaris, polaris_conn):
        with pytest.raises(PolarisInvalidArgumentError, match="properties values must be strings"):
            PolarisCatalogHook(polaris_conn()).create_namespace(
                "sales", properties={"retention-days": 7}, catalog="c1"
            )
        assert not fake_polaris.calls("POST", self.NAMESPACES)


def _table(location: str = "s3://lake/sales/orders", **metadata) -> dict:
    """A LoadTableResult body, including the storage config and credentials Polaris can vend."""
    return {
        "metadata-location": f"{location}/metadata/00001.metadata.json",
        "metadata": {
            "format-version": 2,
            "table-uuid": "uuid-1",
            "location": location,
            "properties": {"owner": "x"},
            **metadata,
        },
        "config": {"s3.access-key-id": "AKIA-VENDED", "s3.secret-access-key": "SECRET-VENDED"},
        "storage-credentials": [{"prefix": "s3://lake", "config": {"s3.secret-access-key": "SECRET-VENDED"}}],
    }


class TestTables:
    TABLES = f"{CAT}/v1/c1/namespaces/sales/tables"
    SCHEMA: ClassVar[dict] = {
        "type": "struct",
        "fields": [
            {"id": 1, "name": "id", "type": "long", "required": True},
            {
                "id": 2,
                "name": "tags",
                "type": {"type": "list", "element-id": 3, "element": "string", "element-required": False},
                "required": False,
            },
        ],
    }

    def test_create_table_sends_the_definition_without_requesting_credentials(
        self, fake_polaris, polaris_conn
    ):
        fake_polaris.route("POST", self.TABLES, (200, _table()))
        partition_spec = {
            "fields": [{"source-id": 1, "field-id": 1000, "name": "id_bucket", "transform": "bucket[16]"}]
        }
        sort_order = {
            "order-id": 1,
            "fields": [
                {"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}
            ],
        }
        result = PolarisCatalogHook(polaris_conn()).create_table(
            "sales",
            "orders",
            self.SCHEMA,
            partition_spec=partition_spec,
            sort_order=sort_order,
            location="s3://lake/sales/orders",
            properties={"owner": "x"},
            catalog="c1",
        )
        assert result["metadata"]["table-uuid"] == "uuid-1"
        (request,) = fake_polaris.calls("POST", self.TABLES)
        assert request.json == {
            "name": "orders",
            "schema": self.SCHEMA,
            "partition-spec": {"spec-id": 0, **partition_spec},  # the SDK alone would drop the ids
            "write-order": sort_order,
            "location": "s3://lake/sales/orders",
            "properties": {"owner": "x"},
        }
        assert "x-iceberg-access-delegation" not in request.headers

    @pytest.mark.parametrize(
        ("sort_order", "expected"),
        [
            (
                {
                    "fields": [
                        {
                            "source-id": 1,
                            "transform": "identity",
                            "direction": "asc",
                            "null-order": "nulls-first",
                        }
                    ]
                },
                1,
            ),
            ({"fields": []}, 0),
            (
                {
                    "order-id": 7,
                    "fields": [
                        {
                            "source-id": 1,
                            "transform": "identity",
                            "direction": "desc",
                            "null-order": "nulls-last",
                        }
                    ],
                },
                7,
            ),
        ],
    )
    def test_create_table_sends_the_ids_polaris_requires(
        self, fake_polaris, polaris_conn, sort_order, expected
    ):
        fake_polaris.route("POST", self.TABLES, (200, _table()))
        PolarisCatalogHook(polaris_conn()).create_table(
            "sales",
            "orders",
            self.SCHEMA,
            partition_spec={"spec-id": 3, "fields": []},
            sort_order=sort_order,
            catalog="c1",
        )
        body = fake_polaris.calls("POST", self.TABLES)[0].json
        assert body["write-order"]["order-id"] == expected
        assert body["partition-spec"]["spec-id"] == 3

    def test_create_table_if_exists_reports_a_view_of_the_same_name(self, fake_polaris, polaris_conn):
        # Polaris says "already exists" for a view too; there is no table to return then.
        fake_polaris.route(
            "POST",
            self.TABLES,
            _error(409, "View with same name already exists: sales.orders", "AlreadyExistsException"),
        )
        fake_polaris.route(
            "GET", f"{self.TABLES}/orders", _error(404, "Table does not exist", "NoSuchTableException")
        )
        with pytest.raises(PolarisConflictError, match="View with same name already exists") as info:
            PolarisCatalogHook(polaris_conn()).create_table(
                "sales", "orders", self.SCHEMA, catalog="c1", if_exists="skip"
            )
        assert info.value.retryable is False
        # The create is tried once more first, in case the table was dropped in between.
        assert len(fake_polaris.calls("POST", self.TABLES)) == 2
        assert len(fake_polaris.calls("GET", f"{self.TABLES}/orders")) == 2

    def test_create_table_if_exists(self, fake_polaris, polaris_conn):
        fake_polaris.route("POST", self.TABLES, _error(409, "Table already exists", "AlreadyExistsException"))
        fake_polaris.route("GET", f"{self.TABLES}/orders", (200, _table()))
        hook = PolarisCatalogHook(polaris_conn())
        existing = hook.create_table("sales", "orders", self.SCHEMA, catalog="c1", if_exists="skip")
        assert existing["metadata-location"] == "s3://lake/sales/orders/metadata/00001.metadata.json"
        assert fake_polaris.calls("GET", f"{self.TABLES}/orders")[0].query == {"snapshots": ["refs"]}
        with pytest.raises(PolarisConflictError) as info:
            hook.create_table("sales", "orders", self.SCHEMA, catalog="c1")
        assert info.value.retryable is False

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"schema": "{'type': 'struct'}"}, "schema must be a dict.*render_template_as_native_obj"),
            ({"partition_spec": "[]"}, "partition_spec must be a dict"),
            ({"properties": ["a"]}, "properties must be a dict, got list"),
        ],
    )
    def test_create_table_rejects_definitions_that_are_not_dicts(self, polaris_conn, kwargs, match):
        args = {"schema": self.SCHEMA, **kwargs}
        with pytest.raises(PolarisInvalidArgumentError, match=match):
            PolarisCatalogHook(polaris_conn()).create_table("sales", "orders", catalog="c1", **args)

    @pytest.mark.parametrize(("purge", "query"), [(False, "false"), (True, "true")])
    def test_drop_table(self, fake_polaris, polaris_conn, purge, query):
        fake_polaris.route("DELETE", f"{self.TABLES}/orders", (204, None))
        assert PolarisCatalogHook(polaris_conn()).drop_table("sales", "orders", purge=purge, catalog="c1")
        assert fake_polaris.calls("DELETE", f"{self.TABLES}/orders")[0].query == {"purgeRequested": [query]}

    def test_drop_table_if_missing(self, fake_polaris, polaris_conn):
        fake_polaris.route("DELETE", f"{self.TABLES}/orders", _error(404, "missing", "NoSuchTableException"))
        hook = PolarisCatalogHook(polaris_conn())
        assert hook.drop_table("sales", "orders", catalog="c1", if_missing="skip") is False
        with pytest.raises(PolarisNotFoundError):
            hook.drop_table("sales", "orders", catalog="c1")

    def test_update_table_properties_commits_only_changes(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "GET", f"{self.TABLES}/orders", (200, _table(properties={"keep": "1", "old": "1", "same": "v"}))
        )
        fake_polaris.route(
            "POST",
            f"{self.TABLES}/orders",
            (
                200,
                {
                    "metadata-location": "m2",
                    "metadata": {
                        "format-version": 2,
                        "table-uuid": "uuid-1",
                        "properties": {"keep": "1", "same": "v", "new": "2"},
                    },
                },
            ),
        )
        result = PolarisCatalogHook(polaris_conn()).update_table_properties(
            "sales", "orders", updates={"same": "v", "new": "2"}, removals=["old", "absent"], catalog="c1"
        )
        assert result == {
            "updated": ["new"],
            "removed": ["old"],
            "properties": {"keep": "1", "same": "v", "new": "2"},
        }
        (commit,) = fake_polaris.calls("POST", f"{self.TABLES}/orders")
        assert commit.json == {
            "identifier": {"namespace": ["sales"], "name": "orders"},
            "requirements": [{"type": "assert-table-uuid", "uuid": "uuid-1"}],
            "updates": [
                {"action": "set-properties", "updates": {"new": "2"}},
                {"action": "remove-properties", "removals": ["old"]},
            ],
        }

    @pytest.mark.parametrize(
        ("updates", "removals", "expected_updates"),
        [
            ({"a": "2"}, None, [{"action": "set-properties", "updates": {"a": "2"}}]),
            (None, ["a"], [{"action": "remove-properties", "removals": ["a"]}]),
        ],
    )
    def test_update_table_properties_commits_one_kind_of_change(
        self, fake_polaris, polaris_conn, updates, removals, expected_updates
    ):
        fake_polaris.route("GET", f"{self.TABLES}/orders", (200, _table(properties={"a": "1"})))
        fake_polaris.route(
            "POST",
            f"{self.TABLES}/orders",
            (200, {"metadata-location": "m2", "metadata": {"format-version": 2, "table-uuid": "uuid-1"}}),
        )
        PolarisCatalogHook(polaris_conn()).update_table_properties(
            "sales", "orders", updates=updates, removals=removals, catalog="c1"
        )
        assert fake_polaris.calls("POST", f"{self.TABLES}/orders")[0].json["updates"] == expected_updates

    def test_update_table_properties_is_a_no_op_when_nothing_changes(self, fake_polaris, polaris_conn):
        fake_polaris.route("GET", f"{self.TABLES}/orders", (200, _table(properties={"a": "1"})))
        result = PolarisCatalogHook(polaris_conn()).update_table_properties(
            "sales", "orders", updates={"a": "1"}, removals=["gone"], catalog="c1"
        )
        assert result == {"updated": [], "removed": [], "properties": {"a": "1"}}
        assert not fake_polaris.calls("POST", f"{self.TABLES}/orders")

    def test_register_table(self, fake_polaris, polaris_conn):
        register = f"{CAT}/v1/c1/namespaces/sales/register"
        fake_polaris.route("POST", register, (200, _table()))
        PolarisCatalogHook(polaris_conn()).register_table("sales", "orders", "s3://lake/m.json", catalog="c1")
        body = fake_polaris.calls("POST", register)[0].json
        assert (body["name"], body["metadata-location"]) == ("orders", "s3://lake/m.json")

    def test_register_table_if_exists(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "POST", f"{CAT}/v1/c1/namespaces/sales/register", _error(409, "exists", "AlreadyExistsException")
        )
        fake_polaris.route("GET", f"{self.TABLES}/orders", (200, _table()))
        hook = PolarisCatalogHook(polaris_conn())
        assert hook.register_table("sales", "orders", "s3://lake/m.json", catalog="c1", if_exists="skip")
        with pytest.raises(PolarisConflictError):
            hook.register_table("sales", "orders", "s3://lake/m.json", catalog="c1")

    def test_rename_table_to_another_namespace(self, fake_polaris, polaris_conn):
        rename = f"{CAT}/v1/c1/tables/rename"
        fake_polaris.route("POST", rename, (204, None))
        assert PolarisCatalogHook(polaris_conn()).rename_table(
            "sales", "orders", "orders_v2", new_namespace="archive.eu", catalog="c1"
        )
        assert fake_polaris.calls("POST", rename)[0].json == {
            "source": {"namespace": ["sales"], "name": "orders"},
            "destination": {"namespace": ["archive", "eu"], "name": "orders_v2"},
        }

    @pytest.mark.parametrize(
        ("if_missing", "destination", "expected"),
        [
            ("skip", (204, None), False),  # re-run: already renamed
            ("skip", (404, None), PolarisNotFoundError),  # gone under both names
            ("fail", (204, None), PolarisNotFoundError),
        ],
    )
    def test_rename_table_when_the_source_is_missing(
        self, fake_polaris, polaris_conn, if_missing, destination, expected
    ):
        fake_polaris.route(
            "POST", f"{CAT}/v1/c1/tables/rename", _error(404, "missing", "NoSuchTableException")
        )
        fake_polaris.route("HEAD", f"{self.TABLES}/orders_v2", destination)
        hook = PolarisCatalogHook(polaris_conn())
        if expected is False:
            assert (
                hook.rename_table("sales", "orders", "orders_v2", catalog="c1", if_missing=if_missing)
                is False
            )
        else:
            with pytest.raises(expected):
                hook.rename_table("sales", "orders", "orders_v2", catalog="c1", if_missing=if_missing)

    @pytest.mark.parametrize(("response", "exists"), [((204, None), True), ((404, None), False)])
    def test_table_exists(self, fake_polaris, polaris_conn, response, exists):
        fake_polaris.route("HEAD", f"{self.TABLES}/orders", response)
        assert PolarisCatalogHook(polaris_conn()).table_exists("sales", "orders", catalog="c1") is exists

    def test_create_table_sends_iceberg_v3_definitions_the_sdk_models_reject(
        self, fake_polaris, polaris_conn
    ):
        # Field defaults (format v3) fail the SDK's models, both in requests and in responses.
        schema = {
            "type": "struct",
            "fields": [{"id": 1, "name": "id", "type": "long", "required": False, "initial-default": 5}],
        }
        fake_polaris.route(
            "POST",
            self.TABLES,
            (200, _table(**{"format-version": 3, "schemas": [{"schema-id": 0, **schema}]})),
        )
        result = PolarisCatalogHook(polaris_conn()).create_table(
            "sales", "orders", schema, properties={"format-version": "3"}, catalog="c1"
        )
        assert result["metadata"]["format-version"] == 3
        assert fake_polaris.calls("POST", self.TABLES)[0].json == {
            "name": "orders",
            "schema": schema,
            "properties": {"format-version": "3"},
        }

    def test_create_table_explains_a_definition_polaris_cannot_parse(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "POST", self.TABLES, _error(400, "HTTP 400 Bad Request", "WebApplicationException")
        )
        with pytest.raises(PolarisApiError, match="could not parse the table definition") as info:
            PolarisCatalogHook(polaris_conn()).create_table("sales", "orders", {"fields": []}, catalog="c1")
        assert info.value.retryable is False

    def test_create_table_keeps_other_bad_requests_unchanged(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "POST",
            self.TABLES,
            _error(400, "Cannot find source column for partition field", "ValidationException"),
        )
        with pytest.raises(PolarisApiError, match="Cannot find source column") as info:
            PolarisCatalogHook(polaris_conn()).create_table("sales", "orders", self.SCHEMA, catalog="c1")
        assert "could not parse" not in str(info.value)

    def test_create_table_rejects_properties_that_are_not_strings(self, fake_polaris, polaris_conn):
        with pytest.raises(PolarisInvalidArgumentError, match="properties values must be strings"):
            PolarisCatalogHook(polaris_conn()).create_table(
                "sales", "orders", self.SCHEMA, properties={"format-version": 2}, catalog="c1"
            )
        assert not fake_polaris.calls("POST", self.TABLES)

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"updates": {"a": True}}, "updates values must be strings"),
            ({"removals": "a"}, "removals must be a list of strings"),
            ({"updates": {"a": "1"}, "removals": ["a"]}, "both updated and removed: a"),
        ],
    )
    def test_update_table_properties_rejects_invalid_changes(self, fake_polaris, polaris_conn, kwargs, match):
        with pytest.raises(PolarisInvalidArgumentError, match=match):
            PolarisCatalogHook(polaris_conn()).update_table_properties(
                "sales", "orders", catalog="c1", **kwargs
            )
        assert not fake_polaris.calls("GET", f"{self.TABLES}/orders")

    def test_update_table_properties_retries_after_the_table_changed(self, fake_polaris, polaris_conn):
        # The UUID requirement failed: the table was dropped and re-created since it was loaded.
        fake_polaris.route("GET", f"{self.TABLES}/orders", (200, _table(properties={})))
        fake_polaris.route(
            "POST",
            f"{self.TABLES}/orders",
            _error(409, "Requirement failed: UUID does not match", "CommitFailedException"),
        )
        with pytest.raises(PolarisConflictError) as info:
            PolarisCatalogHook(polaris_conn()).update_table_properties(
                "sales", "orders", updates={"a": "1"}, catalog="c1"
            )
        assert info.value.retryable is True

    def test_load_table_returns_the_iceberg_rest_json(self, fake_polaris, polaris_conn):
        body = _table()
        body["metadata"]["schemas"] = [
            {
                "type": "struct",
                "schema-id": 0,
                "fields": [{"id": 1, "name": "id", "type": "long", "required": False, "initial-default": 5}],
            }
        ]
        fake_polaris.route("GET", f"{self.TABLES}/orders", (200, body))
        assert PolarisCatalogHook(polaris_conn()).load_table("sales", "orders", catalog="c1") == body

    def test_load_table_missing(self, fake_polaris, polaris_conn):
        fake_polaris.route(
            "GET", f"{self.TABLES}/orders", _error(404, "Table does not exist", "NoSuchTableException")
        )
        with pytest.raises(PolarisNotFoundError, match="Table does not exist"):
            PolarisCatalogHook(polaris_conn()).load_table("sales", "orders", catalog="c1")

    def test_load_table_does_not_request_credentials(self, fake_polaris, polaris_conn):
        fake_polaris.route("GET", f"{self.TABLES}/orders", (200, _table()))
        assert PolarisCatalogHook(polaris_conn()).get_table_location("sales", "orders", catalog="c1") == (
            "s3://lake/sales/orders"
        )
        (request,) = fake_polaris.calls("GET", f"{self.TABLES}/orders")
        assert "x-iceberg-access-delegation" not in request.headers


class TestIcebergCatalog:
    def test_internal_uses_session_credential_flow_with_realm(self, polaris_conn, fake_polaris):
        props = PolarisCatalogHook(
            polaris_conn(realm="PROD", scope="PRINCIPAL_ROLE:r")
        ).get_iceberg_catalog_properties("c1")
        assert props == {
            "type": "rest",
            "uri": f"{fake_polaris.url}/api/catalog",
            "warehouse": "c1",
            "header.Polaris-Realm": "PROD",
            "credential": "client-id:s3cr3t",
            "oauth2-server-uri": f"{fake_polaris.url}{TOKEN_PATH}",
            "scope": "PRINCIPAL_ROLE:r",
        }

    def test_oidc_basic_uses_refreshing_oauth2_manager(self, polaris_conn):
        props = PolarisCatalogHook(
            polaris_conn(auth_type="oidc", oauth2_server_uri="https://idp/token", scope="s")
        ).get_iceberg_catalog_properties("c1")
        assert props["auth"] == {
            "type": "oauth2",
            "oauth2": {
                "client_id": "client-id",
                "client_secret": "s3cr3t",
                "token_url": "https://idp/token",
                "expires_in": 300,  # pyiceberg fails on a token response without expires_in otherwise
                "scope": "s",
            },
        }
        assert "credential" not in props

    def test_oidc_with_audience_uses_credential_flow(self, polaris_conn):
        props = PolarisCatalogHook(
            polaris_conn(auth_type="oidc", oauth2_server_uri="https://idp/token", audience="aud")
        ).get_iceberg_catalog_properties("c1")
        assert props["credential"] == "client-id:s3cr3t"
        assert props["audience"] == "aud"
        assert "auth" not in props

    @mock.patch.object(PolarisCatalogHook, "log", new_callable=mock.PropertyMock)
    def test_oidc_with_custom_tls_uses_credential_flow(self, mock_log, polaris_conn):
        # pyiceberg's OAuth2 manager would request tokens without the connection's CA bundle.
        props = PolarisCatalogHook(
            polaris_conn(auth_type="oidc", oauth2_server_uri="https://idp/token", verify="/ca.pem")
        ).get_iceberg_catalog_properties("c1")
        assert "auth" not in props
        assert props["credential"] == "client-id:s3cr3t"
        assert props["ssl"] == {"cabundle": "/ca.pem"}
        warning = mock_log.return_value.warning
        warning.assert_called_once()
        assert "client_secret_post" in warning.call_args.args[0]

    def test_token_and_tls(self, polaris_conn):
        props = PolarisCatalogHook(
            polaris_conn(login=None, password="tok", auth_type="token", verify="/ca.pem")
        ).get_iceberg_catalog_properties("c1")
        assert props["token"] == "tok"
        assert props["ssl"] == {"cabundle": "/ca.pem"}

    def test_get_iceberg_catalog_builds_rest_catalog(self, polaris_conn):
        pytest.importorskip("pyiceberg")
        with mock.patch("pyiceberg.catalog.rest.RestCatalog", autospec=True) as rest_catalog:
            PolarisCatalogHook(polaris_conn()).get_iceberg_catalog("c1", **{"s3.region": "eu-west-1"})
        name, kwargs = rest_catalog.call_args.args[0], rest_catalog.call_args.kwargs
        assert name == "polaris-polaris_test"
        assert kwargs["warehouse"] == "c1"
        assert kwargs["s3.region"] == "eu-west-1"
