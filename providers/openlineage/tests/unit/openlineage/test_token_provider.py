#
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
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from urllib.parse import parse_qs

import pytest
import requests

from airflow.providers.common.compat.sdk import BaseHook, Connection
from airflow.providers.openlineage.token_provider import (
    AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE,
    AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE,
    OAUTH2_CLIENT_CREDENTIALS_AUTH_TYPE,
    AirflowConnectionConfigProvider,
    AirflowConnectionTokenProvider,
    OAuth2ClientCredentialsTokenProvider,
    OpenLineageAirflowConnectionAuthError,
    OpenLineageAirflowConnectionConfigError,
    OpenLineageOAuth2ConfigError,
    OpenLineageOAuth2TokenError,
    resolve_airflow_connection_auth,
)


@patch.object(BaseHook, "get_connection")
def test_get_api_key_from_connection_password(mock_get_connection):
    mock_get_connection.return_value = Connection(
        conn_id="openlineage_default", conn_type="http", password="api-key"
    )

    provider = AirflowConnectionTokenProvider({"conn_id": "openlineage_default"})

    assert provider.get_api_key() == "api-key"


@patch.object(BaseHook, "get_connection")
def test_get_api_key_from_default_connection_id(mock_get_connection):
    mock_get_connection.return_value = Connection(
        conn_id="openlineage_default", conn_type="http", password="api-key"
    )

    provider = AirflowConnectionTokenProvider({}, default_conn_id="openlineage_default")

    assert provider.get_api_key() == "api-key"


@patch.object(BaseHook, "get_connection")
def test_get_api_key_from_connection_extra(mock_get_connection):
    mock_get_connection.return_value = Connection(
        conn_id="openlineage_default", conn_type="http", extra='{"api_key": "api-key-from-extra"}'
    )

    provider = AirflowConnectionTokenProvider({"conn_id": "openlineage_default"})

    assert provider.get_api_key() == "api-key-from-extra"


def test_missing_conn_id_raises_custom_exception():
    with pytest.raises(OpenLineageAirflowConnectionAuthError, match="requires a non-empty `conn_id`"):
        AirflowConnectionTokenProvider({})


@patch.object(BaseHook, "get_connection")
def test_missing_token_raises_custom_exception(mock_get_connection):
    mock_get_connection.return_value = Connection(conn_id="openlineage_default", conn_type="http")

    provider = AirflowConnectionTokenProvider({"conn_id": "openlineage_default"})

    with pytest.raises(OpenLineageAirflowConnectionAuthError, match="could not find a token"):
        provider.get_api_key()


@patch.object(BaseHook, "get_connection")
def test_resolve_connection_auth_in_composite_transport(mock_get_connection):
    mock_get_connection.return_value = Connection(
        conn_id="openlineage_default", conn_type="http", password="api-key"
    )
    config = {
        "transport": {
            "type": "composite",
            "transports": [
                {
                    "type": "http",
                    "url": "http://ol-api:5000",
                    "auth": {
                        "type": AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE,
                        "conn_id": "openlineage_default",
                    },
                }
            ],
        }
    }

    resolve_airflow_connection_auth(config)

    assert config["transport"]["transports"][0]["auth"] == {
        "type": "api_key",
        "apiKey": "api-key",
    }


@patch.object(BaseHook, "get_connection")
def test_resolve_connection_auth_in_nested_composite_transport(mock_get_connection):
    mock_get_connection.return_value = Connection(
        conn_id="openlineage_default", conn_type="http", password="api-key"
    )
    config = {
        "transport": {
            "type": "composite",
            "transports": [
                {
                    "type": "http",
                    "url": "http://ol-api-1:5000",
                    "auth": {
                        "type": AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE,
                        "conn_id": "openlineage_default",
                    },
                },
                {
                    "type": "composite",
                    "transports": [
                        {
                            "type": "http",
                            "url": "http://ol-api-2:5000",
                            "auth": {
                                "type": AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE,
                                "conn_id": "openlineage_default",
                            },
                        },
                        {"type": "console"},
                    ],
                },
            ],
        }
    }

    resolve_airflow_connection_auth(config)

    assert config["transport"]["transports"][0]["auth"] == {
        "type": "api_key",
        "apiKey": "api-key",
    }
    assert config["transport"]["transports"][1]["transports"][0]["auth"] == {
        "type": "api_key",
        "apiKey": "api-key",
    }
    assert config["transport"]["transports"][1]["transports"][1] == {"type": "console"}


@patch.object(BaseHook, "get_connection")
def test_get_openlineage_config_from_connection_extra(mock_get_connection):
    mock_get_connection.return_value = Connection(
        conn_id="openlineage_default",
        conn_type="generic",
        extra='{"transport": {"type": "console"}}',
    )

    provider = AirflowConnectionConfigProvider("openlineage_default")

    assert provider.get_config() == {"transport": {"type": "console"}}


def test_missing_config_conn_id_raises_custom_exception():
    with pytest.raises(OpenLineageAirflowConnectionConfigError, match="requires a non-empty connection ID"):
        AirflowConnectionConfigProvider("")


@patch.object(BaseHook, "get_connection")
def test_missing_config_raises_custom_exception(mock_get_connection):
    mock_get_connection.return_value = Connection(
        conn_id="openlineage_default",
        conn_type="generic",
        extra='{"url": "http://ol-api:5000"}',
    )

    provider = AirflowConnectionConfigProvider("openlineage_default")

    with pytest.raises(
        OpenLineageAirflowConnectionConfigError,
        match="must contain a `transport` JSON object",
    ):
        provider.get_config()


OAUTH2_TOKEN_ENDPOINT = "https://auth.example.com/token"
OAUTH2_AUTH_CONFIG = {
    "type": OAUTH2_CLIENT_CREDENTIALS_AUTH_TYPE,
    "tokenEndpoint": OAUTH2_TOKEN_ENDPOINT,
    "clientId": "my-client-id",
    "clientSecret": "my-client-secret",
}
OAUTH2_TOKEN_RESPONSE = {"access_token": "access-token", "token_type": "Bearer", "expires_in": 600}


def _oauth2_connection(**kwargs):
    connection_kwargs = {
        "conn_id": "openlineage_default",
        "conn_type": "generic",
        "login": "my-client-id",
        "password": "my-client-secret",
        "host": OAUTH2_TOKEN_ENDPOINT,
        **kwargs,
    }
    return Connection(**connection_kwargs)


def test_oauth2_provider_requests_token_with_client_secret_basic(requests_mock):
    requests_mock.post(OAUTH2_TOKEN_ENDPOINT, json=OAUTH2_TOKEN_RESPONSE)

    provider = OAuth2ClientCredentialsTokenProvider(OAUTH2_AUTH_CONFIG)

    assert provider.get_bearer() == "Bearer access-token"
    request = requests_mock.last_request
    expected_credentials = base64.b64encode(b"my-client-id:my-client-secret").decode()
    assert request.headers["Authorization"] == f"Basic {expected_credentials}"
    assert parse_qs(request.text) == {"grant_type": ["client_credentials"]}


def test_oauth2_provider_requests_token_with_client_secret_post(requests_mock):
    requests_mock.post(OAUTH2_TOKEN_ENDPOINT, json=OAUTH2_TOKEN_RESPONSE)
    config = {**OAUTH2_AUTH_CONFIG, "clientAuthMethod": "client_secret_post", "scope": "openid"}

    provider = OAuth2ClientCredentialsTokenProvider(config)

    assert provider.get_bearer() == "Bearer access-token"
    request = requests_mock.last_request
    assert "Authorization" not in request.headers
    assert parse_qs(request.text) == {
        "grant_type": ["client_credentials"],
        "client_id": ["my-client-id"],
        "client_secret": ["my-client-secret"],
        "scope": ["openid"],
    }


def test_oauth2_provider_accepts_snake_case_options():
    provider = OAuth2ClientCredentialsTokenProvider(
        {
            "token_endpoint": OAUTH2_TOKEN_ENDPOINT,
            "client_id": "my-client-id",
            "client_secret": "my-client-secret",
            "client_auth_method": "client_secret_post",
            "token_refresh_buffer": "30",
        }
    )

    assert provider.token_endpoint == OAUTH2_TOKEN_ENDPOINT
    assert provider.client_id == "my-client-id"
    assert provider.client_secret == "my-client-secret"
    assert provider.client_auth_method == "client_secret_post"
    assert provider.token_refresh_buffer == 30.0


@pytest.mark.parametrize("missing_key", ["tokenEndpoint", "clientId", "clientSecret"])
def test_oauth2_provider_requires_token_endpoint_and_client_credentials(missing_key):
    config = {key: value for key, value in OAUTH2_AUTH_CONFIG.items() if key != missing_key}

    with pytest.raises(OpenLineageOAuth2ConfigError, match=f"requires a non-empty `{missing_key}`"):
        OAuth2ClientCredentialsTokenProvider(config)


def test_oauth2_provider_rejects_unknown_client_auth_method():
    with pytest.raises(OpenLineageOAuth2ConfigError, match="`clientAuthMethod` must be one of"):
        OAuth2ClientCredentialsTokenProvider({**OAUTH2_AUTH_CONFIG, "clientAuthMethod": "private_key_jwt"})


@patch("airflow.providers.openlineage.token_provider.time.monotonic")
def test_oauth2_provider_caches_token_and_refreshes_before_expiry(mock_monotonic, requests_mock):
    requests_mock.post(
        OAUTH2_TOKEN_ENDPOINT,
        [
            {"json": {**OAUTH2_TOKEN_RESPONSE, "access_token": "first"}},
            {"json": {**OAUTH2_TOKEN_RESPONSE, "access_token": "second"}},
        ],
    )
    provider = OAuth2ClientCredentialsTokenProvider(OAUTH2_AUTH_CONFIG)

    mock_monotonic.return_value = 1000.0
    assert provider.get_bearer() == "Bearer first"
    mock_monotonic.return_value = 1479.0
    assert provider.get_bearer() == "Bearer first"
    assert requests_mock.call_count == 1
    mock_monotonic.return_value = 1480.0
    assert provider.get_bearer() == "Bearer second"
    assert requests_mock.call_count == 2


@patch("airflow.providers.openlineage.token_provider.time.monotonic")
def test_oauth2_provider_limits_refresh_buffer_to_half_of_token_lifetime(mock_monotonic, requests_mock):
    requests_mock.post(OAUTH2_TOKEN_ENDPOINT, json={**OAUTH2_TOKEN_RESPONSE, "expires_in": 60})
    provider = OAuth2ClientCredentialsTokenProvider(OAUTH2_AUTH_CONFIG)

    mock_monotonic.return_value = 1000.0
    provider.get_bearer()
    mock_monotonic.return_value = 1029.0
    provider.get_bearer()
    assert requests_mock.call_count == 1
    mock_monotonic.return_value = 1030.0
    provider.get_bearer()
    assert requests_mock.call_count == 2


@patch("airflow.providers.openlineage.token_provider.time.monotonic")
def test_oauth2_provider_assumes_default_lifetime_without_expires_in(mock_monotonic, requests_mock):
    requests_mock.post(OAUTH2_TOKEN_ENDPOINT, json={"access_token": "access-token"})
    provider = OAuth2ClientCredentialsTokenProvider(OAUTH2_AUTH_CONFIG)

    mock_monotonic.return_value = 1000.0
    provider.get_bearer()
    mock_monotonic.return_value = 1179.0
    provider.get_bearer()
    assert requests_mock.call_count == 1
    mock_monotonic.return_value = 1180.0
    provider.get_bearer()
    assert requests_mock.call_count == 2


@pytest.mark.parametrize("expires_in", ["soon", 0, -1])
def test_oauth2_provider_rejects_invalid_expires_in(expires_in, requests_mock):
    requests_mock.post(OAUTH2_TOKEN_ENDPOINT, json={"access_token": "access-token", "expires_in": expires_in})

    with pytest.raises(OpenLineageOAuth2TokenError, match="returned invalid `expires_in`"):
        OAuth2ClientCredentialsTokenProvider(OAUTH2_AUTH_CONFIG).get_bearer()


def test_oauth2_provider_raises_on_error_response(requests_mock):
    requests_mock.post(OAUTH2_TOKEN_ENDPOINT, status_code=401, json={"error": "invalid_client"})

    with pytest.raises(OpenLineageOAuth2TokenError, match="failed: 401 Client Error"):
        OAuth2ClientCredentialsTokenProvider(OAUTH2_AUTH_CONFIG).get_bearer()


@pytest.mark.parametrize(
    "response", [{"text": "not json"}, {"json": {"token": "access-token"}}, {"json": []}]
)
def test_oauth2_provider_requires_access_token_in_response(response, requests_mock):
    requests_mock.post(OAUTH2_TOKEN_ENDPOINT, **response)

    with pytest.raises(OpenLineageOAuth2TokenError, match="did not return an `access_token`"):
        OAuth2ClientCredentialsTokenProvider(OAUTH2_AUTH_CONFIG).get_bearer()


def test_oauth2_provider_wraps_request_errors(requests_mock):
    requests_mock.post(OAUTH2_TOKEN_ENDPOINT, exc=requests.ConnectionError("connection refused"))

    with pytest.raises(OpenLineageOAuth2TokenError, match="failed: connection refused"):
        OAuth2ClientCredentialsTokenProvider(OAUTH2_AUTH_CONFIG).get_bearer()


def test_oauth2_provider_requests_token_once_for_concurrent_calls(requests_mock):
    def slow_token_response(request, context):
        time.sleep(0.05)
        return OAUTH2_TOKEN_RESPONSE

    requests_mock.post(OAUTH2_TOKEN_ENDPOINT, json=slow_token_response)
    provider = OAuth2ClientCredentialsTokenProvider(OAUTH2_AUTH_CONFIG)

    with ThreadPoolExecutor(max_workers=8) as executor:
        bearers = list(executor.map(lambda _: provider.get_bearer(), range(8)))

    assert bearers == ["Bearer access-token"] * 8
    assert requests_mock.call_count == 1


@patch.object(BaseHook, "get_connection")
def test_resolve_oauth2_connection_auth(mock_get_connection):
    mock_get_connection.return_value = _oauth2_connection()
    config = {
        "transport": {
            "type": "http",
            "url": "http://ol-api:5000",
            "auth": {
                "type": AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE,
                "conn_id": "openlineage_default",
                "scope": "openid",
            },
        }
    }

    resolve_airflow_connection_auth(config)

    assert config["transport"]["auth"] == {
        "type": OAUTH2_CLIENT_CREDENTIALS_AUTH_TYPE,
        "tokenEndpoint": OAUTH2_TOKEN_ENDPOINT,
        "clientId": "my-client-id",
        "clientSecret": "my-client-secret",
        "scope": "openid",
    }
    mock_get_connection.assert_called_once_with("openlineage_default")


@patch.object(BaseHook, "get_connection")
def test_resolve_oauth2_connection_auth_prefers_token_endpoint_from_config(mock_get_connection):
    mock_get_connection.return_value = _oauth2_connection(host=None)
    config = {
        "transport": {
            "type": "http",
            "url": "http://ol-api:5000",
            "auth": {
                "type": AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE,
                "conn_id": "openlineage_default",
                "token_endpoint": "https://other.example.com/token",
            },
        }
    }

    resolve_airflow_connection_auth(config)

    assert config["transport"]["auth"] == {
        "type": OAUTH2_CLIENT_CREDENTIALS_AUTH_TYPE,
        "tokenEndpoint": "https://other.example.com/token",
        "clientId": "my-client-id",
        "clientSecret": "my-client-secret",
    }


@patch.object(BaseHook, "get_connection")
def test_resolve_oauth2_connection_auth_uses_config_conn_id_by_default(mock_get_connection):
    mock_get_connection.return_value = _oauth2_connection()
    config = {
        "transport": {
            "type": "http",
            "url": "http://ol-api:5000",
            "auth": {"type": AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE},
        }
    }

    resolve_airflow_connection_auth(config, config_conn_id="openlineage_default")

    assert config["transport"]["auth"]["clientId"] == "my-client-id"
    mock_get_connection.assert_called_once_with("openlineage_default")


def test_resolve_oauth2_connection_auth_requires_conn_id():
    config = {"transport": {"type": "http", "auth": {"type": AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE}}}

    with pytest.raises(OpenLineageAirflowConnectionAuthError, match="requires a non-empty `conn_id`"):
        resolve_airflow_connection_auth(config)


@pytest.mark.parametrize("connection_kwargs", [{"login": None}, {"password": None}, {"host": None}])
@patch.object(BaseHook, "get_connection")
def test_resolve_oauth2_connection_auth_requires_login_password_and_host(
    mock_get_connection, connection_kwargs
):
    mock_get_connection.return_value = _oauth2_connection(**connection_kwargs)
    config = {
        "transport": {
            "type": "http",
            "auth": {"type": AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE, "conn_id": "openlineage_default"},
        }
    }

    with pytest.raises(
        OpenLineageAirflowConnectionAuthError, match="requires connection `openlineage_default` to have login"
    ):
        resolve_airflow_connection_auth(config)


@patch.object(BaseHook, "get_connection")
def test_resolve_connection_auth_in_nested_composite_transport_with_mixed_auth_types(mock_get_connection):
    connections = {
        "api_key_conn": Connection(conn_id="api_key_conn", conn_type="http", password="api-key"),
        "oauth2_conn": _oauth2_connection(conn_id="oauth2_conn"),
    }
    mock_get_connection.side_effect = connections.__getitem__
    config = {
        "transport": {
            "type": "composite",
            "transports": [
                {
                    "type": "http",
                    "url": "http://ol-api-1:5000",
                    "auth": {"type": AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE, "conn_id": "api_key_conn"},
                },
                {
                    "type": "composite",
                    "transports": [
                        {
                            "type": "http",
                            "url": "http://ol-api-2:5000",
                            "auth": {"type": AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE, "conn_id": "oauth2_conn"},
                        },
                        {"type": "console"},
                    ],
                },
            ],
        }
    }

    resolve_airflow_connection_auth(config)

    assert config["transport"]["transports"][0]["auth"] == {"type": "api_key", "apiKey": "api-key"}
    assert config["transport"]["transports"][1]["transports"][0]["auth"] == {
        "type": OAUTH2_CLIENT_CREDENTIALS_AUTH_TYPE,
        "tokenEndpoint": OAUTH2_TOKEN_ENDPOINT,
        "clientId": "my-client-id",
        "clientSecret": "my-client-secret",
    }
    assert config["transport"]["transports"][1]["transports"][1] == {"type": "console"}
