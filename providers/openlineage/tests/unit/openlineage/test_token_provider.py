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

from unittest.mock import patch

import pytest

from airflow.providers.common.compat.sdk import BaseHook, Connection
from airflow.providers.openlineage.token_provider import (
    AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE,
    AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE,
    AirflowConnectionConfigProvider,
    AirflowConnectionTokenProvider,
    OpenLineageAirflowConnectionAuthError,
    OpenLineageAirflowConnectionConfigError,
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
        "type": "oauth2_client_credentials",
        "tokenEndpoint": OAUTH2_TOKEN_ENDPOINT,
        "clientId": "my-client-id",
        "clientSecret": "my-client-secret",
        "scope": "openid",
    }
    mock_get_connection.assert_called_once_with("openlineage_default")


@pytest.mark.parametrize("token_endpoint_key", ["tokenEndpoint", "token_endpoint"])
@patch.object(BaseHook, "get_connection")
def test_resolve_oauth2_connection_auth_prefers_token_endpoint_from_config(
    mock_get_connection, token_endpoint_key
):
    mock_get_connection.return_value = _oauth2_connection(host=None)
    config = {
        "transport": {
            "type": "http",
            "url": "http://ol-api:5000",
            "auth": {
                "type": AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE,
                "conn_id": "openlineage_default",
                token_endpoint_key: "https://other.example.com/token",
            },
        }
    }

    resolve_airflow_connection_auth(config)

    assert config["transport"]["auth"] == {
        "type": "oauth2_client_credentials",
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
def test_resolve_oauth2_connection_auth_requires_token_endpoint_url_with_scheme(mock_get_connection):
    mock_get_connection.return_value = _oauth2_connection(host="auth.example.com")
    config = {
        "transport": {
            "type": "http",
            "auth": {"type": AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE, "conn_id": "openlineage_default"},
        }
    }

    with pytest.raises(OpenLineageAirflowConnectionAuthError, match="must be a full URL"):
        resolve_airflow_connection_auth(config)


@patch.object(BaseHook, "get_connection")
def test_resolve_oauth2_connection_auth_ignores_client_credentials_from_auth_config(mock_get_connection):
    mock_get_connection.return_value = _oauth2_connection()
    config = {
        "transport": {
            "type": "http",
            "auth": {
                "type": AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE,
                "conn_id": "openlineage_default",
                "clientId": "ignored-client-id",
                "client_secret": "ignored-client-secret",
            },
        }
    }

    resolve_airflow_connection_auth(config)

    assert config["transport"]["auth"] == {
        "type": "oauth2_client_credentials",
        "tokenEndpoint": OAUTH2_TOKEN_ENDPOINT,
        "clientId": "my-client-id",
        "clientSecret": "my-client-secret",
    }


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
        "type": "oauth2_client_credentials",
        "tokenEndpoint": OAUTH2_TOKEN_ENDPOINT,
        "clientId": "my-client-id",
        "clientSecret": "my-client-secret",
    }
    assert config["transport"]["transports"][1]["transports"][1] == {"type": "console"}
