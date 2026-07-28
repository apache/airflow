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
