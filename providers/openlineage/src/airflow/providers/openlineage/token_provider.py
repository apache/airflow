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

from typing import Any

from airflow.providers.common.compat.sdk import AirflowException, BaseHook

AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE = "airflow_connection_api_key"
AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE = "airflow_connection_oauth2_client_credentials"
_DEFAULT_EXTRA_KEYS = ("apiKey", "api_key", "apikey", "token", "access_token")
# Supplied by the connection, so they are not passed through from the auth config
_OAUTH2_CONNECTION_PROVIDED_KEYS = (
    "type",
    "conn_id",
    "tokenEndpoint",
    "token_endpoint",
    "clientId",
    "client_id",
    "clientSecret",
    "client_secret",
)


class OpenLineageAirflowConnectionAuthError(AirflowException):
    """Raised when OpenLineage auth cannot be resolved from an Airflow connection."""


class OpenLineageAirflowConnectionConfigError(AirflowException):
    """Raised when OpenLineage config cannot be resolved from an Airflow connection."""


class AirflowConnectionConfigProvider:
    """
    Resolve OpenLineage client configuration from an Airflow connection.

    The connection extra contains the full OpenLineage client config, for example
    ``{"transport": {"type": "console"}}``.
    """

    def __init__(self, conn_id: str) -> None:
        if not conn_id:
            raise OpenLineageAirflowConnectionConfigError(
                "OpenLineage connection config requires a non-empty connection ID."
            )
        self.conn_id = conn_id

    def get_config(self) -> dict[str, Any]:
        connection = BaseHook.get_connection(self.conn_id)
        return self._validate_config(connection.extra_dejson)

    def _validate_config(self, config: Any) -> dict[str, Any]:
        if not isinstance(config, dict):
            raise OpenLineageAirflowConnectionConfigError(
                f"OpenLineage connection config `{config}` is not a dict."
            )
        if not isinstance(config.get("transport"), dict):
            raise OpenLineageAirflowConnectionConfigError(
                "OpenLineage connection config must contain a `transport` JSON object."
            )
        return config


class AirflowConnectionTokenProvider:
    """
    Resolve an OpenLineage API key from an Airflow connection.

    The connection password is preferred. If it is empty and ``extra_key`` is configured, that key
    is read from connection ``extra``. Otherwise, common extra keys are checked.
    """

    def __init__(self, config: dict[str, Any], default_conn_id: str | None = None) -> None:
        self.conn_id = config.get("conn_id") or default_conn_id or ""
        self.extra_key = config.get("extra_key")
        if not self.conn_id:
            raise OpenLineageAirflowConnectionAuthError(
                "OpenLineage `airflow_connection_api_key` auth requires a non-empty `conn_id`."
            )

    def get_api_key(self) -> str:
        connection = BaseHook.get_connection(self.conn_id)
        if connection.password:
            return connection.password.strip()
        api_key = self._get_api_key_from_extra(connection.extra_dejson)
        if api_key:
            return api_key

        raise OpenLineageAirflowConnectionAuthError(
            "OpenLineage `airflow_connection_api_key` auth could not find a token in connection "
            f"`{self.conn_id}`. Expected connection password or token in connection extra."
        )

    def _get_api_key_from_extra(self, extra: dict[str, Any]) -> str | None:
        if self.extra_key:
            value = extra.get(self.extra_key)
            return str(value).strip() if value else None

        for key in _DEFAULT_EXTRA_KEYS:
            value = extra.get(key)
            if value:
                return str(value).strip()
        return None


class AirflowConnectionOAuth2ClientCredentialsProvider:
    """
    Resolve OAuth2 client credentials for OpenLineage HTTP transport from an Airflow connection.

    The client ID is read from the connection login and the client secret from the connection password.
    The token endpoint is read from ``tokenEndpoint`` in the auth config if set, otherwise from the
    connection host. Any other auth options are passed through to the OpenLineage client's
    ``oauth2_client_credentials`` token provider unchanged.
    """

    def __init__(self, config: dict[str, Any], default_conn_id: str | None = None) -> None:
        self.config = config
        self.conn_id = config.get("conn_id") or default_conn_id or ""
        if not self.conn_id:
            raise OpenLineageAirflowConnectionAuthError(
                f"OpenLineage `{AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE}` auth requires a non-empty `conn_id`."
            )

    def get_auth_config(self) -> dict[str, Any]:
        connection = BaseHook.get_connection(self.conn_id)
        token_endpoint = (
            self.config.get("tokenEndpoint") or self.config.get("token_endpoint") or connection.host
        )
        if not (connection.login and connection.password and token_endpoint):
            raise OpenLineageAirflowConnectionAuthError(
                f"OpenLineage `{AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE}` auth requires connection `{self.conn_id}` "
                "to have login (client ID), password (client secret) and host (token endpoint, unless "
                "`tokenEndpoint` is set in auth config)."
            )
        if not str(token_endpoint).startswith(("http://", "https://")):
            raise OpenLineageAirflowConnectionAuthError(
                f"OpenLineage `{AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE}` auth token endpoint "
                f"`{token_endpoint}` must be a full URL. Define connection `{self.conn_id}` in JSON "
                "format, as the URI format drops the scheme from the host."
            )
        options = {
            key: value for key, value in self.config.items() if key not in _OAUTH2_CONNECTION_PROVIDED_KEYS
        }
        return {
            "type": "oauth2_client_credentials",
            **options,
            "tokenEndpoint": token_endpoint,
            "clientId": connection.login,
            "clientSecret": connection.password,
        }


def resolve_airflow_connection_auth(config: dict[str, Any] | None, config_conn_id: str | None = None) -> None:
    """
    Read auth secrets from Airflow connections and put them into the OpenLineage config.

    OpenLineage config can contain one transport, a composite transport, or composite transports
    nested inside each other. This function walks through that structure and updates every matching
    ``auth`` block in place.

    This only makes sense for HTTP transports: ``airflow_connection_api_key`` is replaced with
    ``{"type": "api_key", "apiKey": ...}`` and ``airflow_connection_oauth2_client_credentials`` with
    ``{"type": "oauth2_client_credentials", "clientId": ..., "clientSecret": ..., ...}``.
    """
    if not isinstance(config, dict):
        return

    for key, value in config.items():
        if key == "auth" and isinstance(value, dict):
            if value.get("type") == AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE:
                api_key_provider = AirflowConnectionTokenProvider(value, default_conn_id=config_conn_id)
                config[key] = {"type": "api_key", "apiKey": api_key_provider.get_api_key()}
            elif value.get("type") == AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE:
                oauth2_provider = AirflowConnectionOAuth2ClientCredentialsProvider(
                    value, default_conn_id=config_conn_id
                )
                config[key] = oauth2_provider.get_auth_config()
        elif key == "transports" and isinstance(value, list):
            for item in value:
                resolve_airflow_connection_auth(item, config_conn_id=config_conn_id)
        else:
            resolve_airflow_connection_auth(value, config_conn_id=config_conn_id)
