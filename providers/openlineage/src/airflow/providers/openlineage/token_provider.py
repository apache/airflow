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

import logging
import threading
import time
from typing import Any

import requests
from openlineage.client.transport.http import TokenProvider

from airflow.providers.common.compat.sdk import AirflowException, BaseHook

log = logging.getLogger(__name__)

AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE = "airflow_connection_api_key"
AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE = "airflow_connection_oauth2_client_credentials"
_DEFAULT_EXTRA_KEYS = ("apiKey", "api_key", "apikey", "token", "access_token")


class OpenLineageAirflowConnectionAuthError(AirflowException):
    """Raised when OpenLineage API key auth cannot be resolved from an Airflow connection."""


class OpenLineageAirflowConnectionConfigError(AirflowException):
    """Raised when OpenLineage config cannot be resolved from an Airflow connection."""


class OpenLineageOAuth2ConfigError(AirflowException):
    """Raised when OpenLineage OAuth2 client credentials auth is misconfigured."""


class OpenLineageOAuth2TokenError(AirflowException):
    """Raised when an OAuth2 access token for OpenLineage HTTP transport cannot be obtained."""


def _get_config_value(config: dict[str, Any], *keys: str) -> Any:
    """Return the first non-empty value found under the given keys (camelCase first, then snake_case)."""
    for key in keys:
        value = config.get(key)
        if value is not None and value != "":
            return value
    return None


def _get_required_config_value(config: dict[str, Any], *keys: str) -> str:
    value = _get_config_value(config, *keys)
    if not value:
        raise OpenLineageOAuth2ConfigError(
            f"OpenLineage OAuth2 client credentials auth requires a non-empty `{keys[0]}`."
        )
    return str(value)


class OAuth2ClientCredentialsTokenProvider(TokenProvider):
    """
    OpenLineage HTTP transport ``TokenProvider`` using the OAuth 2.0 client credentials grant (RFC 6749, 4.4).

    Access tokens requested from ``tokenEndpoint`` with ``clientId`` and ``clientSecret`` are cached and
    requested again ``tokenRefreshBuffer`` seconds before they expire. Options are accepted in camelCase and
    snake_case, as OpenLineage client environment variables are read in snake_case.
    """

    DEFAULT_TOKEN_REFRESH_BUFFER = 120.0
    DEFAULT_TOKEN_LIFETIME = 300.0  # used when the token endpoint does not return `expires_in`
    CLIENT_AUTH_METHODS = ("client_secret_basic", "client_secret_post")

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self.token_endpoint = _get_required_config_value(config, "tokenEndpoint", "token_endpoint")
        self.client_id = _get_required_config_value(config, "clientId", "client_id")
        self.client_secret = _get_required_config_value(config, "clientSecret", "client_secret")
        self.scope = _get_config_value(config, "scope")
        self.client_auth_method = str(
            _get_config_value(config, "clientAuthMethod", "client_auth_method") or self.CLIENT_AUTH_METHODS[0]
        ).lower()
        if self.client_auth_method not in self.CLIENT_AUTH_METHODS:
            raise OpenLineageOAuth2ConfigError(
                f"OpenLineage OAuth2 auth option `clientAuthMethod` must be one of {self.CLIENT_AUTH_METHODS}, "
                f"got `{self.client_auth_method}`."
            )
        token_refresh_buffer = _get_config_value(config, "tokenRefreshBuffer", "token_refresh_buffer")
        self.token_refresh_buffer = (
            self.DEFAULT_TOKEN_REFRESH_BUFFER if token_refresh_buffer is None else float(token_refresh_buffer)
        )
        self._lock = threading.Lock()
        self._access_token: str | None = None
        self._refresh_at = 0.0

    def get_bearer(self) -> str | None:
        with self._lock:
            if self._access_token is None or time.monotonic() >= self._refresh_at:
                self._request_token()
            return f"Bearer {self._access_token}"

    def _request_token(self) -> None:
        data = {"grant_type": "client_credentials"}
        if self.scope:
            data["scope"] = self.scope
        basic_auth = None
        if self.client_auth_method == "client_secret_post":
            data["client_id"] = self.client_id
            data["client_secret"] = self.client_secret
        else:
            basic_auth = (self.client_id, self.client_secret)

        log.debug(
            "Requesting OAuth2 access token from `%s` for client `%s`.", self.token_endpoint, self.client_id
        )
        try:
            response = requests.post(self.token_endpoint, data=data, auth=basic_auth, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            raise OpenLineageOAuth2TokenError(
                f"OAuth2 token request to `{self.token_endpoint}` failed: {e}"
            ) from e
        try:
            payload = response.json()
            access_token = payload["access_token"]
        except (ValueError, KeyError, TypeError):
            raise OpenLineageOAuth2TokenError(
                f"OAuth2 token endpoint `{self.token_endpoint}` did not return an `access_token`."
            ) from None
        lifetime = self._get_token_lifetime(payload)
        # Refresh early, but never so early that every event would trigger a new token request.
        self._refresh_at = time.monotonic() + lifetime - min(self.token_refresh_buffer, lifetime / 2)
        self._access_token = str(access_token)
        log.debug(
            "Obtained OAuth2 access token for client `%s`, valid for %s seconds.", self.client_id, lifetime
        )

    def _get_token_lifetime(self, payload: dict[str, Any]) -> float:
        expires_in = payload.get("expires_in", self.DEFAULT_TOKEN_LIFETIME)
        try:
            lifetime = float(expires_in)
        except (TypeError, ValueError):
            lifetime = 0.0
        if lifetime <= 0:
            raise OpenLineageOAuth2TokenError(
                f"OAuth2 token endpoint `{self.token_endpoint}` returned invalid `expires_in` value `{expires_in}`."
            )
        return lifetime


OAUTH2_CLIENT_CREDENTIALS_AUTH_TYPE = (
    f"{OAuth2ClientCredentialsTokenProvider.__module__}.{OAuth2ClientCredentialsTokenProvider.__qualname__}"
)


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
    connection host. Any other auth options are passed through to
    :class:`OAuth2ClientCredentialsTokenProvider` unchanged.
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
        token_endpoint = _get_config_value(self.config, "tokenEndpoint", "token_endpoint") or connection.host
        if not (connection.login and connection.password and token_endpoint):
            raise OpenLineageAirflowConnectionAuthError(
                f"OpenLineage `{AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE}` auth requires connection `{self.conn_id}` "
                "to have login (client ID), password (client secret) and host (token endpoint, unless "
                "`tokenEndpoint` is set in auth config)."
            )
        options = {
            key: value
            for key, value in self.config.items()
            if key not in ("type", "conn_id", "tokenEndpoint", "token_endpoint")
        }
        return {
            "type": OAUTH2_CLIENT_CREDENTIALS_AUTH_TYPE,
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
    :class:`OAuth2ClientCredentialsTokenProvider` config holding the client credentials.
    """
    if not isinstance(config, dict):
        return

    for key, value in config.items():
        if (
            key == "auth"
            and isinstance(value, dict)
            and value.get("type") == AIRFLOW_CONNECTION_API_KEY_AUTH_TYPE
        ):
            provider = AirflowConnectionTokenProvider(value, default_conn_id=config_conn_id)
            config[key] = {"type": "api_key", "apiKey": provider.get_api_key()}
        elif (
            key == "auth"
            and isinstance(value, dict)
            and value.get("type") == AIRFLOW_CONNECTION_OAUTH2_AUTH_TYPE
        ):
            oauth2_provider = AirflowConnectionOAuth2ClientCredentialsProvider(
                value, default_conn_id=config_conn_id
            )
            config[key] = oauth2_provider.get_auth_config()
        elif key == "transports" and isinstance(value, list):
            for item in value:
                resolve_airflow_connection_auth(item, config_conn_id=config_conn_id)
        else:
            resolve_airflow_connection_auth(value, config_conn_id=config_conn_id)
