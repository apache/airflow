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
"""Secrets Backend for sourcing Connections, Variables, and Config from Akeyless."""

from __future__ import annotations

import json
import time
from functools import cached_property
from typing import TYPE_CHECKING, Any

import akeyless

from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.connection import Connection

_SUPPORTED_BACKEND_AUTH_TYPES = ("api_key", "uid")
_DEFAULT_TOKEN_TTL = 600  # 10 minutes


class AkeylessBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieve Connections, Variables, and Configuration from Akeyless.

    Configurable via ``airflow.cfg``:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.akeyless.secrets.akeyless.AkeylessBackend
        backend_kwargs = {
            "connections_path": "/airflow/connections",
            "variables_path": "/airflow/variables",
            "api_url": "https://api.akeyless.io",
            "access_id": "p-xxxx",
            "access_key": "xxxx"
        }

    Secrets are looked up by joining ``<base_path>/<key>``.

    Only ``api_key`` and ``uid`` authentication types are supported in the
    secrets backend.  For cloud-based authentication (``aws_iam``, ``gcp``,
    ``azure_ad``) or other advanced methods, use ``AkeylessHook`` directly.

    :param connections_path: Akeyless path prefix for Connections (None to disable).
    :param variables_path: Akeyless path prefix for Variables (None to disable).
    :param config_path: Akeyless path prefix for Config (None to disable).
    :param sep: Separator between base path and key.
    :param api_url: Akeyless API endpoint.
    :param access_id: Access ID.
    :param access_key: Access Key (for ``api_key`` auth).
    :param access_type: Auth type (``api_key`` or ``uid``).
    :param token_ttl: Seconds to cache the API token before refreshing (default 600).
    """

    def __init__(
        self,
        connections_path: str | None = "/airflow/connections",
        variables_path: str | None = "/airflow/variables",
        config_path: str | None = "/airflow/config",
        sep: str = "/",
        api_url: str = "https://api.akeyless.io",
        access_id: str | None = None,
        access_key: str | None = None,
        access_type: str = "api_key",
        token_ttl: int = _DEFAULT_TOKEN_TTL,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        if access_type not in _SUPPORTED_BACKEND_AUTH_TYPES:
            raise ValueError(
                f"Unsupported access_type {access_type!r} for AkeylessBackend. "
                f"Must be one of: {', '.join(_SUPPORTED_BACKEND_AUTH_TYPES)}. "
                "For other auth methods, use AkeylessHook directly."
            )
        self.connections_path = connections_path.rstrip("/") if connections_path else None
        self.variables_path = variables_path.rstrip("/") if variables_path else None
        self.config_path = config_path.rstrip("/") if config_path else None
        self.sep = sep
        self._api_url = api_url
        self._access_id = access_id
        self._access_key = access_key
        self._access_type = access_type
        self._extra = kwargs
        self._token_ttl = token_ttl
        self._cached_token: str | None = None
        self._token_expiry: float = 0.0

    @cached_property
    def _client(self) -> akeyless.V2Api:
        return akeyless.V2Api(akeyless.ApiClient(akeyless.Configuration(host=self._api_url)))

    def _authenticate(self) -> str:
        """Return an API token, reusing a cached value when still valid."""
        now = time.monotonic()
        if self._cached_token and now < self._token_expiry:
            return self._cached_token

        if self._access_type == "uid":
            token = self._extra["uid_token"]
        else:
            body = akeyless.Auth(access_id=self._access_id, access_key=self._access_key)
            token = self._client.auth(body).token

        self._cached_token = token
        self._token_expiry = now + self._token_ttl
        return token

    def _get_secret(self, base_path: str | None, key: str) -> str | None:
        if base_path is None:
            return None
        path = f"{base_path}{self.sep}{key}"
        try:
            token = self._authenticate()
            res = self._client.get_secret_value(akeyless.GetSecretValue(names=[path], token=token))
            return res.get(path)
        except akeyless.ApiException:
            self.log.debug("Secret not found: %s", path)
            return None

    # ------------------------------------------------------------------
    # BaseSecretsBackend interface
    # ------------------------------------------------------------------

    def get_connection(self, conn_id: str, team_name: str | None = None) -> Connection | None:
        """Build a ``Connection`` from an Akeyless secret (URI or JSON dict)."""
        from airflow.models.connection import Connection

        raw = self._get_secret(self.connections_path, conn_id)
        if raw is None:
            return None
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return Connection(conn_id, uri=raw)
        if isinstance(data, dict):
            uri = data.pop("conn_uri", None)
            return Connection(conn_id, uri=uri) if uri else Connection(conn_id, **data)
        return Connection(conn_id, uri=str(data))

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """Retrieve an Airflow Variable from Akeyless."""
        raw = self._get_secret(self.variables_path, key)
        if raw is None:
            return None
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data.get("value", raw)
        except (json.JSONDecodeError, TypeError):
            pass
        return raw

    def get_config(self, key: str) -> str | None:
        """Retrieve an Airflow Configuration option from Akeyless."""
        raw = self._get_secret(self.config_path, key)
        if raw is None:
            return None
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data.get("value", raw)
        except (json.JSONDecodeError, TypeError):
            pass
        return raw
