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
from functools import cached_property
from typing import TYPE_CHECKING, Any

import akeyless

from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.connection import Connection


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

    :param connections_path: Akeyless path prefix for Connections (None to disable).
    :param variables_path: Akeyless path prefix for Variables (None to disable).
    :param config_path: Akeyless path prefix for Config (None to disable).
    :param sep: Separator between base path and key.
    :param api_url: Akeyless API endpoint.
    :param access_id: Access ID.
    :param access_key: Access Key (for ``api_key`` auth).
    :param access_type: Auth type (default ``api_key``).
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
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.connections_path = connections_path.rstrip("/") if connections_path else None
        self.variables_path = variables_path.rstrip("/") if variables_path else None
        self.config_path = config_path.rstrip("/") if config_path else None
        self.sep = sep
        self._api_url = api_url
        self._access_id = access_id
        self._access_key = access_key
        self._access_type = access_type
        self._extra = kwargs

    @cached_property
    def _client(self) -> akeyless.V2Api:
        return akeyless.V2Api(akeyless.ApiClient(akeyless.Configuration(host=self._api_url)))

    def _authenticate(self) -> str:
        """Return an API token via ``akeyless.Auth``."""
        if self._access_type == "uid":
            return self._extra["uid_token"]
        body = akeyless.Auth(access_id=self._access_id, access_key=self._access_key)
        if self._access_type != "api_key":
            body.access_type = self._access_type
        return self._client.auth(body).token

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
