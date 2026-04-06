# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Secrets Backend for sourcing Connections, Variables, and Config from Akeyless."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from airflow.providers.akeyless._internal_client.akeyless_client import (
    _AkeylessClient,
)
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.connection import Connection


class AkeylessBackend(BaseSecretsBackend, LoggingMixin):
    """Retrieve Connections, Variables, and Configuration from Akeyless.

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

    **Secret naming convention** -- secrets are looked up by joining
    ``<base_path>/<key>``.  For example, with
    ``connections_path="/airflow/connections"`` and ``conn_id="postgres_default"``,
    the backend reads ``/airflow/connections/postgres_default``.

    The stored value may be:

    * A URI string (``conn_uri``) for Connections.
    * A JSON dict with Connection kwargs for Connections.
    * A plain string for Variables (stored under a ``value`` key
      *or* as the raw secret value).
    * A plain string for Configuration options (same rule as Variables).

    :param connections_path: Akeyless folder path for Connection secrets.
        Set to *None* to disable connection lookup.
    :param variables_path: Akeyless folder path for Variable secrets.
        Set to *None* to disable variable lookup.
    :param config_path: Akeyless folder path for Configuration secrets.
        Set to *None* to disable config lookup.
    :param sep: Separator between base path and key.  Defaults to ``/``.
    :param api_url: Akeyless API endpoint.
    :param access_id: Access ID for authentication.
    :param access_key: Access Key (for ``api_key`` auth).
    :param access_type: Auth type.  Defaults to ``api_key``.
    :param uid_token: Universal-Identity token (for ``uid`` auth).
    :param gcp_audience: GCP audience (for ``gcp`` auth).
    :param azure_object_id: Azure Object ID (for ``azure_ad`` auth).
    :param jwt: JWT token (for ``jwt`` auth).
    :param k8s_auth_config_name: K8s auth config name (for ``k8s`` auth).
    :param certificate_data: PEM cert (for ``certificate`` auth).
    :param private_key_data: PEM key  (for ``certificate`` auth).
    """

    def __init__(
        self,
        connections_path: str | None = "/airflow/connections",
        variables_path: str | None = "/airflow/variables",
        config_path: str | None = "/airflow/config",
        sep: str = "/",
        api_url: str | None = None,
        access_id: str | None = None,
        access_key: str | None = None,
        access_type: str = "api_key",
        uid_token: str | None = None,
        gcp_audience: str | None = None,
        azure_object_id: str | None = None,
        jwt: str | None = None,
        k8s_auth_config_name: str | None = None,
        certificate_data: str | None = None,
        private_key_data: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.connections_path = (
            connections_path.rstrip("/") if connections_path else None
        )
        self.variables_path = variables_path.rstrip("/") if variables_path else None
        self.config_path = config_path.rstrip("/") if config_path else None
        self.sep = sep

        self.vault_client = _AkeylessClient(
            api_url=api_url,
            access_id=access_id,
            access_key=access_key,
            access_type=access_type,
            uid_token=uid_token,
            gcp_audience=gcp_audience,
            azure_object_id=azure_object_id,
            jwt=jwt,
            k8s_auth_config_name=k8s_auth_config_name,
            certificate_data=certificate_data,
            private_key_data=private_key_data,
            **kwargs,
        )

    def _build_path(self, base: str, key: str) -> str:
        return f"{base}{self.sep}{key}"

    def _get_secret(self, base_path: str | None, key: str) -> str | None:
        """Fetch a raw secret value from Akeyless."""
        if base_path is None:
            return None
        path = self._build_path(base_path, key)
        return self.vault_client.get_secret_value(path)

    def get_response(self, conn_id: str) -> str | None:
        """Return raw secret data for *conn_id*."""
        return self._get_secret(self.connections_path, conn_id)

    def get_connection(
        self, conn_id: str, team_name: str | None = None
    ) -> Connection | None:
        """Build an Airflow ``Connection`` from an Akeyless secret.

        The stored secret may be a URI string or a JSON dict.
        """
        from airflow.models.connection import Connection

        raw = self.get_response(conn_id)
        if raw is None:
            return None

        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return Connection(conn_id, uri=raw)

        if isinstance(data, dict):
            uri = data.pop("conn_uri", None)
            if uri:
                return Connection(conn_id, uri=uri)
            return Connection(conn_id, **data)

        return Connection(conn_id, uri=str(data))

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """Retrieve an Airflow Variable from Akeyless.

        The stored secret may be a plain string or a JSON object with a
        ``value`` key.
        """
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
