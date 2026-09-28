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

from airflow.providers.common.compat.sdk import conf
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.connection import Connection

_SUPPORTED_BACKEND_AUTH_TYPES = ("api_key", "uid", "aws_iam", "gcp", "azure_ad")
_CLOUD_AUTH_TYPES = ("aws_iam", "gcp", "azure_ad")
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

    In multi-team deployments (``core.multi_team = True``), secrets are first
    looked up under ``{base_path}/{team_name}/{key}``.  If not found, the
    backend falls back to a global path: ``{base_path}/{global_secrets_path}/{key}``
    (when ``global_secrets_path`` is set) or ``{base_path}/{key}`` (default).
    Team-scoped lookup can be disabled with ``use_team_secrets_path = False``.

    Supported authentication types:

    * ``api_key`` -- authenticate with Access ID + Access Key.
    * ``uid`` -- use a pre-existing Universal Identity token.
    * ``aws_iam`` -- authenticate using the host's AWS IAM role (ideal for
      Amazon MWAA and EC2/ECS/EKS workloads).
    * ``gcp`` -- authenticate using GCP workload identity (ideal for Google
      Managed Service for Apache Airflow and GCE/GKE workloads).
    * ``azure_ad`` -- authenticate using Azure AD identity (ideal for Azure
      workloads).

    Cloud-based auth types (``aws_iam``, ``gcp``, ``azure_ad``) require the
    optional ``akeyless_cloud_id`` package::

        pip install apache-airflow-providers-akeyless[cloud_id]

    :param connections_path: Akeyless path prefix for Connections (None to disable).
    :param variables_path: Akeyless path prefix for Variables (None to disable).
    :param config_path: Akeyless path prefix for Config (None to disable).
    :param sep: Separator between base path and key.
    :param use_team_secrets_path: When True (default), look up secrets under
        ``{base_path}/{team_name}/{key}`` in multi-team mode before falling back.
    :param global_secrets_path: Optional path segment inserted between base path
        and key for the global fallback in multi-team mode (e.g. ``"global"``).
    :param api_url: Akeyless API endpoint.
    :param access_id: Access ID.
    :param access_key: Access Key (for ``api_key`` auth).
    :param access_type: Auth type (``api_key``, ``uid``, ``aws_iam``, ``gcp``,
        or ``azure_ad``).
    :param gcp_audience: GCP audience for ``gcp`` auth (optional).
    :param azure_object_id: Azure AD Object ID for ``azure_ad`` auth (optional).
    :param token_ttl: Seconds to cache the API token before refreshing (default 600).
    """

    def __init__(
        self,
        connections_path: str | None = "/airflow/connections",
        variables_path: str | None = "/airflow/variables",
        config_path: str | None = "/airflow/config",
        sep: str = "/",
        use_team_secrets_path: bool = True,
        global_secrets_path: str | None = None,
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
        self.use_team_secrets_path = use_team_secrets_path
        self.global_secrets_path = (
            global_secrets_path.rstrip("/") if global_secrets_path is not None else None
        )
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
        elif self._access_type in _CLOUD_AUTH_TYPES:
            body = akeyless.Auth(
                access_id=self._access_id,
                access_type=self._access_type,
                cloud_id=self._get_cloud_id(),
            )
            token = self._client.auth(body).token
        else:
            body = akeyless.Auth(access_id=self._access_id, access_key=self._access_key)
            token = self._client.auth(body).token

        self._cached_token = token
        self._token_expiry = now + self._token_ttl
        return token

    def _get_cloud_id(self) -> str:
        """Generate a cloud identity token for AWS IAM / GCP / Azure AD auth."""
        try:
            from akeyless_cloud_id import CloudId
        except ImportError:
            raise ImportError(
                f"`akeyless_cloud_id` is required for {self._access_type} authentication. "
                "Install it with: pip install apache-airflow-providers-akeyless[cloud_id]"
            )
        cid = CloudId()
        if self._access_type == "aws_iam":
            return cid.generate()
        if self._access_type == "gcp":
            return cid.generateGcp(self._extra.get("gcp_audience"))
        if self._access_type == "azure_ad":
            return cid.generateAzure(self._extra.get("azure_object_id"))
        raise ValueError(f"No cloud-id generator for {self._access_type!r}")

    def _multi_team_enabled(self) -> bool:
        """Whether the deployment runs in multi-team mode."""
        return conf.getboolean("core", "multi_team", fallback=False)

    def _escapes_its_namespace(self, key: str, team_name: str | None) -> bool:
        """
        Whether looking ``key`` up for ``team_name`` could resolve another team's secret.

        Only the team-scoped lookup crosses a namespace boundary. It is tried under
        ``<base path><sep><team><sep><key>`` and, when that misses, falls back to
        ``<base path><sep><key>`` -- the prefix every *other* team's secrets sit under. A
        caller in team ``alpha`` asking for ``beta<sep>db_password`` therefore reaches team
        ``beta``'s secret through the fallback. The key is Dag-author controlled and the
        execution API variables route is declared with a ``:path`` converter, so a separator
        survives the round trip.

        The refusal is deliberately narrow, because in this backend the separator is the
        ordinary path separator and nested keys are a legitimate, documented layout. It
        applies only when this backend actually builds a team-scoped path and can fall back
        past it:

        * ``use_team_secrets_path=False`` disables team-scoped lookup entirely, so no team
          path is constructed and no boundary is crossed -- nested keys keep working.
        * A caller with no ``team_name`` resolves in the shared namespace directly rather
          than falling back into it. Whether a global-scope caller should be able to name a
          team's namespace is a separate question about global scope, not this fallback, and
          is left alone here.
        * Outside multi-team mode there are no team namespaces at all.

        The key is never parsed to work out *which* team it names, because it cannot be:
        nothing distinguishes a nested key in the shared namespace from one naming a team.
        """
        return (
            self._multi_team_enabled()
            and self.use_team_secrets_path
            and team_name is not None
            and self.sep in key
        )

    def _log_refusal(self, kind: str, key: str) -> None:
        self.log.warning(
            "%s id %r contains %r, which separates path segments in an Akeyless secret name. "
            "Looked up for a team, such an id can resolve another team's namespace through "
            "the team-agnostic fallback, so it is not looked up. Returning None.",
            kind.capitalize(),
            key,
            self.sep,
        )

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

    def _get_team_or_global_secret(
        self, base_path: str | None, team_name: str | None, key: str
    ) -> str | None:
        """Look up a secret with team-scoped path, falling back to global."""
        if base_path is None:
            return None
        multi_team = self._multi_team_enabled()
        if multi_team and self.use_team_secrets_path and team_name is not None:
            team_path = f"{base_path}{self.sep}{team_name}"
            response = self._get_secret(team_path, key)
            if response is not None:
                return response
        if multi_team and self.global_secrets_path is not None:
            return self._get_secret(f"{base_path}{self.sep}{self.global_secrets_path}", key)
        return self._get_secret(base_path, key)

    # ------------------------------------------------------------------
    # BaseSecretsBackend interface
    # ------------------------------------------------------------------

    def get_connection(self, conn_id: str, team_name: str | None = None) -> Connection | None:
        """Build a ``Connection`` from an Akeyless secret (URI or JSON dict)."""
        from airflow.models.connection import Connection

        if self._escapes_its_namespace(conn_id, team_name):
            self._log_refusal("connection", conn_id)
            return None
        raw = self._get_team_or_global_secret(self.connections_path, team_name, conn_id)
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
        if self._escapes_its_namespace(key, team_name):
            self._log_refusal("variable", key)
            return None
        raw = self._get_team_or_global_secret(self.variables_path, team_name, key)
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
        # No guard here. Config lookups carry no team_name and Airflow does not perform
        # team-scoped config lookups through a secrets backend, so this path never builds a
        # team-scoped name and has no boundary to cross. Refusing nested ids here would only
        # break subfolder config layouts.
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
