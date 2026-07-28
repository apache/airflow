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
"""Objects relating to sourcing connections & variables from Hashicorp Vault."""

from __future__ import annotations

import json
import warnings
from collections.abc import Sequence

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import conf
from airflow.providers.hashicorp._internal_client.vault_client import _VaultClient
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class VaultBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connections and Variables from Hashicorp Vault.

    Configurable via ``airflow.cfg`` as follows:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
        backend_kwargs = {
            "connections_path": "connections",
            "url": "http://127.0.0.1:8200",
            "mount_point": "airflow"
            }

    For example, if your keys are under ``connections`` path in ``airflow`` mount_point, this
    would be accessible if you provide ``{"connections_path": "connections"}`` and request
    conn_id ``smtp_default``.

    :param connections_path: Specifies the path(s) of the secret to read to get Connections. Accepts a
        single path or a list of paths, tried in order until a secret is found.
        (default: 'connections'). If set to None (null), requests for connections will not be sent to Vault.
    :param variables_path: Specifies the path(s) of the secret to read to get Variable. Accepts a
        single path or a list of paths, tried in order until a secret is found.
        (default: 'variables'). If set to None (null), requests for variables will not be sent to Vault.
    :param config_path: Specifies the path(s) of the secret to read Airflow Configurations. Accepts a
        single path or a list of paths, tried in order until a secret is found.
        (default: 'config'). If set to None (null), requests for configurations will not be sent to Vault.
    :param use_team_secrets_path: Flag to enable team scoped secret retrieval from {base_path}/{team_name}/{key}
        in multi team deployments. (default: true)
    :param global_secrets_path: (Deprecated) Path appended as an extra fallback entry to
        ``connections_path``, ``variables_path``, and ``config_path``. (default: No prefix) Use a list of
        paths in those parameters instead, e.g. ``["connections/team1", "connections/common"]``.
    :param url: Base URL for the Vault instance being addressed.
    :param auth_type: Authentication Type for Vault. Default is ``token``. Available values are:
        ('approle', 'aws_iam', 'azure', 'github', 'gcp', 'jwt', 'kubernetes', 'ldap', 'radius', 'token', 'userpass')
    :param auth_mount_point: It can be used to define mount_point for authentication chosen
          Default depends on the authentication method used.
    :param mount_point: The "path" the secret engine was mounted on. Default is "secret". Note that
         this mount_point is not used for authentication if authentication is done via a
         different engine. If set to None, the mount secret should be provided as a prefix for each
         variable/connection_id. For authentication mount_points see, auth_mount_point.
    :param kv_engine_version: Select the version of the engine to run (``1`` or ``2``, default: ``2``).
    :param token: Authentication token to include in requests sent to Vault.
        (for ``token`` and ``github`` auth_type)
    :param token_path: path to file containing authentication token to include in requests sent to Vault
        (for ``token`` and ``github`` auth_type).
    :param username: Username for Authentication (for ``ldap`` and ``userpass`` auth_type).
    :param password: Password for Authentication (for ``ldap`` and ``userpass`` auth_type).
    :param key_id: Key ID for Authentication (for ``aws_iam`` and ''azure`` auth_type).
    :param secret_id: Secret ID for Authentication (for ``approle``, ``aws_iam`` and ``azure`` auth_types).
    :param role_id: Role ID for Authentication (for ``approle``, ``aws_iam`` and ``gcp`` auth_types).
    :param assume_role_kwargs: AWS assume role param.
        See AWS STS Docs:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts/client/assume_role.html
    :param region: AWS region for STS API calls (for ``aws_iam`` auth_type).
    :param kubernetes_role: Role for Authentication (for ``kubernetes`` auth_type).
    :param kubernetes_jwt_path: Path for kubernetes jwt token (for ``kubernetes`` auth_type, default:
        ``/var/run/secrets/kubernetes.io/serviceaccount/token``).
    :param gcp_key_path: Path to Google Cloud Service Account key file (JSON) (for ``gcp`` auth_type).
           Mutually exclusive with gcp_keyfile_dict.
    :param gcp_keyfile_dict: Dictionary of keyfile parameters. (for ``gcp`` auth_type).
           Mutually exclusive with gcp_key_path.
    :param gcp_scopes: Comma-separated string containing OAuth2 scopes (for ``gcp`` auth_type).
    :param azure_tenant_id: The tenant id for the Azure Active Directory (for ``azure`` auth_type).
    :param azure_resource: The configured URL for the application registered in Azure Active Directory
           (for ``azure`` auth_type).
    :param radius_host: Host for radius (for ``radius`` auth_type).
    :param radius_secret: Secret for radius (for ``radius`` auth_type).
    :param radius_port: Port for radius (for ``radius`` auth_type).
    :param jwt_role: Role for Authentication (for ``jwt`` auth_type).
    :param jwt_token: JWT token for Authentication (for ``jwt`` auth_type).
    :param jwt_token_path: Path to file containing JWT token for Authentication (for ``jwt`` auth_type).
    """

    def __init__(
        self,
        connections_path: str | Sequence[str] | None = "connections",
        variables_path: str | Sequence[str] | None = "variables",
        config_path: str | Sequence[str] | None = "config",
        use_team_secrets_path: bool = True,
        global_secrets_path: str | None = None,
        url: str | None = None,
        auth_type: str = "token",
        auth_mount_point: str | None = None,
        mount_point: str | None = "secret",
        kv_engine_version: int = 2,
        token: str | None = None,
        token_path: str | None = None,
        username: str | None = None,
        password: str | None = None,
        key_id: str | None = None,
        secret_id: str | None = None,
        role_id: str | None = None,
        assume_role_kwargs: dict | None = None,
        region: str | None = None,
        kubernetes_role: str | None = None,
        kubernetes_jwt_path: str = "/var/run/secrets/kubernetes.io/serviceaccount/token",
        gcp_key_path: str | None = None,
        gcp_keyfile_dict: dict | None = None,
        gcp_scopes: str | None = None,
        azure_tenant_id: str | None = None,
        azure_resource: str | None = None,
        radius_host: str | None = None,
        radius_secret: str | None = None,
        radius_port: int | None = None,
        jwt_role: str | None = None,
        jwt_token: str | None = None,
        jwt_token_path: str | None = None,
        **kwargs,
    ):
        super().__init__()
        self.connections_path = self._normalize_paths(connections_path)
        self.variables_path = self._normalize_paths(variables_path)
        self.config_path = self._normalize_paths(config_path)
        self.use_team_secrets_path = use_team_secrets_path
        if global_secrets_path is not None:
            warnings.warn(
                "The `global_secrets_path` parameter is deprecated and will be removed in a future "
                "release. Provide a list of fallback paths to `connections_path`, `variables_path`, "
                'and `config_path` instead, e.g. `connections_path=["connections/foobar", "connections/common"]`.',
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            global_secrets_path = global_secrets_path.rstrip("/")
            self.connections_path = self._append_path(self.connections_path, global_secrets_path)
            self.variables_path = self._append_path(self.variables_path, global_secrets_path)
            self.config_path = self._append_path(self.config_path, global_secrets_path)

        self.mount_point = mount_point
        self.kv_engine_version = kv_engine_version
        self.vault_client = _VaultClient(
            url=url,
            auth_type=auth_type,
            auth_mount_point=auth_mount_point,
            mount_point=mount_point,
            kv_engine_version=kv_engine_version,
            token=token,
            token_path=token_path,
            username=username,
            password=password,
            key_id=key_id,
            secret_id=secret_id,
            role_id=role_id,
            assume_role_kwargs=assume_role_kwargs,
            region=region,
            kubernetes_role=kubernetes_role,
            kubernetes_jwt_path=kubernetes_jwt_path,
            gcp_key_path=gcp_key_path,
            gcp_keyfile_dict=gcp_keyfile_dict,
            gcp_scopes=gcp_scopes,
            azure_tenant_id=azure_tenant_id,
            azure_resource=azure_resource,
            radius_host=radius_host,
            radius_secret=radius_secret,
            radius_port=radius_port,
            jwt_role=jwt_role,
            jwt_token=jwt_token,
            jwt_token_path=jwt_token_path,
            **kwargs,
        )

    @staticmethod
    def _normalize_paths(paths: str | Sequence[str] | None) -> list[str] | None:
        """
        Normalize a single path or a sequence of paths into a list, stripping trailing slashes.

        :param paths: Paths to normalize.
        :return: Normalized paths.
        """
        if paths is None:
            return None
        if isinstance(paths, str):
            paths = [paths]
        return [path.rstrip("/") for path in paths]

    @staticmethod
    def _append_path(paths: list[str] | None, path: str) -> list[str]:
        """
        Append a path into a list (which could be None).

        :param paths: List of paths to append to.
        :param path: Path to append.
        :return: Resulting list of paths.
        """
        return [*(paths or []), path]

    def _parse_path(self, secret_path: str) -> tuple[str | None, str | None]:
        if not self.mount_point:
            split_secret_path = secret_path.split("/", 1)
            if len(split_secret_path) < 2:
                return None, None
            return split_secret_path[0], split_secret_path[1]
        return "", secret_path

    def _get_secret_with_base(self, base_path: str | None, key: str) -> dict | None:
        """Resolve mount and base path, then fetch the secret from Vault."""
        mount_point, key_part = self._parse_path(key)

        if base_path is None or key_part is None:
            return None

        if base_path == "":
            secret_path = key_part
        else:
            secret_path = self.build_path(base_path, key_part)

        return self.vault_client.get_secret(
            secret_path=(mount_point + "/" if mount_point else "") + secret_path
        )

    def _get_secret(self, base_paths: list[str] | None, team_name: str | None, key: str):
        """
        Get a secret, trying each of ``base_paths`` in order until one yields a value.

        If multi team is enabled and a team name is given, check {base_path}/{team_name}/{key}.
        Otherwise, check {base_path}/{key}.

        :param base_paths: Base paths to try, in order.
        :param team_name: Team name associated to the task trying to access the secret (if any).
        :param key: Secret key.
        """
        if not base_paths:
            return None
        multi_team_enabled = conf.getboolean("core", "multi_team", fallback=False)
        for base_path in base_paths:
            if multi_team_enabled and self.use_team_secrets_path and team_name is not None:
                path_ = self.build_path(base_path, team_name)
            else:
                path_ = base_path

            response = self._get_secret_with_base(path_, key)
            if response is not None:
                return response

        return None

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        """
        Retrieve a connection from Vault as a serialized string.

        Returns the ``conn_uri`` value verbatim when present, otherwise serializes
        the secret dict to JSON.  On Airflow 3.2+, the base-class ``get_connection``
        deserializes the returned string using the Connection class that the framework
        injects per execution context (ORM Connection on the server, SDK Connection in
        workers), which avoids triggering SQLAlchemy mapper initialization in
        task-execution subprocesses such as PythonVirtualenvOperator.  On the older
        releases this provider still supports (2.11 / 3.0 / 3.1) there is no such
        injection: the base ``deserialize_connection`` imports the ORM ``Connection``
        directly, so the mapper-initialization avoidance does not apply there.

        :param conn_id: connection id
        :param team_name: Team name associated to the task trying to access the connection (if any)
        :return: Serialized connection string or None
        """
        response = self._get_secret(self.connections_path, team_name, conn_id)
        if response is None:
            return None

        uri = response.get("conn_uri")
        if uri:
            return uri

        return json.dumps(response)

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Get Airflow Variable.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
        :return: Variable Value retrieved from the vault
        """
        response = self._get_secret(self.variables_path, team_name, key)

        if not response:
            return None
        try:
            return response["value"]
        except KeyError:
            self.log.warning('Vault secret %s fetched but does not have required key "value"', key)
            return None

    def get_config(self, key: str) -> str | None:
        """
        Get Airflow Configuration.

        Tries each path in ``config_path`` in order and returns the value from the first one found.

        :param key: Configuration Option Key
        :return: Configuration Option Value retrieved from the vault
        """
        response = None
        for base_path in self.config_path or []:
            response = self._get_secret_with_base(base_path, key)
            if response is not None:
                break
        if not response:
            return None
        try:
            return response["value"]
        except KeyError:
            self.log.warning('Vault config %s fetched but does not have required key "value"', key)
            return None
