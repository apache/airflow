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

from typing import TYPE_CHECKING

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

    :param connections_path: Specifies the path of the secret to read to get Connections.
        (default: 'connections'). If set to None (null), requests for connections will not be sent to Vault.
    :param variables_path: Specifies the path of the secret to read to get Variable.
        (default: 'variables'). If set to None (null), requests for variables will not be sent to Vault.
    :param config_path: Specifies the path of the secret to read Airflow Configurations
        (default: 'config'). If set to None (null), requests for configurations will not be sent to Vault.
    :param url: Base URL for the Vault instance being addressed.
    :param auth_type: Authentication Type for Vault. Default is ``token``. Available values are:
        ('approle', 'aws_iam', 'azure', 'github', 'gcp', 'kubernetes', 'ldap', 'radius', 'token', 'userpass')
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
    :param role_id: Role ID for Authentication (for ``approle``, ``aws_iam`` auth_types).
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
    """

    def __init__(
        self,
        connections_path: str | None = "connections",
        variables_path: str | None = "variables",
        config_path: str | None = "config",
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
        **kwargs,
    ):
        super().__init__()
        self.connections_path = connections_path.rstrip("/") if connections_path is not None else None
        self.variables_path = variables_path.rstrip("/") if variables_path is not None else None
        self.config_path = config_path.rstrip("/") if config_path is not None else None
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
            **kwargs,
        )

    def _parse_path(self, secret_path: str) -> tuple[str | None, str | None]:
        if not self.mount_point:
            split_secret_path = secret_path.split("/", 1)
            if len(split_secret_path) < 2:
                return None, None
            return split_secret_path[0], split_secret_path[1]
        return "", secret_path

    def get_response(self, conn_id: str) -> dict | None:
        """
        Get data from Vault.

        :return: The data from the Vault path if exists
        """
        mount_point, conn_key = self._parse_path(conn_id)
        if self.connections_path is None or conn_key is None:
            return None
        if self.connections_path == "":
            secret_path = conn_key
        else:
            secret_path = self.build_path(self.connections_path, conn_key)
        return self.vault_client.get_secret(
            secret_path=(mount_point + "/" if mount_point else "") + secret_path
        )

    # Make sure connection is imported this way for type checking, otherwise when importing
    # the backend it will get a circular dependency and fail
    if TYPE_CHECKING:
        from airflow.models.connection import Connection

    def get_connection(self, conn_id: str) -> Connection | None:
        """
        Get connection from Vault as secret.

        Prioritize conn_uri if exists, if not fall back to normal Connection creation.

        :return: A Connection object constructed from Vault data
        """
        # The Connection needs to be locally imported because otherwise we get into cyclic import
        # problems when instantiating the backend during configuration
        from airflow.models.connection import Connection

        response = self.get_response(conn_id)
        if response is None:
            return None

        uri = response.get("conn_uri")
        if uri:
            return Connection(conn_id, uri=uri)

        return Connection(conn_id, **response)

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Get Airflow Variable.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
        :return: Variable Value retrieved from the vault
        """
        mount_point, variable_key = self._parse_path(key)
        if self.variables_path is None or variable_key is None:
            return None
        if self.variables_path == "":
            secret_path = variable_key
        else:
            secret_path = self.build_path(self.variables_path, variable_key)
        response = self.vault_client.get_secret(
            secret_path=(mount_point + "/" if mount_point else "") + secret_path
        )
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

        :param key: Configuration Option Key
        :return: Configuration Option Value retrieved from the vault
        """
        mount_point, config_key = self._parse_path(key)
        if self.config_path is None or config_key is None:
            return None
        if self.config_path == "":
            secret_path = config_key
        else:
            secret_path = self.build_path(self.config_path, config_key)
        response = self.vault_client.get_secret(
            secret_path=(mount_point + "/" if mount_point else "") + secret_path
        )
        return response.get("value") if response else None
