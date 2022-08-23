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
"""Objects relating to sourcing connections & variables from Hashicorp Vault"""
import warnings
from typing import TYPE_CHECKING, Optional

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
         different engine. For authentication mount_points see, auth_mount_point.
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
        connections_path: str = 'connections',
        variables_path: str = 'variables',
        config_path: str = 'config',
        url: Optional[str] = None,
        auth_type: str = 'token',
        auth_mount_point: Optional[str] = None,
        mount_point: str = 'secret',
        kv_engine_version: int = 2,
        token: Optional[str] = None,
        token_path: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        key_id: Optional[str] = None,
        secret_id: Optional[str] = None,
        role_id: Optional[str] = None,
        kubernetes_role: Optional[str] = None,
        kubernetes_jwt_path: str = '/var/run/secrets/kubernetes.io/serviceaccount/token',
        gcp_key_path: Optional[str] = None,
        gcp_keyfile_dict: Optional[dict] = None,
        gcp_scopes: Optional[str] = None,
        azure_tenant_id: Optional[str] = None,
        azure_resource: Optional[str] = None,
        radius_host: Optional[str] = None,
        radius_secret: Optional[str] = None,
        radius_port: Optional[int] = None,
        **kwargs,
    ):
        super().__init__()
        if connections_path is not None:
            self.connections_path = connections_path.rstrip('/')
        else:
            self.connections_path = connections_path
        if variables_path is not None:
            self.variables_path = variables_path.rstrip('/')
        else:
            self.variables_path = variables_path
        if config_path is not None:
            self.config_path = config_path.rstrip('/')
        else:
            self.config_path = config_path
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

    def get_response(self, conn_id: str) -> Optional[dict]:
        """
        Get data from Vault

        :rtype: dict
        :return: The data from the Vault path if exists
        """
        if self.connections_path is None:
            return None

        secret_path = self.build_path(self.connections_path, conn_id)
        return self.vault_client.get_secret(secret_path=secret_path)

    def get_conn_uri(self, conn_id: str) -> Optional[str]:
        """
        Get serialized representation of connection

        :param conn_id: The connection id
        :rtype: str
        :return: The connection uri retrieved from the secret
        """
        # Since VaultBackend implements `get_connection`, `get_conn_uri` is not used. So we
        # don't need to implement (or direct users to use) method `get_conn_value` instead
        warnings.warn(
            f"Method `{self.__class__.__name__}.get_conn_uri` is deprecated and will be removed "
            "in a future release.",
            DeprecationWarning,
            stacklevel=2,
        )
        response = self.get_response(conn_id)
        return response.get("conn_uri") if response else None

    # Make sure connection is imported this way for type checking, otherwise when importing
    # the backend it will get a circular dependency and fail
    if TYPE_CHECKING:
        from airflow.models.connection import Connection

    def get_connection(self, conn_id: str) -> 'Optional[Connection]':
        """
        Get connection from Vault as secret. Prioritize conn_uri if exists,
        if not fall back to normal Connection creation.

        :rtype: Connection
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

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get Airflow Variable

        :param key: Variable Key
        :rtype: str
        :return: Variable Value retrieved from the vault
        """
        if self.variables_path is None:
            return None
        else:
            secret_path = self.build_path(self.variables_path, key)
            response = self.vault_client.get_secret(secret_path=secret_path)
            return response.get("value") if response else None

    def get_config(self, key: str) -> Optional[str]:
        """
        Get Airflow Configuration

        :param key: Configuration Option Key
        :rtype: str
        :return: Configuration Option Value retrieved from the vault
        """
        if self.config_path is None:
            return None
        else:
            secret_path = self.build_path(self.config_path, key)
            response = self.vault_client.get_secret(secret_path=secret_path)
            return response.get("value") if response else None
