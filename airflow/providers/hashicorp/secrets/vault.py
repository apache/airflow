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
"""
Objects relating to sourcing connections & variables from Hashicorp Vault
"""
from typing import Optional

from airflow.providers.hashicorp._internal_client.vault_client import _VaultClient  # noqa
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


# pylint: disable=too-many-instance-attributes,too-many-locals
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

    :param connections_path: Specifies the path of the secret to read to get Connections
        (default: 'connections').
    :type connections_path: str
    :param variables_path: Specifies the path of the secret to read to get Variables
        (default: 'variables').
    :type variables_path: str
    :param url: Base URL for the Vault instance being addressed.
    :type url: str
    :param auth_type: Authentication Type for Vault. Default is ``token``. Available values are:
        ('approle', 'github', 'gcp', 'kubernetes', 'ldap', 'token', 'userpass')
    :type auth_type: str
    :param mount_point: The "path" the secret engine was mounted on. Default is "secret". Note that
         this mount_point is not used for authentication if authentication is done via a
         different engine.
    :type mount_point: str
    :param kv_engine_version: Select the version of the engine to run (``1`` or ``2``, default: ``2``).
    :type kv_engine_version: int
    :param token: Authentication token to include in requests sent to Vault.
        (for ``token`` and ``github`` auth_type)
    :type token: str
    :param token_auto_renew: Specifies whether the token should automatically be renewed, or let to
        expire when its expire time comes. (for ``token`` only auth_type)
    :type token_auto_renew: bool
    :param username: Username for Authentication (for ``ldap`` and ``userpass`` auth_type).
    :type username: str
    :param password: Password for Authentication (for ``ldap`` and ``userpass`` auth_type).
    :type password: str
    :param secret_id: Secret ID for Authentication (for ``approle`` auth_type).
    :type secret_id: str
    :param role_id: Role ID for Authentication (for ``approle`` auth_type).
    :type role_id: str
    :param kubernetes_role: Role for Authentication (for ``kubernetes`` auth_type).
    :type kubernetes_role: str
    :param kubernetes_jwt_path: Path for kubernetes jwt token (for ``kubernetes`` auth_type, default:
        ``/var/run/secrets/kubernetes.io/serviceaccount/token``).
    :type kubernetes_jwt_path: str
    :param gcp_key_path: Path to GCP Credential JSON file (for ``gcp`` auth_type).
    :type gcp_key_path: str
    :param gcp_scopes: Comma-separated string containing GCP scopes (for ``gcp`` auth_type).
    :type gcp_scopes: str
    """
    def __init__(  # pylint: disable=too-many-arguments
        self,
        connections_path: str = 'connections',
        variables_path: str = 'variables',
        url: Optional[str] = None,
        auth_type: str = 'token',
        mount_point: str = 'secret',
        kv_engine_version: int = 2,
        token: Optional[str] = None,
        token_auto_renew: Optional[bool] = False,
        username: Optional[str] = None,
        password: Optional[str] = None,
        secret_id: Optional[str] = None,
        role_id: Optional[str] = None,
        kubernetes_role: Optional[str] = None,
        kubernetes_jwt_path: str = '/var/run/secrets/kubernetes.io/serviceaccount/token',
        gcp_key_path: Optional[str] = None,
        gcp_scopes: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.connections_path = connections_path.rstrip('/')
        self.variables_path = variables_path.rstrip('/')
        self.mount_point = mount_point
        self.kv_engine_version = kv_engine_version
        self.token_auto_renew = token_auto_renew
        self.vault_client = _VaultClient(
            url=url,
            auth_type=auth_type,
            mount_point=mount_point,
            kv_engine_version=kv_engine_version,
            token=token,
            username=username,
            password=password,
            secret_id=secret_id,
            role_id=role_id,
            kubernetes_role=kubernetes_role,
            kubernetes_jwt_path=kubernetes_jwt_path,
            gcp_key_path=gcp_key_path,
            gcp_scopes=gcp_scopes,
            **kwargs
        )

    def get_conn_uri(self, conn_id: str) -> Optional[str]:
        """
        Get secret value from Vault. Store the secret in the form of URI

        :param conn_id: The connection id
        :type conn_id: str
        :rtype: str
        :return: The connection uri retrieved from the secret
        """
        secret_path = self.build_path(self.connections_path, conn_id)
        self._renew_token()
        response = self.vault_client.get_secret(secret_path=secret_path)
        return response.get("conn_uri") if response else None

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get Airflow Variable from Environment Variable

        :param key: Variable Key
        :type key: str
        :rtype: str
        :return: Variable Value retrieved from the vault
        """
        secret_path = self.build_path(self.variables_path, key)
        self._renew_token()
        response = self.vault_client.get_secret(secret_path=secret_path)
        return response.get("value") if response else None

    def _renew_token(self) -> None:
        if (self.token_auto_renew):
            self.vault_client.client.renew_token()
