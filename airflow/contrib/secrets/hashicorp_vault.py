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

import hvac
from cached_property import cached_property
from hvac.exceptions import InvalidPath, VaultError

from airflow.exceptions import AirflowException
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class VaultBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connections and Variables from Hashicorp Vault

    Configurable via ``airflow.cfg`` as follows:

    .. code-block:: ini

        [secrets]
        backend = airflow.contrib.secrets.hashicorp_vault.VaultBackend
        backend_kwargs = {
            "connections_path": "connections",
            "url": "http://127.0.0.1:8200",
            "mount_point": "airflow"
            }

    For example, if your keys are under ``connections`` path in ``airflow`` mount_point, this
    would be accessible if you provide ``{"connections_path": "connections"}`` and request
    conn_id ``smtp_default``.

    :param connections_path: Specifies the path of the secret to read to get Connections.
        (default: 'connections')
    :type connections_path: str
    :param variables_path: Specifies the path of the secret to read to get Variables.
        (default: 'variables')
    :type variables_path: str
    :param config_path: Specifies the path of the secret to read Airflow Configurations
        (default: 'configs').
    :type config_path: str
    :param url: Base URL for the Vault instance being addressed.
    :type url: str
    :param auth_type: Authentication Type for Vault (one of 'token', 'ldap', 'userpass', 'approle',
        'github', 'gcp', 'kubernetes'). Default is ``token``.
    :type auth_type: str
    :param mount_point: The "path" the secret engine was mounted on. (Default: ``secret``)
    :type mount_point: str
    :param token: Authentication token to include in requests sent to Vault.
        (for ``token`` and ``github`` auth_type)
    :type token: str
    :param kv_engine_version: Select the version of the engine to run (``1`` or ``2``, default: ``2``)
    :type kv_engine_version: int
    :param username: Username for Authentication (for ``ldap`` and ``userpass`` auth_type)
    :type username: str
    :param password: Password for Authentication (for ``ldap`` and ``userpass`` auth_type)
    :type password: str
    :param role_id: Role ID for Authentication (for ``approle`` auth_type)
    :type role_id: str
    :param kubernetes_role: Role for Authentication (for ``kubernetes`` auth_type)
    :type kubernetes_role: str
    :param kubernetes_jwt_path: Path for kubernetes jwt token (for ``kubernetes`` auth_type, deafult:
        ``/var/run/secrets/kubernetes.io/serviceaccount/token``)
    :type kubernetes_jwt_path: str
    :param secret_id: Secret ID for Authentication (for ``approle`` auth_type)
    :type secret_id: str
    :param gcp_key_path: Path to GCP Credential JSON file (for ``gcp`` auth_type)
    :type gcp_key_path: str
    :param gcp_scopes: Comma-separated string containing GCP scopes (for ``gcp`` auth_type)
    :type gcp_scopes: str
    """
    def __init__(  # pylint: disable=too-many-arguments
        self,
        connections_path='connections',  # type: str
        variables_path='variables',  # type: str
        config_path='config',  # type: str
        url=None,  # type: Optional[str]
        auth_type='token',  # type: str
        mount_point='secret',  # type: str
        kv_engine_version=2,  # type: int
        token=None,  # type: Optional[str]
        username=None,  # type: Optional[str]
        password=None,  # type: Optional[str]
        role_id=None,  # type: Optional[str]
        kubernetes_role=None,  # type: Optional[str]
        kubernetes_jwt_path='/var/run/secrets/kubernetes.io/serviceaccount/token',  # type: str
        secret_id=None,  # type: Optional[str]
        gcp_key_path=None,  # type: Optional[str]
        gcp_scopes=None,  # type: Optional[str]
        **kwargs
    ):
        super(VaultBackend, self).__init__()
        self.connections_path = connections_path.rstrip('/')
        self.variables_path = variables_path.rstrip('/')
        self.config_path = config_path.rstrip('/')
        self.url = url
        self.auth_type = auth_type
        self.kwargs = kwargs
        self.token = token
        self.username = username
        self.password = password
        self.role_id = role_id
        self.kubernetes_role = kubernetes_role
        self.kubernetes_jwt_path = kubernetes_jwt_path
        self.secret_id = secret_id
        self.mount_point = mount_point
        self.kv_engine_version = kv_engine_version
        self.gcp_key_path = gcp_key_path
        self.gcp_scopes = gcp_scopes

    @cached_property
    def client(self):
        # type: () -> hvac.Client
        """
        Return an authenticated Hashicorp Vault client
        """

        _client = hvac.Client(url=self.url, **self.kwargs)
        if self.auth_type == "token":
            if not self.token:
                raise VaultError("token cannot be None for auth_type='token'")
            _client.token = self.token
        elif self.auth_type == "ldap":
            _client.auth.ldap.login(
                username=self.username, password=self.password)
        elif self.auth_type == "userpass":
            _client.auth_userpass(username=self.username, password=self.password)
        elif self.auth_type == "approle":
            _client.auth_approle(role_id=self.role_id, secret_id=self.secret_id)
        elif self.auth_type == "kubernetes":
            if not self.kubernetes_role:
                raise VaultError("kubernetes_role cannot be None for auth_type='kubernetes'")
            with open(self.kubernetes_jwt_path) as f:
                jwt = f.read()
                _client.auth_kubernetes(role=self.kubernetes_role, jwt=jwt)
        elif self.auth_type == "github":
            _client.auth.github.login(token=self.token)
        elif self.auth_type == "gcp":
            from airflow.contrib.utils.gcp_credentials_provider import (
                get_credentials_and_project_id,
                _get_scopes
            )
            scopes = _get_scopes(self.gcp_scopes)
            credentials, _ = get_credentials_and_project_id(key_path=self.gcp_key_path, scopes=scopes)
            _client.auth.gcp.configure(credentials=credentials)
        else:
            raise AirflowException("Authentication type '{}' not supported".format(self.auth_type))

        if _client.is_authenticated():
            return _client
        else:
            raise VaultError("Vault Authentication Error!")

    def get_conn_uri(self, conn_id):
        # type: (str) -> Optional[str]
        """
        Get secret value from Vault. Store the secret in the form of URI

        :param conn_id: connection id
        :type conn_id: str
        """
        response = self._get_secret(self.connections_path, conn_id)
        return response.get("conn_uri") if response else None

    def get_variable(self, key):
        # type: (str) -> Optional[str]
        """
        Get Airflow Variable

        :param key: Variable Key
        :return: Variable Value
        """
        response = self._get_secret(self.variables_path, key)
        return response.get("value") if response else None

    def _get_secret(self, path_prefix, secret_id):
        # type: (str, str) -> Optional[dict]
        """
        Get secret value from Vault.

        :param path_prefix: Prefix for the Path to get Secret
        :type path_prefix: str
        :param secret_id: Secret Key
        :type secret_id: str
        """
        secret_path = self.build_path(path_prefix, secret_id)

        try:
            if self.kv_engine_version == 1:
                response = self.client.secrets.kv.v1.read_secret(
                    path=secret_path, mount_point=self.mount_point
                )
            else:
                response = self.client.secrets.kv.v2.read_secret_version(
                    path=secret_path, mount_point=self.mount_point)
        except InvalidPath:
            self.log.info("Secret %s not found in Path: %s", secret_id, secret_path)
            return None

        return_data = response["data"] if self.kv_engine_version == 1 else response["data"]["data"]
        return return_data

    def get_config(self, key):
        # type: (str) -> Optional[str]
        """
        Get Airflow Configuration

        :param key: Configuration Option Key
        :type key: str
        :rtype: str
        :return: Configuration Option Value retrieved from the vault
        """
        response = self._get_secret(self.config_path, key)
        return response.get("value") if response else None
