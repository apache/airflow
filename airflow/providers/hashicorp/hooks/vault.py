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

"""Hook for HashiCorp Vault"""
from typing import Optional, Tuple

import hvac
from hvac.exceptions import VaultError
from requests import Response

from airflow.hooks.base_hook import BaseHook
# noinspection PyProtectedMember
from airflow.providers.hashicorp._internal_client.vault_client import (  # noqa
    DEFAULT_KUBERNETES_JWT_PATH, DEFAULT_KV_ENGINE_VERSION, _VaultClient,
)


class VaultHook(BaseHook):
    """
    Hook to Interact with HashiCorp Vault KeyValue Secret engine.

    HashiCorp hvac documentation:
       * https://hvac.readthedocs.io/en/stable/

    You connect to the host specified as host in the connection. The login/password from the connection
    are used as credentials usually and you can specify different authentication parameters
    via init params or via corresponding extras in the connection.

    The extras in the connection are named the same as the parameters (`mount_point`,'kv_engine_version' ...).

    Login/Password are used as credentials:

        * approle: password -> secret_id
        * ldap: login -> username,   password -> password
        * userpass: login -> username, password -> password

    :param vault_conn_id: The id of the connection to use
    :type vault_conn_id: str
    :param auth_type: Authentication Type for Vault. Default is ``token``. Available values are in
        :py:const:`airflow.providers.hashicorp.hooks.vault.VALID_AUTH_TYPES`.
    :type auth_type: str
    :param mount_point: The "path" the secret engine was mounted on. Default is "secret". Note that
         this ``mount_point`` is not used for authentication if authentication is done via a
         different engine.
    :type mount_point: str
    :param kv_engine_version: Select the version of the engine to run (``1`` or ``2``). Defaults to
          version defined in connection or ``2`` if not defined in connection.
    :type kv_engine_version: int
    :param token: Authentication token to include in requests sent to Vault.
        (for ``token`` and ``github`` auth_type)
    :type token: str
    :param role_id: Role ID for Authentication (for ``approle`` auth_type)
    :type role_id: str
    :param kubernetes_role: Role for Authentication (for ``kubernetes`` auth_type)
    :type kubernetes_role: str
    :param kubernetes_jwt_path: Path for kubernetes jwt token (for ``kubernetes`` auth_type, default:
        ``/var/run/secrets/kubernetes.io/serviceaccount/token``)
    :type kubernetes_jwt_path: str
    :param gcp_key_path: Path to GCP Credential JSON file (for ``gcp`` auth_type)
    :type gcp_key_path: str
    :param gcp_scopes: Comma-separated string containing GCP scopes (for ``gcp`` auth_type)
    :type gcp_scopes: str

    """
    def __init__(  # pylint: disable=too-many-arguments
        self,
        vault_conn_id: str,
        auth_type: Optional[str] = None,
        mount_point: Optional[str] = None,
        kv_engine_version: Optional[int] = None,
        token: Optional[str] = None,
        role_id: Optional[str] = None,
        kubernetes_role: Optional[str] = None,
        kubernetes_jwt_path: Optional[str] = None,
        gcp_key_path: Optional[str] = None,
        gcp_scopes: Optional[str] = None,
    ):
        super().__init__()
        self.connection = self.get_connection(vault_conn_id)

        if not auth_type:
            auth_type = self.connection.extra_dejson.get('auth_type') or "token"

        if not mount_point:
            mount_point = self.connection.extra_dejson.get('mount_point')
        if not mount_point:
            mount_point = 'secret'

        if not kv_engine_version:
            conn_version = self.connection.extra_dejson.get("kv_engine_version")
            try:
                kv_engine_version = int(conn_version) if conn_version else DEFAULT_KV_ENGINE_VERSION
            except ValueError:
                raise VaultError(f"The version is not an int: {conn_version}. ")

        if auth_type in ["approle"]:
            if not role_id:
                role_id = self.connection.extra_dejson.get('role_id')

        if auth_type in ['github', "token"]:
            if not token:
                token = self.connection.extra_dejson.get('token')
        gcp_key_path, gcp_scopes = \
            self._get_gcp_parameters_from_connection(gcp_key_path, gcp_scopes) \
            if auth_type == 'gcp' else (None, None)
        kubernetes_jwt_path, kubernetes_role = \
            self._get_kubernetes_parameters_from_connection(kubernetes_jwt_path, kubernetes_role) \
            if auth_type == 'kubernetes' else (None, None)

        url = f"{self.connection.schema}://{self.connection.host}"
        if self.connection.port:
            url += f":{self.connection.port}"

        self.vault_client = _VaultClient(
            url=url,
            auth_type=auth_type,
            mount_point=mount_point,
            kv_engine_version=kv_engine_version,
            token=token,
            username=self.connection.login,
            password=self.connection.password,
            secret_id=self.connection.password,
            role_id=role_id,
            kubernetes_role=kubernetes_role,
            kubernetes_jwt_path=kubernetes_jwt_path,
            gcp_key_path=gcp_key_path,
            gcp_scopes=gcp_scopes,
        )

    def _get_kubernetes_parameters_from_connection(
        self, kubernetes_jwt_path: Optional[str], kubernetes_role: Optional[str]) \
            -> Tuple[str, Optional[str]]:
        if not kubernetes_jwt_path:
            kubernetes_jwt_path = self.connection.extra_dejson.get("kubernetes_jwt_path")
            if not kubernetes_jwt_path:
                kubernetes_jwt_path = DEFAULT_KUBERNETES_JWT_PATH
        if not kubernetes_role:
            kubernetes_role = self.connection.extra_dejson.get("kubernetes_role")
        return kubernetes_jwt_path, kubernetes_role

    def _get_gcp_parameters_from_connection(
        self,
        gcp_key_path: Optional[str],
        gcp_scopes: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        if not gcp_scopes:
            gcp_scopes = self.connection.extra_dejson.get("gcp_scopes")
        if not gcp_key_path:
            gcp_key_path = self.connection.extra_dejson.get("gcp_key_path")
        return gcp_key_path, gcp_scopes

    def get_conn(self) -> hvac.Client:
        """
        Retrieves connection to Vault.

        :rtype: hvac.Client
        :return: connection used.
        """
        return self.vault_client.client

    def get_secret(self, secret_path: str, secret_version: Optional[int] = None) -> Optional[dict]:
        """
        Get secret value from the engine.

        :param secret_path: Path of the secret
        :type secret_path: str
        :param secret_version: Optional version of key to read - can only be used in case of version 2 of KV
        :type secret_version: int

        See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v1.html
        and https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        :param secret_path: Path of the secret
        :type secret_path: str
        :rtype: dict
        :return: secret stored in the vault as a dictionary
        """
        return self.vault_client.get_secret(secret_path=secret_path, secret_version=secret_version)

    def get_secret_metadata(self, secret_path: str) -> Optional[dict]:
        """
        Reads secret metadata (including versions) from the engine. It is only valid for KV version 2.

        :param secret_path: Path to read from
        :type secret_path: str
        :rtype: dict
        :return: secret metadata. This is a Dict containing metadata for the secret.

        See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        """
        return self.vault_client.get_secret_metadata(secret_path=secret_path)

    def get_secret_including_metadata(self,
                                      secret_path: str,
                                      secret_version: Optional[int] = None) -> Optional[dict]:
        """
        Reads secret including metadata. It is only valid for KV version 2.

        See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        :param secret_path: Path of the secret
        :type secret_path: str
        :param secret_version: Optional version of key to read - can only be used in case of version 2 of KV
        :type secret_version: int
        :rtype: dict
        :return: key info. This is a Dict with "data" mapping keeping secret
            and "metadata" mapping keeping metadata of the secret.

        """
        return self.vault_client.get_secret_including_metadata(
            secret_path=secret_path, secret_version=secret_version)

    def create_or_update_secret(self,
                                secret_path: str,
                                secret: dict,
                                method: Optional[str] = None,
                                cas: Optional[int] = None) -> Response:
        """
        Creates or updates secret.

        :param secret_path: Path to read from
        :type secret_path: str
        :param secret: Secret to create or update for the path specified
        :type secret: dict
        :param method: Optional parameter to explicitly request a POST (create) or PUT (update) request to
            the selected kv secret engine. If no argument is provided for this parameter, hvac attempts to
            intelligently determine which method is appropriate. Only valid for KV engine version 1
        :type method: str
        :param cas: Set the "cas" value to use a Check-And-Set operation. If not set the write will be
            allowed. If set to 0 a write will only be allowed if the key doesn't exist.
            If the index is non-zero the write will only be allowed if the key's current version
            matches the version specified in the cas parameter. Only valid for KV engine version 2.
        :type cas: int
        :rtype: requests.Response
        :return: The response of the create_or_update_secret request.

        See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v1.html
        and https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        """
        return self.vault_client.create_or_update_secret(
            secret_path=secret_path,
            secret=secret,
            method=method,
            cas=cas)
