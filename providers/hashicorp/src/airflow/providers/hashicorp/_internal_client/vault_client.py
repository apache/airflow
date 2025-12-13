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
from __future__ import annotations

import os
from functools import cached_property

import hvac
from hvac.api.auth_methods import Kubernetes
from hvac.exceptions import InvalidPath, VaultError
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from airflow.utils.log.logging_mixin import LoggingMixin

DEFAULT_KUBERNETES_JWT_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
DEFAULT_KV_ENGINE_VERSION = 2


VALID_KV_VERSIONS: list[int] = [1, 2]
VALID_AUTH_TYPES: list[str] = [
    "approle",
    "aws_iam",
    "azure",
    "github",
    "gcp",
    "kubernetes",
    "ldap",
    "radius",
    "token",
    "userpass",
]


class _VaultClient(LoggingMixin):
    """
    Retrieves Authenticated client from Hashicorp Vault.

    This is purely internal class promoting authentication code reuse between the Hook and the
    SecretBackend, it should not be used directly in Airflow DAGs. Use VaultBackend for backend
    integration and Hook in case you want to communicate with VaultHook using standard Airflow
    Connection definition.

    :param url: Base URL for the Vault instance being addressed.
    :param auth_type: Authentication Type for Vault. Default is ``token``. Available values are in
        ('approle', 'aws_iam', 'azure', 'github', 'gcp', 'kubernetes', 'ldap', 'radius', 'token', 'userpass')
    :param auth_mount_point: It can be used to define mount_point for authentication chosen
          Default depends on the authentication method used.
    :param mount_point: The "path" the secret engine was mounted on. Default is "secret". Note that
         this mount_point is not used for authentication if authentication is done via a
         different engine. For authentication mount_points see, auth_mount_point.
    :param kv_engine_version: Selects the version of the engine to run (``1`` or ``2``, default: ``2``).
    :param token: Authentication token to include in requests sent to Vault
        (for ``token`` and ``github`` auth_type).
    :param token_path: path to file containing authentication token to include in requests sent to Vault
        (for ``token`` and ``github`` auth_type).
    :param username: Username for Authentication (for ``ldap`` and ``userpass`` auth_types).
    :param password: Password for Authentication (for ``ldap`` and ``userpass`` auth_types).
    :param key_id: Key ID for Authentication (for ``aws_iam`` and ''azure`` auth_type).
    :param secret_id: Secret ID for Authentication (for ``approle``, ``aws_iam`` and ``azure`` auth_types).
    :param role_id: Role ID for Authentication (for ``approle``, ``aws_iam`` auth_types).
    :param assume_role_kwargs: AWS assume role param.
        See AWS STS Docs:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts/client/assume_role.html
    :param region: AWS region for STS API calls. Inferred from the boto3 client configuration if not provided
        (for ``aws_iam`` auth_type).
    :param kubernetes_role: Role for Authentication (for ``kubernetes`` auth_type).
    :param kubernetes_jwt_path: Path for kubernetes jwt token (for ``kubernetes`` auth_type, default:
        ``/var/run/secrets/kubernetes.io/serviceaccount/token``).
    :param gcp_key_path: Path to Google Cloud Service Account key file (JSON)  (for ``gcp`` auth_type).
           Mutually exclusive with gcp_keyfile_dict
    :param gcp_keyfile_dict: Dictionary of keyfile parameters. (for ``gcp`` auth_type).
           Mutually exclusive with gcp_key_path
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
        url: str | None = None,
        auth_type: str = "token",
        auth_mount_point: str | None = None,
        mount_point: str | None = "secret",
        kv_engine_version: int | None = None,
        token: str | None = None,
        token_path: str | None = None,
        username: str | None = None,
        password: str | None = None,
        key_id: str | None = None,
        secret_id: str | None = None,
        assume_role_kwargs: dict | None = None,
        role_id: str | None = None,
        region: str | None = None,
        kubernetes_role: str | None = None,
        kubernetes_jwt_path: str | None = "/var/run/secrets/kubernetes.io/serviceaccount/token",
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
        if kv_engine_version and kv_engine_version not in VALID_KV_VERSIONS:
            raise VaultError(
                f"The version is not supported: {kv_engine_version}. It should be one of {VALID_KV_VERSIONS}"
            )
        if auth_type not in VALID_AUTH_TYPES:
            raise VaultError(
                f"The auth_type is not supported: {auth_type}. It should be one of {VALID_AUTH_TYPES}"
            )
        if auth_type == "token" and not token and not token_path and "VAULT_TOKEN" not in os.environ:
            raise VaultError("The 'token' authentication type requires 'token' or 'token_path'")
        if auth_type == "github" and not token and not token_path:
            raise VaultError("The 'github' authentication type requires 'token' or 'token_path'")
        if auth_type == "approle" and not role_id:
            raise VaultError("The 'approle' authentication type requires 'role_id'")
        if auth_type == "kubernetes":
            if not kubernetes_role:
                raise VaultError("The 'kubernetes' authentication type requires 'kubernetes_role'")
            if not kubernetes_jwt_path:
                raise VaultError("The 'kubernetes' authentication type requires 'kubernetes_jwt_path'")
        if auth_type == "azure":
            if not azure_resource:
                raise VaultError("The 'azure' authentication type requires 'azure_resource'")
            if not azure_tenant_id:
                raise VaultError("The 'azure' authentication type requires 'azure_tenant_id'")
        if auth_type == "radius":
            if not radius_host:
                raise VaultError("The 'radius' authentication type requires 'radius_host'")
            if not radius_secret:
                raise VaultError("The 'radius' authentication type requires 'radius_secret'")
        if auth_type == "gcp":
            if not gcp_scopes:
                raise VaultError("The 'gcp' authentication type requires 'gcp_scopes'")
            if not role_id:
                raise VaultError("The 'gcp' authentication type requires 'role_id'")
            if not gcp_key_path and not gcp_keyfile_dict:
                raise VaultError(
                    "The 'gcp' authentication type requires 'gcp_key_path' or 'gcp_keyfile_dict'"
                )

        self.kv_engine_version = kv_engine_version or 2
        self.url = url
        self.auth_type = auth_type
        self.kwargs = kwargs
        self.token = token or os.getenv("VAULT_TOKEN", None)
        self.token_path = token_path
        self.auth_mount_point = auth_mount_point
        self.mount_point = mount_point
        self.username = username
        self.password = password
        self.key_id = key_id
        self.secret_id = secret_id
        self.role_id = role_id
        self.assume_role_kwargs = assume_role_kwargs
        self.region = region
        self.kubernetes_role = kubernetes_role
        self.kubernetes_jwt_path = kubernetes_jwt_path
        self.gcp_key_path = gcp_key_path
        self.gcp_keyfile_dict = gcp_keyfile_dict
        self.gcp_scopes = gcp_scopes
        self.azure_tenant_id = azure_tenant_id
        self.azure_resource = azure_resource
        self.radius_host = radius_host
        self.radius_secret = radius_secret
        self.radius_port = radius_port

    @property
    def client(self):
        """
        Checks that it is still authenticated to Vault and invalidates the cache if this is not the case.

        :return: Vault Client
        """
        if not self._client.is_authenticated():
            # Invalidate the cache:
            # https://github.com/pydanny/cached-property#invalidating-the-cache
            self.__dict__.pop("_client", None)
        return self._client

    @cached_property
    def _client(self) -> hvac.Client:
        """
        Return an authenticated Hashicorp Vault client.

        :return: Vault Client

        """
        if "session" not in self.kwargs:
            # If no session object provide one with retry as per hvac documentation:
            # https://hvac.readthedocs.io/en/stable/advanced_usage.html#retrying-failed-requests
            adapter = HTTPAdapter(
                max_retries=Retry(
                    total=3,
                    backoff_factor=0.1,
                    status_forcelist=[412, 500, 502, 503],
                    raise_on_status=False,
                )
            )
            session = Session()
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            session.verify = self.kwargs.get("verify", session.verify)
            session.cert = self.kwargs.get("cert", session.cert)
            session.proxies = self.kwargs.get("proxies", session.proxies)
            self.kwargs["session"] = session

        _client = hvac.Client(url=self.url, **self.kwargs)
        if self.auth_type == "approle":
            self._auth_approle(_client)
        elif self.auth_type == "aws_iam":
            self._auth_aws_iam(_client)
        elif self.auth_type == "azure":
            self._auth_azure(_client)
        elif self.auth_type == "gcp":
            self._auth_gcp(_client)
        elif self.auth_type == "github":
            self._auth_github(_client)
        elif self.auth_type == "kubernetes":
            self._auth_kubernetes(_client)
        elif self.auth_type == "ldap":
            self._auth_ldap(_client)
        elif self.auth_type == "radius":
            self._auth_radius(_client)
        elif self.auth_type == "token":
            self._set_token(_client)
        elif self.auth_type == "userpass":
            self._auth_userpass(_client)
        else:
            raise VaultError(f"Authentication type '{self.auth_type}' not supported")

        if _client.is_authenticated():
            return _client
        raise VaultError("Vault Authentication Error!")

    def _auth_userpass(self, _client: hvac.Client) -> None:
        if self.auth_mount_point:
            _client.auth.userpass.login(
                username=self.username, password=self.password, mount_point=self.auth_mount_point
            )
        else:
            _client.auth.userpass.login(username=self.username, password=self.password)

    def _auth_radius(self, _client: hvac.Client) -> None:
        if self.auth_mount_point:
            _client.auth.radius.configure(
                host=self.radius_host,
                secret=self.radius_secret,
                port=self.radius_port,
                mount_point=self.auth_mount_point,
            )
        else:
            _client.auth.radius.configure(
                host=self.radius_host, secret=self.radius_secret, port=self.radius_port
            )

    def _auth_ldap(self, _client: hvac.Client) -> None:
        if self.auth_mount_point:
            _client.auth.ldap.login(
                username=self.username, password=self.password, mount_point=self.auth_mount_point
            )
        else:
            _client.auth.ldap.login(username=self.username, password=self.password)

    def _auth_kubernetes(self, _client: hvac.Client) -> None:
        if not self.kubernetes_jwt_path:
            raise VaultError("The kubernetes_jwt_path should be set here. This should not happen.")
        with open(self.kubernetes_jwt_path) as f:
            jwt = f.read().strip()
            if self.auth_mount_point:
                Kubernetes(_client.adapter).login(
                    role=self.kubernetes_role, jwt=jwt, mount_point=self.auth_mount_point
                )
            else:
                Kubernetes(_client.adapter).login(role=self.kubernetes_role, jwt=jwt)

    def _auth_github(self, _client: hvac.Client) -> None:
        if self.auth_mount_point:
            _client.auth.github.login(token=self.token, mount_point=self.auth_mount_point)
        else:
            _client.auth.github.login(token=self.token)

    def _auth_gcp(self, _client: hvac.Client) -> None:
        from airflow.providers.google.cloud.utils.credentials_provider import (
            _get_scopes,
            get_credentials_and_project_id,
        )

        scopes = _get_scopes(self.gcp_scopes)
        credentials, project_id = get_credentials_and_project_id(
            key_path=self.gcp_key_path, keyfile_dict=self.gcp_keyfile_dict, scopes=scopes
        )

        import json
        import time

        import googleapiclient

        if self.gcp_keyfile_dict:
            creds = self.gcp_keyfile_dict
        elif self.gcp_key_path:
            with open(self.gcp_key_path) as f:
                creds = json.load(f)

        service_account = creds["client_email"]

        # Generate a payload for subsequent "signJwt()" call
        # Reference: https://googleapis.dev/python/google-auth/latest/reference/google.auth.jwt.html#google.auth.jwt.Credentials
        now = int(time.time())
        expires = now + 900  # 15 mins in seconds, can't be longer.
        payload = {"iat": now, "exp": expires, "sub": credentials, "aud": f"vault/{self.role_id}"}
        body = {"payload": json.dumps(payload)}
        name = f"projects/{project_id}/serviceAccounts/{service_account}"

        # Perform the GCP API call
        iam = googleapiclient.discovery.build("iam", "v1", credentials=credentials)
        request = iam.projects().serviceAccounts().signJwt(name=name, body=body)
        resp = request.execute()
        jwt = resp["signedJwt"]

        if self.auth_mount_point:
            _client.auth.gcp.login(role=self.role_id, jwt=jwt, mount_point=self.auth_mount_point)
        else:
            _client.auth.gcp.login(role=self.role_id, jwt=jwt)

    def _auth_azure(self, _client: hvac.Client) -> None:
        if self.auth_mount_point:
            _client.auth.azure.configure(
                tenant_id=self.azure_tenant_id,
                resource=self.azure_resource,
                client_id=self.key_id,
                client_secret=self.secret_id,
                mount_point=self.auth_mount_point,
            )
        else:
            _client.auth.azure.configure(
                tenant_id=self.azure_tenant_id,
                resource=self.azure_resource,
                client_id=self.key_id,
                client_secret=self.secret_id,
            )

    def _auth_aws_iam(self, _client: hvac.Client) -> None:
        if self.key_id and self.secret_id:
            auth_args = {
                "access_key": self.key_id,
                "secret_key": self.secret_id,
            }
        else:
            import boto3

            if self.assume_role_kwargs:
                sts_client = boto3.client("sts")
                credentials = sts_client.assume_role(**self.assume_role_kwargs)
                auth_args = {
                    "access_key": credentials["Credentials"]["AccessKeyId"],
                    "secret_key": credentials["Credentials"]["SecretAccessKey"],
                    "session_token": credentials["Credentials"]["SessionToken"],
                    "region": sts_client.meta.region_name,
                }
            else:
                session = boto3.Session()
                credentials = session.get_credentials()
                auth_args = {
                    "access_key": credentials.access_key,
                    "secret_key": credentials.secret_key,
                    "session_token": credentials.token,
                    "region": session.region_name,
                }

        if self.auth_mount_point:
            auth_args["mount_point"] = self.auth_mount_point
        if self.region:
            auth_args["region"] = self.region
        if self.role_id:
            auth_args["role"] = self.role_id

        _client.auth.aws.iam_login(**auth_args)

    def _auth_approle(self, _client: hvac.Client) -> None:
        if self.auth_mount_point:
            _client.auth.approle.login(
                role_id=self.role_id, secret_id=self.secret_id, mount_point=self.auth_mount_point
            )
        else:
            _client.auth.approle.login(role_id=self.role_id, secret_id=self.secret_id)

    def _set_token(self, _client: hvac.Client) -> None:
        if self.token_path:
            with open(self.token_path) as f:
                _client.token = f.read().strip()
        else:
            _client.token = self.token

    def _parse_secret_path(self, secret_path: str) -> tuple[str, str]:
        if not self.mount_point:
            split_secret_path = secret_path.split("/", 1)
            if len(split_secret_path) < 2:
                raise InvalidPath(
                    "The variable path you have provided is invalid. Please provide a full path: path/to/secret/variable"
                    )
            return split_secret_path[0], split_secret_path[1]
        return self.mount_point, secret_path

    def get_secret(self, secret_path: str, secret_version: int | None = None) -> dict | None:
        """
        Get secret value from the KV engine.

        :param secret_path: The path of the secret.
        :param secret_version: Specifies the version of Secret to return. If not set, the latest
            version is returned. (Can only be used in case of version 2 of KV).

        See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v1.html
        and https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        :return: secret stored in the vault as a dictionary
        """
        mount_point = None
        try:
            mount_point, secret_path = self._parse_secret_path(secret_path)
            if self.kv_engine_version == 1:
                if secret_version:
                    raise VaultError("Secret version can only be used with version 2 of the KV engine")
                response = self.client.secrets.kv.v1.read_secret(path=secret_path, mount_point=mount_point)
            else:
                response = self.client.secrets.kv.v2.read_secret_version(
                    path=secret_path,
                    mount_point=mount_point,
                    version=secret_version,
                    raise_on_deleted_version=True,
                )
        except InvalidPath:
            self.log.debug("Secret not found %s with mount point %s", secret_path, mount_point)
            return None

        return_data = response["data"] if self.kv_engine_version == 1 else response["data"]["data"]
        return return_data

    def get_secret_metadata(self, secret_path: str) -> dict | None:
        """
        Read secret metadata (including versions) from the engine. It is only valid for KV version 2.

        :param secret_path: The path of the secret.
        :return: secret metadata. This is a Dict containing metadata for the secret.

                 See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        """
        if self.kv_engine_version == 1:
            raise VaultError("Metadata might only be used with version 2 of the KV engine.")
        mount_point = None
        try:
            mount_point, secret_path = self._parse_secret_path(secret_path)
            return self.client.secrets.kv.v2.read_secret_metadata(path=secret_path, mount_point=mount_point)
        except InvalidPath:
            self.log.debug("Secret not found %s with mount point %s", secret_path, mount_point)
            return None

    def get_secret_including_metadata(
        self, secret_path: str, secret_version: int | None = None
    ) -> dict | None:
        """
        Read secret including metadata. It is only valid for KV version 2.

        See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        :param secret_path: The path of the secret.
        :param secret_version: Specifies the version of Secret to return. If not set, the latest
            version is returned. (Can only be used in case of version 2 of KV).
        :return: The key info. This is a Dict with "data" mapping keeping secret
                 and "metadata" mapping keeping metadata of the secret.
        """
        if self.kv_engine_version == 1:
            raise VaultError("Metadata might only be used with version 2 of the KV engine.")
        mount_point = None
        try:
            mount_point, secret_path = self._parse_secret_path(secret_path)
            return self.client.secrets.kv.v2.read_secret_version(
                path=secret_path,
                mount_point=mount_point,
                version=secret_version,
                raise_on_deleted_version=True,
            )
        except InvalidPath:
            self.log.debug(
                "Secret not found %s with mount point %s and version %s",
                secret_path,
                mount_point,
                secret_version,
            )
            return None

    def create_or_update_secret(
        self, secret_path: str, secret: dict, method: str | None = None, cas: int | None = None
    ) -> Response:
        """
        Create or updates secret.

        :param secret_path: The path of the secret.
        :param secret: Secret to create or update for the path specified
        :param method: Optional parameter to explicitly request a POST (create) or PUT (update) request to
            the selected kv secret engine. If no argument is provided for this parameter, hvac attempts to
            intelligently determine which method is appropriate. Only valid for KV engine version 1
        :param cas: Set the "cas" value to use a Check-And-Set operation. If not set the write will be
            allowed. If set to 0 a write will only be allowed if the key doesn't exist.
            If the index is non-zero the write will only be allowed if the key's current version
            matches the version specified in the cas parameter. Only valid for KV engine version 2.
        :return: The response of the create_or_update_secret request.

                 See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v1.html
                 and https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        """
        if self.kv_engine_version == 2 and method:
            raise VaultError("The method parameter is only valid for version 1")
        if self.kv_engine_version == 1 and cas:
            raise VaultError("The cas parameter is only valid for version 2")
        mount_point, secret_path = self._parse_secret_path(secret_path)
        if self.kv_engine_version == 1:
            response = self.client.secrets.kv.v1.create_or_update_secret(
                path=secret_path, secret=secret, mount_point=mount_point, method=method
            )
        else:
            response = self.client.secrets.kv.v2.create_or_update_secret(
                path=secret_path, secret=secret, mount_point=mount_point, cas=cas
            )
        return response
