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
"""Hook for HashiCorp Vault."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from hvac.exceptions import VaultError

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.hashicorp._internal_client.vault_client import (
    DEFAULT_KUBERNETES_JWT_PATH,
    DEFAULT_KV_ENGINE_VERSION,
    _VaultClient,
)
from airflow.utils.helpers import merge_dicts

if TYPE_CHECKING:
    import hvac
    from requests import Response


class VaultHook(BaseHook):
    """
    Hook to Interact with HashiCorp Vault KeyValue Secret engine.

    HashiCorp hvac documentation:
       * https://hvac.readthedocs.io/en/stable/

    You connect to the host specified as host in the connection. The login/password from the connection
    are used as credentials usually and you can specify different authentication parameters
    via init params or via corresponding extras in the connection.

    The mount point should be placed as a path in the URL - similarly to Vault's URL schema:
    This indicates the "path" the secret engine is mounted on. Default id not specified is "secret".
    Note that this ``mount_point`` is not used for authentication if authentication is done via a
    different engines. Each engine uses its own engine-specific authentication mount_point.

    The extras in the connection are named the same as the parameters ('kv_engine_version', 'auth_type', ...).

    You can also use gcp_keyfile_dict extra to pass json-formatted dict in case of 'gcp' authentication.

    The URL schemas supported are "vault", "http" (using http to connect to the vault) or
    "vaults" and "https" (using https to connect to the vault).

    Example URL:

    .. code-block::

        vault://user:password@host:port/mount_point?kv_engine_version=1&auth_type=github


    Login/Password are used as credentials:

        * approle: login -> connection.login
        * github: password -> token
        * token: password -> token
        * aws_iam: login -> key_id, password -> secret_id
        * azure: login -> client_id, password -> client_secret
        * ldap: login -> username,   password -> password
        * userpass: login -> username, password -> password
        * radius: password -> radius_secret

    :param vault_conn_id: The id of the connection to use
    :param auth_type: Authentication Type for the Vault. Default is ``token``. Available values are:
        ('approle', 'github', 'gcp', 'kubernetes', 'ldap', 'token', 'userpass')
    :param auth_mount_point: It can be used to define mount_point for authentication chosen
          Default depends on the authentication method used.
    :param kv_engine_version: Select the version of the engine to run (``1`` or ``2``). Defaults to
          version defined in connection or ``2`` if not defined in connection.
    :param role_id: Role ID for ``aws_iam`` Authentication.
    :param region: AWS region for STS API calls (for ``aws_iam`` auth_type).
    :param kubernetes_role: Role for Authentication (for ``kubernetes`` auth_type)
    :param kubernetes_jwt_path: Path for kubernetes jwt token (for ``kubernetes`` auth_type, default:
        ``/var/run/secrets/kubernetes.io/serviceaccount/token``)
    :param token_path: path to file containing authentication token to include in requests sent to Vault
        (for ``token`` and ``github`` auth_type).
    :param gcp_key_path: Path to Google Cloud Service Account key file (JSON) (for ``gcp`` auth_type)
           Mutually exclusive with gcp_keyfile_dict
    :param gcp_scopes: Comma-separated string containing OAuth2  scopes (for ``gcp`` auth_type)
    :param azure_tenant_id: The tenant id for the Azure Active Directory (for ``azure`` auth_type)
    :param azure_resource: The configured URL for the application registered in Azure Active Directory
           (for ``azure`` auth_type)
    :param radius_host: Host for radius (for ``radius`` auth_type)
    :param radius_port: Port for radius (for ``radius`` auth_type)

    """

    conn_name_attr = "vault_conn_id"
    default_conn_name = "vault_default"
    conn_type = "vault"
    hook_name = "Hashicorp Vault"

    def __init__(
        self,
        vault_conn_id: str = default_conn_name,
        auth_type: str | None = None,
        auth_mount_point: str | None = None,
        kv_engine_version: int | None = None,
        role_id: str | None = None,
        region: str | None = None,
        kubernetes_role: str | None = None,
        kubernetes_jwt_path: str | None = None,
        token_path: str | None = None,
        gcp_key_path: str | None = None,
        gcp_scopes: str | None = None,
        azure_tenant_id: str | None = None,
        azure_resource: str | None = None,
        radius_host: str | None = None,
        radius_port: int | None = None,
        **kwargs,
    ):
        super().__init__()
        self.connection = self.get_connection(vault_conn_id)

        if not auth_type:
            auth_type = self.connection.extra_dejson.get("auth_type") or "token"

        if not auth_mount_point:
            auth_mount_point = self.connection.extra_dejson.get("auth_mount_point")

        if not kv_engine_version:
            conn_version = self.connection.extra_dejson.get("kv_engine_version")
            try:
                kv_engine_version = int(conn_version) if conn_version else DEFAULT_KV_ENGINE_VERSION
            except ValueError:
                raise VaultError(f"The version is not an int: {conn_version}. ")

        client_kwargs = self.connection.extra_dejson.get("client_kwargs", {})

        if kwargs:
            client_kwargs = merge_dicts(client_kwargs, kwargs)

        if auth_type == "approle" and self.connection.login:
            role_id = self.connection.login

        if auth_type == "aws_iam":
            if not role_id:
                role_id = self.connection.extra_dejson.get("role_id")
            if not region:
                region = self.connection.extra_dejson.get("region")

        azure_resource, azure_tenant_id = (
            self._get_azure_parameters_from_connection(azure_resource, azure_tenant_id)
            if auth_type == "azure"
            else (None, None)
        )
        gcp_key_path, gcp_keyfile_dict, gcp_scopes = (
            self._get_gcp_parameters_from_connection(gcp_key_path, gcp_scopes)
            if auth_type == "gcp"
            else (None, None, None)
        )
        kubernetes_jwt_path, kubernetes_role = (
            self._get_kubernetes_parameters_from_connection(kubernetes_jwt_path, kubernetes_role)
            if auth_type == "kubernetes"
            else (None, None)
        )
        radius_host, radius_port = (
            self._get_radius_parameters_from_connection(radius_host, radius_port)
            if auth_type == "radius"
            else (None, None)
        )

        if self.connection.extra_dejson.get("use_tls") is not None:
            if bool(self.connection.extra_dejson.get("use_tls")):
                conn_protocol = "https"
            else:
                conn_protocol = "http"
        else:
            if self.connection.conn_type == "vault":
                conn_protocol = "http"
            elif self.connection.conn_type == "vaults":
                conn_protocol = "https"
            elif self.connection.conn_type == "http":
                conn_protocol = "http"
            elif self.connection.conn_type == "https":
                conn_protocol = "https"
            else:
                raise VaultError("The url schema must be one of ['http', 'https', 'vault', 'vaults' ]")

        url = f"{conn_protocol}://{self.connection.host}"
        if self.connection.port:
            url += f":{self.connection.port}"

        # Schema is really path in the Connection definition. This is pretty confusing because of URL schema
        mount_point = self.connection.schema if self.connection.schema else "secret"

        client_kwargs.update(
            url=url,
            auth_type=auth_type,
            auth_mount_point=auth_mount_point,
            mount_point=mount_point,
            kv_engine_version=kv_engine_version,
            token=self.connection.password,
            token_path=token_path,
            username=self.connection.login,
            password=self.connection.password,
            key_id=self.connection.login,
            secret_id=self.connection.password,
            role_id=role_id,
            region=region,
            kubernetes_role=kubernetes_role,
            kubernetes_jwt_path=kubernetes_jwt_path,
            gcp_key_path=gcp_key_path,
            gcp_keyfile_dict=gcp_keyfile_dict,
            gcp_scopes=gcp_scopes,
            azure_tenant_id=azure_tenant_id,
            azure_resource=azure_resource,
            radius_host=radius_host,
            radius_secret=self.connection.password,
            radius_port=radius_port,
        )

        self.vault_client = _VaultClient(**client_kwargs)

    def _get_kubernetes_parameters_from_connection(
        self, kubernetes_jwt_path: str | None, kubernetes_role: str | None
    ) -> tuple[str | None, str | None]:
        if not kubernetes_jwt_path:
            kubernetes_jwt_path = self.connection.extra_dejson.get("kubernetes_jwt_path")
            if not kubernetes_jwt_path:
                kubernetes_jwt_path = DEFAULT_KUBERNETES_JWT_PATH
        if not kubernetes_role:
            kubernetes_role = self.connection.extra_dejson.get("kubernetes_role")
        return kubernetes_jwt_path, kubernetes_role

    def _get_gcp_parameters_from_connection(
        self,
        gcp_key_path: str | None,
        gcp_scopes: str | None,
    ) -> tuple[str | None, dict | None, str | None]:
        if not gcp_scopes:
            gcp_scopes = self.connection.extra_dejson.get("gcp_scopes")
        if not gcp_key_path:
            gcp_key_path = self.connection.extra_dejson.get("gcp_key_path")
        string_keyfile_dict = self.connection.extra_dejson.get("gcp_keyfile_dict")
        gcp_keyfile_dict = json.loads(string_keyfile_dict) if string_keyfile_dict else None
        return gcp_key_path, gcp_keyfile_dict, gcp_scopes

    def _get_azure_parameters_from_connection(
        self, azure_resource: str | None, azure_tenant_id: str | None
    ) -> tuple[str | None, str | None]:
        if not azure_tenant_id:
            azure_tenant_id = self.connection.extra_dejson.get("azure_tenant_id")
        if not azure_resource:
            azure_resource = self.connection.extra_dejson.get("azure_resource")
        return azure_resource, azure_tenant_id

    def _get_radius_parameters_from_connection(
        self, radius_host: str | None, radius_port: int | None
    ) -> tuple[str | None, int | None]:
        if not radius_port:
            radius_port_str = self.connection.extra_dejson.get("radius_port")
            if radius_port_str:
                try:
                    radius_port = int(radius_port_str)
                except ValueError:
                    raise VaultError(f"Radius port was wrong: {radius_port_str}")
        if not radius_host:
            radius_host = self.connection.extra_dejson.get("radius_host")
        return radius_host, radius_port

    def get_conn(self) -> hvac.Client:
        """
        Retrieve connection to Vault.

        :return: connection used.
        """
        return self.vault_client.client

    def get_secret(self, secret_path: str, secret_version: int | None = None) -> dict | None:
        """
        Get secret value from the engine.

        :param secret_path: Path of the secret
        :param secret_version: Optional version of key to read - can only be used in case of version 2 of KV

        See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v1.html
        and https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        :param secret_path: Path of the secret
        :return: secret stored in the vault as a dictionary
        """
        return self.vault_client.get_secret(secret_path=secret_path, secret_version=secret_version)

    def get_secret_metadata(self, secret_path: str) -> dict | None:
        """
        Read secret metadata (including versions) from the engine. It is only valid for KV version 2.

        :param secret_path: Path to read from
        :return: secret metadata. This is a Dict containing metadata for the secret.

        See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        """
        return self.vault_client.get_secret_metadata(secret_path=secret_path)

    def get_secret_including_metadata(
        self, secret_path: str, secret_version: int | None = None
    ) -> dict | None:
        """
        Read secret including metadata. It is only valid for KV version 2.

        See https://hvac.readthedocs.io/en/stable/usage/secrets_engines/kv_v2.html for details.

        :param secret_path: Path of the secret
        :param secret_version: Optional version of key to read - can only be used in case of version 2 of KV
        :return: key info. This is a Dict with "data" mapping keeping secret
            and "metadata" mapping keeping metadata of the secret.

        """
        return self.vault_client.get_secret_including_metadata(
            secret_path=secret_path, secret_version=secret_version
        )

    def create_or_update_secret(
        self, secret_path: str, secret: dict, method: str | None = None, cas: int | None = None
    ) -> Response:
        """
        Create or updates secret.

        :param secret_path: Path to read from
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
        return self.vault_client.create_or_update_secret(
            secret_path=secret_path, secret=secret, method=method, cas=cas
        )

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, IntegerField, StringField
        from wtforms.validators import NumberRange, Optional, any_of

        return {
            "auth_type": StringField(lazy_gettext("Auth type"), widget=BS3TextFieldWidget()),
            "auth_mount_point": StringField(lazy_gettext("Auth mount point"), widget=BS3TextFieldWidget()),
            "kv_engine_version": IntegerField(
                lazy_gettext("KV engine version"),
                validators=[any_of([1, 2])],
                widget=BS3TextFieldWidget(),
                description="Must be 1 or 2.",
                default=DEFAULT_KV_ENGINE_VERSION,
            ),
            "kubernetes_role": StringField(lazy_gettext("Kubernetes role"), widget=BS3TextFieldWidget()),
            "kubernetes_jwt_path": StringField(
                lazy_gettext("Kubernetes jwt path"), widget=BS3TextFieldWidget()
            ),
            "token_path": StringField(lazy_gettext("Token path"), widget=BS3TextFieldWidget()),
            "gcp_key_path": StringField(lazy_gettext("GCP key path"), widget=BS3TextFieldWidget()),
            "gcp_scopes": StringField(lazy_gettext("GCP scopes"), widget=BS3TextFieldWidget()),
            "azure_tenant_id": StringField(lazy_gettext("Azure tenant ID"), widget=BS3TextFieldWidget()),
            "azure_resource": StringField(lazy_gettext("Azure resource"), widget=BS3TextFieldWidget()),
            "radius_host": StringField(lazy_gettext("Radius host"), widget=BS3TextFieldWidget()),
            "radius_port": IntegerField(
                lazy_gettext("Radius port"),
                widget=BS3TextFieldWidget(),
                validators=[Optional(), NumberRange(min=0)],
            ),
            "use_tls": BooleanField(lazy_gettext("Use TLS"), default=True),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["extra"],
            "relabeling": {},
        }

    def test_connection(self) -> tuple[bool, str]:
        """Test Vault connectivity from UI."""
        try:
            self.get_conn()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)
