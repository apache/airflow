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

from typing import TYPE_CHECKING, Any

from azure.common.client_factory import get_client_from_auth_file, get_client_from_json_dict
from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import ClientSecretCredential, DefaultAzureCredential

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.utils import (
    AzureIdentityCredentialAdapter,
    add_managed_identity_connection_widgets,
    get_sync_default_azure_credential,
)

if TYPE_CHECKING:
    from azure.core.credentials import AccessToken

    from airflow.sdk import Connection


class AzureBaseHook(BaseHook):
    """
    This hook acts as a base hook for azure services.

    It offers several authentication mechanisms to authenticate
    the client library used for upstream azure hooks.

    :param sdk_client: The SDKClient to use.
    :param conn_id: The :ref:`Azure connection id<howto/connection:azure>`
        which refers to the information to connect to the service.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "azure_default"
    conn_type = "azure"
    hook_name = "Azure"

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenantId": StringField(lazy_gettext("Azure Tenant ID"), widget=BS3TextFieldWidget()),
            "subscriptionId": StringField(lazy_gettext("Azure Subscription ID"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        import json

        return {
            "hidden_fields": ["schema", "port", "host"],
            "relabeling": {
                "login": "Azure Client ID",
                "password": "Azure Secret",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "key_path": "path to json file for auth",
                        "key_json": "specifies json dict for auth",
                    },
                    indent=1,
                ),
                "login": "client_id (token credentials auth)",
                "password": "secret (token credentials auth)",
                "tenantId": "tenantId (token credentials auth)",
                "subscriptionId": "subscriptionId (token credentials auth)",
            },
        }

    def __init__(self, sdk_client: Any = None, conn_id: str = "azure_default"):
        self.sdk_client = sdk_client
        self.conn_id = conn_id
        super().__init__()

    def get_conn(self) -> Any:
        """
        Authenticate the resource using the connection id passed during init.

        :return: the authenticated client.
        """
        if not self.sdk_client:
            raise ValueError("`sdk_client` must be provided to AzureBaseHook to use `get_conn` method.")
        conn = self.get_connection(self.conn_id)
        subscription_id = conn.extra_dejson.get("subscriptionId")
        key_path = conn.extra_dejson.get("key_path")
        if key_path:
            if not key_path.endswith(".json"):
                raise AirflowException("Unrecognised extension for key file.")
            self.log.info("Getting connection using a JSON key file.")
            return get_client_from_auth_file(client_class=self.sdk_client, auth_path=key_path)

        key_json = conn.extra_dejson.get("key_json")
        if key_json:
            self.log.info("Getting connection using a JSON config.")
            return get_client_from_json_dict(client_class=self.sdk_client, config_dict=key_json)

        credentials = self.get_credential(conn=conn)

        return self.sdk_client(
            credentials=credentials,
            subscription_id=subscription_id,
        )

    def get_credential(
        self, *, conn: Connection | None = None
    ) -> (
        ServicePrincipalCredentials
        | AzureIdentityCredentialAdapter
        | ClientSecretCredential
        | DefaultAzureCredential
    ):
        """
        Get Azure credential object for the connection.

        Azure Identity based credential object (``ClientSecretCredential``, ``DefaultAzureCredential``) can be used to get OAuth token using ``get_token`` method.
        Older Credential objects (``ServicePrincipalCredentials``, ``AzureIdentityCredentialAdapter``) are supported for backward compatibility.

        :return: The Azure credential object
        """
        if not conn:
            conn = self.get_connection(self.conn_id)
        tenant = conn.extra_dejson.get("tenantId")
        credential: (
            ServicePrincipalCredentials
            | AzureIdentityCredentialAdapter
            | ClientSecretCredential
            | DefaultAzureCredential
        )
        if all([conn.login, conn.password, tenant]):
            credential = self._get_client_secret_credential(conn)
        else:
            credential = self._get_default_azure_credential(conn)
        return credential

    def _get_client_secret_credential(
        self, conn: Connection
    ) -> ServicePrincipalCredentials | ClientSecretCredential:
        self.log.info("Getting credentials using specific credentials and subscription_id.")
        extra_dejson = conn.extra_dejson
        tenant = extra_dejson.get("tenantId")
        use_azure_identity_object = extra_dejson.get("use_azure_identity_object", False)
        if use_azure_identity_object:
            return ClientSecretCredential(
                client_id=conn.login,  # type: ignore[arg-type]
                client_secret=conn.password,  # type: ignore[arg-type]
                tenant_id=tenant,  # type: ignore[arg-type]
            )
        return ServicePrincipalCredentials(client_id=conn.login, secret=conn.password, tenant=tenant)

    def _get_default_azure_credential(
        self, conn: Connection
    ) -> DefaultAzureCredential | AzureIdentityCredentialAdapter:
        self.log.info("Using DefaultAzureCredential as credential")
        extra_dejson = conn.extra_dejson
        managed_identity_client_id = extra_dejson.get("managed_identity_client_id")
        workload_identity_tenant_id = extra_dejson.get("workload_identity_tenant_id")
        use_azure_identity_object = extra_dejson.get("use_azure_identity_object", False)
        if use_azure_identity_object:
            return get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )
        return AzureIdentityCredentialAdapter(
            managed_identity_client_id=managed_identity_client_id,
            workload_identity_tenant_id=workload_identity_tenant_id,
        )

    def get_token(self, *scopes, **kwargs) -> AccessToken:
        """
        Request an access token for `scopes`.

        To use this method, set `use_azure_identity_object: True` in the connection extra field.
        ServicePrincipalCredentials and AzureIdentityCredentialAdapter don't support `get_token` method.
        """
        credential = self.get_credential()
        if isinstance(credential, ServicePrincipalCredentials) or isinstance(
            credential, AzureIdentityCredentialAdapter
        ):
            raise AttributeError(
                "ServicePrincipalCredentials and AzureIdentityCredentialAdapter don't support get_token method. "
                "Please set `use_azure_identity_object: True` in the connection extra field to use credential that support get_token method."
            )
        return credential.get_token(*scopes, **kwargs)
