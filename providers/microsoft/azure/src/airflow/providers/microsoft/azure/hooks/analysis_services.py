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

from typing import Any

from azure.identity import ClientSecretCredential, DefaultAzureCredential

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_field,
    get_sync_default_azure_credential,
)


class AzureAnalysisServicesHook(BaseHook):
    """
    A hook to interact with Azure Analysis Services.

    This hook allows you to refresh models in Azure Analysis Services
    using the REST API without requiring Azure Data Factory.

    :param azure_analysis_services_conn_id: The :ref:`Azure Analysis Services connection id<howto/connection:azure_analysis_services>`.
    """

    conn_type: str = "azure_analysis_services"
    conn_name_attr: str = "azure_analysis_services_conn_id"
    default_conn_name: str = "azure_analysis_services_default"
    hook_name: str = "Azure Analysis Services"

    RESOURCE_SCOPE = "https://*.asazure.windows.net/.default"

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenantId": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
            "region": StringField(lazy_gettext("Region (e.g., westus2)"), widget=BS3TextFieldWidget()),
            "server_name": StringField(lazy_gettext("Server Name"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "host", "extra"],
            "relabeling": {
                "login": "Client ID",
                "password": "Secret",
            },
        }

    def __init__(self, azure_analysis_services_conn_id: str = default_conn_name):
        self.conn_id = azure_analysis_services_conn_id
        self._credential: ClientSecretCredential | DefaultAzureCredential | None = None
        super().__init__()

    def _get_field(self, extras: dict, field_name: str) -> str:
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=field_name,
        )

    def get_credential(self) -> ClientSecretCredential | DefaultAzureCredential:
        """Get Azure credential object for authentication."""
        if self._credential is not None:
            return self._credential

        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        tenant = self._get_field(extras, "tenantId")

        credential: ClientSecretCredential | DefaultAzureCredential

        if conn.login is not None and conn.password is not None:
            if not tenant:
                raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

            self.log.info("Using ClientSecretCredential for authentication.")
            credential = ClientSecretCredential(
                client_id=conn.login,
                client_secret=conn.password,
                tenant_id=tenant,
            )
        else:
            self.log.info("Using DefaultAzureCredential for authentication.")
            managed_identity_client_id = self._get_field(extras, "managed_identity_client_id")
            workload_identity_tenant_id = self._get_field(extras, "workload_identity_tenant_id")
            credential = get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )

        self._credential = credential
        return credential

    def _get_access_token(self) -> str:
        """Get access token for Azure Analysis Services API."""
        credential = self.get_credential()
        token = credential.get_token(self.RESOURCE_SCOPE)
        return token.token

    def test_connection(self) -> tuple[bool, str]:
        """Test a configured Azure Analysis Services connection."""
        try:
            token = self._get_access_token()
            if token:
                return (True, "Successfully connected to Azure Analysis Services.")
            return (False, "Failed to obtain access token.")
        except Exception as e:
            return (False, str(e))
