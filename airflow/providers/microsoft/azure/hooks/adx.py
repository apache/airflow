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
This module contains Azure Data Explorer hook.

.. spelling:word-list::

    KustoResponseDataSetV
    kusto
"""
from __future__ import annotations

import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any

from azure.kusto.data import ClientRequestProperties, KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_sync_default_azure_credential,
)

if TYPE_CHECKING:
    from azure.kusto.data.response import KustoResponseDataSetV2


class AzureDataExplorerHook(BaseHook):
    """
    Interacts with Azure Data Explorer (Kusto).

    **Cluster**:

    Azure Data Explorer cluster is specified by a URL, for example: "https://help.kusto.windows.net".
    The parameter must be provided through the Data Explorer Cluster URL connection detail.

    **Tenant ID**:

    To learn about tenants refer to: https://docs.microsoft.com/en-us/onedrive/find-your-office-365-tenant-id

    **Authentication methods**:

    Available authentication methods are:

      - AAD_APP: Authentication with AAD application certificate. A Tenant ID is required when using this
        method. Provide application ID and application key through Username and Password parameters.

      - AAD_APP_CERT: Authentication with AAD application certificate. Tenant ID, Application PEM Certificate,
        and Application Certificate Thumbprint are required when using this method.

      - AAD_CREDS: Authentication with AAD username and password. A Tenant ID is required when using this
        method. Username and Password parameters are used for authentication with AAD.

      - AAD_DEVICE: Authenticate with AAD device code. Please note that if you choose this option, you'll need
        to authenticate for every new instance that is initialized. It is highly recommended to create one
        instance and use it for all queries.

    :param azure_data_explorer_conn_id: Reference to the
        :ref:`Azure Data Explorer connection<howto/connection:adx>`.
    """

    conn_name_attr = "azure_data_explorer_conn_id"
    default_conn_name = "azure_data_explorer_default"
    conn_type = "azure_data_explorer"
    hook_name = "Azure Data Explorer"

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField

        return {
            "tenant": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
            "auth_method": StringField(lazy_gettext("Authentication Method"), widget=BS3TextFieldWidget()),
            "certificate": PasswordField(
                lazy_gettext("Application PEM Certificate"), widget=BS3PasswordFieldWidget()
            ),
            "thumbprint": PasswordField(
                lazy_gettext("Application Certificate Thumbprint"), widget=BS3PasswordFieldWidget()
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "login": "Username",
                "host": "Data Explorer Cluster URL",
            },
            "placeholders": {
                "login": "Varies with authentication method",
                "password": "Varies with authentication method",
                "auth_method": "AAD_APP/AAD_APP_CERT/AAD_CREDS/AAD_DEVICE/AZURE_TOKEN_CRED",
                "tenant": "Used with AAD_APP/AAD_APP_CERT/AAD_CREDS",
                "certificate": "Used with AAD_APP_CERT",
                "thumbprint": "Used with AAD_APP_CERT",
            },
        }

    def __init__(self, azure_data_explorer_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = azure_data_explorer_conn_id

    @cached_property
    def connection(self) -> KustoClient:
        """Return a KustoClient object (cached)."""
        return self.get_conn()

    def get_conn(self) -> KustoClient:
        """Return a KustoClient object."""
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        cluster = conn.host
        if not cluster:
            raise AirflowException("Host connection option is required")

        def warn_if_collison(key, backcompat_key):
            if backcompat_key in extras:
                warnings.warn(
                    f"Conflicting params `{key}` and `{backcompat_key}` found in extras for conn "
                    f"{self.conn_id}. Using value for `{key}`.  Please ensure this is the correct value "
                    f"and remove the backcompat key `{backcompat_key}`."
                )

        def get_required_param(name: str) -> str:
            """
            Extract required parameter value from connection, raise exception if not found.

            Warns if both ``foo`` and ``extra__azure_data_explorer__foo`` found in conn extra.

            Prefers unprefixed field.
            """
            backcompat_prefix = "extra__azure_data_explorer__"
            backcompat_key = f"{backcompat_prefix}{name}"
            value = extras.get(name)
            if value:
                warn_if_collison(name, backcompat_key)
            if not value and extras.get(backcompat_key):
                warnings.warn(
                    f"`{backcompat_key}` is deprecated in azure connection extra,"
                    f" please use `{name}` instead",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
                value = extras.get(backcompat_key)
            if not value:
                raise AirflowException(f"Required connection parameter is missing: `{name}`")
            return value

        auth_method = get_required_param("auth_method")

        if auth_method == "AAD_APP":
            tenant = get_required_param("tenant")
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                cluster, conn.login, conn.password, tenant
            )
        elif auth_method == "AAD_APP_CERT":
            certificate = get_required_param("certificate")
            thumbprint = get_required_param("thumbprint")
            tenant = get_required_param("tenant")
            kcsb = KustoConnectionStringBuilder.with_aad_application_certificate_authentication(
                cluster,
                conn.login,
                certificate,
                thumbprint,
                tenant,
            )
        elif auth_method == "AAD_CREDS":
            tenant = get_required_param("tenant")
            kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(
                cluster, conn.login, conn.password, tenant
            )
        elif auth_method == "AAD_DEVICE":
            kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(cluster)
        elif auth_method == "AZURE_TOKEN_CRED":
            managed_identity_client_id = conn.extra_dejson.get("managed_identity_client_id")
            workload_identity_tenant_id = conn.extra_dejson.get("workload_identity_tenant_id")
            credential = get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )
            kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                connection_string=cluster,
                credential=credential,
            )
        else:
            raise AirflowException(f"Unknown authentication method: {auth_method}")

        return KustoClient(kcsb)

    def run_query(self, query: str, database: str, options: dict | None = None) -> KustoResponseDataSetV2:
        """
        Run KQL query using provided configuration, and return KustoResponseDataSet instance.

        See: `azure.kusto.data.response.KustoResponseDataSet`
        If query is unsuccessful AirflowException is raised.

        :param query: KQL query to run
        :param database: Database to run the query on.
        :param options: Optional query options. See:
           https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties#list-of-clientrequestproperties
        :return: dict
        """
        properties = ClientRequestProperties()
        if options:
            for k, v in options.items():
                properties.set_option(k, v)
        try:
            return self.connection.execute(database, query, properties=properties)
        except KustoServiceError as error:
            raise AirflowException(f"Error running Kusto query: {error}")
