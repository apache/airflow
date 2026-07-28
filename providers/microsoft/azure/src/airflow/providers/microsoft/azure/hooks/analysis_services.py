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

import time
from typing import TYPE_CHECKING, Any, Literal

import requests
from azure.identity import ClientSecretCredential

from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_sync_default_azure_credential,
)

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

TOKEN_SCOPE = "https://*.asazure.windows.net/.default"

RefreshType = Literal["full", "clearValues", "calculate", "dataOnly", "automatic", "defragment"]
VALID_REFRESH_TYPES: frozenset[str] = frozenset(
    {"full", "clearValues", "calculate", "dataOnly", "automatic", "defragment"}
)


class AzureAnalysisServicesRefreshStatus:
    """Azure Analysis Services model refresh statuses."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timedOut"
    NOT_STARTED = "notStarted"
    IN_PROGRESS = "inProgress"

    TERMINAL_STATUSES = {SUCCEEDED, FAILED, CANCELLED, TIMED_OUT}
    FAILURE_STATUSES = {FAILED, CANCELLED, TIMED_OUT}


class AzureAnalysisServicesRefreshException(AirflowException):
    """An exception that indicates a model refresh failed to complete."""


class AzureAnalysisServicesHook(BaseHook):
    """
    A hook to interact with Azure Analysis Services.

    :param azure_analysis_services_conn_id: The :ref:`Azure Analysis Services connection id<howto/connection:azure_analysis_services>`.

    The connection must have:

    - **host**: Region endpoint, e.g. ``eastus.asazure.windows.net``
    - **login**: Client ID (for client secret auth)
    - **password**: Client Secret (for client secret auth)
    - **tenantId** (extra): Azure tenant ID (required with client secret auth)

    Managed identity and workload identity auth are also supported via the
    ``managed_identity_client_id`` and ``workload_identity_tenant_id`` extras.
    """

    conn_type: str = "azure_analysis_services"
    conn_name_attr: str = "azure_analysis_services_conn_id"
    default_conn_name: str = "azure_analysis_services_default"
    hook_name: str = "Azure Analysis Services"

    @classmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenantId": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "host": "Region Endpoint",
                "login": "Client ID",
                "password": "Client Secret",
            },
            "placeholders": {
                "host": "eastus.asazure.windows.net",
            },
        }

    def __init__(self, azure_analysis_services_conn_id: str = default_conn_name):
        self.conn_id = azure_analysis_services_conn_id
        self._credential: TokenCredential | None = None
        super().__init__()

    def get_conn(self) -> TokenCredential:
        """Return the Azure credential used to authenticate REST API requests."""
        if self._credential is not None:
            return self._credential

        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        tenant = extras.get("tenantId") or extras.get("extra__azure_analysis_services__tenantId")

        if conn.login and conn.password:
            if not tenant:
                raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")
            self._credential = ClientSecretCredential(
                client_id=conn.login,
                client_secret=conn.password,
                tenant_id=tenant,
            )
        else:
            managed_identity_client_id = extras.get("managed_identity_client_id")
            workload_identity_tenant_id = extras.get("workload_identity_tenant_id")
            self._credential = get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )

        return self._credential

    def _get_base_url(self) -> str:
        conn = self.get_connection(self.conn_id)
        if not conn.host:
            raise ValueError(
                "A region endpoint (host) is required in the Azure Analysis Services connection."
            )
        return f"https://{conn.host.rstrip('/')}"

    def _get_headers(self) -> dict[str, str]:
        token = self.get_conn().get_token(TOKEN_SCOPE)
        return {
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json",
        }

    def trigger_refresh(self, server_name: str, database: str, refresh_type: RefreshType = "full") -> str:
        """
        Trigger a model refresh and return the refresh ID.

        :param server_name: The Analysis Services server name.
        :param database: The model (database) name.
        :param refresh_type: The type of processing to perform. Default is ``full``.
            Valid values: ``full``, ``clearValues``, ``calculate``, ``dataOnly``,
            ``automatic``, ``defragment``.
        :return: The refresh ID extracted from the ``Location`` response header.
        """
        if refresh_type not in VALID_REFRESH_TYPES:
            raise ValueError(
                f"Invalid refresh_type {refresh_type!r}. Valid values are: {sorted(VALID_REFRESH_TYPES)}"
            )
        url = f"{self._get_base_url()}/servers/{server_name}/models/{database}/refreshes"
        try:
            response = requests.post(url, json={"type": refresh_type}, headers=self._get_headers())
            response.raise_for_status()
        except requests.HTTPError as e:
            raise AzureAnalysisServicesRefreshException(f"Failed to trigger refresh: {e}") from e

        location = response.headers.get("Location", "")
        refresh_id = location.rstrip("/").rsplit("/", 1)[-1]
        if not refresh_id:
            raise AzureAnalysisServicesRefreshException(
                "Failed to retrieve refresh ID from the API response Location header."
            )
        return refresh_id

    def get_refresh_status(self, server_name: str, database: str, refresh_id: str) -> str:
        """
        Return the current status of a model refresh.

        :param server_name: The Analysis Services server name.
        :param database: The model (database) name.
        :param refresh_id: The refresh ID.
        :return: The refresh status string as returned by the API.
        """
        url = f"{self._get_base_url()}/servers/{server_name}/models/{database}/refreshes/{refresh_id}"
        try:
            response = requests.get(url, headers=self._get_headers())
            response.raise_for_status()
        except requests.HTTPError as e:
            raise AzureAnalysisServicesRefreshException(f"Failed to get refresh status: {e}") from e
        return response.json().get("status", "")

    def wait_for_refresh(
        self,
        server_name: str,
        database: str,
        refresh_id: str,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
    ) -> bool:
        """
        Poll until the refresh reaches a terminal state or the timeout is exceeded.

        :return: ``True`` if the refresh succeeded, ``False`` if it failed or was cancelled.
        :raises AzureAnalysisServicesRefreshException: If the timeout is exceeded.
        """
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            status = self.get_refresh_status(server_name, database, refresh_id)
            self.log.info("Refresh %s status: %s", refresh_id, status)
            if status == AzureAnalysisServicesRefreshStatus.SUCCEEDED:
                return True
            if status in AzureAnalysisServicesRefreshStatus.FAILURE_STATUSES:
                return False
            time.sleep(check_interval)
        raise AzureAnalysisServicesRefreshException(f"Timeout waiting for refresh {refresh_id} to complete.")
