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
from typing import Any

import requests
from azure.identity import ClientSecretCredential, DefaultAzureCredential

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_field,
    get_sync_default_azure_credential,
)


class AzureAnalysisServicesRefreshStatus:
    """Azure Analysis Services refresh operation statuses."""

    IN_PROGRESS = "inProgress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    TERMINAL_STATUSES = {SUCCEEDED, FAILED, CANCELLED}
    INTERMEDIATE_STATES = {IN_PROGRESS}
    FAILURE_STATES = {FAILED, CANCELLED}


class AzureAnalysisServicesRefreshException(AirflowException):
    """An exception that indicates a refresh operation failed to complete."""


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

    def _build_api_url(
        self, server_name: str, model_name: str, region: str, refresh_id: str | None = None
    ) -> str:
        """Build the API URL for Azure Analysis Services."""
        base_url = f"https://{region}.asazure.windows.net/servers/{server_name}/models/{model_name}/refreshes"
        if refresh_id:
            return f"{base_url}/{refresh_id}"
        return base_url

    def refresh_model(
        self,
        server_name: str,
        model_name: str,
        region: str,
        refresh_type: str = "full",
        commit_mode: str = "transactional",
        max_parallelism: int | None = None,
        retry_count: int | None = None,
        objects: list[dict] | None = None,
    ) -> str:
        """
        Trigger a refresh operation on an Azure Analysis Services model.

        :param server_name: Name of the Analysis Services server
        :param model_name: Name of the model to refresh
        :param region: Azure region (e.g., 'westus2')
        :param refresh_type: Type of refresh ('full', 'automatic', 'dataOnly', 'calculate', 'clearValues')
        :param commit_mode: Commit mode ('transactional' or 'partialBatch')
        :param max_parallelism: Maximum number of parallel threads
        :param retry_count: Number of retries on failure
        :param objects: List of objects to refresh (empty list means all objects)
        :return: Refresh operation ID
        """
        url = self._build_api_url(server_name, model_name, region)
        token = self._get_access_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        body: dict[str, Any] = {
            "Type": refresh_type,
            "CommitMode": commit_mode,
            "Objects": objects if objects is not None else [],
        }

        if max_parallelism is not None:
            body["MaxParallelism"] = max_parallelism
        if retry_count is not None:
            body["RetryCount"] = retry_count

        self.log.info(
            "Triggering refresh for model '%s' on server '%s' in region '%s' with type '%s'",
            model_name,
            server_name,
            region,
            refresh_type,
        )

        response = requests.post(url, json=body, headers=headers, timeout=30)

        if response.status_code == 202:
            location = response.headers.get("Location")
            if not location:
                raise AirflowException("Refresh started but no Location header in response")

            refresh_id = location.rstrip("/").split("/")[-1]
            self.log.info("Refresh operation started with ID: %s", refresh_id)
            return refresh_id

        raise AirflowException(
            f"Failed to start refresh operation. Status: {response.status_code}, Response: {response.text}"
        )

    def get_refresh_status(
        self,
        server_name: str,
        model_name: str,
        region: str,
        refresh_id: str,
    ) -> dict[str, Any]:
        """Get the status of a refresh operation."""
        url = self._build_api_url(server_name, model_name, region, refresh_id)
        token = self._get_access_token()

        headers = {"Authorization": f"Bearer {token}"}

        self.log.debug("Checking refresh status for refresh_id: %s", refresh_id)
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            return response.json()

        raise AirflowException(
            f"Failed to get refresh status. Status: {response.status_code}, Response: {response.text}"
        )

    def wait_for_refresh_completion(
        self,
        server_name: str,
        model_name: str,
        region: str,
        refresh_id: str,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24,
    ) -> dict[str, Any]:
        """Wait for a refresh operation to complete."""
        elapsed_time = 0

        while elapsed_time < timeout:
            status_info = self.get_refresh_status(server_name, model_name, region, refresh_id)
            current_status = status_info.get("status")

            self.log.info(
                "Refresh %s status: %s (elapsed: %s seconds)", refresh_id, current_status, elapsed_time
            )

            if current_status in AzureAnalysisServicesRefreshStatus.TERMINAL_STATUSES:
                if current_status in AzureAnalysisServicesRefreshStatus.FAILURE_STATES:
                    raise AzureAnalysisServicesRefreshException(
                        f"Refresh operation {refresh_id} failed with status: {current_status}. "
                        f"Details: {status_info}"
                    )
                return status_info

            time.sleep(check_interval)
            elapsed_time += check_interval

        raise AirflowException(
            f"Refresh operation {refresh_id} did not complete within {timeout} seconds. "
            f"Last known status: {status_info}"
        )
