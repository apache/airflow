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
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal, get_args
from urllib.parse import quote, unquote, urlsplit

import httpx
from azure.core.exceptions import AzureError
from azure.identity import ClientSecretCredential

from airflow.providers.common.compat.sdk import AirflowException, BaseHook

if TYPE_CHECKING:
    from urllib.parse import SplitResult

    from azure.core.credentials import TokenCredential

    from airflow.sdk import Connection

TOKEN_SCOPE = "https://*.asazure.windows.net/.default"

RefreshType = Literal["full", "clearValues", "calculate", "dataOnly", "automatic", "defragment"]
VALID_REFRESH_TYPES: frozenset[str] = frozenset(get_args(RefreshType))


def _format_request_error(error: httpx.HTTPError) -> str:
    # Only HTTPStatusError carries a response; transport errors have no such attribute.
    response = getattr(error, "response", None)
    response_body = getattr(response, "text", "").strip()[:1000]
    response_detail = f"; response body: {response_body}" if response_body else ""
    return f"{error}{response_detail}"


class AzureAnalysisServicesRefreshStatus:
    """Azure Analysis Services model refresh statuses."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timedOut"
    NOT_STARTED = "notStarted"
    IN_PROGRESS = "inProgress"

    FAILURE_STATUSES = frozenset({FAILED, CANCELLED, TIMED_OUT})
    VALID_STATUSES = frozenset({SUCCEEDED, FAILED, CANCELLED, TIMED_OUT, NOT_STARTED, IN_PROGRESS})


class AzureAnalysisServicesRefreshException(AirflowException):
    """Indicate that an Azure Analysis Services model refresh operation failed."""


class AzureAnalysisServicesHook(BaseHook):
    """
    Interact with the Azure Analysis Services asynchronous refresh REST API.

    :param azure_analysis_services_conn_id: The Azure Analysis Services connection ID.
    :param request_timeout: Timeout in seconds for each HTTP request.

    The connection must define the region endpoint in ``host``, the service principal client ID in
    ``login``, the client secret in ``password``, and the Microsoft Entra tenant ID in the
    ``tenantId`` extra field.
    """

    conn_type: str = "azure_analysis_services"
    conn_name_attr: str = "azure_analysis_services_conn_id"
    default_conn_name: str = "azure_analysis_services_default"
    hook_name: str = "Azure Analysis Services"

    def __init__(
        self,
        azure_analysis_services_conn_id: str = default_conn_name,
        request_timeout: float = 60,
    ) -> None:
        super().__init__()
        if request_timeout <= 0:
            raise ValueError("request_timeout must be greater than zero")
        self.azure_analysis_services_conn_id = azure_analysis_services_conn_id
        self.request_timeout = request_timeout
        self._credential: TokenCredential | None = None

    @cached_property
    def connection(self) -> Connection:
        """Return the Azure Analysis Services connection."""
        return self.get_connection(self.azure_analysis_services_conn_id)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to the connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "tenantId": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour for the connection form."""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "host": "Region Endpoint",
                "login": "Client ID",
                "password": "Client Secret",
            },
            "placeholders": {
                "host": "westus.asazure.windows.net",
            },
        }

    def get_conn(self) -> TokenCredential:
        """Return and cache the service principal credential."""
        if self._credential is not None:
            return self._credential

        connection = self.connection
        tenant_id = connection.extra_dejson.get("tenantId")
        if not connection.login:
            raise ValueError("Client ID is required for Azure Analysis Services authentication")
        if not connection.password:
            raise ValueError("Client secret is required for Azure Analysis Services authentication")
        if not isinstance(tenant_id, str) or not tenant_id:
            raise ValueError("Tenant ID is required for Azure Analysis Services authentication")

        self._credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=connection.login,
            client_secret=connection.password,
        )
        return self._credential

    def get_refresh_status(self, server_name: str, database: str, refresh_id: str) -> str:
        """Return the validated status of an Azure Analysis Services model refresh."""
        refresh_url = f"{self._get_refreshes_url(server_name, database)}/{quote(refresh_id, safe='')}"
        try:
            with httpx.Client() as client:
                response = client.get(
                    refresh_url,
                    headers=self._get_headers(),
                    timeout=self.request_timeout,
                )
                response.raise_for_status()
        except httpx.HTTPError as error:
            raise AzureAnalysisServicesRefreshException(
                f"Failed to get status for Azure Analysis Services refresh {refresh_id}: "
                f"{_format_request_error(error)}"
            ) from error

        try:
            response_body = response.json()
        except ValueError as error:
            raise AzureAnalysisServicesRefreshException(
                f"Azure Analysis Services returned a non-JSON status response for refresh {refresh_id}"
            ) from error

        if not isinstance(response_body, dict):
            raise AzureAnalysisServicesRefreshException(
                f"Azure Analysis Services returned an invalid status response for refresh {refresh_id}"
            )
        status = response_body.get("status")
        if not isinstance(status, str) or status not in AzureAnalysisServicesRefreshStatus.VALID_STATUSES:
            raise AzureAnalysisServicesRefreshException(
                f"Azure Analysis Services returned unknown status {status!r} for refresh {refresh_id}"
            )
        return status

    def trigger_refresh(self, server_name: str, database: str, refresh_type: RefreshType = "full") -> str:
        """Trigger a model refresh and return its refresh ID."""
        if refresh_type not in VALID_REFRESH_TYPES:
            raise ValueError(
                f"Invalid refresh_type {refresh_type!r}. Valid values are: {sorted(VALID_REFRESH_TYPES)}"
            )

        try:
            with httpx.Client() as client:
                response = client.post(
                    self._get_refreshes_url(server_name, database),
                    json={"Type": refresh_type},
                    headers=self._get_headers(),
                    timeout=self.request_timeout,
                )
                response.raise_for_status()
        except httpx.HTTPError as error:
            raise AzureAnalysisServicesRefreshException(
                f"Failed to trigger an Azure Analysis Services model refresh: {_format_request_error(error)}"
            ) from error

        location = response.headers.get("Location")
        if not location:
            raise AzureAnalysisServicesRefreshException(
                "Azure Analysis Services did not return a refresh ID in the Location header"
            )
        try:
            location_parts = [part for part in urlsplit(location).path.split("/") if part]
        except ValueError as error:
            raise AzureAnalysisServicesRefreshException(
                "Azure Analysis Services returned an invalid refresh Location header"
            ) from error
        if len(location_parts) < 2 or location_parts[-2] != "refreshes":
            raise AzureAnalysisServicesRefreshException(
                "Azure Analysis Services returned an invalid refresh Location header"
            )
        return unquote(location_parts[-1])

    def wait_for_refresh(
        self,
        server_name: str,
        database: str,
        refresh_id: str,
        check_interval: float = 60,
        timeout: float = 60 * 60 * 24 * 7,
    ) -> None:
        """Poll until the refresh reaches a terminal status or the timeout expires."""
        if check_interval <= 0:
            raise ValueError("check_interval must be greater than zero")
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")

        deadline = time.monotonic() + timeout
        while True:
            status = self.get_refresh_status(server_name, database, refresh_id)
            self.log.info("Refresh %s status: %s", refresh_id, status)
            if status == AzureAnalysisServicesRefreshStatus.SUCCEEDED:
                return
            if status in AzureAnalysisServicesRefreshStatus.FAILURE_STATUSES:
                raise AzureAnalysisServicesRefreshException(
                    f"Azure Analysis Services refresh {refresh_id} finished with status {status}"
                )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AzureAnalysisServicesRefreshException(
                    f"Timeout waiting for Azure Analysis Services refresh {refresh_id} to complete"
                )
            time.sleep(min(check_interval, remaining))

    def _assert_host(self, host: str, parsed_host: SplitResult) -> None:
        # netloc, not .username/.port: those miss "@host" and ":0", and raise on ":abc".
        if (
            not host
            or not parsed_host.hostname
            or "@" in parsed_host.netloc
            or ":" in parsed_host.netloc
            or parsed_host.path
            or parsed_host.query
            or parsed_host.fragment
        ):
            raise ValueError(
                "A valid region endpoint without a URL scheme, credentials, port, or path is "
                "required in the Azure Analysis Services connection host"
            )

    def _get_base_url(self) -> str:
        host = (self.connection.host or "").strip().rstrip("/")
        self._assert_host(host, urlsplit(f"//{host}"))
        return f"https://{host}"

    def _get_headers(self) -> dict[str, str]:
        try:
            token = self.get_conn().get_token(TOKEN_SCOPE)
        except AzureError as error:
            raise AzureAnalysisServicesRefreshException(
                "Failed to authenticate with Azure Analysis Services"
            ) from error
        return {
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json",
        }

    def _get_refreshes_url(self, server_name: str, database: str) -> str:
        if not server_name:
            raise ValueError("server_name must not be empty")
        if not database:
            raise ValueError("database must not be empty")
        return (
            f"{self._get_base_url()}/servers/{quote(server_name, safe='')}"
            f"/models/{quote(database, safe='')}/refreshes"
        )
