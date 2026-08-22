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

from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseSensorOperator, conf
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesHook,
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
)
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
    validate_refresh_event,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


class AzureAnalysisServicesSensor(BaseSensorOperator):
    """
    Wait for an Azure Analysis Services model refresh to finish.

    .. seealso::
        For more information, see
        :ref:`howto/sensor:AzureAnalysisServicesSensor`.

    :param server_name: The Analysis Services server name.
    :param database: The model database name.
    :param refresh_id: The refresh operation ID to monitor.
    :param azure_analysis_services_conn_id: The Azure Analysis Services connection ID.
    :param request_timeout: Timeout in seconds for each HTTP request.
    :param deferrable: Defer polling to the triggerer.
    """

    template_fields: Sequence[str] = (
        "azure_analysis_services_conn_id",
        "server_name",
        "database",
        "refresh_id",
    )
    ui_color = "#0078d4"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        server_name: str,
        database: str,
        refresh_id: str,
        azure_analysis_services_conn_id: str = AzureAnalysisServicesHook.default_conn_name,
        request_timeout: float = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if request_timeout <= 0:
            raise ValueError("request_timeout must be greater than zero")
        self.server_name = server_name
        self.database = database
        self.refresh_id = refresh_id
        self.azure_analysis_services_conn_id = azure_analysis_services_conn_id
        self.request_timeout = request_timeout
        self.deferrable = deferrable

    @cached_property
    def hook(self) -> AzureAnalysisServicesHook:
        """Return the Azure Analysis Services hook."""
        return AzureAnalysisServicesHook(
            azure_analysis_services_conn_id=self.azure_analysis_services_conn_id,
            request_timeout=self.request_timeout,
        )

    def poke(self, context: Context) -> bool:
        """Return whether the model refresh succeeded."""
        status = self.hook.get_refresh_status(
            server_name=self.server_name,
            database=self.database,
            refresh_id=self.refresh_id,
        )
        self.log.info("Refresh %s status: %s", self.refresh_id, status)
        if status in AzureAnalysisServicesRefreshStatus.FAILURE_STATUSES:
            raise AzureAnalysisServicesRefreshException(
                f"Azure Analysis Services refresh {self.refresh_id} finished with status {status}"
            )
        return status == AzureAnalysisServicesRefreshStatus.SUCCEEDED

    def execute(self, context: Context) -> None:
        """Wait synchronously or defer status polling to the triggerer."""
        if not self.deferrable:
            super().execute(context=context)
            return
        if self.poke(context=context):
            return

        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=AzureAnalysisServicesRefreshTrigger(
                conn_id=self.azure_analysis_services_conn_id,
                server_name=self.server_name,
                database=self.database,
                refresh_id=self.refresh_id,
                poke_interval=self.poke_interval,
                request_timeout=self.request_timeout,
            ),
            method_name=self.execute_complete.__name__,
        )

    def execute_complete(self, context: Context, event: dict[str, Any] | None) -> None:
        """Validate the terminal trigger event."""
        refresh_id = validate_refresh_event(event)
        self.log.info("Azure Analysis Services refresh %s completed successfully", refresh_id)
