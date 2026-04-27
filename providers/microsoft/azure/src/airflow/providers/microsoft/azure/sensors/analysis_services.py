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
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseSensorOperator, conf
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesHook,
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
)
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


class AzureAnalysisServicesSensor(BaseSensorOperator):
    """
    Poll an Azure Analysis Services model refresh until it succeeds or fails.

    :param server_name: The Analysis Services server name.
    :param database: The model (database) name.
    :param refresh_id: The refresh ID to monitor.
    :param azure_analysis_services_conn_id: The connection identifier for Azure Analysis Services.
    :param deferrable: Run the sensor in deferrable mode. Default is ``False``.
    """

    template_fields: Sequence[str] = (
        "azure_analysis_services_conn_id",
        "server_name",
        "database",
        "refresh_id",
    )

    ui_color = "#0078d4"

    def __init__(
        self,
        *,
        server_name: str,
        database: str,
        refresh_id: str,
        azure_analysis_services_conn_id: str = AzureAnalysisServicesHook.default_conn_name,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.server_name = server_name
        self.database = database
        self.refresh_id = refresh_id
        self.azure_analysis_services_conn_id = azure_analysis_services_conn_id
        self.deferrable = deferrable

    @cached_property
    def hook(self) -> AzureAnalysisServicesHook:
        """Create and return an AzureAnalysisServicesHook (cached)."""
        return AzureAnalysisServicesHook(azure_analysis_services_conn_id=self.azure_analysis_services_conn_id)

    def poke(self, context: Context) -> bool:
        status = self.hook.get_refresh_status(self.server_name, self.database, self.refresh_id)
        self.log.info("Refresh %s status: %s", self.refresh_id, status)

        if status in AzureAnalysisServicesRefreshStatus.FAILURE_STATUSES:
            raise AzureAnalysisServicesRefreshException(f"Refresh {self.refresh_id} {status}.")

        return status == AzureAnalysisServicesRefreshStatus.SUCCEEDED

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context=context)
        else:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=AzureAnalysisServicesRefreshTrigger(
                        conn_id=self.azure_analysis_services_conn_id,
                        server_name=self.server_name,
                        database=self.database,
                        refresh_id=self.refresh_id,
                        poke_interval=self.poke_interval,
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: Context, event: dict) -> None:
        """
        Return immediately — callback for when the trigger fires.

        Relies on the trigger to emit an error event on failure.
        """
        if event.get("status") == "error":
            raise AzureAnalysisServicesRefreshException(event["message"])
        self.log.info(event.get("message"))
