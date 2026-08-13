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
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesHook,
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
    RefreshType,
)
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


class AzureAnalysisServicesRefreshOperator(BaseOperator):
    """
    Trigger a model refresh in Azure Analysis Services and optionally wait for completion.

    :param server_name: The Analysis Services server name.
    :param database: The model (database) name.
    :param azure_analysis_services_conn_id: The connection identifier for Azure Analysis Services.
    :param refresh_type: The type of processing to perform. Default is ``full``.
    :param wait_for_termination: Whether to wait for the refresh to complete. Default is ``True``.
    :param check_interval: Seconds between status polls when waiting synchronously. Default is ``60``.
    :param timeout: Maximum seconds to wait for completion. Default is one week.
    :param deferrable: Run the operator in deferrable mode. Default is ``False``.
    """

    template_fields: Sequence[str] = (
        "azure_analysis_services_conn_id",
        "server_name",
        "database",
        "refresh_type",
    )

    ui_color = "#0078d4"

    def __init__(
        self,
        *,
        server_name: str,
        database: str,
        azure_analysis_services_conn_id: str = AzureAnalysisServicesHook.default_conn_name,
        refresh_type: RefreshType = "full",
        wait_for_termination: bool = True,
        check_interval: int = 60,
        timeout: int = 60 * 60 * 24 * 7,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.server_name = server_name
        self.database = database
        self.azure_analysis_services_conn_id = azure_analysis_services_conn_id
        self.refresh_type = refresh_type
        self.wait_for_termination = wait_for_termination
        self.check_interval = check_interval
        self.timeout = timeout
        self.deferrable = deferrable

    @cached_property
    def hook(self) -> AzureAnalysisServicesHook:
        """Create and return an AzureAnalysisServicesHook (cached)."""
        return AzureAnalysisServicesHook(azure_analysis_services_conn_id=self.azure_analysis_services_conn_id)

    def execute(self, context: Context) -> str:
        self.log.info(
            "Triggering refresh of model %s on server %s (type: %s).",
            self.database,
            self.server_name,
            self.refresh_type,
        )
        refresh_id = self.hook.trigger_refresh(self.server_name, self.database, self.refresh_type)
        self.log.info("Refresh triggered with ID: %s", refresh_id)
        context["ti"].xcom_push(key="refresh_id", value=refresh_id)

        if not self.wait_for_termination:
            return refresh_id

        if self.deferrable:
            status = self.hook.get_refresh_status(self.server_name, self.database, refresh_id)
            if status not in AzureAnalysisServicesRefreshStatus.TERMINAL_STATUSES:
                self.defer(
                    trigger=AzureAnalysisServicesRefreshTrigger(
                        conn_id=self.azure_analysis_services_conn_id,
                        server_name=self.server_name,
                        database=self.database,
                        refresh_id=refresh_id,
                        poke_interval=self.check_interval,
                    ),
                    method_name="execute_complete",
                )
            elif status != AzureAnalysisServicesRefreshStatus.SUCCEEDED:
                raise AzureAnalysisServicesRefreshException(f"Refresh {refresh_id} {status}.")
        else:
            if not self.hook.wait_for_refresh(
                self.server_name,
                self.database,
                refresh_id,
                check_interval=self.check_interval,
                timeout=self.timeout,
            ):
                raise AzureAnalysisServicesRefreshException(f"Refresh {refresh_id} failed or was cancelled.")
            self.log.info("Refresh %s completed successfully.", refresh_id)

        return refresh_id

    def execute_complete(self, context: Context, event: dict) -> str:
        """
        Return immediately — callback for when the trigger fires.

        Relies on the trigger to emit an error event on failure.
        """
        if event.get("status") == "error":
            raise AzureAnalysisServicesRefreshException(event["message"])
        self.log.info(event.get("message"))
        return event.get("refresh_id", "")
