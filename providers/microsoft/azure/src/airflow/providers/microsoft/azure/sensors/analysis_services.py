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

from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseSensorOperator
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesHook,
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureAnalysisServicesRefreshSensor(BaseSensorOperator):
    """
    Check the status of an Azure Analysis Services refresh operation.

    This sensor polls the status of a refresh operation until it reaches a terminal state
    (succeeded, failed, or cancelled).

    :param server_name: Name of the Analysis Services server.
    :param model_name: Name of the model being refreshed.
    :param region: Azure region (e.g., 'westus2').
    :param refresh_id: The refresh operation ID to monitor.
    :param azure_analysis_services_conn_id: Reference to the
        Azure Analysis Services connection.
    """

    template_fields = ("server_name", "model_name", "region", "refresh_id")

    ui_color = "#50C878"

    def __init__(
        self,
        *,
        server_name: str,
        model_name: str,
        region: str,
        refresh_id: str,
        azure_analysis_services_conn_id: str = AzureAnalysisServicesHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.server_name = server_name
        self.model_name = model_name
        self.region = region
        self.refresh_id = refresh_id
        self.azure_analysis_services_conn_id = azure_analysis_services_conn_id

    @cached_property
    def hook(self) -> AzureAnalysisServicesHook:
        """Create and return an AzureAnalysisServicesHook (cached)."""
        return AzureAnalysisServicesHook(azure_analysis_services_conn_id=self.azure_analysis_services_conn_id)

    def poke(self, context: Context) -> bool:
        """
        Check the status of the refresh operation.

        :param context: The task execution context.
        :return: True if the refresh succeeded, False if still in progress.
        :raises AzureAnalysisServicesRefreshException: If the refresh failed or was cancelled.
        """
        self.log.info(
            "Checking status of refresh operation '%s' for model '%s' on server '%s'",
            self.refresh_id,
            self.model_name,
            self.server_name,
        )

        status_info = self.hook.get_refresh_status(
            server_name=self.server_name,
            model_name=self.model_name,
            region=self.region,
            refresh_id=self.refresh_id,
        )

        current_status = status_info.get("status")
        self.log.info("Current status: %s", current_status)

        if current_status == AzureAnalysisServicesRefreshStatus.SUCCEEDED:
            self.log.info("Refresh operation completed successfully")
            return True

        if current_status in AzureAnalysisServicesRefreshStatus.FAILURE_STATES:
            raise AzureAnalysisServicesRefreshException(
                f"Refresh operation {self.refresh_id} failed with status: {current_status}. "
                f"Details: {status_info}"
            )

        if current_status == AzureAnalysisServicesRefreshStatus.IN_PROGRESS:
            return False

        self.log.warning("Unexpected status '%s', treating as in progress", current_status)
        return False
