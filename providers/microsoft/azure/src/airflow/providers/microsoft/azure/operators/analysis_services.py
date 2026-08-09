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

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesHook,
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
    RefreshType,
)
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
    validate_refresh_event,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


class AzureAnalysisServicesRefreshOperator(BaseOperator):
    """
    Trigger an Azure Analysis Services model refresh and optionally wait for completion.

    .. seealso::
        For more information, see
        :ref:`howto/operator:AzureAnalysisServicesRefreshOperator`.

    :param server_name: The Analysis Services server name.
    :param database: The model database name.
    :param azure_analysis_services_conn_id: The Azure Analysis Services connection ID.
    :param refresh_type: The processing type to request.
    :param wait_for_termination: Wait for the refresh to reach a terminal status.
    :param check_interval: Time in seconds between status requests.
    :param timeout: Maximum time in seconds to wait for the refresh.
    :param request_timeout: Timeout in seconds for each HTTP request.
    :param deferrable: Defer polling to the triggerer when waiting for completion.
    """

    template_fields: Sequence[str] = (
        "azure_analysis_services_conn_id",
        "server_name",
        "database",
        "refresh_type",
    )
    ui_color = "#0078d4"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        server_name: str,
        database: str,
        azure_analysis_services_conn_id: str = AzureAnalysisServicesHook.default_conn_name,
        refresh_type: RefreshType = "full",
        wait_for_termination: bool = True,
        check_interval: float = 60,
        timeout: float = 60 * 60 * 24 * 7,
        request_timeout: float = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if check_interval <= 0:
            raise ValueError("check_interval must be greater than zero")
        if timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if request_timeout <= 0:
            raise ValueError("request_timeout must be greater than zero")
        self.server_name = server_name
        self.database = database
        self.azure_analysis_services_conn_id = azure_analysis_services_conn_id
        self.refresh_type = refresh_type
        self.wait_for_termination = wait_for_termination
        self.check_interval = check_interval
        self.timeout = timeout
        self.request_timeout = request_timeout
        self.deferrable = deferrable

    @cached_property
    def hook(self) -> AzureAnalysisServicesHook:
        """Return the Azure Analysis Services hook."""
        return AzureAnalysisServicesHook(
            azure_analysis_services_conn_id=self.azure_analysis_services_conn_id,
            request_timeout=self.request_timeout,
        )

    def execute(self, context: Context) -> str:
        """Trigger the model refresh and wait when requested."""
        refresh_id = self.hook.trigger_refresh(
            server_name=self.server_name,
            database=self.database,
            refresh_type=self.refresh_type,
        )
        self.log.info("Triggered Azure Analysis Services refresh %s", refresh_id)
        if not self.wait_for_termination:
            return refresh_id

        if self.deferrable:
            status = self.hook.get_refresh_status(
                server_name=self.server_name,
                database=self.database,
                refresh_id=refresh_id,
            )
            if status == AzureAnalysisServicesRefreshStatus.SUCCEEDED:
                return refresh_id
            if status in AzureAnalysisServicesRefreshStatus.FAILURE_STATUSES:
                raise AzureAnalysisServicesRefreshException(
                    f"Azure Analysis Services refresh {refresh_id} finished with status {status}"
                )
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=AzureAnalysisServicesRefreshTrigger(
                    conn_id=self.azure_analysis_services_conn_id,
                    server_name=self.server_name,
                    database=self.database,
                    refresh_id=refresh_id,
                    poke_interval=self.check_interval,
                    request_timeout=self.request_timeout,
                ),
                method_name=self.execute_complete.__name__,
            )

        self.hook.wait_for_refresh(
            server_name=self.server_name,
            database=self.database,
            refresh_id=refresh_id,
            check_interval=self.check_interval,
            timeout=self.timeout,
        )
        self.log.info("Azure Analysis Services refresh %s completed successfully", refresh_id)
        return refresh_id

    def execute_complete(self, context: Context, event: dict[str, Any] | None) -> str:
        """Validate the terminal trigger event and return the refresh ID."""
        refresh_id = validate_refresh_event(event)
        self.log.info("Azure Analysis Services refresh %s completed successfully", refresh_id)
        return refresh_id
