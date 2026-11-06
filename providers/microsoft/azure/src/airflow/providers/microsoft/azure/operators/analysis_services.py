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
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesHook,
    RefreshType,
)
from airflow.providers.microsoft.azure.triggers.analysis_services import (
    AzureAnalysisServicesRefreshTrigger,
    validate_completed_refresh_event,
    validate_refresh_event,
)

if TYPE_CHECKING:
    from airflow.sdk import Context


class AzureAnalysisServicesRefreshOperator(BaseOperator):
    """
    Trigger an Azure Analysis Services model refresh and optionally wait for completion.

    The operator always runs deferred: both the request that starts the refresh and the status
    polling happen in the triggerer, so no worker slot is held while the model is refreshing.
    A triggerer must therefore be running in the deployment.

    .. seealso::
        For more information, see
        :ref:`howto/operator:AzureAnalysisServicesRefreshOperator`.

    :param server_name: The Analysis Services server name.
    :param database: The model database name.
    :param azure_analysis_services_conn_id: The Azure Analysis Services connection ID.
    :param refresh_type: The processing type to request.
    :param wait_for_termination: Wait for the refresh to reach a terminal status.
    :param check_interval: Time in seconds between status requests.
    :param timeout: Maximum time in seconds to wait for the refresh to complete. The clock starts
        once the refresh has been submitted.
    :param request_timeout: Timeout in seconds for each HTTP request.
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

    def execute(self, context: Context) -> None:
        """Defer to the trigger so the refresh is submitted off the worker."""
        self.defer(
            trigger=self._build_trigger(refresh_id=None),
            method_name=self.handle_refresh.__name__,
        )

    def handle_refresh(self, context: Context, event: dict[str, Any] | None) -> str | None:
        """Record the new refresh ID and defer again when the refresh has to be awaited."""
        refresh_id = validate_refresh_event(event)
        self.log.info("Triggered Azure Analysis Services refresh %s", refresh_id)
        context["ti"].xcom_push(key=f"{self.task_id}.refresh_id", value=refresh_id)
        if not self.wait_for_termination:
            return refresh_id

        # The timeout covers waiting for the refresh, so it starts once it has been submitted.
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=self._build_trigger(refresh_id=refresh_id),
            method_name=self.execute_complete.__name__,
        )

    def _build_trigger(self, *, refresh_id: str | None) -> AzureAnalysisServicesRefreshTrigger:
        return AzureAnalysisServicesRefreshTrigger(
            conn_id=self.azure_analysis_services_conn_id,
            server_name=self.server_name,
            database=self.database,
            refresh_id=refresh_id,
            refresh_type=self.refresh_type,
            poke_interval=self.check_interval,
            request_timeout=self.request_timeout,
        )

    def execute_complete(self, context: Context, event: dict[str, Any] | None) -> str:
        """Validate the terminal trigger event and return the refresh ID."""
        refresh_id = validate_completed_refresh_event(event)
        self.log.info("Azure Analysis Services refresh %s completed successfully", refresh_id)
        return refresh_id
