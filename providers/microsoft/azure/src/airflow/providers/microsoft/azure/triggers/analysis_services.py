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

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.microsoft.azure.hooks.analysis_services import (
    AzureAnalysisServicesHook,
    AzureAnalysisServicesRefreshStatus,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AzureAnalysisServicesRefreshTrigger(BaseTrigger):
    """
    Trigger that polls an Azure Analysis Services model refresh until it reaches a terminal state.

    :param conn_id: The connection identifier for Azure Analysis Services.
    :param server_name: The Analysis Services server name.
    :param database: The model (database) name.
    :param refresh_id: The refresh ID to monitor.
    :param poke_interval: Polling interval in seconds.
    """

    def __init__(
        self,
        conn_id: str,
        server_name: str,
        database: str,
        refresh_id: str,
        poke_interval: float = 60,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.server_name = server_name
        self.database = database
        self.refresh_id = refresh_id
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize AzureAnalysisServicesRefreshTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "conn_id": self.conn_id,
                "server_name": self.server_name,
                "database": self.database,
                "refresh_id": self.refresh_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll Azure Analysis Services for the refresh status until a terminal state is reached."""
        hook = AzureAnalysisServicesHook(azure_analysis_services_conn_id=self.conn_id)
        loop = asyncio.get_running_loop()
        try:
            while True:
                status = await loop.run_in_executor(
                    None,
                    hook.get_refresh_status,
                    self.server_name,
                    self.database,
                    self.refresh_id,
                )
                self.log.info("Refresh %s status: %s", self.refresh_id, status)

                if status == AzureAnalysisServicesRefreshStatus.SUCCEEDED:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"Refresh {self.refresh_id} completed successfully.",
                            "refresh_id": self.refresh_id,
                        }
                    )
                    return

                if status in AzureAnalysisServicesRefreshStatus.FAILURE_STATUSES:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Refresh {self.refresh_id} {status}.",
                            "refresh_id": self.refresh_id,
                        }
                    )
                    return

                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e), "refresh_id": self.refresh_id})
