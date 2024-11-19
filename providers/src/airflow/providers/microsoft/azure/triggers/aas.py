#
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
from typing import Any

from bassie.providers.microsoft.azure.hooks.aas import AasHook, AasModelRefreshState
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AasExecutionTrigger(BaseTrigger):
    """
    The trigger handles the logic of async communication with Azure Analysis Services API.
    """

    def __init__(
        self,
        aas_conn_id: str,
        server_name: str,
        server_location: str,
        database_name: str,
        operation_id: str,
        polling_period_seconds: int = 30,
    ) -> None:
        """
        Initialize the AasExecutionTrigger.

        Parameters
        ----------
        aas_conn_id : str
            Reference to the Azure Analysis Services connection.
        server_name : str
            The name of the Azure Analysis Services server.
        server_location : str
            The location of the Azure Analysis Services server.
        database_name : str
            The name of the database in the Azure Analysis Services server.
        operation_id : str
            The ID of the refresh operation.
        polling_period_seconds : int, optional
            Controls the rate of the poll for the result of this run (default is 30 seconds).
        """
        super().__init__()
        self.server_name = server_name,
        self.server_location = server_location,
        self.database_name = database_name,
        self.operation_id = operation_id
        self.aas_conn_id = aas_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.hook = AasHook(
            aas_conn_id,
        )

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize the trigger for deferred execution.

        Returns
        -------
        tuple[str, dict[str, Any]]
            A tuple containing the fully qualified class name and a dictionary of the trigger's parameters.
        """
        return (
            "bassie.providers.microsoft.azure.triggers.aas.AasExecutionTrigger",
            {
                "server_name": self.server_name,
                "server_location": self.server_location,
                "database_name": self.database_name,
                "operation_id": self.operation_id,
                "aas_conn_id": self.aas_conn_id,
                "polling_period_seconds": self.polling_period_seconds,
            },
        )

    async def run(self):
        """
        Run the trigger to monitor the refresh operation.

        This method polls the status of the refresh operation at regular intervals until the operation
        reaches a terminal state. Once the operation is complete, it yields a TriggerEvent with the
        final state of the operation.

        Yields
        ------
        TriggerEvent
            An event containing the final state of the refresh operation.
        """
        while True:           
            run = await self.hook.get_refresh_state(
                server_name=self.server_name,
                server_location=self.server_location,
                database_name=self.database_name,
                operation_id=self.operation_id,
            )
            if not run.get("is_terminal"):
                self.log.info(
                    "Refresh operation %s is in run state %s. Sleeping for %s seconds",
                    self.operation_id,
                    run.get("state"),
                    self.polling_period_seconds,
                )
                await asyncio.sleep(self.polling_period_seconds)
                continue

            yield TriggerEvent(run)
            return
