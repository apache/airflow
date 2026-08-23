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
    AzureAnalysisServicesRefreshException,
    AzureAnalysisServicesRefreshStatus,
    RefreshType,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


def validate_refresh_event(event: dict[str, Any] | None) -> str:
    """Validate a trigger event and return its refresh ID."""
    if not isinstance(event, dict):
        raise AzureAnalysisServicesRefreshException(
            "Did not receive a valid event from the Azure Analysis Services trigger"
        )

    # Errors are reported before the refresh ID is validated: a failed POST yields an event
    # without one, and its message is the only useful diagnostic.
    event_status = event.get("status")
    if event_status == "error":
        message = event.get("message")
        if not isinstance(message, str) or not message:
            message = "Azure Analysis Services refresh failed"
        raise AzureAnalysisServicesRefreshException(message)
    if event_status != "success":
        raise AzureAnalysisServicesRefreshException(
            f"Azure Analysis Services trigger returned unknown event status {event_status!r}"
        )

    refresh_id = event.get("refresh_id")
    if not isinstance(refresh_id, str) or not refresh_id:
        raise AzureAnalysisServicesRefreshException(
            "Azure Analysis Services trigger event did not contain a valid refresh ID"
        )
    return refresh_id


def validate_completed_refresh_event(event: dict[str, Any] | None) -> str:
    """Validate a terminal trigger event and return the completed refresh ID."""
    refresh_id = validate_refresh_event(event)

    refresh_status = (event or {}).get("refresh_status")
    if refresh_status != AzureAnalysisServicesRefreshStatus.SUCCEEDED:
        raise AzureAnalysisServicesRefreshException(
            f"Azure Analysis Services trigger returned unexpected refresh status {refresh_status!r}"
        )
    return refresh_id


class AzureAnalysisServicesRefreshTrigger(BaseTrigger):
    """
    Poll an Azure Analysis Services model refresh until it reaches a terminal status.

    When ``refresh_id`` is ``None`` the trigger starts a new refresh and yields its ID without
    polling; the caller defers again with that ID to wait for completion. Serializing the actual
    refresh ID is what makes the polling stage survive a triggerer restart.

    :param conn_id: The Azure Analysis Services connection ID.
    :param server_name: The Analysis Services server name.
    :param database: The model database name.
    :param refresh_id: The refresh operation ID to poll, or ``None`` to start a new refresh.
    :param refresh_type: The refresh type used when starting a new refresh.
    :param poke_interval: Time in seconds between status requests.
    :param request_timeout: Timeout in seconds for each HTTP request.
    """

    def __init__(
        self,
        *,
        conn_id: str,
        server_name: str,
        database: str,
        refresh_id: str | None = None,
        refresh_type: RefreshType = "full",
        poke_interval: float = 60,
        request_timeout: float = 60,
    ) -> None:
        super().__init__()
        if poke_interval <= 0:
            raise ValueError("poke_interval must be greater than zero")
        if request_timeout <= 0:
            raise ValueError("request_timeout must be greater than zero")
        self.conn_id = conn_id
        self.server_name = server_name
        self.database = database
        self.refresh_id = refresh_id
        self.refresh_type = refresh_type
        self.poke_interval = poke_interval
        self.request_timeout = request_timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "conn_id": self.conn_id,
                "server_name": self.server_name,
                "database": self.database,
                "refresh_id": self.refresh_id,
                "refresh_type": self.refresh_type,
                "poke_interval": self.poke_interval,
                "request_timeout": self.request_timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Start the refresh when needed, then poll it without blocking the triggerer."""
        hook = AzureAnalysisServicesHook(
            azure_analysis_services_conn_id=self.conn_id,
            request_timeout=self.request_timeout,
        )
        refresh_id = self.refresh_id
        try:
            if refresh_id is None:
                refresh_id = await hook.trigger_refresh(
                    server_name=self.server_name,
                    database=self.database,
                    refresh_type=self.refresh_type,
                )
                self.log.info("Triggered Azure Analysis Services refresh %s", refresh_id)
                yield TriggerEvent(
                    {
                        "status": "success",
                        "refresh_status": None,
                        "message": f"Refresh {refresh_id} has been triggered",
                        "refresh_id": refresh_id,
                    }
                )
                return

            while True:
                status = await hook.get_refresh_status(
                    server_name=self.server_name,
                    database=self.database,
                    refresh_id=refresh_id,
                )
                self.log.info("Refresh %s status: %s", refresh_id, status)

                if status == AzureAnalysisServicesRefreshStatus.SUCCEEDED:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "refresh_status": status,
                            "message": f"Refresh {refresh_id} completed successfully",
                            "refresh_id": refresh_id,
                        }
                    )
                    return
                if status in AzureAnalysisServicesRefreshStatus.FAILURE_STATUSES:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "refresh_status": status,
                            "message": f"Refresh {refresh_id} finished with status {status}",
                            "refresh_id": refresh_id,
                        }
                    )
                    return

                await asyncio.sleep(self.poke_interval)
        except Exception as error:
            message = str(error) or type(error).__name__
            yield TriggerEvent(
                {
                    "status": "error",
                    "refresh_status": None,
                    "message": message,
                    "refresh_id": refresh_id,
                }
            )
        finally:
            await hook.aclose()
