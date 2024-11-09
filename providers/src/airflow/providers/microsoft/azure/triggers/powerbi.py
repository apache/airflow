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
import time
from typing import TYPE_CHECKING, AsyncIterator

from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshStatus,
    PowerBIHook,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from msgraph_core import APIVersion


class PowerBITrigger(BaseTrigger):
    """
    Triggers when Power BI dataset refresh is completed.

    Wait for termination will always be True.

    :param conn_id: The connection Id to connect to PowerBI.
    :param timeout: The HTTP timeout being used by the `KiotaRequestAdapter` (default is None).
        When no timeout is specified or set to None then there is no HTTP timeout on each request.
    :param proxies: A dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as `v1.0` or `beta`.
    :param dataset_id: The dataset Id to refresh.
    :param group_id: The workspace Id where dataset is located.
    :param end_time: Time in seconds when trigger should stop polling.
    :param check_interval: Time in seconds to wait between each poll.
    :param wait_for_termination: Wait for the dataset refresh to complete or fail.
    """

    def __init__(
        self,
        conn_id: str,
        dataset_id: str,
        group_id: str,
        timeout: float = 60 * 60 * 24 * 7,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
        check_interval: int = 60,
        wait_for_termination: bool = True,
    ):
        super().__init__()
        self.hook = PowerBIHook(conn_id=conn_id, proxies=proxies, api_version=api_version, timeout=timeout)
        self.dataset_id = dataset_id
        self.timeout = timeout
        self.group_id = group_id
        self.check_interval = check_interval
        self.wait_for_termination = wait_for_termination

    def serialize(self):
        """Serialize the trigger instance."""
        return (
            "airflow.providers.microsoft.azure.triggers.powerbi.PowerBITrigger",
            {
                "conn_id": self.conn_id,
                "proxies": self.proxies,
                "api_version": self.api_version,
                "dataset_id": self.dataset_id,
                "group_id": self.group_id,
                "timeout": self.timeout,
                "check_interval": self.check_interval,
                "wait_for_termination": self.wait_for_termination,
            },
        )

    @property
    def conn_id(self) -> str:
        return self.hook.conn_id

    @property
    def proxies(self) -> dict | None:
        return self.hook.proxies

    @property
    def api_version(self) -> APIVersion | str:
        return self.hook.api_version

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to the PowerBI and polls for the dataset refresh status."""
        self.dataset_refresh_id = await self.hook.trigger_dataset_refresh(
            dataset_id=self.dataset_id,
            group_id=self.group_id,
        )

        async def fetch_refresh_status() -> str:
            """Fetch the current status of the dataset refresh."""
            refresh_details = await self.hook.get_refresh_details_by_refresh_id(
                dataset_id=self.dataset_id,
                group_id=self.group_id,
                refresh_id=self.dataset_refresh_id,
            )
            return refresh_details["status"]

        try:
            dataset_refresh_status = await fetch_refresh_status()
            start_time = time.monotonic()
            while start_time + self.timeout > time.monotonic():
                dataset_refresh_status = await fetch_refresh_status()

                if dataset_refresh_status == PowerBIDatasetRefreshStatus.COMPLETED:
                    yield TriggerEvent(
                        {
                            "status": dataset_refresh_status,
                            "message": f"The dataset refresh {self.dataset_refresh_id} has {dataset_refresh_status}.",
                            "dataset_refresh_id": self.dataset_refresh_id,
                        }
                    )
                    return
                elif dataset_refresh_status == PowerBIDatasetRefreshStatus.FAILED:
                    yield TriggerEvent(
                        {
                            "status": dataset_refresh_status,
                            "message": f"The dataset refresh {self.dataset_refresh_id} has {dataset_refresh_status}.",
                            "dataset_refresh_id": self.dataset_refresh_id,
                        }
                    )
                    return

                self.log.info(
                    "Sleeping for %s. The dataset refresh status is %s.",
                    self.check_interval,
                    dataset_refresh_status,
                )
                await asyncio.sleep(self.check_interval)

            yield TriggerEvent(
                {
                    "status": "error",
                    "message": f"Timeout occurred while waiting for dataset refresh to complete: The dataset refresh {self.dataset_refresh_id} has status {dataset_refresh_status}.",
                    "dataset_refresh_id": self.dataset_refresh_id,
                }
            )
            return
        except Exception as error:
            if self.dataset_refresh_id:
                try:
                    self.log.info(
                        "Unexpected error %s caught. Canceling dataset refresh %s",
                        error,
                        self.dataset_refresh_id,
                    )
                    await self.hook.cancel_dataset_refresh(
                        dataset_id=self.dataset_id,
                        group_id=self.group_id,
                        dataset_refresh_id=self.dataset_refresh_id,
                    )
                except Exception as e:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"An error occurred while canceling dataset: {e}",
                            "dataset_refresh_id": self.dataset_refresh_id,
                        }
                    )
                    return
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": f"An error occurred: {error}",
                    "dataset_refresh_id": self.dataset_refresh_id,
                }
            )
