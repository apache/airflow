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
from collections.abc import AsyncIterator
from functools import cached_property
from typing import TYPE_CHECKING, Any

import tenacity

from airflow.providers.microsoft.azure.hooks.powerbi import (
    PowerBIDatasetRefreshException,
    PowerBIDatasetRefreshStatus,
    PowerBIHook,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from kiota_abstractions.request_adapter import RequestAdapter
    from msgraph_core import APIVersion


class BasePowerBITrigger(BaseTrigger):
    """
    Base class for all PowerBI related triggers.

    :param conn_id: The connection Id to connect to PowerBI.
    :param timeout: The HTTP timeout being used by the `KiotaRequestAdapter` (default is None).
        When no timeout is specified or set to None then there is no HTTP timeout on each request.
    :param proxies: A dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as `v1.0` or `beta`.
    """

    def __init__(
        self,
        conn_id: str,
        timeout: float = 60 * 60 * 24 * 7,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.timeout = timeout
        self.proxies = proxies
        self.api_version = api_version

    def get_conn(self) -> RequestAdapter:
        """
        Initiate a new RequestAdapter connection.

        .. warning::
           This method is deprecated.
        """
        return self.hook.get_conn()

    @cached_property
    def hook(self) -> PowerBIHook:
        return PowerBIHook(
            conn_id=self.conn_id,
            timeout=self.timeout,
            proxies=self.proxies,
            api_version=self.api_version,
        )


class PowerBITrigger(BasePowerBITrigger):
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
    :param dataset_refresh_id: The dataset refresh Id to poll for the status, if not provided a new refresh will be triggered.
    :param group_id: The workspace Id where dataset is located.
    :param check_interval: Time in seconds to wait between each poll.
    :param wait_for_termination: Wait for the dataset refresh to complete or fail.
    :param request_body: Additional arguments to pass to the request body, as described in https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/refresh-dataset-in-group#request-body.
    """

    def __init__(
        self,
        conn_id: str,
        dataset_id: str,
        group_id: str,
        timeout: float = 60 * 60 * 24 * 7,
        dataset_refresh_id: str | None = None,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
        check_interval: int = 60,
        wait_for_termination: bool = True,
        request_body: dict[str, Any] | None = None,
    ):
        super().__init__(conn_id=conn_id, timeout=timeout, proxies=proxies, api_version=api_version)
        self.dataset_id = dataset_id
        self.dataset_refresh_id = dataset_refresh_id
        self.group_id = group_id
        self.check_interval = check_interval
        self.wait_for_termination = wait_for_termination
        self.request_body = request_body

    def serialize(self):
        """Serialize the trigger instance."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "conn_id": self.conn_id,
                "proxies": self.proxies,
                "api_version": self.api_version,
                "dataset_id": self.dataset_id,
                "dataset_refresh_id": self.dataset_refresh_id,
                "group_id": self.group_id,
                "timeout": self.timeout,
                "check_interval": self.check_interval,
                "wait_for_termination": self.wait_for_termination,
                "request_body": self.request_body,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to the PowerBI and polls for the dataset refresh status."""
        if not self.dataset_refresh_id:
            # Trigger the dataset refresh
            dataset_refresh_id = await self.hook.trigger_dataset_refresh(
                dataset_id=self.dataset_id,
                group_id=self.group_id,
                request_body=self.request_body,
            )

            if dataset_refresh_id:
                self.log.info("Triggered dataset refresh %s", dataset_refresh_id)
                yield TriggerEvent(
                    {
                        "status": "success",
                        "dataset_refresh_status": None,
                        "message": f"The dataset refresh {dataset_refresh_id} has been triggered.",
                        "dataset_refresh_id": dataset_refresh_id,
                    }
                )
                return

            yield TriggerEvent(
                {
                    "status": "error",
                    "dataset_refresh_status": None,
                    "message": "Failed to trigger the dataset refresh.",
                    "dataset_refresh_id": None,
                }
            )
            return

        # The dataset refresh is already triggered. Poll for the dataset refresh status.
        @tenacity.retry(
            stop=tenacity.stop_after_attempt(3),
            wait=tenacity.wait_exponential(min=5, multiplier=2),
            reraise=True,
            retry=tenacity.retry_if_exception_type(PowerBIDatasetRefreshException),
        )
        async def fetch_refresh_status_and_error() -> tuple[str, str]:
            """Fetch the current status and error of the dataset refresh."""
            if self.dataset_refresh_id:
                refresh_details = await self.hook.get_refresh_details_by_refresh_id(
                    dataset_id=self.dataset_id,
                    group_id=self.group_id,
                    refresh_id=self.dataset_refresh_id,
                )
                return refresh_details["status"], refresh_details["error"]

            raise PowerBIDatasetRefreshException("Dataset refresh Id is missing.")

        try:
            dataset_refresh_status, dataset_refresh_error = await fetch_refresh_status_and_error()
            start_time = time.monotonic()
            while start_time + self.timeout > time.monotonic():
                dataset_refresh_status, dataset_refresh_error = await fetch_refresh_status_and_error()

                if dataset_refresh_status == PowerBIDatasetRefreshStatus.COMPLETED:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "dataset_refresh_status": dataset_refresh_status,
                            "message": f"The dataset refresh {self.dataset_refresh_id} has {dataset_refresh_status}.",
                            "dataset_refresh_id": self.dataset_refresh_id,
                        }
                    )
                    return
                elif dataset_refresh_status in PowerBIDatasetRefreshStatus.FAILURE_STATUSES:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "dataset_refresh_status": dataset_refresh_status,
                            "message": f"The dataset refresh {self.dataset_refresh_id} has {dataset_refresh_status}. Error: {dataset_refresh_error}",
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
                    "dataset_refresh_status": dataset_refresh_status,
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
                            "dataset_refresh_status": None,
                            "message": f"An error occurred while canceling dataset: {e}",
                            "dataset_refresh_id": self.dataset_refresh_id,
                        }
                    )
                    return
            yield TriggerEvent(
                {
                    "status": "error",
                    "dataset_refresh_status": None,
                    "message": f"An error occurred: {error}",
                    "dataset_refresh_id": self.dataset_refresh_id,
                }
            )


class PowerBIWorkspaceListTrigger(BasePowerBITrigger):
    """
    Triggers a call to the API to request the available workspace IDs.

    :param conn_id: The connection Id to connect to PowerBI.
    :param timeout: The HTTP timeout being used by the `KiotaRequestAdapter`. Default is 1 week (60s * 60m * 24h * 7d).
        When no timeout is specified or set to None then there is no HTTP timeout on each request.
    :param proxies: A dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as `v1.0` or `beta`.
    """

    def __init__(
        self,
        conn_id: str,
        workspace_ids: list[str] | None = None,
        timeout: float = 60 * 60 * 24 * 7,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
    ):
        super().__init__(conn_id=conn_id, timeout=timeout, proxies=proxies, api_version=api_version)
        self.workspace_ids = workspace_ids

    def serialize(self):
        """Serialize the trigger instance."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "conn_id": self.conn_id,
                "proxies": self.proxies,
                "api_version": self.api_version,
                "timeout": self.timeout,
                "workspace_ids": self.workspace_ids,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to the PowerBI and polls for the list of workspace IDs."""
        # Trigger the API to get the workspace list
        workspace_ids = await self.hook.get_workspace_list()

        if workspace_ids:
            self.log.info("Triggered request to get workspace list.")
            yield TriggerEvent(
                {
                    "status": "success",
                    "message": "The workspace list get request has been successful.",
                    "workspace_ids": workspace_ids,
                }
            )
            return

        yield TriggerEvent(
            {
                "status": "error",
                "message": "Error grabbing the workspace list.",
                "workspace_ids": None,
            }
        )
        return


class PowerBIDatasetListTrigger(BasePowerBITrigger):
    """
    Triggers a call to the API to request the available dataset IDs.

    :param conn_id: The connection Id to connect to PowerBI.
    :param group_id: The group Id to list discoverable datasets.
    :param timeout: The HTTP timeout being used by the `KiotaRequestAdapter`. Default is 1 week (60s * 60m * 24h * 7d).
        When no timeout is specified or set to None then there is no HTTP timeout on each request.
    :param proxies: A dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the Microsoft Graph API to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as `v1.0` or `beta`.
    """

    def __init__(
        self,
        conn_id: str,
        group_id: str,
        dataset_ids: list[str] | None = None,
        timeout: float = 60 * 60 * 24 * 7,
        proxies: dict | None = None,
        api_version: APIVersion | str | None = None,
    ):
        super().__init__(conn_id=conn_id, timeout=timeout, proxies=proxies, api_version=api_version)
        self.group_id = group_id
        self.dataset_ids = dataset_ids

    def serialize(self):
        """Serialize the trigger instance."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "conn_id": self.conn_id,
                "proxies": self.proxies,
                "api_version": self.api_version,
                "timeout": self.timeout,
                "group_id": self.group_id,
                "dataset_ids": self.dataset_ids,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to the PowerBI and polls for the list of dataset IDs."""
        # Trigger the API to get the dataset list
        dataset_ids = await self.hook.get_dataset_list(
            group_id=self.group_id,
        )

        if dataset_ids:
            self.log.info("Triggered request to get dataset list.")
            yield TriggerEvent(
                {
                    "status": "success",
                    "message": f"The dataset list get request from workspace {self.group_id} has been successful.",
                    "dataset_ids": dataset_ids,
                }
            )
            return

        yield TriggerEvent(
            {
                "status": "error",
                "message": "Error grabbing the dataset list.",
                "dataset_ids": None,
            }
        )
        return
