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
from datetime import UTC, datetime
from typing import Any

from azure.core.exceptions import AzureError

from airflow.providers.microsoft.azure.hooks.container_apps import (
    AzureContainerAppsJobAsyncHook,
    get_execution_result,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AzureContainerAppsJobTrigger(BaseTrigger):
    """
    Monitor the exact execution returned by an Azure Container Apps Job submission.

    :param resource_group: Resource group containing the job.
    :param job_name: Name of the existing job.
    :param execution_name: Execution to monitor.
    :param azure_conn_id: Azure connection ID.
    :param subscription_id: Explicit subscription ID or None to use the connection.
    :param poll_interval_seconds: Seconds between polls.
    :param deadline: Absolute UTC timeout deadline, as an ISO formatted string.
    :param cancel_on_termination: Stop the execution on explicit task termination.
        Requires Airflow support for trigger ``on_kill``.
    """

    def __init__(
        self,
        *,
        resource_group: str,
        job_name: str,
        execution_name: str,
        azure_conn_id: str = "azure_default",
        subscription_id: str | None = None,
        poll_interval_seconds: float = 60,
        deadline: str | None = None,
        cancel_on_termination: bool = True,
    ) -> None:
        super().__init__()
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than zero")
        self.resource_group = resource_group
        self.job_name = job_name
        self.execution_name = execution_name
        self.azure_conn_id = azure_conn_id
        self.subscription_id = subscription_id
        self.poll_interval_seconds = poll_interval_seconds
        self.deadline = deadline
        self.cancel_on_termination = cancel_on_termination
        self._terminal_observed = False

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the execution identity and original deadline for redistribution."""
        return (
            "airflow.providers.microsoft.azure.triggers.container_apps.AzureContainerAppsJobTrigger",
            {
                "resource_group": self.resource_group,
                "job_name": self.job_name,
                "execution_name": self.execution_name,
                "azure_conn_id": self.azure_conn_id,
                "subscription_id": self.subscription_id,
                "poll_interval_seconds": self.poll_interval_seconds,
                "deadline": self.deadline,
                "cancel_on_termination": self.cancel_on_termination,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll until terminal; stop best effort on timeout and emit one result."""
        expires_at = None
        if self.deadline is not None:
            initial_remaining = (datetime.fromisoformat(self.deadline) - datetime.now(UTC)).total_seconds()
            expires_at = time.monotonic() + max(0, initial_remaining)
        hook = AzureContainerAppsJobAsyncHook(self.azure_conn_id, self.subscription_id)
        while True:
            remaining = None if expires_at is None else expires_at - time.monotonic()
            if remaining is not None and remaining <= 0:
                break
            lookup_timeout = asyncio.timeout(remaining)
            try:
                async with lookup_timeout:
                    execution = await hook.async_get_execution(
                        self.resource_group, self.job_name, self.execution_name
                    )
            except TimeoutError:
                if not lookup_timeout.expired():
                    raise
                break
            if expires_at is not None:
                remaining = expires_at - time.monotonic()
                if remaining <= 0:
                    break
            result = get_execution_result(self.execution_name, execution)
            if result is not None:
                self._terminal_observed = True
                yield TriggerEvent(result)
                return
            delay = self.poll_interval_seconds
            if remaining is not None:
                delay = min(delay, remaining)
            await asyncio.sleep(delay)
        await self._stop_execution()
        self._terminal_observed = True
        yield TriggerEvent(
            {
                "status": "error",
                "execution_name": self.execution_name,
                "message": f"Execution {self.execution_name!r} timed out",
            }
        )

    async def _stop_execution(self) -> None:
        try:
            hook = AzureContainerAppsJobAsyncHook(self.azure_conn_id, self.subscription_id)
            await hook.async_stop_execution(self.resource_group, self.job_name, self.execution_name)
        except AzureError:
            self.log.warning("Failed to stop execution %s", self.execution_name, exc_info=True)

    async def on_kill(self) -> None:
        """Stop on explicit termination only; generic cleanup must never cancel Azure work."""
        if self.cancel_on_termination and not self._terminal_observed:
            await self._stop_execution()
