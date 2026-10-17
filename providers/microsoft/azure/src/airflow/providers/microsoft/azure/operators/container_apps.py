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

import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from azure.core.exceptions import AzureError, ServiceRequestTimeoutError, ServiceResponseTimeoutError

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.microsoft.azure.exceptions import AzureContainerAppsJobExecutionError
from airflow.providers.microsoft.azure.hooks.container_apps import (
    AzureContainerAppsJobHook,
    get_execution_result,
)
from airflow.providers.microsoft.azure.triggers.container_apps import AzureContainerAppsJobTrigger

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class AzureContainerAppsJobOperator(BaseOperator):
    """
    Start an existing Azure Container Apps Job and wait for its exact execution.

    Set task ``retries=0``: after an ambiguous submission response, retrying may
    start another execution. Azure transport retries are disabled for submission.

    :param resource_group: Resource group containing the job.
    :param job_name: Name of the existing job.
    :param azure_conn_id: Azure connection ID.
    :param subscription_id: Subscription ID; defaults to connection extra ``subscriptionId``.
    :param container_name: Regular container to override. Required with any overrides.
    :param image: Replacement image for the named container.
    :param command: Replacement command for the named container.
    :param args: Replacement arguments for the named container.
    :param env_vars: Replacement environment for the named container, not a merge.
    :param cpu: Replacement CPU allocation for the named container.
    :param memory: Replacement memory allocation, for example ``"1Gi"``.
    :param deferrable: Release the worker while waiting for the execution.
    :param poll_interval_seconds: Seconds between execution status polls.
    :param timeout_seconds: Maximum wait after submission, or None for no deadline.
    :param cancel_on_termination: Stop this execution on explicit termination.
        Deferred cancellation requires Airflow support for trigger ``on_kill``.
    """

    template_fields: Sequence[str] = (
        "azure_conn_id",
        "subscription_id",
        "resource_group",
        "job_name",
        "container_name",
        "image",
        "command",
        "args",
        "env_vars",
        "cpu",
        "memory",
    )
    template_fields_renderers = {"command": "json", "args": "json", "env_vars": "json"}

    def __init__(
        self,
        *,
        resource_group: str,
        job_name: str,
        azure_conn_id: str = "azure_default",
        subscription_id: str | None = None,
        container_name: str | None = None,
        image: str | None = None,
        command: Sequence[str] | None = None,
        args: Sequence[str] | None = None,
        env_vars: Mapping[str, str] | None = None,
        cpu: float | str | None = None,
        memory: str | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval_seconds: float = 60,
        timeout_seconds: float | None = None,
        cancel_on_termination: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than zero")
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        self.resource_group = resource_group
        self.job_name = job_name
        self.azure_conn_id = azure_conn_id
        self.subscription_id = subscription_id
        self.container_name = container_name
        self.image = image
        self.command = command
        self.args = args
        self.env_vars = env_vars
        self.cpu = cpu
        self.memory = memory
        self.deferrable = deferrable
        self.poll_interval_seconds = poll_interval_seconds
        self.timeout_seconds = timeout_seconds
        self.cancel_on_termination = cancel_on_termination
        self._execution_name: str | None = None
        self._terminal_observed = False

    @cached_property
    def hook(self) -> AzureContainerAppsJobHook:
        """Create the hook after target and override fields have been templated."""
        return AzureContainerAppsJobHook(self.azure_conn_id, self.subscription_id)

    def execute(self, context: Context) -> str:
        """Submit once, then poll on the worker or defer to the triggerer."""
        if not self.resource_group or not self.job_name:
            raise ValueError("resource_group and job_name are required")
        execution_name = self.hook.start_job(
            self.resource_group,
            self.job_name,
            container_name=self.container_name,
            image=self.image,
            command=self.command,
            args=self.args,
            env_vars=self.env_vars,
            cpu=float(self.cpu) if self.cpu is not None else None,
            memory=self.memory,
        )
        self._execution_name = execution_name
        started_at = time.monotonic()
        if self.deferrable:
            deadline = None
            if self.timeout_seconds is not None:
                deadline = (datetime.now(UTC) + timedelta(seconds=self.timeout_seconds)).isoformat()
            self.defer(
                trigger=AzureContainerAppsJobTrigger(
                    resource_group=self.resource_group,
                    job_name=self.job_name,
                    execution_name=execution_name,
                    azure_conn_id=self.azure_conn_id,
                    subscription_id=self.subscription_id,
                    poll_interval_seconds=self.poll_interval_seconds,
                    deadline=deadline,
                    cancel_on_termination=self.cancel_on_termination,
                ),
                method_name="execute_complete",
            )
        while True:
            remaining = (
                None
                if self.timeout_seconds is None
                else self.timeout_seconds - (time.monotonic() - started_at)
            )
            if remaining is not None and remaining <= 0:
                break
            try:
                execution = self.hook.get_execution(
                    self.resource_group, self.job_name, execution_name, timeout_seconds=remaining
                )
            except (TimeoutError, ServiceRequestTimeoutError, ServiceResponseTimeoutError):
                if self.timeout_seconds is None or time.monotonic() - started_at < self.timeout_seconds:
                    raise
                break
            if self.timeout_seconds is not None:
                remaining = self.timeout_seconds - (time.monotonic() - started_at)
                if remaining <= 0:
                    break
            result = get_execution_result(execution_name, execution)
            if result is not None:
                self._terminal_observed = True
                return self.execute_complete(context, result)
            delay = self.poll_interval_seconds
            if remaining is not None:
                delay = min(delay, remaining)
            time.sleep(delay)
        self._stop_execution()
        self._terminal_observed = True
        raise AzureContainerAppsJobExecutionError(f"Execution {execution_name!r} timed out")

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> str:
        """Return the execution name for XCom, or raise the reported execution failure."""
        if not event or event.get("status") != "success" or not event.get("execution_name"):
            raise AzureContainerAppsJobExecutionError(
                (event or {}).get("message") or "Container Apps Job trigger returned no successful execution"
            )
        return event["execution_name"]

    def _stop_execution(self) -> None:
        if self._execution_name is None:
            return
        try:
            self.hook.stop_execution(self.resource_group, self.job_name, self._execution_name)
        except (AzureError, TimeoutError):
            self.log.warning("Failed to stop execution %s", self._execution_name, exc_info=True)

    def on_kill(self) -> None:
        """Stop the submitted execution on explicit worker termination when enabled."""
        if self.cancel_on_termination and not self._terminal_observed:
            self._stop_execution()
