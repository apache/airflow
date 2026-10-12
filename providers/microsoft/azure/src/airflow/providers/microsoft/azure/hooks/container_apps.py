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

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from azure.core.exceptions import ResourceNotFoundError
from azure.core.pipeline.policies import AsyncRetryPolicy, RetryPolicy
from azure.identity import ClientSecretCredential
from azure.identity.aio import ClientSecretCredential as AsyncClientSecretCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.appcontainers.aio import ContainerAppsAPIClient as AsyncContainerAppsAPIClient
from azure.mgmt.appcontainers.models import (
    ContainerResources,
    EnvironmentVar,
    JobExecution,
    JobExecutionContainer,
    JobExecutionTemplate,
    JobTemplate,
)

from airflow.providers.common.compat.connection import get_async_connection
from airflow.providers.microsoft.azure.exceptions import AzureContainerAppsJobExecutionError
from airflow.providers.microsoft.azure.hooks.base_azure import AzureBaseHook
from airflow.providers.microsoft.azure.utils import (
    get_async_default_azure_credential,
    get_sync_default_azure_credential,
)

if TYPE_CHECKING:
    from azure.identity import DefaultAzureCredential
    from azure.identity.aio import DefaultAzureCredential as AsyncDefaultAzureCredential
    from azure.mgmt.appcontainers.models import Container, InitContainer

    from airflow.sdk import Connection


def build_execution_template(
    template: JobTemplate,
    container_name: str,
    *,
    image: str | None = None,
    command: Sequence[str] | None = None,
    args: Sequence[str] | None = None,
    env_vars: Mapping[str, str] | None = None,
    cpu: float | None = None,
    memory: str | None = None,
) -> JobExecutionTemplate:
    """Copy regular and init containers and override only the named regular container."""

    def copy_container(container: Container | InitContainer) -> JobExecutionContainer:
        return JobExecutionContainer(
            name=container.name,
            image=container.image,
            command=deepcopy(container.command),
            args=deepcopy(container.args),
            env=deepcopy(container.env),
            resources=deepcopy(container.resources),
        )

    execution_template = JobExecutionTemplate(
        containers=[copy_container(container) for container in template.containers or ()],
        init_containers=[copy_container(container) for container in template.init_containers or ()],
    )
    target = next(
        (container for container in execution_template.containers or () if container.name == container_name),
        None,
    )
    if target is None:
        raise ValueError(f"No regular container {container_name!r} in the job template")
    if image is not None:
        target.image = image
    if command is not None:
        target.command = list(command)
    if args is not None:
        target.args = list(args)
    if env_vars is not None:
        target.env = [EnvironmentVar(name=name, value=value) for name, value in env_vars.items()]
    if cpu is not None or memory is not None:
        target.resources = target.resources or ContainerResources()
        if cpu is not None:
            target.resources.cpu = cpu
        if memory is not None:
            target.resources.memory = memory
    return execution_template


def get_execution_result(execution_name: str, execution: JobExecution | None) -> dict[str, Any] | None:
    """Return a terminal result, or None while the execution remains active."""
    if execution is None:
        return {
            "status": "error",
            "execution_name": execution_name,
            "message": f"Execution {execution_name!r} was not found in Azure execution history",
        }
    status = execution.status or ""
    normalized = status.strip().lower()
    if normalized in {"", "running", "processing", "unknown"}:
        return None
    if normalized == "succeeded":
        return {"status": "success", "execution_name": execution_name}
    diagnostics = execution.message or execution.reason
    suffix = f": {diagnostics}" if diagnostics else ""
    return {
        "status": "error",
        "execution_name": execution_name,
        "message": f"Execution {execution_name!r} reached status {status!r}{suffix}",
    }


class AzureContainerAppsJobHook(AzureBaseHook):
    """
    Start and monitor executions of existing Azure Container Apps Jobs.

    Supports service principal credentials (login, password and ``tenantId``)
    and DefaultAzureCredential, including workload identity.

    :param azure_conn_id: Azure connection ID.
    :param subscription_id: Subscription ID; defaults to the connection's ``subscriptionId``.
    """

    def __init__(self, azure_conn_id: str = "azure_default", subscription_id: str | None = None) -> None:
        super().__init__(conn_id=azure_conn_id)
        self.subscription_id = subscription_id

    def get_subscription_id(self, connection: Connection) -> str:
        """Resolve the subscription from an explicit value or the Azure connection."""
        subscription_id = self.subscription_id or connection.extra_dejson.get("subscriptionId")
        if not subscription_id:
            raise ValueError("subscription_id or Azure connection extra subscriptionId is required")
        return subscription_id

    @contextmanager
    def get_client(self, *, submission: bool = False) -> Iterator[ContainerAppsAPIClient]:
        """Yield a client, closing both its transport and credential on exit."""
        connection = self.get_connection(self.conn_id)
        subscription_id = self.get_subscription_id(connection)
        tenant_id = connection.extra_dejson.get("tenantId")
        credential: ClientSecretCredential | DefaultAzureCredential
        if connection.login and connection.password and tenant_id:
            credential = ClientSecretCredential(
                tenant_id=tenant_id, client_id=connection.login, client_secret=connection.password
            )
        else:
            credential = get_sync_default_azure_credential(
                managed_identity_client_id=connection.extra_dejson.get("managed_identity_client_id"),
                workload_identity_tenant_id=connection.extra_dejson.get("workload_identity_tenant_id"),
            )
        retries = 0 if submission else 3
        with credential:
            with ContainerAppsAPIClient(
                credential=credential,
                subscription_id=subscription_id,
                retry_policy=RetryPolicy(
                    retry_total=retries, retry_connect=retries, retry_read=retries, retry_status=retries
                ),
            ) as client:
                yield client

    def start_job(
        self,
        resource_group: str,
        job_name: str,
        *,
        container_name: str | None = None,
        image: str | None = None,
        command: Sequence[str] | None = None,
        args: Sequence[str] | None = None,
        env_vars: Mapping[str, str] | None = None,
        cpu: float | None = None,
        memory: str | None = None,
    ) -> str:
        """Submit once and return the Azure execution name; never provision a job."""
        template = None
        if any(value is not None for value in (image, command, args, env_vars, cpu, memory)):
            if not container_name:
                raise ValueError("container_name is required when supplying overrides")
            with self.get_client() as client:
                definition = client.jobs.get(resource_group_name=resource_group, job_name=job_name)
            if definition.template is None:
                raise ValueError(f"Job {job_name!r} has no template")
            template = build_execution_template(
                definition.template,
                container_name,
                image=image,
                command=command,
                args=args,
                env_vars=env_vars,
                cpu=cpu,
                memory=memory,
            )
        with self.get_client(submission=True) as client:
            execution = client.jobs.begin_start(
                resource_group_name=resource_group, job_name=job_name, template=template
            ).result()
        if not execution.name:
            raise AzureContainerAppsJobExecutionError("Job submission returned no execution name")
        return execution.name

    def get_execution(self, resource_group: str, job_name: str, execution_name: str) -> JobExecution | None:
        """Read an exact execution, falling back to history only on a direct lookup 404."""
        with self.get_client() as client:
            try:
                return client.job_execution(
                    resource_group_name=resource_group, job_name=job_name, job_execution_name=execution_name
                )
            except ResourceNotFoundError:
                return next(
                    (
                        execution
                        for execution in client.jobs_executions.list(
                            resource_group_name=resource_group, job_name=job_name
                        )
                        if execution.name == execution_name
                    ),
                    None,
                )

    def stop_execution(self, resource_group: str, job_name: str, execution_name: str) -> None:
        """Stop only the specified execution, with bounded SDK retries."""
        with self.get_client() as client:
            client.jobs.begin_stop_execution(
                resource_group_name=resource_group, job_name=job_name, job_execution_name=execution_name
            ).result()


class AzureContainerAppsJobAsyncHook(AzureContainerAppsJobHook):
    """Use the native asynchronous Azure client to monitor and stop executions."""

    @asynccontextmanager
    async def get_async_client(self) -> AsyncIterator[AsyncContainerAppsAPIClient]:
        """Yield an async client, closing both its transport and credential on exit."""
        connection = await get_async_connection(self.conn_id)
        subscription_id = self.get_subscription_id(connection)
        tenant_id = connection.extra_dejson.get("tenantId")
        credential: AsyncClientSecretCredential | AsyncDefaultAzureCredential
        if connection.login and connection.password and tenant_id:
            credential = AsyncClientSecretCredential(
                tenant_id=tenant_id, client_id=connection.login, client_secret=connection.password
            )
        else:
            credential = get_async_default_azure_credential(
                managed_identity_client_id=connection.extra_dejson.get("managed_identity_client_id"),
                workload_identity_tenant_id=connection.extra_dejson.get("workload_identity_tenant_id"),
            )
        async with credential:
            async with AsyncContainerAppsAPIClient(
                credential=credential,
                subscription_id=subscription_id,
                retry_policy=AsyncRetryPolicy(retry_total=3, retry_connect=3, retry_read=3, retry_status=3),
            ) as client:
                yield client

    async def async_get_execution(
        self, resource_group: str, job_name: str, execution_name: str
    ) -> JobExecution | None:
        """Read an exact execution asynchronously, falling back to execution history on 404."""
        async with self.get_async_client() as client:
            try:
                return await client.job_execution(
                    resource_group_name=resource_group, job_name=job_name, job_execution_name=execution_name
                )
            except ResourceNotFoundError:
                async for execution in client.jobs_executions.list(
                    resource_group_name=resource_group, job_name=job_name
                ):
                    if execution.name == execution_name:
                        return execution
                return None

    async def async_stop_execution(self, resource_group: str, job_name: str, execution_name: str) -> None:
        """Stop only the specified execution using the native async SDK."""
        async with self.get_async_client() as client:
            poller = await client.jobs.begin_stop_execution(
                resource_group_name=resource_group, job_name=job_name, job_execution_name=execution_name
            )
            await poller.result()
