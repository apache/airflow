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

from functools import cached_property
from typing import Any, cast

from azure.common.client_factory import get_client_from_auth_file, get_client_from_json_dict
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.identity.aio import (
    ClientSecretCredential as AsyncClientSecretCredential,
    DefaultAzureCredential as AsyncDefaultAzureCredential,
)
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.aio import ComputeManagementClient as AsyncComputeManagementClient

from airflow.providers.common.compat.connection import get_async_connection
from airflow.providers.microsoft.azure.hooks.base_azure import AzureBaseHook
from airflow.providers.microsoft.azure.utils import (
    get_async_default_azure_credential,
    get_sync_default_azure_credential,
)


class AzureComputeHook(AzureBaseHook):
    """
    A hook to interact with Azure Compute to manage Virtual Machines.

    :param azure_conn_id: :ref:`Azure connection id<howto/connection:azure>` of
        a service principal which will be used to manage virtual machines.
    """

    conn_name_attr = "azure_conn_id"
    default_conn_name = "azure_default"
    conn_type = "azure_compute"
    hook_name = "Azure Compute"

    def __init__(self, azure_conn_id: str = default_conn_name) -> None:
        super().__init__(sdk_client=ComputeManagementClient, conn_id=azure_conn_id)
        self._async_conn: AsyncComputeManagementClient | None = None

    @cached_property
    def connection(self) -> ComputeManagementClient:
        return self.get_conn()

    def get_conn(self) -> ComputeManagementClient:
        """
        Authenticate the resource using the connection id passed during init.

        :return: the authenticated ComputeManagementClient.
        """
        conn = self.get_connection(self.conn_id)
        tenant = conn.extra_dejson.get("tenantId")

        key_path = conn.extra_dejson.get("key_path")
        if key_path:
            if not key_path.endswith(".json"):
                raise ValueError("Unrecognised extension for key file.")
            self.log.info("Getting connection using a JSON key file.")
            return get_client_from_auth_file(client_class=self.sdk_client, auth_path=key_path)

        key_json = conn.extra_dejson.get("key_json")
        if key_json:
            self.log.info("Getting connection using a JSON config.")
            return get_client_from_json_dict(client_class=self.sdk_client, config_dict=key_json)

        credential: ClientSecretCredential | DefaultAzureCredential
        if all([conn.login, conn.password, tenant]):
            self.log.info("Getting connection using specific credentials and subscription_id.")
            credential = ClientSecretCredential(
                client_id=cast("str", conn.login),
                client_secret=cast("str", conn.password),
                tenant_id=cast("str", tenant),
            )
        else:
            self.log.info("Using DefaultAzureCredential as credential")
            managed_identity_client_id = conn.extra_dejson.get("managed_identity_client_id")
            workload_identity_tenant_id = conn.extra_dejson.get("workload_identity_tenant_id")
            credential = get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )

        subscription_id = cast("str", conn.extra_dejson.get("subscriptionId"))
        return ComputeManagementClient(
            credential=credential,
            subscription_id=subscription_id,
        )

    def start_instance(
        self, resource_group_name: str, vm_name: str, wait_for_completion: bool = True
    ) -> None:
        """
        Start a virtual machine instance.

        :param resource_group_name: Name of the resource group.
        :param vm_name: Name of the virtual machine.
        :param wait_for_completion: Wait for the operation to complete.
        """
        self.log.info("Starting VM %s in resource group %s", vm_name, resource_group_name)
        poller = self.connection.virtual_machines.begin_start(resource_group_name, vm_name)
        if wait_for_completion:
            poller.result()

    def stop_instance(self, resource_group_name: str, vm_name: str, wait_for_completion: bool = True) -> None:
        """
        Stop (deallocate) a virtual machine instance.

        Uses ``begin_deallocate`` to release compute resources and stop billing.

        :param resource_group_name: Name of the resource group.
        :param vm_name: Name of the virtual machine.
        :param wait_for_completion: Wait for the operation to complete.
        """
        self.log.info("Stopping (deallocating) VM %s in resource group %s", vm_name, resource_group_name)
        poller = self.connection.virtual_machines.begin_deallocate(resource_group_name, vm_name)
        if wait_for_completion:
            poller.result()

    def restart_instance(
        self, resource_group_name: str, vm_name: str, wait_for_completion: bool = True
    ) -> None:
        """
        Restart a virtual machine instance.

        :param resource_group_name: Name of the resource group.
        :param vm_name: Name of the virtual machine.
        :param wait_for_completion: Wait for the operation to complete.
        """
        self.log.info("Restarting VM %s in resource group %s", vm_name, resource_group_name)
        poller = self.connection.virtual_machines.begin_restart(resource_group_name, vm_name)
        if wait_for_completion:
            poller.result()

    def get_power_state(self, resource_group_name: str, vm_name: str) -> str:
        """
        Get the power state of a virtual machine.

        :param resource_group_name: Name of the resource group.
        :param vm_name: Name of the virtual machine.
        :return: Power state string, e.g. ``running``, ``deallocated``, ``stopped``.
        """
        instance_view = self.connection.virtual_machines.instance_view(resource_group_name, vm_name)
        for status in instance_view.statuses or []:
            if status.code and status.code.startswith("PowerState/"):
                return status.code.split("/", 1)[1]
        return "unknown"

    def test_connection(self) -> tuple[bool, str]:
        """Test the Azure Compute connection."""
        try:
            next(self.connection.virtual_machines.list_all(), None)
        except Exception as e:
            return False, str(e)
        return True, "Successfully connected to Azure Compute."

    # ------------------------------------------------------------------
    # Async interface (used by AzureVirtualMachineStateTrigger)
    # ------------------------------------------------------------------

    async def get_async_conn(self) -> AsyncComputeManagementClient:
        """
        Return an authenticated async :class:`~azure.mgmt.compute.aio.ComputeManagementClient`.

        Supports service-principal (login/password + tenantId) and
        DefaultAzureCredential auth.  Legacy ``key_path`` / ``key_json``
        auth files are not supported in the async path.
        """
        if self._async_conn is not None:
            return self._async_conn

        conn = await get_async_connection(self.conn_id)
        tenant = conn.extra_dejson.get("tenantId")
        subscription_id = cast("str", conn.extra_dejson.get("subscriptionId"))

        credential: AsyncClientSecretCredential | AsyncDefaultAzureCredential
        if conn.login and conn.password and tenant:
            credential = AsyncClientSecretCredential(
                client_id=conn.login,
                client_secret=conn.password,
                tenant_id=tenant,
            )
        else:
            managed_identity_client_id = conn.extra_dejson.get("managed_identity_client_id")
            workload_identity_tenant_id = conn.extra_dejson.get("workload_identity_tenant_id")
            credential = get_async_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )

        self._async_conn = AsyncComputeManagementClient(
            credential=credential,
            subscription_id=subscription_id,
        )
        return self._async_conn

    async def async_get_power_state(self, resource_group_name: str, vm_name: str) -> str:
        """
        Get the power state of a virtual machine using the native async client.

        :param resource_group_name: Name of the resource group.
        :param vm_name: Name of the virtual machine.
        :return: Power state string, e.g. ``running``, ``deallocated``, ``stopped``.
        """
        client = await self.get_async_conn()
        instance_view = await client.virtual_machines.instance_view(resource_group_name, vm_name)
        for status in instance_view.statuses or []:
            if status.code and status.code.startswith("PowerState/"):
                return status.code.split("/", 1)[1]
        return "unknown"

    async def close(self) -> None:
        """Close the async connection."""
        if self._async_conn is not None:
            await self._async_conn.close()
            self._async_conn = None

    async def __aenter__(self) -> AzureComputeHook:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
