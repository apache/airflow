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
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent


class AzureVirtualMachineStateTrigger(BaseTrigger):
    """
    Poll the Azure VM power state and yield a TriggerEvent once it matches the target.

    Uses the native async Azure SDK client (``azure.mgmt.compute.aio``) so that
    the triggerer event loop is never blocked.

    :param resource_group_name: Name of the Azure resource group.
    :param vm_name: Name of the virtual machine.
    :param target_state: Desired power state, e.g. ``running``, ``deallocated``.
    :param azure_conn_id: Azure connection id.
    :param poke_interval: Polling interval in seconds.
    """

    def __init__(
        self,
        resource_group_name: str,
        vm_name: str,
        target_state: str,
        azure_conn_id: str = "azure_default",
        poke_interval: float = 30.0,
    ) -> None:
        super().__init__()
        self.resource_group_name = resource_group_name
        self.vm_name = vm_name
        self.target_state = target_state
        self.azure_conn_id = azure_conn_id
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize AzureVirtualMachineStateTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "resource_group_name": self.resource_group_name,
                "vm_name": self.vm_name,
                "target_state": self.target_state,
                "azure_conn_id": self.azure_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll VM power state asynchronously until it matches the target state."""
        from airflow.providers.microsoft.azure.hooks.compute import AzureComputeHook

        try:
            async with AzureComputeHook(azure_conn_id=self.azure_conn_id) as hook:
                while True:
                    power_state = await hook.async_get_power_state(self.resource_group_name, self.vm_name)
                    if power_state == self.target_state:
                        message = f"VM {self.vm_name} reached state '{self.target_state}'."
                        yield TriggerEvent({"status": "success", "message": message})
                        return
                    self.log.info(
                        "VM %s power state: %s. Waiting for %s. Sleeping for %s seconds.",
                        self.vm_name,
                        power_state,
                        self.target_state,
                        self.poke_interval,
                    )
                    await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return
