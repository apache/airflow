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

from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseSensorOperator, conf
from airflow.providers.microsoft.azure.hooks.compute import AzureComputeHook
from airflow.providers.microsoft.azure.triggers.compute import AzureVirtualMachineStateTrigger

if TYPE_CHECKING:
    from airflow.sdk import Context


class AzureVirtualMachineStateSensor(BaseSensorOperator):
    """
    Poll an Azure Virtual Machine until it reaches a target power state.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:AzureVirtualMachineStateSensor`

    :param resource_group_name: Name of the Azure resource group.
    :param vm_name: Name of the virtual machine.
    :param target_state: Desired power state, e.g. ``running``, ``deallocated``.
    :param azure_conn_id: Azure connection id.
    :param deferrable: If True, run in deferrable mode.
    """

    template_fields: Sequence[str] = ("resource_group_name", "vm_name", "target_state")
    ui_color = "#0078d4"
    ui_fgcolor = "#ffffff"

    VALID_STATES = frozenset({"running", "deallocated", "stopped", "starting", "deallocating"})

    def __init__(
        self,
        *,
        resource_group_name: str,
        vm_name: str,
        target_state: str,
        azure_conn_id: str = "azure_default",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        if target_state not in self.VALID_STATES:
            raise ValueError(
                f"Invalid target_state: {target_state}. Must be one of {sorted(self.VALID_STATES)}"
            )
        super().__init__(**kwargs)
        self.resource_group_name = resource_group_name
        self.vm_name = vm_name
        self.target_state = target_state
        self.azure_conn_id = azure_conn_id
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        hook = AzureComputeHook(azure_conn_id=self.azure_conn_id)
        current_state = hook.get_power_state(self.resource_group_name, self.vm_name)
        self.log.info("VM %s power state: %s", self.vm_name, current_state)
        return current_state == self.target_state

    def execute(self, context: Context) -> None:
        """
        Poll for the VM power state.

        In deferrable mode, the polling is deferred to the triggerer. Otherwise
        the sensor waits synchronously.
        """
        if not self.deferrable:
            super().execute(context=context)
        else:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=AzureVirtualMachineStateTrigger(
                        resource_group_name=self.resource_group_name,
                        vm_name=self.vm_name,
                        target_state=self.target_state,
                        azure_conn_id=self.azure_conn_id,
                        poke_interval=self.poke_interval,
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:
        """
        Handle callback from the trigger.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event:
            if event["status"] == "error":
                raise RuntimeError(event["message"])
            self.log.info(event["message"])
        else:
            raise RuntimeError("Did not receive valid event from the triggerer")
