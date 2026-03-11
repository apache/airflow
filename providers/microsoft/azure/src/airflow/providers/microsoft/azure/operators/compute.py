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

from abc import abstractmethod
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.microsoft.azure.hooks.compute import AzureComputeHook

if TYPE_CHECKING:
    from airflow.sdk import Context


class BaseAzureVirtualMachineOperator(BaseOperator):
    """
    Base operator for Azure Virtual Machine operations.

    :param resource_group_name: Name of the Azure resource group.
    :param vm_name: Name of the virtual machine.
    :param wait_for_completion: Wait for the operation to complete. Default True.
    :param azure_conn_id: Azure connection id.
    """

    template_fields: Sequence[str] = ("resource_group_name", "vm_name")
    ui_color = "#0078d4"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        resource_group_name: str,
        vm_name: str,
        wait_for_completion: bool = True,
        azure_conn_id: str = "azure_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource_group_name = resource_group_name
        self.vm_name = vm_name
        self.wait_for_completion = wait_for_completion
        self.azure_conn_id = azure_conn_id

    @cached_property
    def hook(self) -> AzureComputeHook:
        return AzureComputeHook(azure_conn_id=self.azure_conn_id)

    @abstractmethod
    def execute(self, context: Context) -> None: ...


class AzureVirtualMachineStartOperator(BaseAzureVirtualMachineOperator):
    """
    Start an Azure Virtual Machine.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureVirtualMachineStartOperator`

    :param resource_group_name: Name of the Azure resource group.
    :param vm_name: Name of the virtual machine.
    :param wait_for_completion: Wait for the VM to reach 'running' state. Default True.
    :param azure_conn_id: Azure connection id.
    """

    def execute(self, context: Context) -> None:
        self.hook.start_instance(
            resource_group_name=self.resource_group_name,
            vm_name=self.vm_name,
            wait_for_completion=self.wait_for_completion,
        )
        self.log.info("VM %s started successfully.", self.vm_name)


class AzureVirtualMachineStopOperator(BaseAzureVirtualMachineOperator):
    """
    Stop (deallocate) an Azure Virtual Machine.

    Uses ``begin_deallocate`` to release compute resources and stop billing.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureVirtualMachineStopOperator`

    :param resource_group_name: Name of the Azure resource group.
    :param vm_name: Name of the virtual machine.
    :param wait_for_completion: Wait for the VM to reach 'deallocated' state. Default True.
    :param azure_conn_id: Azure connection id.
    """

    def execute(self, context: Context) -> None:
        self.hook.stop_instance(
            resource_group_name=self.resource_group_name,
            vm_name=self.vm_name,
            wait_for_completion=self.wait_for_completion,
        )
        self.log.info("VM %s stopped (deallocated) successfully.", self.vm_name)


class AzureVirtualMachineRestartOperator(BaseAzureVirtualMachineOperator):
    """
    Restart an Azure Virtual Machine.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureVirtualMachineRestartOperator`

    :param resource_group_name: Name of the Azure resource group.
    :param vm_name: Name of the virtual machine.
    :param wait_for_completion: Wait for the VM to reach 'running' state. Default True.
    :param azure_conn_id: Azure connection id.
    """

    def execute(self, context: Context) -> None:
        self.hook.restart_instance(
            resource_group_name=self.resource_group_name,
            vm_name=self.vm_name,
            wait_for_completion=self.wait_for_completion,
        )
        self.log.info("VM %s restarted successfully.", self.vm_name)
