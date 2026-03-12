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

from unittest.mock import patch

from airflow.providers.microsoft.azure.operators.compute import (
    AzureVirtualMachineRestartOperator,
    AzureVirtualMachineStartOperator,
    AzureVirtualMachineStopOperator,
)

RESOURCE_GROUP = "test-rg"
VM_NAME = "test-vm"
CONN_ID = "azure_default"


class TestAzureVirtualMachineStartOperator:
    def test_init(self):
        op = AzureVirtualMachineStartOperator(
            task_id="start_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            azure_conn_id=CONN_ID,
        )
        assert op.resource_group_name == RESOURCE_GROUP
        assert op.vm_name == VM_NAME
        assert op.wait_for_completion is True
        assert op.azure_conn_id == CONN_ID

    def test_template_fields(self):
        op = AzureVirtualMachineStartOperator(
            task_id="start_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
        )
        assert "resource_group_name" in op.template_fields
        assert "vm_name" in op.template_fields

    @patch("airflow.providers.microsoft.azure.operators.compute.AzureComputeHook")
    def test_execute_start_instance(self, mock_hook_cls):
        op = AzureVirtualMachineStartOperator(
            task_id="start_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
        )
        op.execute(context=None)

        mock_hook_cls.return_value.start_instance.assert_called_once_with(
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            wait_for_completion=True,
        )

    @patch("airflow.providers.microsoft.azure.operators.compute.AzureComputeHook")
    def test_execute_start_instance_no_wait(self, mock_hook_cls):
        op = AzureVirtualMachineStartOperator(
            task_id="start_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            wait_for_completion=False,
        )
        op.execute(context=None)

        mock_hook_cls.return_value.start_instance.assert_called_once_with(
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            wait_for_completion=False,
        )


class TestAzureVirtualMachineStopOperator:
    def test_init(self):
        op = AzureVirtualMachineStopOperator(
            task_id="stop_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
        )
        assert op.resource_group_name == RESOURCE_GROUP
        assert op.vm_name == VM_NAME
        assert op.wait_for_completion is True

    @patch("airflow.providers.microsoft.azure.operators.compute.AzureComputeHook")
    def test_execute_stop_instance(self, mock_hook_cls):
        op = AzureVirtualMachineStopOperator(
            task_id="stop_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
        )
        op.execute(context=None)

        mock_hook_cls.return_value.stop_instance.assert_called_once_with(
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            wait_for_completion=True,
        )


class TestAzureVirtualMachineRestartOperator:
    def test_init(self):
        op = AzureVirtualMachineRestartOperator(
            task_id="restart_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
        )
        assert op.resource_group_name == RESOURCE_GROUP
        assert op.vm_name == VM_NAME
        assert op.wait_for_completion is True

    @patch("airflow.providers.microsoft.azure.operators.compute.AzureComputeHook")
    def test_execute_restart_instance(self, mock_hook_cls):
        op = AzureVirtualMachineRestartOperator(
            task_id="restart_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
        )
        op.execute(context=None)

        mock_hook_cls.return_value.restart_instance.assert_called_once_with(
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            wait_for_completion=True,
        )
