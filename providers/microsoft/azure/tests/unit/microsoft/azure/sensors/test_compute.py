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

import pytest

from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.microsoft.azure.sensors.compute import AzureVirtualMachineStateSensor

RESOURCE_GROUP = "test-rg"
VM_NAME = "test-vm"
CONN_ID = "azure_default"


class TestAzureVirtualMachineStateSensor:
    def test_init(self):
        sensor = AzureVirtualMachineStateSensor(
            task_id="sense_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            target_state="running",
            azure_conn_id=CONN_ID,
        )
        assert sensor.resource_group_name == RESOURCE_GROUP
        assert sensor.vm_name == VM_NAME
        assert sensor.target_state == "running"
        assert sensor.azure_conn_id == CONN_ID

    def test_init_invalid_target_state(self):
        with pytest.raises(ValueError, match="Invalid target_state"):
            AzureVirtualMachineStateSensor(
                task_id="sense_vm",
                resource_group_name=RESOURCE_GROUP,
                vm_name=VM_NAME,
                target_state="invalid_state",
            )

    def test_template_fields(self):
        sensor = AzureVirtualMachineStateSensor(
            task_id="sense_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            target_state="running",
        )
        assert "resource_group_name" in sensor.template_fields
        assert "vm_name" in sensor.template_fields
        assert "target_state" in sensor.template_fields

    @pytest.mark.parametrize(
        ("return_value", "expected"),
        [
            ("running", True),
            ("deallocated", False),
        ],
    )
    @patch("airflow.providers.microsoft.azure.sensors.compute.AzureComputeHook")
    def test_poke(self, mock_hook_cls, return_value, expected):
        mock_hook_cls.return_value.get_power_state.return_value = return_value

        sensor = AzureVirtualMachineStateSensor(
            task_id="sense_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            target_state="running",
        )
        assert sensor.poke(context=None) is expected

    @patch("airflow.providers.microsoft.azure.sensors.compute.AzureComputeHook")
    def test_deferrable_mode(self, mock_hook_cls):
        mock_hook_cls.return_value.get_power_state.return_value = "deallocated"

        sensor = AzureVirtualMachineStateSensor(
            task_id="sense_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            target_state="running",
            deferrable=True,
        )
        with pytest.raises(TaskDeferred):
            sensor.execute(context=None)

    def test_execute_complete_success(self):
        sensor = AzureVirtualMachineStateSensor(
            task_id="sense_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            target_state="running",
        )
        # Should not raise
        sensor.execute_complete(
            context=None,
            event={"status": "success", "message": "VM reached running state"},
        )

    def test_execute_complete_error(self):
        sensor = AzureVirtualMachineStateSensor(
            task_id="sense_vm",
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            target_state="running",
        )
        with pytest.raises(RuntimeError, match="Something went wrong"):
            sensor.execute_complete(
                context=None,
                event={"status": "error", "message": "Something went wrong"},
            )
