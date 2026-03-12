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

from unittest import mock

import pytest

from airflow.providers.microsoft.azure.triggers.compute import AzureVirtualMachineStateTrigger
from airflow.triggers.base import TriggerEvent

RESOURCE_GROUP = "test-rg"
VM_NAME = "test-vm"
TARGET_STATE = "running"
CONN_ID = "azure_default"
POKE_INTERVAL = 10.0


class TestAzureVirtualMachineStateTrigger:
    def test_serialize(self):
        trigger = AzureVirtualMachineStateTrigger(
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            target_state=TARGET_STATE,
            azure_conn_id=CONN_ID,
            poke_interval=POKE_INTERVAL,
        )

        actual = trigger.serialize()

        assert isinstance(actual, tuple)
        assert (
            actual[0]
            == f"{AzureVirtualMachineStateTrigger.__module__}.{AzureVirtualMachineStateTrigger.__name__}"
        )
        assert actual[1] == {
            "resource_group_name": RESOURCE_GROUP,
            "vm_name": VM_NAME,
            "target_state": TARGET_STATE,
            "azure_conn_id": CONN_ID,
            "poke_interval": POKE_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.compute.AzureComputeHook.async_get_power_state",
        new_callable=mock.AsyncMock,
    )
    async def test_run_immediate_success(self, mock_get_power_state):
        mock_get_power_state.return_value = TARGET_STATE

        trigger = AzureVirtualMachineStateTrigger(
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            target_state=TARGET_STATE,
            azure_conn_id=CONN_ID,
            poke_interval=POKE_INTERVAL,
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "message": f"VM {VM_NAME} reached state '{TARGET_STATE}'."}
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep", return_value=None)
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.compute.AzureComputeHook.async_get_power_state",
        new_callable=mock.AsyncMock,
    )
    async def test_run_polls_until_success(self, mock_get_power_state, mock_sleep):
        mock_get_power_state.side_effect = ["deallocated", TARGET_STATE]

        trigger = AzureVirtualMachineStateTrigger(
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            target_state=TARGET_STATE,
            azure_conn_id=CONN_ID,
            poke_interval=POKE_INTERVAL,
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert mock_get_power_state.call_count == 2
        assert response == TriggerEvent(
            {"status": "success", "message": f"VM {VM_NAME} reached state '{TARGET_STATE}'."}
        )

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.compute.AzureComputeHook.async_get_power_state",
        new_callable=mock.AsyncMock,
    )
    async def test_run_error(self, mock_get_power_state):
        mock_get_power_state.side_effect = Exception("API error")

        trigger = AzureVirtualMachineStateTrigger(
            resource_group_name=RESOURCE_GROUP,
            vm_name=VM_NAME,
            target_state=TARGET_STATE,
            azure_conn_id=CONN_ID,
            poke_interval=POKE_INTERVAL,
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "error", "message": "API error"})
