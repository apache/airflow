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
from unittest.mock import AsyncMock

import pytest
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerPropertiesInstanceView,
    ContainerState,
)

from airflow.providers.microsoft.azure.hooks.container_instance import AzureContainerInstanceAsyncHook
from airflow.providers.microsoft.azure.triggers.container_instance import AzureContainerInstanceTrigger
from airflow.triggers.base import TriggerEvent

RESOURCE_GROUP = "test-resource-group"
CONTAINER_NAME = "test-container"
CI_CONN_ID = "azure_container_instance_test"
POLLING_INTERVAL = 5.0

MODULE = "airflow.providers.microsoft.azure.triggers.container_instance"


def _make_cg_state(state: str, exit_code: int = 0, detail_status: str = "test") -> ContainerGroup:
    """Build a minimal fake ContainerGroup using real SDK models (read-only attrs use type: ignore)."""
    container_state = ContainerState()
    container_state.state = state  # type: ignore[assignment]
    container_state.exit_code = exit_code  # type: ignore[assignment]
    container_state.detail_status = detail_status  # type: ignore[assignment]
    instance_view = ContainerPropertiesInstanceView()
    instance_view.current_state = container_state  # type: ignore[assignment]
    container = Container(name="test", image="test", resources="test")  # type: ignore[arg-type]
    container.instance_view = instance_view  # type: ignore[assignment]
    cg = ContainerGroup(containers=[container], os_type="Linux")
    cg.provisioning_state = state  # type: ignore[assignment]
    return cg


def _make_provisioning_state(state: str) -> ContainerGroup:
    """Build a ContainerGroup where instance_view is None (provisioning phase)."""
    container = Container(name="test", image="test", resources="test")  # type: ignore[arg-type]
    container.instance_view = None  # type: ignore[assignment]
    cg = ContainerGroup(containers=[container], os_type="Linux")
    cg.provisioning_state = state  # type: ignore[assignment]
    return cg


class TestAzureContainerInstanceTrigger:
    def test_serialize(self):
        trigger = AzureContainerInstanceTrigger(
            resource_group=RESOURCE_GROUP,
            name=CONTAINER_NAME,
            ci_conn_id=CI_CONN_ID,
            polling_interval=POLLING_INTERVAL,
        )
        classpath, kwargs = trigger.serialize()

        assert classpath == (
            "airflow.providers.microsoft.azure.triggers.container_instance.AzureContainerInstanceTrigger"
        )
        assert kwargs == {
            "resource_group": RESOURCE_GROUP,
            "name": CONTAINER_NAME,
            "ci_conn_id": CI_CONN_ID,
            "polling_interval": POLLING_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureContainerInstanceAsyncHook")
    async def test_run_yields_success_on_terminated_with_exit_code_zero(self, mock_hook_cls):
        mock_hook = AsyncMock(spec=AzureContainerInstanceAsyncHook)
        mock_hook_cls.return_value = mock_hook
        mock_hook.__aenter__ = AsyncMock(return_value=mock_hook)
        mock_hook.__aexit__ = AsyncMock(return_value=None)
        mock_hook.get_state = AsyncMock(return_value=_make_cg_state("Terminated", exit_code=0))

        trigger = AzureContainerInstanceTrigger(
            resource_group=RESOURCE_GROUP,
            name=CONTAINER_NAME,
            ci_conn_id=CI_CONN_ID,
            polling_interval=POLLING_INTERVAL,
        )
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert isinstance(events[0], TriggerEvent)
        assert events[0].payload["status"] == "success"
        assert events[0].payload["exit_code"] == 0

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureContainerInstanceAsyncHook")
    async def test_run_yields_success_on_succeeded_state(self, mock_hook_cls):
        mock_hook = AsyncMock(spec=AzureContainerInstanceAsyncHook)
        mock_hook_cls.return_value = mock_hook
        mock_hook.__aenter__ = AsyncMock(return_value=mock_hook)
        mock_hook.__aexit__ = AsyncMock(return_value=None)
        mock_hook.get_state = AsyncMock(return_value=_make_cg_state("Succeeded", exit_code=0))

        trigger = AzureContainerInstanceTrigger(
            resource_group=RESOURCE_GROUP,
            name=CONTAINER_NAME,
            ci_conn_id=CI_CONN_ID,
        )
        events = [event async for event in trigger.run()]

        assert events[0].payload["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureContainerInstanceAsyncHook")
    async def test_run_yields_error_on_terminated_with_nonzero_exit_code(self, mock_hook_cls):
        mock_hook = AsyncMock(spec=AzureContainerInstanceAsyncHook)
        mock_hook_cls.return_value = mock_hook
        mock_hook.__aenter__ = AsyncMock(return_value=mock_hook)
        mock_hook.__aexit__ = AsyncMock(return_value=None)
        mock_hook.get_state = AsyncMock(return_value=_make_cg_state("Terminated", exit_code=1))

        trigger = AzureContainerInstanceTrigger(
            resource_group=RESOURCE_GROUP,
            name=CONTAINER_NAME,
            ci_conn_id=CI_CONN_ID,
        )
        events = [event async for event in trigger.run()]

        assert events[0].payload["status"] == "error"
        assert events[0].payload["exit_code"] == 1

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureContainerInstanceAsyncHook")
    @pytest.mark.parametrize("terminal_state", ["Failed", "Unhealthy"])
    async def test_run_yields_error_on_failed_states(self, mock_hook_cls, terminal_state):
        mock_hook = AsyncMock(spec=AzureContainerInstanceAsyncHook)
        mock_hook_cls.return_value = mock_hook
        mock_hook.__aenter__ = AsyncMock(return_value=mock_hook)
        mock_hook.__aexit__ = AsyncMock(return_value=None)
        mock_hook.get_state = AsyncMock(return_value=_make_cg_state(terminal_state, exit_code=1))

        trigger = AzureContainerInstanceTrigger(
            resource_group=RESOURCE_GROUP,
            name=CONTAINER_NAME,
            ci_conn_id=CI_CONN_ID,
        )
        events = [event async for event in trigger.run()]

        assert events[0].payload["status"] == "error"

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock)
    @mock.patch(f"{MODULE}.AzureContainerInstanceAsyncHook")
    async def test_run_polls_through_non_terminal_states(self, mock_hook_cls, mock_sleep):
        mock_hook = AsyncMock(spec=AzureContainerInstanceAsyncHook)
        mock_hook_cls.return_value = mock_hook
        mock_hook.__aenter__ = AsyncMock(return_value=mock_hook)
        mock_hook.__aexit__ = AsyncMock(return_value=None)
        mock_hook.get_state = AsyncMock(
            side_effect=[
                _make_provisioning_state("Creating"),
                _make_cg_state("Running", exit_code=0),
                _make_cg_state("Terminated", exit_code=0),
            ]
        )

        trigger = AzureContainerInstanceTrigger(
            resource_group=RESOURCE_GROUP,
            name=CONTAINER_NAME,
            ci_conn_id=CI_CONN_ID,
            polling_interval=POLLING_INTERVAL,
        )
        events = [event async for event in trigger.run()]

        assert mock_hook.get_state.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(POLLING_INTERVAL)
        assert events[0].payload["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureContainerInstanceAsyncHook")
    async def test_run_yields_error_event_on_exception(self, mock_hook_cls):
        mock_hook = AsyncMock(spec=AzureContainerInstanceAsyncHook)
        mock_hook_cls.return_value = mock_hook
        mock_hook.__aenter__ = AsyncMock(return_value=mock_hook)
        mock_hook.__aexit__ = AsyncMock(return_value=None)
        mock_hook.get_state = AsyncMock(side_effect=Exception("Azure API error"))

        trigger = AzureContainerInstanceTrigger(
            resource_group=RESOURCE_GROUP,
            name=CONTAINER_NAME,
            ci_conn_id=CI_CONN_ID,
        )
        events = [event async for event in trigger.run()]

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
        assert "Azure API error" in events[0].payload["message"]

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.AzureContainerInstanceAsyncHook")
    async def test_run_provisioning_state_does_not_yield_until_terminal(self, mock_hook_cls):
        """Container in 'Creating' (no instance_view) should keep polling."""
        mock_hook = AsyncMock(spec=AzureContainerInstanceAsyncHook)
        mock_hook_cls.return_value = mock_hook
        mock_hook.__aenter__ = AsyncMock(return_value=mock_hook)
        mock_hook.__aexit__ = AsyncMock(return_value=None)

        with mock.patch(f"{MODULE}.asyncio.sleep", new_callable=AsyncMock):
            mock_hook.get_state = AsyncMock(
                side_effect=[
                    _make_provisioning_state("Creating"),
                    _make_cg_state("Terminated", exit_code=0),
                ]
            )

            trigger = AzureContainerInstanceTrigger(
                resource_group=RESOURCE_GROUP,
                name=CONTAINER_NAME,
                ci_conn_id=CI_CONN_ID,
            )
            events = [event async for event in trigger.run()]

        assert mock_hook.get_state.call_count == 2
        assert events[0].payload["status"] == "success"

    def test_serialize_deserialize_roundtrip(self):
        original = AzureContainerInstanceTrigger(
            resource_group=RESOURCE_GROUP,
            name=CONTAINER_NAME,
            ci_conn_id=CI_CONN_ID,
            polling_interval=10.0,
        )
        classpath, kwargs = original.serialize()
        reconstructed = AzureContainerInstanceTrigger(**kwargs)
        _, kwargs2 = reconstructed.serialize()
        assert kwargs == kwargs2
