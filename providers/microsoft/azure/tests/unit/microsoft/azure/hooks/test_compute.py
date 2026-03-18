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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.compute import AzureComputeHook

CONN_ID = "azure_compute_test"


class TestAzureComputeHook:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connection):
        create_mock_connection(
            Connection(
                conn_id=CONN_ID,
                conn_type="azure_compute",
                login="client_id",
                password="client_secret",
                extra={
                    "tenantId": "tenant_id",
                    "subscriptionId": "subscription_id",
                },
            )
        )

    @patch("airflow.providers.microsoft.azure.hooks.compute.ComputeManagementClient")
    @patch("airflow.providers.microsoft.azure.hooks.compute.ClientSecretCredential")
    def test_get_conn_returns_compute_management_client(self, mock_credential, mock_client):
        hook = AzureComputeHook(azure_conn_id=CONN_ID)
        result = hook.get_conn()

        mock_credential.assert_called_once_with(
            client_id="client_id",
            client_secret="client_secret",
            tenant_id="tenant_id",
        )
        mock_client.assert_called_once_with(
            credential=mock_credential.return_value,
            subscription_id="subscription_id",
        )
        assert result == mock_client.return_value

    @patch("airflow.providers.microsoft.azure.hooks.compute.ComputeManagementClient")
    @patch("airflow.providers.microsoft.azure.hooks.compute.get_sync_default_azure_credential")
    def test_get_conn_with_default_azure_credential(
        self, mock_default_cred, mock_client, create_mock_connection
    ):
        create_mock_connection(
            Connection(
                conn_id="azure_no_login",
                conn_type="azure_compute",
                extra={"subscriptionId": "sub_id"},
            )
        )
        hook = AzureComputeHook(azure_conn_id="azure_no_login")
        hook.get_conn()

        mock_default_cred.assert_called_once_with(
            managed_identity_client_id=None,
            workload_identity_tenant_id=None,
        )
        mock_client.assert_called_once_with(
            credential=mock_default_cred.return_value,
            subscription_id="sub_id",
        )

    @patch("airflow.providers.microsoft.azure.hooks.compute.get_client_from_auth_file")
    def test_get_conn_with_key_path(self, mock_get_client_from_auth_file, create_mock_connection):
        create_mock_connection(
            Connection(
                conn_id="azure_with_key_path",
                conn_type="azure_compute",
                extra={"key_path": "/tmp/azure-key.json"},
            )
        )
        hook = AzureComputeHook(azure_conn_id="azure_with_key_path")
        conn = hook.get_conn()

        mock_get_client_from_auth_file.assert_called_once_with(
            client_class=hook.sdk_client, auth_path="/tmp/azure-key.json"
        )
        assert conn == mock_get_client_from_auth_file.return_value

    @patch("airflow.providers.microsoft.azure.hooks.compute.get_client_from_json_dict")
    def test_get_conn_with_key_json(self, mock_get_client_from_json_dict, create_mock_connection):
        create_mock_connection(
            Connection(
                conn_id="azure_with_key_json",
                conn_type="azure_compute",
                extra={"key_json": {"tenantId": "tenant", "subscriptionId": "sub"}},
            )
        )
        hook = AzureComputeHook(azure_conn_id="azure_with_key_json")
        hook.get_conn()

        mock_get_client_from_json_dict.assert_called_once_with(
            client_class=hook.sdk_client, config_dict={"tenantId": "tenant", "subscriptionId": "sub"}
        )

    @patch("airflow.providers.microsoft.azure.hooks.compute.ComputeManagementClient")
    @patch("airflow.providers.microsoft.azure.hooks.compute.ClientSecretCredential")
    def test_start_instance(self, mock_credential, mock_client):
        mock_poller = MagicMock()
        mock_client.return_value.virtual_machines.begin_start.return_value = mock_poller

        hook = AzureComputeHook(azure_conn_id=CONN_ID)
        hook.start_instance(resource_group_name="rg", vm_name="vm1")

        mock_client.return_value.virtual_machines.begin_start.assert_called_once_with("rg", "vm1")
        mock_poller.result.assert_called_once()
        mock_poller.result.reset_mock()

        hook.start_instance(resource_group_name="rg", vm_name="vm1", wait_for_completion=False)
        mock_poller.result.assert_not_called()

    @patch("airflow.providers.microsoft.azure.hooks.compute.ComputeManagementClient")
    @patch("airflow.providers.microsoft.azure.hooks.compute.ClientSecretCredential")
    def test_stop_instance(self, mock_credential, mock_client):
        mock_poller = MagicMock()
        mock_client.return_value.virtual_machines.begin_deallocate.return_value = mock_poller

        hook = AzureComputeHook(azure_conn_id=CONN_ID)
        hook.stop_instance(resource_group_name="rg", vm_name="vm1")

        mock_client.return_value.virtual_machines.begin_deallocate.assert_called_once_with("rg", "vm1")
        mock_poller.result.assert_called_once()

    @patch("airflow.providers.microsoft.azure.hooks.compute.ComputeManagementClient")
    @patch("airflow.providers.microsoft.azure.hooks.compute.ClientSecretCredential")
    def test_restart_instance(self, mock_credential, mock_client):
        mock_poller = MagicMock()
        mock_client.return_value.virtual_machines.begin_restart.return_value = mock_poller

        hook = AzureComputeHook(azure_conn_id=CONN_ID)
        hook.restart_instance(resource_group_name="rg", vm_name="vm1")

        mock_client.return_value.virtual_machines.begin_restart.assert_called_once_with("rg", "vm1")
        mock_poller.result.assert_called_once()

    @pytest.mark.parametrize(
        ("status_code", "expected"),
        [
            ("PowerState/running", "running"),
            ("PowerState/deallocated", "deallocated"),
            ("foo/bar", "unknown"),
            (None, "unknown"),
        ],
    )
    @patch("airflow.providers.microsoft.azure.hooks.compute.ComputeManagementClient")
    @patch("airflow.providers.microsoft.azure.hooks.compute.ClientSecretCredential")
    def test_get_power_state(self, mock_credential, mock_client, status_code, expected):
        mock_instance_view = MagicMock()
        mock_instance_view.statuses = [
            MagicMock(code="ProvisioningState/succeeded"),
            MagicMock(code=status_code),
        ]
        mock_client.return_value.virtual_machines.instance_view.return_value = mock_instance_view

        hook = AzureComputeHook(azure_conn_id=CONN_ID)
        state = hook.get_power_state(resource_group_name="rg", vm_name="vm1")

        assert state == expected

    @patch("airflow.providers.microsoft.azure.hooks.compute.ComputeManagementClient")
    @patch("airflow.providers.microsoft.azure.hooks.compute.ClientSecretCredential")
    def test_test_connection_success(self, mock_credential, mock_client):
        mock_client.return_value.virtual_machines.list_all.return_value = iter([])

        hook = AzureComputeHook(azure_conn_id=CONN_ID)
        result, message = hook.test_connection()

        assert result is True
        assert "Successfully" in message

    @patch("airflow.providers.microsoft.azure.hooks.compute.ComputeManagementClient")
    @patch("airflow.providers.microsoft.azure.hooks.compute.ClientSecretCredential")
    def test_test_connection_failure(self, mock_credential, mock_client):
        mock_client.return_value.virtual_machines.list_all.side_effect = Exception("Auth failed")

        hook = AzureComputeHook(azure_conn_id=CONN_ID)
        result, message = hook.test_connection()

        assert result is False
        assert "Auth failed" in message

    # ------------------------------------------------------------------
    # Async interface tests
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    @patch("airflow.providers.microsoft.azure.hooks.compute.AsyncComputeManagementClient")
    @patch("airflow.providers.microsoft.azure.hooks.compute.AsyncClientSecretCredential")
    @patch(
        "airflow.providers.microsoft.azure.hooks.compute.get_async_connection",
        new_callable=AsyncMock,
    )
    async def test_get_async_conn_with_service_principal(
        self, mock_get_async_connection, mock_async_cred, mock_async_client
    ):
        mock_conn = MagicMock()
        mock_conn.login = "client_id"
        mock_conn.password = "client_secret"
        mock_conn.extra_dejson = {"tenantId": "tenant_id", "subscriptionId": "sub_id"}
        mock_get_async_connection.return_value = mock_conn

        hook = AzureComputeHook(azure_conn_id=CONN_ID)
        conn = await hook.get_async_conn()

        mock_async_cred.assert_called_once_with(
            client_id="client_id",
            client_secret="client_secret",
            tenant_id="tenant_id",
        )
        mock_async_client.assert_called_once_with(
            credential=mock_async_cred.return_value,
            subscription_id="sub_id",
        )
        assert conn == mock_async_client.return_value

    @pytest.mark.asyncio
    @patch(
        "airflow.providers.microsoft.azure.hooks.compute.AzureComputeHook.get_async_conn",
        new_callable=AsyncMock,
    )
    async def test_async_get_power_state(self, mock_get_async_conn):
        mock_status_power = MagicMock(code="PowerState/running")
        mock_status_prov = MagicMock(code="ProvisioningState/succeeded")
        mock_instance_view = MagicMock(statuses=[mock_status_prov, mock_status_power])
        mock_client = AsyncMock()
        mock_client.virtual_machines.instance_view.return_value = mock_instance_view
        mock_get_async_conn.return_value = mock_client

        hook = AzureComputeHook(azure_conn_id=CONN_ID)
        state = await hook.async_get_power_state("rg", "vm1")

        assert state == "running"
        mock_client.virtual_machines.instance_view.assert_called_once_with("rg", "vm1")

    @pytest.mark.asyncio
    @patch(
        "airflow.providers.microsoft.azure.hooks.compute.AzureComputeHook.get_async_conn",
        new_callable=AsyncMock,
    )
    async def test_async_context_manager_closes_conn(self, mock_get_async_conn):
        mock_client = AsyncMock()
        mock_get_async_conn.return_value = mock_client

        hook = AzureComputeHook(azure_conn_id=CONN_ID)
        async with hook as h:
            assert h is hook
            h._async_conn = mock_client

        mock_client.close.assert_called_once()
