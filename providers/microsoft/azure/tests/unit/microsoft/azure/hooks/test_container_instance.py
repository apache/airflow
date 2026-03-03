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
from azure.identity.aio import ClientSecretCredential as AsyncClientSecretCredential
from azure.mgmt.containerinstance.aio import (
    ContainerInstanceManagementClient as AsyncContainerInstanceManagementClient,
)
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    Logs,
    ResourceRequests,
    ResourceRequirements,
)

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.container_instance import (
    AzureContainerInstanceAsyncHook,
    AzureContainerInstanceHook,
)


@pytest.fixture
def connection_without_login_password_tenant_id(create_mock_connection):
    return create_mock_connection(
        Connection(
            conn_id="azure_container_instance_test",
            conn_type="azure_container_instances",
            extra={"subscriptionId": "subscription_id"},
        )
    )


class TestAzureContainerInstanceHook:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connection):
        mock_connection = create_mock_connection(
            Connection(
                conn_id="azure_container_instance_test",
                conn_type="azure_container_instances",
                login="login",
                password="key",
                extra={
                    "tenantId": "63e85d06-62e4-11ee-8c99-0242ac120002",
                    "subscriptionId": "63e85d06-62e4-11ee-8c99-0242ac120003",
                },
            )
        )
        self.resources = ResourceRequirements(requests=ResourceRequests(memory_in_gb="4", cpu="1"))
        self.hook = AzureContainerInstanceHook(azure_conn_id=mock_connection.conn_id)
        with (
            patch("azure.mgmt.containerinstance.ContainerInstanceManagementClient"),
            patch(
                "azure.common.credentials.ServicePrincipalCredentials.__init__",
                autospec=True,
                return_value=None,
            ),
        ):
            yield

    @patch("azure.mgmt.containerinstance.models.ContainerGroup")
    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.begin_create_or_update")
    def test_create_or_update(self, create_or_update_mock, container_group_mock):
        self.hook.create_or_update("resource_group", "aci-test", container_group_mock)
        create_or_update_mock.assert_called_once_with("resource_group", "aci-test", container_group_mock)

    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.get")
    def test_get_state(self, get_state_mock):
        self.hook.get_state("resource_group", "aci-test")
        get_state_mock.assert_called_once_with("resource_group", "aci-test")

    @patch("azure.mgmt.containerinstance.operations.ContainersOperations.list_logs")
    def test_get_logs(self, list_logs_mock):
        expected_messages = ["log line 1\n", "log line 2\n", "log line 3\n"]
        logs = Logs(content="".join(expected_messages))
        list_logs_mock.return_value = logs

        logs = self.hook.get_logs("resource_group", "name", "name")

        assert logs == expected_messages

    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.begin_delete")
    def test_delete(self, delete_mock):
        self.hook.delete("resource_group", "aci-test")
        delete_mock.assert_called_once_with("resource_group", "aci-test")

    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.list_by_resource_group")
    def test_exists_with_existing(self, list_mock):
        list_mock.return_value = [
            ContainerGroup(
                os_type="Linux",
                containers=[Container(name="test1", image="hello-world", resources=self.resources)],
            )
        ]
        assert not self.hook.exists("test", "test1")

    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.list_by_resource_group")
    def test_exists_with_not_existing(self, list_mock):
        list_mock.return_value = [
            ContainerGroup(
                os_type="Linux",
                containers=[Container(name="test1", image="hello-world", resources=self.resources)],
            )
        ]
        assert not self.hook.exists("test", "not found")

    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.list")
    def test_connection_success(self, mock_container_groups_list):
        mock_container_groups_list.return_value = iter([])
        status, msg = self.hook.test_connection()
        assert status is True
        assert msg == "Successfully connected to Azure Container Instance."

    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.list")
    def test_connection_failure(self, mock_container_groups_list):
        mock_container_groups_list.side_effect = Exception("Authentication failed.")
        status, msg = self.hook.test_connection()
        assert status is False
        assert msg == "Authentication failed."


class TestAzureContainerInstanceHookWithoutSetupCredential:
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.ContainerInstanceManagementClient")
    @patch("azure.common.credentials.ServicePrincipalCredentials")
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.get_sync_default_azure_credential")
    def test_get_conn_fallback_to_default_azure_credential(
        self,
        mock_default_azure_credential,
        mock_service_pricipal_credential,
        mock_client_cls,
        connection_without_login_password_tenant_id,
    ):
        mock_credential = MagicMock()
        mock_default_azure_credential.return_value = mock_credential

        mock_client_instance = MagicMock()
        mock_client_cls.return_value = mock_client_instance

        hook = AzureContainerInstanceHook(azure_conn_id=connection_without_login_password_tenant_id.conn_id)
        conn = hook.get_conn()

        mock_default_azure_credential.assert_called_with(
            managed_identity_client_id=None, workload_identity_tenant_id=None
        )
        assert not mock_service_pricipal_credential.called
        assert conn == mock_client_instance
        mock_client_cls.assert_called_once_with(
            credential=mock_credential,
            subscription_id="subscription_id",
        )


@pytest.fixture
def async_conn_with_credentials(create_mock_connection):
    return create_mock_connection(
        Connection(
            conn_id="azure_aci_async_test",
            conn_type="azure_container_instance",
            login="client-id",
            password="client-secret",
            extra={
                "tenantId": "tenant-id",
                "subscriptionId": "subscription-id",
            },
        )
    )


@pytest.fixture
def async_conn_without_credentials(create_mock_connection):
    return create_mock_connection(
        Connection(
            conn_id="azure_aci_async_no_creds",
            conn_type="azure_container_instance",
            extra={"subscriptionId": "subscription-id"},
        )
    )


class TestAzureContainerInstanceAsyncHook:
    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.AsyncContainerInstanceManagementClient"
    )
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.AsyncClientSecretCredential")
    @pytest.mark.asyncio
    async def test_get_async_conn_with_client_secret(
        self,
        mock_credential_cls,
        mock_client_cls,
        async_conn_with_credentials,
    ):
        mock_credential = MagicMock(spec=AsyncClientSecretCredential)
        mock_credential_cls.return_value = mock_credential
        mock_client_instance = MagicMock(spec=AsyncContainerInstanceManagementClient)
        mock_client_cls.return_value = mock_client_instance

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        conn = await hook.get_async_conn()

        mock_credential_cls.assert_called_once_with(
            client_id="client-id",
            client_secret="client-secret",
            tenant_id="tenant-id",
        )
        mock_client_cls.assert_called_once_with(
            credential=mock_credential,
            subscription_id="subscription-id",
        )
        assert conn == mock_client_instance

    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.AsyncContainerInstanceManagementClient"
    )
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.get_async_default_azure_credential")
    @pytest.mark.asyncio
    async def test_get_async_conn_with_default_credential(
        self,
        mock_default_cred,
        mock_client_cls,
        async_conn_without_credentials,
    ):
        mock_credential = MagicMock(spec=AsyncClientSecretCredential)
        mock_default_cred.return_value = mock_credential
        mock_client_instance = MagicMock(spec=AsyncContainerInstanceManagementClient)
        mock_client_cls.return_value = mock_client_instance

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_without_credentials.conn_id)
        conn = await hook.get_async_conn()

        mock_default_cred.assert_called_once_with(
            managed_identity_client_id=None,
            workload_identity_tenant_id=None,
        )
        assert conn == mock_client_instance

    @pytest.mark.asyncio
    async def test_get_async_conn_returns_cached_connection(self, async_conn_with_credentials):
        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        mock_conn = MagicMock(spec=AsyncContainerInstanceManagementClient)
        hook._async_conn = mock_conn

        conn = await hook.get_async_conn()
        assert conn is mock_conn

    @pytest.mark.asyncio
    async def test_get_state(self, async_conn_with_credentials):
        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        mock_client = MagicMock()
        mock_cg = MagicMock(spec=ContainerGroup)
        mock_client.container_groups.get = AsyncMock(return_value=mock_cg)
        hook._async_conn = mock_client

        result = await hook.get_state("my-rg", "my-container")

        mock_client.container_groups.get.assert_called_once_with("my-rg", "my-container")
        assert result is mock_cg

    @pytest.mark.asyncio
    async def test_get_logs(self, async_conn_with_credentials):
        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        mock_client = MagicMock()
        mock_logs = MagicMock(spec=Logs)
        mock_logs.content = "line1\nline2\n"
        mock_client.containers.list_logs = AsyncMock(return_value=mock_logs)
        hook._async_conn = mock_client

        result = await hook.get_logs("my-rg", "my-container")

        mock_client.containers.list_logs.assert_called_once_with(
            "my-rg", "my-container", "my-container", tail=1000
        )
        assert result == ["line1\n", "line2\n"]

    @pytest.mark.asyncio
    async def test_get_logs_returns_none_sentinel_when_content_is_none(self, async_conn_with_credentials):
        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        mock_client = MagicMock()
        mock_logs = MagicMock(spec=Logs)
        mock_logs.content = None
        mock_client.containers.list_logs = AsyncMock(return_value=mock_logs)
        hook._async_conn = mock_client

        result = await hook.get_logs("my-rg", "my-container")
        assert result == [None]

    @pytest.mark.asyncio
    async def test_delete(self, async_conn_with_credentials):
        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        mock_client = MagicMock()
        mock_client.container_groups.begin_delete = AsyncMock()
        hook._async_conn = mock_client

        await hook.delete("my-rg", "my-container")

        mock_client.container_groups.begin_delete.assert_called_once_with("my-rg", "my-container")

    @pytest.mark.asyncio
    async def test_close(self, async_conn_with_credentials):
        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        mock_client = AsyncMock(spec=AsyncContainerInstanceManagementClient)
        hook._async_conn = mock_client

        await hook.close()

        mock_client.close.assert_called_once()
        assert hook._async_conn is None

    @pytest.mark.asyncio
    async def test_close_when_no_connection(self, async_conn_with_credentials):
        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        hook._async_conn = None
        await hook.close()

    @pytest.mark.asyncio
    async def test_async_context_manager_calls_close(self, async_conn_with_credentials):
        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        mock_client = AsyncMock(spec=AsyncContainerInstanceManagementClient)
        hook._async_conn = mock_client

        async with hook as h:
            assert h is hook

        mock_client.close.assert_called_once()
        assert hook._async_conn is None
