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
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import AzureAuthorityHosts
from azure.identity.aio import ClientSecretCredential as AsyncClientSecretCredential
from azure.mgmt.containerinstance.aio import (
    ContainerInstanceManagementClient as AsyncContainerInstanceManagementClient,
)
from azure.mgmt.containerinstance.models import (
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

    @pytest.mark.parametrize(
        ("content", "expected"),
        [
            ("log line 1\nlog line 2\nlog line 3\n", ["log line 1\n", "log line 2\n", "log line 3\n"]),
            (None, []),
        ],
    )
    @patch("azure.mgmt.containerinstance.operations.ContainersOperations.list_logs")
    def test_get_logs(self, list_logs_mock, content, expected):
        logs = Logs(content=content)
        list_logs_mock.return_value = logs

        result = self.hook.get_logs("resource_group", "name", "name")

        assert result == expected

    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.begin_delete")
    def test_delete(self, delete_mock):
        self.hook.delete("resource_group", "aci-test")
        delete_mock.assert_called_once_with("resource_group", "aci-test")

    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.get")
    def test_exists_with_existing(self, get_mock):
        get_mock.return_value = MagicMock()

        assert self.hook.exists("test", "test1")
        get_mock.assert_called_once_with("test", "test1")

    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.get")
    def test_exists_with_not_existing(self, get_mock):
        get_mock.side_effect = ResourceNotFoundError("not found")

        assert not self.hook.exists("test", "not found")
        get_mock.assert_called_once_with("test", "not found")

    @patch("azure.mgmt.containerinstance.operations.ContainerGroupsOperations.get")
    def test_exists_unexpected_exception(self, get_mock):
        get_mock.side_effect = RuntimeError("Unexpected Exception")

        with pytest.raises(RuntimeError):
            self.hook.exists("test", "test")

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
            base_url="https://management.azure.com",
            credential_scopes=["https://management.azure.com/.default"],
        )


class TestAzureContainerInstanceHookCloudEnvironment:
    @pytest.mark.parametrize(
        ("cloud_env", "expected_authority", "expected_base_url", "expected_scopes"),
        [
            pytest.param(
                None,
                AzureAuthorityHosts.AZURE_PUBLIC_CLOUD,
                "https://management.azure.com",
                ["https://management.azure.com/.default"],
                id="default_public_cloud",
            ),
            pytest.param(
                "AzurePublicCloud",
                AzureAuthorityHosts.AZURE_PUBLIC_CLOUD,
                "https://management.azure.com",
                ["https://management.azure.com/.default"],
                id="explicit_public_cloud",
            ),
            pytest.param(
                "AzureUSGovernment",
                AzureAuthorityHosts.AZURE_GOVERNMENT,
                "https://management.usgovcloudapi.net",
                ["https://management.usgovcloudapi.net/.default"],
                id="us_government",
            ),
            pytest.param(
                "AzureChinaCloud",
                AzureAuthorityHosts.AZURE_CHINA,
                "https://management.chinacloudapi.cn",
                ["https://management.chinacloudapi.cn/.default"],
                id="china_cloud",
            ),
        ],
    )
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.ContainerInstanceManagementClient")
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.ClientSecretCredential")
    def test_get_conn_cloud_environment(
        self,
        mock_credential_cls,
        mock_client_cls,
        cloud_env,
        expected_authority,
        expected_base_url,
        expected_scopes,
        create_mock_connection,
    ):
        extras = {
            "tenantId": "my-tenant",
            "subscriptionId": "my-subscription",
        }
        if cloud_env is not None:
            extras["cloud_environment"] = cloud_env

        mock_connection = create_mock_connection(
            Connection(
                conn_id="azure_container_instance_cloud_test",
                conn_type="azure_container_instances",
                login="my-client-id",
                password="my-secret",
                extra=extras,
            )
        )

        mock_credential_cls.return_value = MagicMock()
        mock_client_cls.return_value = MagicMock()

        hook = AzureContainerInstanceHook(azure_conn_id=mock_connection.conn_id)
        hook.get_conn()

        mock_credential_cls.assert_called_once_with(
            client_id="my-client-id",
            client_secret="my-secret",
            tenant_id="my-tenant",
            authority=expected_authority,
        )
        mock_client_cls.assert_called_once_with(
            credential=mock_credential_cls.return_value,
            subscription_id="my-subscription",
            base_url=expected_base_url,
            credential_scopes=expected_scopes,
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
        mock_credential.close = AsyncMock()
        mock_credential_cls.return_value = mock_credential
        mock_client_instance = MagicMock(spec=AsyncContainerInstanceManagementClient)
        mock_client_instance.close = AsyncMock()
        mock_client_cls.return_value = mock_client_instance

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        async with hook.get_async_conn() as conn:
            mock_credential_cls.assert_called_once_with(
                client_id="client-id",
                client_secret="client-secret",
                tenant_id="tenant-id",
            )
            mock_client_cls.assert_called_once_with(
                credential=mock_credential,
                subscription_id="subscription-id",
            )
            assert conn is mock_client_instance

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
        mock_credential = MagicMock()
        mock_credential.close = AsyncMock()
        mock_default_cred.return_value = mock_credential
        mock_client_instance = MagicMock(spec=AsyncContainerInstanceManagementClient)
        mock_client_instance.close = AsyncMock()
        mock_client_cls.return_value = mock_client_instance

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_without_credentials.conn_id)
        async with hook.get_async_conn() as conn:
            mock_default_cred.assert_called_once_with(
                managed_identity_client_id=None,
                workload_identity_tenant_id=None,
            )
            assert conn is mock_client_instance

    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.AsyncContainerInstanceManagementClient"
    )
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.AsyncClientSecretCredential")
    @pytest.mark.asyncio
    async def test_get_async_conn_closes_client_and_credential(
        self,
        mock_credential_cls,
        mock_client_cls,
        async_conn_with_credentials,
    ):
        mock_credential = AsyncMock(spec=AsyncClientSecretCredential)
        mock_credential_cls.return_value = mock_credential
        mock_client_instance = AsyncMock(spec=AsyncContainerInstanceManagementClient)
        mock_client_cls.return_value = mock_client_instance

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        async with hook.get_async_conn() as conn:
            assert conn is mock_client_instance

        mock_client_instance.close.assert_called_once()
        mock_credential.close.assert_called_once()

    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.AsyncContainerInstanceManagementClient"
    )
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.get_async_default_azure_credential")
    @pytest.mark.asyncio
    async def test_get_async_conn_skips_credential_close_when_not_closeable(
        self,
        mock_default_cred,
        mock_client_cls,
        async_conn_without_credentials,
    ):
        # Credential without a close method (spec=[] removes all attributes)
        mock_credential = MagicMock(spec=[])
        mock_default_cred.return_value = mock_credential
        mock_client_instance = AsyncMock(spec=AsyncContainerInstanceManagementClient)
        mock_client_cls.return_value = mock_client_instance

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_without_credentials.conn_id)
        async with hook.get_async_conn():
            pass

        mock_client_instance.close.assert_called_once()
        assert not hasattr(mock_credential, "close")

    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.AsyncContainerInstanceManagementClient"
    )
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.AsyncClientSecretCredential")
    @pytest.mark.asyncio
    async def test_get_async_conn_closes_on_exception(
        self,
        mock_credential_cls,
        mock_client_cls,
        async_conn_with_credentials,
    ):
        mock_credential = AsyncMock(spec=AsyncClientSecretCredential)
        mock_credential_cls.return_value = mock_credential
        mock_client_instance = AsyncMock(spec=AsyncContainerInstanceManagementClient)
        mock_client_cls.return_value = mock_client_instance

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        with pytest.raises(RuntimeError, match="boom"):
            async with hook.get_async_conn():
                raise RuntimeError("boom")

        mock_client_instance.close.assert_called_once()
        mock_credential.close.assert_called_once()

    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.AsyncContainerInstanceManagementClient"
    )
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.AsyncClientSecretCredential")
    @pytest.mark.asyncio
    async def test_get_state(
        self,
        mock_credential_cls,
        mock_client_cls,
        async_conn_with_credentials,
    ):
        mock_credential_cls.return_value = AsyncMock(spec=AsyncClientSecretCredential)
        mock_cg = MagicMock(spec=ContainerGroup)
        mock_client = MagicMock()
        mock_client.close = AsyncMock()
        mock_client.container_groups.get = AsyncMock(return_value=mock_cg)
        mock_client_cls.return_value = mock_client

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        result = await hook.get_state("my-rg", "my-container")

        mock_client.container_groups.get.assert_called_once_with("my-rg", "my-container")
        assert result is mock_cg

    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.AsyncContainerInstanceManagementClient"
    )
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.AsyncClientSecretCredential")
    @pytest.mark.asyncio
    async def test_get_logs(
        self,
        mock_credential_cls,
        mock_client_cls,
        async_conn_with_credentials,
    ):
        mock_credential_cls.return_value = AsyncMock(spec=AsyncClientSecretCredential)
        mock_logs = MagicMock(spec=Logs)
        mock_logs.content = "line1\nline2\n"
        mock_client = MagicMock()
        mock_client.close = AsyncMock()
        mock_client.containers.list_logs = AsyncMock(return_value=mock_logs)
        mock_client_cls.return_value = mock_client

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        result = await hook.get_logs("my-rg", "my-container")

        mock_client.containers.list_logs.assert_called_once_with(
            "my-rg", "my-container", "my-container", tail=1000
        )
        assert result == ["line1\n", "line2\n"]

    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.AsyncContainerInstanceManagementClient"
    )
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.AsyncClientSecretCredential")
    @pytest.mark.asyncio
    async def test_get_logs_returns_none_sentinel_when_content_is_none(
        self,
        mock_credential_cls,
        mock_client_cls,
        async_conn_with_credentials,
    ):
        mock_credential_cls.return_value = AsyncMock(spec=AsyncClientSecretCredential)
        mock_logs = MagicMock(spec=Logs)
        mock_logs.content = None
        mock_client = MagicMock()
        mock_client.close = AsyncMock()
        mock_client.containers.list_logs = AsyncMock(return_value=mock_logs)
        mock_client_cls.return_value = mock_client

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        result = await hook.get_logs("my-rg", "my-container")
        assert result == [None]

    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.AsyncContainerInstanceManagementClient"
    )
    @patch("airflow.providers.microsoft.azure.hooks.container_instance.AsyncClientSecretCredential")
    @pytest.mark.asyncio
    async def test_delete(
        self,
        mock_credential_cls,
        mock_client_cls,
        async_conn_with_credentials,
    ):
        mock_credential_cls.return_value = AsyncMock(spec=AsyncClientSecretCredential)
        mock_client = MagicMock()
        mock_client.close = AsyncMock()
        mock_client.container_groups.begin_delete = AsyncMock()
        mock_client_cls.return_value = mock_client

        hook = AzureContainerInstanceAsyncHook(azure_conn_id=async_conn_with_credentials.conn_id)
        await hook.delete("my-rg", "my-container")

        mock_client.container_groups.begin_delete.assert_called_once_with("my-rg", "my-container")
