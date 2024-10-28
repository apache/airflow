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

from unittest.mock import MagicMock, patch

import pytest
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    Logs,
    ResourceRequests,
    ResourceRequirements,
)

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.container_instance import (
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
        self.resources = ResourceRequirements(
            requests=ResourceRequests(memory_in_gb="4", cpu="1")
        )
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
    @patch(
        "azure.mgmt.containerinstance.operations.ContainerGroupsOperations.begin_create_or_update"
    )
    def test_create_or_update(self, create_or_update_mock, container_group_mock):
        self.hook.create_or_update("resource_group", "aci-test", container_group_mock)
        create_or_update_mock.assert_called_once_with(
            "resource_group", "aci-test", container_group_mock
        )

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

    @patch(
        "azure.mgmt.containerinstance.operations.ContainerGroupsOperations.begin_delete"
    )
    def test_delete(self, delete_mock):
        self.hook.delete("resource_group", "aci-test")
        delete_mock.assert_called_once_with("resource_group", "aci-test")

    @patch(
        "azure.mgmt.containerinstance.operations.ContainerGroupsOperations.list_by_resource_group"
    )
    def test_exists_with_existing(self, list_mock):
        list_mock.return_value = [
            ContainerGroup(
                os_type="Linux",
                containers=[
                    Container(name="test1", image="hello-world", resources=self.resources)
                ],
            )
        ]
        assert not self.hook.exists("test", "test1")

    @patch(
        "azure.mgmt.containerinstance.operations.ContainerGroupsOperations.list_by_resource_group"
    )
    def test_exists_with_not_existing(self, list_mock):
        list_mock.return_value = [
            ContainerGroup(
                os_type="Linux",
                containers=[
                    Container(name="test1", image="hello-world", resources=self.resources)
                ],
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
    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.ContainerInstanceManagementClient"
    )
    @patch("azure.common.credentials.ServicePrincipalCredentials")
    @patch(
        "airflow.providers.microsoft.azure.hooks.container_instance.get_sync_default_azure_credential"
    )
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

        hook = AzureContainerInstanceHook(
            azure_conn_id=connection_without_login_password_tenant_id.conn_id
        )
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
