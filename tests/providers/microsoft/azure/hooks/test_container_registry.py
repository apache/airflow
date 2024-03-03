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

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.container_registry import AzureContainerRegistryHook


class TestAzureContainerRegistryHook:
    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="azure_container_registry",
                conn_type="azure_container_registry",
                login="myuser",
                password="password",
                host="test.cr",
            )
        ],
        indirect=True,
    )
    def test_get_conn(self, mocked_connection):
        hook = AzureContainerRegistryHook(conn_id=mocked_connection.conn_id)
        assert hook.connection is not None
        assert hook.connection.username == "myuser"
        assert hook.connection.password == "password"
        assert hook.connection.server == "test.cr"

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="azure_container_registry",
                conn_type="azure_container_registry",
                login="myuser",
                password="",
                host="test.cr",
                extra={"subscription_id": "subscription_id", "resource_group": "resource_group"},
            )
        ],
        indirect=True,
    )
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.container_registry.ContainerRegistryManagementClient"
    )
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.container_registry.get_sync_default_azure_credential"
    )
    def test_get_conn_with_default_azure_credential(
        self, mocked_default_azure_credential, mocked_client, mocked_connection
    ):
        mocked_client.return_value.registries.list_credentials.return_value.as_dict.return_value = {
            "username": "myuser",
            "passwords": [
                {"name": "password", "value": "password"},
            ],
        }

        hook = AzureContainerRegistryHook(conn_id=mocked_connection.conn_id)
        assert hook.connection is not None
        assert hook.connection.username == "myuser"
        assert hook.connection.password == "password"
        assert hook.connection.server == "test.cr"

        assert mocked_default_azure_credential.called_with(
            managed_identity_client_id=None, workload_identity_tenant_id=None
        )
