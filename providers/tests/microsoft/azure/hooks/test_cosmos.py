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

import logging
import uuid
from unittest import mock
from unittest.mock import PropertyMock

import pytest
from azure.cosmos import PartitionKey
from azure.cosmos.cosmos_client import CosmosClient

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.cosmos import AzureCosmosDBHook

MODULE = "airflow.providers.microsoft.azure.hooks.cosmos"


class TestAzureCosmosDbHook:
    # Set up an environment to test with
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, create_mock_connection):
        # set up some test variables
        self.test_end_point = "https://test_endpoint:443"
        self.test_master_key = "magic_test_key"
        self.test_database_name = "test_database_name"
        self.test_collection_name = "test_collection_name"
        self.test_database_default = "test_database_default"
        self.test_collection_default = "test_collection_default"
        self.test_partition_key = "/test_partition_key"
        create_mock_connection(
            Connection(
                conn_id="azure_cosmos_test_key_id",
                conn_type="azure_cosmos",
                login=self.test_end_point,
                password=self.test_master_key,
                extra={
                    "database_name": self.test_database_default,
                    "collection_name": self.test_collection_default,
                    "partition_key": self.test_partition_key,
                },
            )
        )

    @pytest.mark.parametrize(
        "mocked_connection",
        [
            Connection(
                conn_id="azure_cosmos_test_default_credential",
                conn_type="azure_cosmos",
                login="https://test_endpoint:443",
                extra={
                    "resource_group_name": "resource-group-name",
                    "subscription_id": "subscription_id",
                    "managed_identity_client_id": "test_client_id",
                    "workload_identity_tenant_id": "test_tenant_id",
                },
            )
        ],
        indirect=True,
    )
    @mock.patch(f"{MODULE}.get_sync_default_azure_credential")
    @mock.patch(f"{MODULE}.CosmosDBManagementClient")
    @mock.patch(f"{MODULE}.CosmosClient")
    def test_get_conn(
        self,
        mock_cosmos,
        mock_cosmos_db,
        mock_default_azure_credential,
        mocked_connection,
    ):
        mock_cosmos_db.return_value.database_accounts.list_keys.return_value.primary_master_key = "master-key"

        hook = AzureCosmosDBHook(
            azure_cosmos_conn_id="azure_cosmos_test_default_credential"
        )
        hook.get_conn()

        mock_default_azure_credential.assert_called()
        args = mock_default_azure_credential.call_args
        assert args.kwargs["managed_identity_client_id"] == "test_client_id"
        assert args.kwargs["workload_identity_tenant_id"] == "test_tenant_id"

    @mock.patch(f"{MODULE}.CosmosClient", autospec=True)
    def test_client(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        assert isinstance(hook.get_conn(), CosmosClient)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_create_database(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        hook.create_database(self.test_database_name)
        expected_calls = [mock.call().create_database("test_database_name")]
        mock_cosmos.assert_any_call(
            self.test_end_point, {"masterKey": self.test_master_key}
        )
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_create_database_exception(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        with pytest.raises(AirflowException):
            hook.create_database(None)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_create_container_exception(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        with pytest.raises(AirflowException):
            hook.create_collection(None)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_create_container(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        hook.create_collection(
            self.test_collection_name, self.test_database_name, partition_key="/id"
        )
        expected_calls = [
            mock.call()
            .get_database_client("test_database_name")
            .create_container(
                "test_collection_name", partition_key=PartitionKey(path="/id")
            )
        ]
        mock_cosmos.assert_any_call(
            self.test_end_point, {"masterKey": self.test_master_key}
        )
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_create_container_default(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        hook.create_collection(self.test_collection_name)
        expected_calls = [
            mock.call()
            .get_database_client("test_database_name")
            .create_container(
                "test_collection_name",
                partition_key=PartitionKey(path=self.test_partition_key),
            )
        ]
        mock_cosmos.assert_any_call(
            self.test_end_point, {"masterKey": self.test_master_key}
        )
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_upsert_document_default(self, mock_cosmos):
        test_id = str(uuid.uuid4())

        (
            mock_cosmos.return_value.get_database_client.return_value.get_container_client.return_value.upsert_item.return_value
        ) = {"id": test_id}

        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        returned_item = hook.upsert_document({"id": test_id})
        expected_calls = [
            mock.call()
            .get_database_client("test_database_name")
            .get_container_client("test_collection_name")
            .upsert_item({"id": test_id})
        ]
        mock_cosmos.assert_any_call(
            self.test_end_point, {"masterKey": self.test_master_key}
        )
        mock_cosmos.assert_has_calls(expected_calls)
        logging.getLogger().info(returned_item)
        assert returned_item["id"] == test_id

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_upsert_document(self, mock_cosmos):
        test_id = str(uuid.uuid4())

        (
            mock_cosmos.return_value.get_database_client.return_value.get_container_client.return_value.upsert_item.return_value
        ) = {"id": test_id}

        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        returned_item = hook.upsert_document(
            {"data1": "somedata"},
            database_name=self.test_database_name,
            collection_name=self.test_collection_name,
            document_id=test_id,
        )

        expected_calls = [
            mock.call()
            .get_database_client("test_database_name")
            .get_container_client("test_collection_name")
            .upsert_item({"data1": "somedata", "id": test_id})
        ]

        mock_cosmos.assert_any_call(
            self.test_end_point, {"masterKey": self.test_master_key}
        )
        mock_cosmos.assert_has_calls(expected_calls)
        logging.getLogger().info(returned_item)
        assert returned_item["id"] == test_id

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_insert_documents(self, mock_cosmos):
        test_id1 = str(uuid.uuid4())
        test_id2 = str(uuid.uuid4())
        test_id3 = str(uuid.uuid4())
        documents = [
            {"id": test_id1, "data": "data1"},
            {"id": test_id2, "data": "data2"},
            {"id": test_id3, "data": "data3"},
        ]

        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        returned_item = hook.insert_documents(documents)
        expected_calls = [
            mock.call()
            .get_database_client("test_database_name")
            .get_container_client("test_collection_name")
            .create_item({"data": "data1", "id": test_id1}),
            mock.call()
            .get_database_client("test_database_name")
            .get_container_client("test_collection_name")
            .create_item({"data": "data2", "id": test_id2}),
            mock.call()
            .get_database_client("test_database_name")
            .get_container_client("test_collection_name")
            .create_item({"data": "data3", "id": test_id3}),
        ]
        logging.getLogger().info(returned_item)
        mock_cosmos.assert_any_call(
            self.test_end_point, {"masterKey": self.test_master_key}
        )
        mock_cosmos.assert_has_calls(expected_calls, any_order=True)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_delete_database(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        hook.delete_database(self.test_database_name)
        expected_calls = [mock.call().delete_database("test_database_name")]
        mock_cosmos.assert_any_call(
            self.test_end_point, {"masterKey": self.test_master_key}
        )
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_delete_database_exception(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        with pytest.raises(AirflowException):
            hook.delete_database(None)

    @mock.patch("azure.cosmos.cosmos_client.CosmosClient")
    def test_delete_container_exception(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        with pytest.raises(AirflowException):
            hook.delete_collection(None)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_delete_container(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        hook.delete_collection(self.test_collection_name, self.test_database_name)
        expected_calls = [
            mock.call()
            .get_database_client("test_database_name")
            .delete_container("test_collection_name")
        ]
        mock_cosmos.assert_any_call(
            self.test_end_point, {"masterKey": self.test_master_key}
        )
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_delete_container_default(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        hook.delete_collection(self.test_collection_name)
        expected_calls = [
            mock.call()
            .get_database_client("test_database_name")
            .delete_container("test_collection_name")
        ]
        mock_cosmos.assert_any_call(
            self.test_end_point, {"masterKey": self.test_master_key}
        )
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_connection_success(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        hook.get_conn().list_databases.return_value = {"id": self.test_database_name}
        status, msg = hook.test_connection()
        assert status is True
        assert msg == "Successfully connected to Azure Cosmos."

    @mock.patch(f"{MODULE}.CosmosClient")
    def test_connection_failure(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id="azure_cosmos_test_key_id")
        hook.get_conn().list_databases = PropertyMock(
            side_effect=Exception("Authentication failed.")
        )
        status, msg = hook.test_connection()
        assert status is False
        assert msg == "Authentication failed."
