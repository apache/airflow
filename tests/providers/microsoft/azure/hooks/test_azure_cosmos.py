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
#


import json
import logging
import unittest
import uuid
from unittest import mock

import pytest
from azure.cosmos.cosmos_client import CosmosClient

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.azure_cosmos import AzureCosmosDBHook
from airflow.utils import db


class TestAzureCosmosDbHook(unittest.TestCase):

    # Set up an environment to test with
    def setUp(self):
        # set up some test variables
        self.test_end_point = 'https://test_endpoint:443'
        self.test_master_key = 'magic_test_key'
        self.test_database_name = 'test_database_name'
        self.test_collection_name = 'test_collection_name'
        self.test_database_default = 'test_database_default'
        self.test_collection_default = 'test_collection_default'
        db.merge_conn(
            Connection(
                conn_id='azure_cosmos_test_key_id',
                conn_type='azure_cosmos',
                login=self.test_end_point,
                password=self.test_master_key,
                extra=json.dumps(
                    {
                        'database_name': self.test_database_default,
                        'collection_name': self.test_collection_default,
                    }
                ),
            )
        )

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient', autospec=True)
    def test_client(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        assert hook._conn is None
        assert isinstance(hook.get_conn(), CosmosClient)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_create_database(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        hook.create_database(self.test_database_name)
        expected_calls = [mock.call().CreateDatabase({'id': self.test_database_name})]
        mock_cosmos.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_create_database_exception(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        with pytest.raises(AirflowException):
            hook.create_database(None)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_create_container_exception(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        with pytest.raises(AirflowException):
            hook.create_collection(None)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_create_container(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        hook.create_collection(self.test_collection_name, self.test_database_name)
        expected_calls = [
            mock.call().CreateContainer('dbs/test_database_name', {'id': self.test_collection_name})
        ]
        mock_cosmos.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_create_container_default(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        hook.create_collection(self.test_collection_name)
        expected_calls = [
            mock.call().CreateContainer('dbs/test_database_default', {'id': self.test_collection_name})
        ]
        mock_cosmos.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_upsert_document_default(self, mock_cosmos):
        test_id = str(uuid.uuid4())
        mock_cosmos.return_value.CreateItem.return_value = {'id': test_id}
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        returned_item = hook.upsert_document({'id': test_id})
        expected_calls = [
            mock.call().CreateItem(
                'dbs/' + self.test_database_default + '/colls/' + self.test_collection_default,
                {'id': test_id},
            )
        ]
        mock_cosmos.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        mock_cosmos.assert_has_calls(expected_calls)
        logging.getLogger().info(returned_item)
        assert returned_item['id'] == test_id

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_upsert_document(self, mock_cosmos):
        test_id = str(uuid.uuid4())
        mock_cosmos.return_value.CreateItem.return_value = {'id': test_id}
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        returned_item = hook.upsert_document(
            {'data1': 'somedata'},
            database_name=self.test_database_name,
            collection_name=self.test_collection_name,
            document_id=test_id,
        )

        expected_calls = [
            mock.call().CreateItem(
                'dbs/' + self.test_database_name + '/colls/' + self.test_collection_name,
                {'data1': 'somedata', 'id': test_id},
            )
        ]

        mock_cosmos.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        mock_cosmos.assert_has_calls(expected_calls)
        logging.getLogger().info(returned_item)
        assert returned_item['id'] == test_id

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_insert_documents(self, mock_cosmos):
        test_id1 = str(uuid.uuid4())
        test_id2 = str(uuid.uuid4())
        test_id3 = str(uuid.uuid4())
        documents = [
            {'id': test_id1, 'data': 'data1'},
            {'id': test_id2, 'data': 'data2'},
            {'id': test_id3, 'data': 'data3'},
        ]

        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        returned_item = hook.insert_documents(documents)
        expected_calls = [
            mock.call().CreateItem(
                'dbs/' + self.test_database_default + '/colls/' + self.test_collection_default,
                {'data': 'data1', 'id': test_id1},
            ),
            mock.call().CreateItem(
                'dbs/' + self.test_database_default + '/colls/' + self.test_collection_default,
                {'data': 'data2', 'id': test_id2},
            ),
            mock.call().CreateItem(
                'dbs/' + self.test_database_default + '/colls/' + self.test_collection_default,
                {'data': 'data3', 'id': test_id3},
            ),
        ]
        logging.getLogger().info(returned_item)
        mock_cosmos.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        mock_cosmos.assert_has_calls(expected_calls, any_order=True)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_delete_database(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        hook.delete_database(self.test_database_name)
        expected_calls = [mock.call().DeleteDatabase('dbs/test_database_name')]
        mock_cosmos.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_delete_database_exception(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        with pytest.raises(AirflowException):
            hook.delete_database(None)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_container_exception(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        with pytest.raises(AirflowException):
            hook.delete_collection(None)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_delete_container(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        hook.delete_collection(self.test_collection_name, self.test_database_name)
        expected_calls = [mock.call().DeleteContainer('dbs/test_database_name/colls/test_collection_name')]
        mock_cosmos.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        mock_cosmos.assert_has_calls(expected_calls)

    @mock.patch('airflow.providers.microsoft.azure.hooks.azure_cosmos.CosmosClient')
    def test_delete_container_default(self, mock_cosmos):
        hook = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        hook.delete_collection(self.test_collection_name)
        expected_calls = [mock.call().DeleteContainer('dbs/test_database_default/colls/test_collection_name')]
        mock_cosmos.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        mock_cosmos.assert_has_calls(expected_calls)
