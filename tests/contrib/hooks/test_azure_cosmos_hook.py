# -*- coding: utf-8 -*-
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
import unittest
import uuid

from airflow.exceptions import AirflowException
from airflow.contrib.hooks.azure_cosmos_hook import AzureCosmosDBHook

from airflow import configuration
from airflow.models import Connection
from airflow.utils import db
from tests.compat import mock

import logging


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
        configuration.load_test_config()
        db.merge_conn(
            Connection(
                conn_id='azure_cosmos_test_key_id',
                conn_type='azure_cosmos',
                login=self.test_end_point,
                password=self.test_master_key,
                extra=json.dumps({'database_name': self.test_database_default,
                                  'collection_name': self.test_collection_default})
            )
        )

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_create_database(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.create_database(self.test_database_name)
        expected_calls = [mock.call().CreateDatabase({'id': self.test_database_name})]
        cosmos_mock.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_create_database_exception(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.assertRaises(AirflowException, self.cosmos.create_database, None)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_create_container_exception(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.assertRaises(AirflowException, self.cosmos.create_collection, None)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_create_container(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.create_collection(self.test_collection_name, self.test_database_name)
        expected_calls = [mock.call().CreateContainer(
            'dbs/test_database_name',
            {'id': self.test_collection_name})]
        cosmos_mock.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_create_container_default(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.create_collection(self.test_collection_name)
        expected_calls = [mock.call().CreateContainer(
            'dbs/test_database_default',
            {'id': self.test_collection_name})]
        cosmos_mock.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_upsert_document_default(self, cosmos_mock):
        test_id = str(uuid.uuid4())
        cosmos_mock.return_value.CreateItem.return_value = {'id': test_id}
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        returned_item = self.cosmos.upsert_document({'id': test_id})
        expected_calls = [mock.call().CreateItem(
            'dbs/' + self.test_database_default + '/colls/' + self.test_collection_default,
            {'id': test_id})]
        cosmos_mock.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        cosmos_mock.assert_has_calls(expected_calls)
        logging.getLogger().info(returned_item)
        self.assertEqual(returned_item['id'], test_id)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_upsert_document(self, cosmos_mock):
        test_id = str(uuid.uuid4())
        cosmos_mock.return_value.CreateItem.return_value = {'id': test_id}
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        returned_item = self.cosmos.upsert_document(
            {'data1': 'somedata'},
            database_name=self.test_database_name,
            collection_name=self.test_collection_name,
            document_id=test_id)

        expected_calls = [mock.call().CreateItem(
            'dbs/' + self.test_database_name + '/colls/' + self.test_collection_name,
            {'data1': 'somedata', 'id': test_id})]

        cosmos_mock.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        cosmos_mock.assert_has_calls(expected_calls)
        logging.getLogger().info(returned_item)
        self.assertEqual(returned_item['id'], test_id)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_insert_documents(self, cosmos_mock):
        test_id1 = str(uuid.uuid4())
        test_id2 = str(uuid.uuid4())
        test_id3 = str(uuid.uuid4())
        documents = [
            {'id': test_id1, 'data': 'data1'},
            {'id': test_id2, 'data': 'data2'},
            {'id': test_id3, 'data': 'data3'}]

        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        returned_item = self.cosmos.insert_documents(documents)
        expected_calls = [
            mock.call().CreateItem(
                'dbs/' + self.test_database_default + '/colls/' + self.test_collection_default,
                {'data': 'data1', 'id': test_id1}),
            mock.call().CreateItem(
                'dbs/' + self.test_database_default + '/colls/' + self.test_collection_default,
                {'data': 'data2', 'id': test_id2}),
            mock.call().CreateItem(
                'dbs/' + self.test_database_default + '/colls/' + self.test_collection_default,
                {'data': 'data3', 'id': test_id3})]
        logging.getLogger().info(returned_item)
        cosmos_mock.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_database(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.delete_database(self.test_database_name)
        expected_calls = [mock.call().DeleteDatabase('dbs/test_database_name')]
        cosmos_mock.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_database_exception(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.assertRaises(AirflowException, self.cosmos.delete_database, None)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_container_exception(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.assertRaises(AirflowException, self.cosmos.delete_collection, None)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_container(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.delete_collection(self.test_collection_name, self.test_database_name)
        expected_calls = [mock.call().DeleteContainer('dbs/test_database_name/colls/test_collection_name')]
        cosmos_mock.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        cosmos_mock.assert_has_calls(expected_calls)

    @mock.patch('azure.cosmos.cosmos_client.CosmosClient')
    def test_delete_container_default(self, cosmos_mock):
        self.cosmos = AzureCosmosDBHook(azure_cosmos_conn_id='azure_cosmos_test_key_id')
        self.cosmos.delete_collection(self.test_collection_name)
        expected_calls = [mock.call().DeleteContainer('dbs/test_database_default/colls/test_collection_name')]
        cosmos_mock.assert_any_call(self.test_end_point, {'masterKey': self.test_master_key})
        cosmos_mock.assert_has_calls(expected_calls)


if __name__ == '__main__':
    unittest.main()
