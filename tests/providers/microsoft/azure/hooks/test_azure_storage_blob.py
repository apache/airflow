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
from collections import namedtuple

import mock

import azure.storage.blob as storage_blob

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.azure_storage_blob import AzureStorageBlobHook
from airflow.utils import db


class TestAzureStorageBlobHook(unittest.TestCase):

    def setUp(self):
        self.connection_type = 'azure_storage_blob'
        self.connection_string_id = 'azure_test_connection_string'
        self.shared_key_conn_id = 'azure_shared_key_test'
        self.ad_conn_id = 'azure_AD_test'
        self.sas_conn_id = 'sas_token_id'
        db.merge_conn(
            Connection(
                conn_id=self.connection_string_id, conn_type=self.connection_type,
                extra=json.dumps({'connection_string': 'connection-string'})
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.shared_key_conn_id, conn_type=self.connection_type,
                host='http://test_account_url',
                extra=json.dumps({'shared_access_key': 'token'})
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.ad_conn_id, conn_type=self.connection_type,
                extra=json.dumps({'tenant_id': 'token', 'application_id': 'appID',
                                  'application_secret': "appsecret"})
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.sas_conn_id, conn_type=self.connection_type,
                extra=json.dumps({'sas_token': 'token'})
            )
        )

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlockServiceClient")
    def test_public_read(self, mock_client):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.connection_string_id, public_read=True)
        self.assertIsInstance(hook.get_conn(), mock_client)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlockServiceClient")
    def test_connection_string(self, mock_client):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.connection_string_id)
        self.assertEqual(hook.conn_id, self.connection_string_id)
        self.assertIsInstance(hook.get_conn(), mock_client.return_value.BlockServiceClient)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlockServiceClient")
    def test_shared_key_connection(self, mock_client):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.shared_key_conn_id)
        self.assertIsInstance(hook.get_conn(), mock_client)

    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlockServiceClient")
    def test_sas_token_connection(self,mock_client):
        hook = AzureStorageBlobHook(azure_blob_conn_id=self.sas_conn_id)
        self.assertIsInstance(hook.get_conn(), mock_client)


