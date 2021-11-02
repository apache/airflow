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

import unittest
from unittest import mock

from airflow.providers.microsoft.azure.sensors.azure_cosmos import AzureCosmosDocumentSensor

DB_NAME = 'test-db-name'
COLLECTION_NAME = 'test-db-collection-name'
DOCUMENT_ID = 'test-document-id'


class TestAzureCosmosSensor(unittest.TestCase):
    @mock.patch('airflow.providers.microsoft.azure.sensors.azure_cosmos.AzureCosmosDBHook')
    def test_should_call_hook_with_args(self, mock_hook):
        mock_instance = mock_hook.return_value
        mock_instance.get_document.return_value = True  # Indicate document returned
        sensor = AzureCosmosDocumentSensor(
            task_id="test-task-1",
            database_name=DB_NAME,
            collection_name=COLLECTION_NAME,
            document_id=DOCUMENT_ID,
        )
        result = sensor.poke(None)
        mock_instance.get_document.assert_called_once_with(DOCUMENT_ID, DB_NAME, COLLECTION_NAME)
        assert result is True

    @mock.patch('airflow.providers.microsoft.azure.sensors.azure_cosmos.AzureCosmosDBHook')
    def test_should_return_false_on_no_document(self, mock_hook):
        mock_instance = mock_hook.return_value
        mock_instance.get_document.return_value = None  # Indicate document not returned
        sensor = AzureCosmosDocumentSensor(
            task_id="test-task-2",
            database_name=DB_NAME,
            collection_name=COLLECTION_NAME,
            document_id=DOCUMENT_ID,
        )
        result = sensor.poke(None)
        mock_instance.get_document.assert_called_once_with(DOCUMENT_ID, DB_NAME, COLLECTION_NAME)
        assert result is False
