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
import datetime
import io
import json
import os
import shutil
import tempfile
import unittest

import mock

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.azure_storage_blob_to_local import (
    AzureStorageBlobDownloadOperator,
)
from airflow.utils import db

# connection_string has a format
CONN_STRING = 'DefaultEndpointsProtocol=https;AccountName=testname;' \
              'AccountKey=wK7BOz;EndpointSuffix=core.windows.net'


class TestAzureStorageBlobDownloadOperrator(unittest.TestCase):

    # set up the test environment
    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.AzureStorageBlobHook")
    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def setUp(self, mock_service, mock_hook):
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2017, 1, 1)
        }
        self.connection_string_id = 'azure_test_connection'
        self.connection_type = 'azure_storage_blob'
        self.dag = DAG('test_dag_id', default_args=args)
        self.test_dir = tempfile.TemporaryDirectory()
        self.testfile = tempfile.NamedTemporaryFile(delete=False)
        self.testfile.write(b"some test content")
        self.testfile.flush()

        db.merge_conn(
            Connection(
                conn_id='test-shared-key-id', conn_type='azure_storage_blob',
                host='https://accountname.blob.core.windows.net',
                extra=json.dumps({'shared_access_key': 'token'})
            )
        )

        self._config = {
            'source': 'myblobs',
            'container_name': 'container',
            'destination_path': self.test_dir.name,
            'azure_blob_conn_id': 'test-shared-key-id',
            'offset': 4,
            'length': 5,
        }

        operator = AzureStorageBlobDownloadOperator(
            task_id='download_operator',
            dag=self.dag,
            **self._config
        )
        self.assertEqual(operator.source, self._config['source'])
        self.assertEqual(operator.container_name,
                         self._config['container_name'])
        self.assertEqual(operator.destination_path, self._config['destination_path'])
        self.assertEqual(operator.download_options, {})
        self.assertEqual(operator.length, self._config['length'])

        self.operator = AzureStorageBlobDownloadOperator(
            task_id='download_operator2',
            dag=self.dag,
            search_options={'delimiter': '/'},
            **self._config
        )
        self.assertEqual(self.operator.search_options, {'delimiter': '/'},)
        self.blob_client = mock_service.return_value
        self.hook = mock_hook.return_value
        self.assertEqual(self.blob_client, self.operator.hook.get_conn())

    def tearDown(self):
        os.unlink(self.testfile.name)
        shutil.rmtree(self.test_dir.name)

    @mock.patch("airflow.providers.microsoft.azure.operators."
                "azure_storage_blob_download.AzureStorageBlobHook")
    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def test_execute(self, mock_service, mock_hook):
        container_client = mock_service.return_value.get_container_client
        blob_client = container_client.return_value.get_blob_client
        blob_client.return_value.download_blob.return_value = io.FileIO(self.testfile.name)
        self.operator.execute(None)
        self.assertIsNotNone(self.operator)
        container_client.assert_called()
        container_client.return_value.walk_blobs.assert_called()
        blob_client.assert_called()
        blob_client.return_value.download_blob.assert_called()


if __name__ == '__main__':
    unittest.main()
