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
import json
import os
import shutil
import tempfile
import unittest

import mock

from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.local_to_azure_storage_blob import (
    LocalFileSystemToAzureStorageBlobOperator,
)
from airflow.utils import db


class TestLocalFileSystemToAzureStorageblobOperator(unittest.TestCase):
    # set up the test environment
    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.AzureStorageBlobHook")
    @mock.patch("airflow.providers.microsoft.azure.hooks.azure_storage_blob.BlobServiceClient")
    def setUp(self, mock_service, mock_hook):
        db.merge_conn(
            Connection(
                conn_id='test-key-id', conn_type='azure_storage_blob',
                host='https://accountname.blob.core.windows.net',
                extra=json.dumps({'shared_access_key': 'token'})
            )
        )
        self.test_dir = tempfile.TemporaryDirectory()
        self.testfile = tempfile.NamedTemporaryFile(delete=False)
        self.testfile.write(b"some test content")
        self.testfile.flush()
        shutil.copy(self.testfile.name, self.test_dir.name)
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2017, 1, 1)
        }
        self.dag = DAG('test_dag_id', default_args=args)

        self._config = {
            'source_path': self.testfile.name,
            'container_name': 'container',
            'destination_path': 'blob',
            'azure_blob_conn_id': 'test-key-id',
            'length': 3
        }

        self.operator = LocalFileSystemToAzureStorageBlobOperator(
            task_id='localtoblob_operator',
            dag=self.dag,
            **self._config
        )
        self.assertEqual(self.operator.source_path, self._config['source_path'])
        self.assertEqual(self.operator.container_name,
                         self._config['container_name'])
        self.assertEqual(self.operator.destination_path, self._config['destination_path'])
        self.assertEqual(self.operator.azure_blob_conn_id, self._config['azure_blob_conn_id'])
        self.assertEqual(self.operator.upload_options, {})
        self.assertEqual(self.operator.length, self._config['length'])
        self.blob_client = mock_service.return_value
        self.hook = mock_hook.return_value
        self.assertEqual(self.blob_client, self.operator.hook.get_conn())

    def tearDown(self):
        os.unlink(self.testfile.name)
        shutil.rmtree(self.test_dir.name)

    @mock.patch("airflow.providers.microsoft.azure.operators."
                "local_to_azure_storage_blob.AzureStorageBlobHook")
    @mock.patch("airflow.providers.microsoft.azure.hooks."
                "azure_storage_blob.BlobServiceClient")
    def test_execute_with_file_without_failures(self, mock_client, mock_azhook):
        self.operator.execute(None)
        self.assertIsNotNone(self.operator)
        mock_client.return_value.get_container_client.\
            assert_called_once_with(self._config['container_name'])
        mock_client.return_value.get_container_client.return_value.get_blob_client. \
            return_value.upload_blob.assert_called()

    @mock.patch("airflow.providers.microsoft.azure.operators."
                "local_to_azure_storage_blob.AzureStorageBlobHook")
    @mock.patch("airflow.providers.microsoft.azure.hooks."
                "azure_storage_blob.BlobServiceClient")
    def test_execute_with_dir_without_failures(self, mock_client, mock_azhook):
        self.operator.source_path = self.test_dir.name
        self.operator.execute(None)
        self.assertIsNotNone(self.operator)
        mock_client.return_value.get_container_client.assert_called_with(self._config['container_name'])
        mock_client.return_value.get_container_client.return_value.get_blob_client. \
            return_value.upload_blob.assert_called()
