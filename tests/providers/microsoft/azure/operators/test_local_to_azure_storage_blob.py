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
import os
import datetime
import tempfile
import unittest

import mock

from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.local_to_azure_storage_blob import \
    LocalFileSystemToAzureStorageBlobOperator


class TestLocalFileSystemToAzureStorageblobOperator(unittest.TestCase):

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2017, 1, 1)
        }
        self.dag = DAG('test_dag_id', default_args=args)
        self.testfile = tempfile.NamedTemporaryFile(delete=False)
        self.testfile.write(b"x" * 393216)
        self.testfile.flush()
        self._config = {
            'source_path': self.testfile,
            'container_name': 'container',
            'destination_path': 'blob',
            'azure_blob_conn_id': 'azure_blob_default',
            'length': 3
        }

    def tearDown(self):
        os.unlink(self.testfile.name)

    def test_init(self):
        operator = LocalFileSystemToAzureStorageBlobOperator(
            task_id='localtoblob_operator',
            dag=self.dag,
            **self._config
        )
        self.assertEqual(operator.source_path, self._config['source_path'])
        self.assertEqual(operator.container_name,
                         self._config['container_name'])
        self.assertEqual(operator.destination_path, self._config['destination_path'])
        self.assertEqual(operator.azure_blob_conn_id, self._config['azure_blob_conn_id'])
        self.assertEqual(operator.load_options, {})
        self.assertEqual(operator.length, self._config['length'])

        operator = LocalFileSystemToAzureStorageBlobOperator(
            task_id='localtoblob_operator',
            dag=self.dag,
            load_options={'timeout': 2},
            **self._config
        )
        self.assertEqual(operator.load_options, {'timeout': 2})

    @mock.patch('airflow.providers.microsoft.azure.operators.'
                'local_to_azure_storage_blob.AzureStorageBlobHook',
                autospec=True)
    def test_execute(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = LocalFileSystemToAzureStorageBlobOperator(
            task_id='blob_sensor',
            dag=self.dag,
            load_options={'timeout': 2},
            **self._config
        )
        operator.execute(None)
        mock_instance.upload.assert_called()


if __name__ == '__main__':
    unittest.main()
