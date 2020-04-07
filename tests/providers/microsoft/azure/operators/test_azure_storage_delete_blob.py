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
import unittest

import mock

from airflow.models.dag import DAG
from airflow.providers.microsoft.azure.operators.azure_storage_delete_blob import AzureDeleteBlobOperator


class TestAzureDeleteBlobOperator(unittest.TestCase):

    _config = {
        'container_name': 'container',
        'blob_name': 'blob',
    }

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2017, 1, 1)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_init(self):
        operator = AzureDeleteBlobOperator(
            task_id='azure_operator',
            dag=self.dag,
            **self._config
        )
        self.assertEqual(operator.container_name,
                         self._config['container_name'])
        self.assertEqual(operator.blob_name, self._config['blob_name'])
        self.assertEqual(operator.is_prefix, False)

        operator = AzureDeleteBlobOperator(
            task_id='azure_operator',
            dag=self.dag,
            is_prefix=True,
            **self._config
        )
        self.assertEqual(operator.is_prefix, True)

    @mock.patch('airflow.providers.microsoft.azure.operators.azure_storage_delete_blob.AzureStorageBlobHook',
                autospec=True)
    def test_execute(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = AzureDeleteBlobOperator(
            task_id='azure_operator',
            dag=self.dag,
            is_prefix=False,
            **self._config
        )
        operator.execute(None)
        mock_instance.delete_blob.assert_called_once_with(
            'container', 'blob'
        )

    @mock.patch('airflow.providers.microsoft.azure.operators.azure_storage_delete_blob.AzureStorageBlobHook',
                autospec=True)
    def test_execute_with_is_prefix_true(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = AzureDeleteBlobOperator(
            task_id='azure_operator',
            dag=self.dag,
            is_prefix=True,
            **self._config
        )
        operator.execute(None)
        mock_instance.list.return_value = ['blob']
        mock_instance.delete_blobs.assert_called_once_with(
            'container'
        )


if __name__ == '__main__':
    unittest.main()
