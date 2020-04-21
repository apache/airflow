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

import datetime
import unittest

from airflow import DAG
from airflow.operators.local_to_gcs import FileToGoogleCloudStorageOperator
from tests.compat import mock


class TestFileToGcsOperator(unittest.TestCase):

    _config = {
        'src': '/tmp/fake.csv',
        'dst': 'fake.csv',
        'bucket': 'dummy',
        'mime_type': 'application/octet-stream',
        'gzip': False
    }

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2017, 1, 1)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_init(self):
        operator = FileToGoogleCloudStorageOperator(
            task_id='file_to_gcs_operator',
            dag=self.dag,
            **self._config
        )
        self.assertEqual(operator.src, self._config['src'])
        self.assertEqual(operator.dst, self._config['dst'])
        self.assertEqual(operator.bucket, self._config['bucket'])
        self.assertEqual(operator.mime_type, self._config['mime_type'])
        self.assertEqual(operator.gzip, self._config['gzip'])

    @mock.patch('airflow.operators.local_to_gcs.GoogleCloudStorageHook',
                autospec=True)
    def test_execute(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = FileToGoogleCloudStorageOperator(
            task_id='gcs_to_file_sensor',
            dag=self.dag,
            **self._config
        )
        operator.execute(None)
        mock_instance.upload.assert_called_once_with(
            bucket_name=self._config['bucket'],
            filename=self._config['src'],
            gzip=self._config['gzip'],
            mime_type=self._config['mime_type'],
            object_name=self._config['dst']
        )


if __name__ == '__main__':
    unittest.main()
