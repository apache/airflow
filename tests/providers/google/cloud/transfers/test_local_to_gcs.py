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
import os
import tempfile
import unittest
from glob import glob

import mock
import pytest

from airflow.models.dag import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


class TestFileToGcsOperator(unittest.TestCase):

    _config = {
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
        self.testfile1 = tempfile.NamedTemporaryFile(delete=False)
        self.testfile1.write(b"x" * 393216)
        self.testfile1.flush()
        self.testfile2 = tempfile.NamedTemporaryFile(delete=False)
        self.testfile2.write(b"x" * 393216)
        self.testfile2.flush()
        self.testfiles = [self.testfile1.name, self.testfile2.name]

    def tearDown(self):
        print('cleaning up files')
        os.remove(self.testfile1.name)
        os.remove(self.testfile2.name)

    def test_init(self):
        operator = LocalFilesystemToGCSOperator(
            task_id='file_to_gcs_operator',
            dag=self.dag,
            src=self.testfile1.name,
            dst='test/test1.csv',
            **self._config
        )
        self.assertEqual(operator.src, self.testfile1.name)
        self.assertEqual(operator.dst, 'test/test1.csv')
        self.assertEqual(operator.bucket, self._config['bucket'])
        self.assertEqual(operator.mime_type, self._config['mime_type'])
        self.assertEqual(operator.gzip, self._config['gzip'])

    @mock.patch('airflow.providers.google.cloud.transfers.local_to_gcs.GCSHook',
                autospec=True)
    def test_execute(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = LocalFilesystemToGCSOperator(
            task_id='gcs_to_file_sensor',
            dag=self.dag,
            src=self.testfile1.name,
            dst='test/test1.csv',
            **self._config
        )
        operator.execute(None)
        mock_instance.upload.assert_called_once_with(
            bucket_name=self._config['bucket'],
            filename=self.testfile1.name,
            gzip=self._config['gzip'],
            mime_type=self._config['mime_type'],
            object_name='test/test1.csv'
        )

    @mock.patch('airflow.providers.google.cloud.transfers.local_to_gcs.GCSHook',
                autospec=True)
    def test_execute_multiple(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = LocalFilesystemToGCSOperator(
            task_id='gcs_to_file_sensor',
            dag=self.dag,
            src=self.testfiles,
            dst='test/',
            **self._config
        )
        operator.execute(None)
        files_objects = zip(self.testfiles, ['test/' + os.path.basename(testfile)
                                             for testfile in self.testfiles])
        calls = [
            mock.call(
                bucket_name=self._config['bucket'],
                filename=filepath,
                gzip=self._config['gzip'],
                mime_type=self._config['mime_type'],
                object_name=object_name
            )
            for filepath, object_name in files_objects
        ]
        mock_instance.upload.assert_has_calls(calls)

    @mock.patch('airflow.providers.google.cloud.transfers.local_to_gcs.GCSHook',
                autospec=True)
    def test_execute_wildcard(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = LocalFilesystemToGCSOperator(
            task_id='gcs_to_file_sensor',
            dag=self.dag,
            src='/tmp/tmp[0-9]*',
            dst='test/',
            **self._config
        )
        operator.execute(None)
        object_names = ['test/' + os.path.basename(fp) for fp in glob('/tmp/tmp[0-9].*')]
        files_objects = zip(glob('/tmp/tmp[0-9]*'), object_names)
        calls = [
            mock.call(
                bucket_name=self._config['bucket'],
                filename=filepath,
                gzip=self._config['gzip'],
                mime_type=self._config['mime_type'],
                object_name=object_name
            )
            for filepath, object_name in files_objects
        ]
        mock_instance.upload.assert_has_calls(calls)

    @mock.patch('airflow.providers.google.cloud.transfers.local_to_gcs.GCSHook',
                autospec=True)
    def test_execute_negative(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = LocalFilesystemToGCSOperator(
            task_id='gcs_to_file_sensor',
            dag=self.dag,
            src='/tmp/tmp[0-9]*',
            dst='test/test1.csv',
            **self._config
        )
        print(glob('/tmp/tmp[0-9]*'))
        with pytest.raises(ValueError):
            operator.execute(None)
        mock_instance.assert_not_called()
