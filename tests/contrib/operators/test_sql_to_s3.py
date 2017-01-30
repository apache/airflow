# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import zipfile

from airflow.contrib.operators.sql_to_s3 import SqlToS3

from airflow import configuration
from airflow import models
from airflow.utils import db

try:
    from unittest import mock
    from mock import  Mock, patch, mock_open
except ImportError:
    try:
        import mock
        from mock import  Mock, patch, mock_open
    except ImportError:
        mock = None

TASK_ID= 'task_id'
SOURCE_CONN_ID='mysql_default'
SQL='select 1'
S3_BUCKET='test-bucket-'
S3_FILE_KEY='test-file-key'
S3_CONN_ID='s3_default'
S3_REPLACE=True
S3_ZIP=False


class TestSqlToS3Operator(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.sql_to_s3.S3Hook')
    @mock.patch('airflow.contrib.operators.sql_to_s3.BaseHook')
    def setUp(self, base_hook_mock, s3_hook_mock):
        configuration.load_test_config()

        self.base_hook_mock=base_hook_mock
        self.s3_hook_mock=s3_hook_mock
        self.sql_to_s3 = SqlToS3(
            task_id=TASK_ID,
            db_conn_id=SOURCE_CONN_ID,
            sql=SQL,
            s3_bucket=S3_BUCKET,
            s3_file_key=S3_FILE_KEY,
            s3_conn_id=S3_CONN_ID,
            s3_replace_file=S3_REPLACE,
            s3_zip_file= S3_ZIP)
        db.merge_conn(
            models.Connection(
                conn_id='s3_default',
                conn_type='s3',
                extra='{"aws_access_key_id":"default_access_key", "aws_secret_access_key": "default_secret_key"}'))

    def test_init(self):

        self.assertEqual(self.sql_to_s3.task_id, TASK_ID)
        self.assertEqual(self.sql_to_s3.db_conn_id, SOURCE_CONN_ID)
        self.assertEqual(self.sql_to_s3.sql, SQL)
        self.assertEqual(self.sql_to_s3.s3_bucket, S3_BUCKET)
        self.assertEqual(self.sql_to_s3.s3_file_key, S3_FILE_KEY)
        self.assertEqual(self.sql_to_s3.s3_conn_id, S3_CONN_ID)
        self.assertEqual(self.sql_to_s3.s3_replace_file, S3_REPLACE)
        self.assertEqual(self.sql_to_s3.s3_zip_file, S3_ZIP)
        self.assertEqual(self.sql_to_s3.s3_hook,  self.s3_hook_mock.return_value)
        self.base_hook_mock.get_hook.assert_called_once_with(SOURCE_CONN_ID)


    @mock.patch('airflow.contrib.operators.sql_to_s3.os')
    @mock.patch('airflow.contrib.operators.sql_to_s3.tempfile')
    @mock.patch('airflow.contrib.operators.sql_to_s3.csv')
    @mock.patch('airflow.contrib.operators.sql_to_s3.open', new_callable=mock_open())
    def test_execute_without_zip(self, open_mock, csv_mock, tempfile_mock, os_mock):
                self.sql_to_s3.execute(None)
                mock_cursor = self.sql_to_s3.source_hook.get_conn.return_value.cursor.return_value
                mock_cursor.execute.assert_called_once_with(SQL)
                open_mock.assert_called_with(os_mock.path.join.return_value, 'w')
                os_mock.umask.assert_called_with(077)
                csv_mock.writer.assert_called_once_with(open_mock().__enter__())
                self.sql_to_s3.s3_hook.load_file.assert_called_once_with(os_mock.path.join.return_value, S3_FILE_KEY, S3_BUCKET, S3_REPLACE)
                self.sql_to_s3.s3_hook.connection.close.assert_called_once_with()

    @mock.patch('airflow.contrib.operators.sql_to_s3.os')
    @mock.patch('airflow.contrib.operators.sql_to_s3.tempfile')
    @mock.patch('airflow.contrib.operators.sql_to_s3.csv')
    @mock.patch('airflow.contrib.operators.sql_to_s3.zipfile.ZipFile')
    @mock.patch('airflow.contrib.operators.sql_to_s3.open', new_callable=mock_open())
    def test_execute_with_zip(self, open_mock, zip_file_mock, csv_mock, tempfile_mock, os_mock):
                #overrides setUp props
                self.sql_to_s3.s3_zip_file=True
                self.sql_to_s3.execute(None)
                mock_cursor = self.sql_to_s3.source_hook.get_conn.return_value.cursor.return_value
                mock_cursor.execute.assert_called_once_with(SQL)
                open_mock.assert_called_once_with(os_mock.path.join.return_value, 'w')
                csv_mock.writer.assert_called_once_with(open_mock().__enter__())

                zip_file_mock.assert_called_with(os_mock.path.join.return_value, 'w', zipfile.ZIP_DEFLATED)
                os_mock.umask.assert_called_with(077)
                self.sql_to_s3.s3_hook.load_file.assert_called_once_with(os_mock.path.join.return_value, S3_FILE_KEY, S3_BUCKET, S3_REPLACE)
                self.sql_to_s3.s3_hook.connection.close.assert_called_once_with()


if __name__ == '__main__':
    unittest.main()
