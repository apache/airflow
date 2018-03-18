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
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys
import unittest

from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PY3 = sys.version_info[0] == 3

TASK_ID = 'test-mysql-to-gcs'
MYSQL_CONN_ID = 'mysql_conn_test'
SQL = 'select 1'
BUCKET = 'gs://test'
FILENAME = 'test_{}.ndjson'

if PY3:
    ROWS = [
        ('mock_row_content_1', 42),
        ('mock_row_content_2', 43),
        ('mock_row_content_3', 44)
    ]
    CURSOR_DESCRIPTION = (
        ('some_str', 0, 0, 0, 0, 0, False),
        ('some_num', 1005, 0, 0, 0, 0, False)
    )
else:
    ROWS = [
        (b'mock_row_content_1', 42),
        (b'mock_row_content_2', 43),
        (b'mock_row_content_3', 44)
    ]
    CURSOR_DESCRIPTION = (
        (b'some_str', 0, 0, 0, 0, 0, False),
        (b'some_num', 1005, 0, 0, 0, 0, False)
    )
NDJSON_LINES = [
    b'{"some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_num": 44, "some_str": "mock_row_content_3"}\n'
]
CSV_LINES = [
    b'mock_row_content_1,42\r\n',
    b'mock_row_content_2,43\r\n',
    b'mock_row_content_3,44\r\n'
]
SCHEMA_FILENAME = 'schema_test.json'
SCHEMA_JSON = [
    b'[{"mode": "REQUIRED", "name": "some_str", "type": "FLOAT"}, ',
    b'{"mode": "REQUIRED", "name": "some_num", "type": "STRING"}]'
]


class MySqlToGoogleCloudStorageOperatorTest(unittest.TestCase):
    def test_init(self):
        """Test MySqlToGoogleCloudStorageOperator instance is properly initialized."""
        op = MySqlToGoogleCloudStorageOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=FILENAME)
        self.assertEqual(op.task_id, TASK_ID)
        self.assertEqual(op.sql, SQL)
        self.assertEqual(op.bucket, BUCKET)
        self.assertEqual(op.filename, FILENAME)

    @mock.patch('airflow.contrib.operators.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.contrib.operators.mysql_to_gcs.GoogleCloudStorageHook')
    def test_exec_success_json(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test the execute function in case where the run is successful."""
        op = MySqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            mysql_conn_id=MYSQL_CONN_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME)

        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, content_type):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual(FILENAME.format(0), obj)
            self.assertEqual('application/json', content_type)
            with open(tmp_filename, 'rb') as f:
                self.assertEqual(b''.join(NDJSON_LINES), f.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mysql_hook_mock_class.assert_called_once_with(mysql_conn_id=MYSQL_CONN_ID)
        mysql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch('airflow.contrib.operators.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.contrib.operators.mysql_to_gcs.GoogleCloudStorageHook')
    def test_exec_success_csv(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test the execute function in case where the run is successful."""
        op = MySqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            mysql_conn_id=MYSQL_CONN_ID,
            sql=SQL,
            export_format={'file_format': 'csv', 'csv_dialect': 'excel'},
            bucket=BUCKET,
            filename=FILENAME)

        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, content_type):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual(FILENAME.format(0), obj)
            self.assertEqual('application/csv', content_type)
            with open(tmp_filename, 'rb') as f:
                self.assertEqual(b''.join(CSV_LINES), f.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mysql_hook_mock_class.assert_called_once_with(mysql_conn_id=MYSQL_CONN_ID)
        mysql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch('airflow.contrib.operators.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.contrib.operators.mysql_to_gcs.GoogleCloudStorageHook')
    def test_file_splitting(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test that ndjson is split by approx_max_file_size_bytes param."""
        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value
        expected_upload = {
            FILENAME.format(0): b''.join(NDJSON_LINES[:2]),
            FILENAME.format(1): NDJSON_LINES[2],
        }

        def _assert_upload(bucket, obj, tmp_filename, content_type):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual('application/json', content_type)
            with open(tmp_filename, 'rb') as f:
                self.assertEqual(expected_upload[obj], f.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MySqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            approx_max_file_size_bytes=len(expected_upload[FILENAME.format(0)]))
        op.execute(None)

    @mock.patch('airflow.contrib.operators.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.contrib.operators.mysql_to_gcs.GoogleCloudStorageHook')
    def test_schema_file(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test writing schema files."""
        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, content_type):
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, 'rb') as f:
                    self.assertEqual(b''.join(SCHEMA_JSON), f.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MySqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            schema_filename=SCHEMA_FILENAME)
        op.execute(None)

        # once for the file and once for the schema
        self.assertEqual(2, gcs_hook_mock.upload.call_count)
