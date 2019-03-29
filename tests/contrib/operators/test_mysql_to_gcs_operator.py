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

import sys
import unittest

from airflow.contrib.operators.mysql_to_gcs import \
    MySqlToGoogleCloudStorageOperator
from tests.compat import mock

PY3 = sys.version_info[0] == 3

TASK_ID = 'test-mysql-to-gcs'
MYSQL_CONN_ID = 'mysql_conn_test'
SQL = 'select 1'
BUCKET = 'gs://test'
JSON_FILENAME = 'test_{}.ndjson'
CSV_FILENAME = 'test_{}.csv'

ROWS = [
    ('mock_row_content_1', 42),
    ('mock_row_content_2', 43),
    ('mock_row_content_3', 44)
]
CURSOR_DESCRIPTION = (
    ('some_str', 0, 0, 0, 0, 0, False),
    ('some_num', 1005, 0, 0, 0, 0, False)
)
NDJSON_LINES = [
    b'{"some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_num": 44, "some_str": "mock_row_content_3"}\n'
]
CSV_LINES = [
    b'some_str,some_num\r\n'
    b'mock_row_content_1,42\r\n',
    b'mock_row_content_2,43\r\n',
    b'mock_row_content_3,44\r\n'
]
CSV_LINES_PIPE_DELIMITED = [
    b'some_str|some_num\r\n'
    b'mock_row_content_1|42\r\n',
    b'mock_row_content_2|43\r\n',
    b'mock_row_content_3|44\r\n'
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
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME,
            export_format='CSV', field_delimiter='|')
        self.assertEqual(op.task_id, TASK_ID)
        self.assertEqual(op.sql, SQL)
        self.assertEqual(op.bucket, BUCKET)
        self.assertEqual(op.filename, JSON_FILENAME)
        self.assertEqual(op.export_format, 'csv')
        self.assertEqual(op.field_delimiter, '|')

    @mock.patch('airflow.contrib.operators.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.contrib.operators.mysql_to_gcs.GoogleCloudStorageHook')
    def test_exec_success_json(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test successful run of execute function for JSON"""
        op = MySqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            mysql_conn_id=MYSQL_CONN_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME)

        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual(JSON_FILENAME.format(0), obj)
            self.assertEqual('application/json', mime_type)
            with open(tmp_filename, 'rb') as f:
                self.assertEqual(b''.join(NDJSON_LINES), f.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mysql_hook_mock_class.assert_called_once_with(mysql_conn_id=MYSQL_CONN_ID)
        mysql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch('airflow.contrib.operators.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.contrib.operators.mysql_to_gcs.GoogleCloudStorageHook')
    def test_exec_success_csv(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test successful run of execute function for CSV"""
        op = MySqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            mysql_conn_id=MYSQL_CONN_ID,
            sql=SQL,
            export_format='CSV',
            bucket=BUCKET,
            filename=CSV_FILENAME)

        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual(CSV_FILENAME.format(0), obj)
            self.assertEqual('text/csv', mime_type)
            with open(tmp_filename, 'rb') as f:
                self.assertEqual(b''.join(CSV_LINES), f.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mysql_hook_mock_class.assert_called_once_with(mysql_conn_id=MYSQL_CONN_ID)
        mysql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch('airflow.contrib.operators.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.contrib.operators.mysql_to_gcs.GoogleCloudStorageHook')
    def test_exec_success_csv_with_delimiter(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test successful run of execute function for CSV with a field delimiter"""
        op = MySqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            mysql_conn_id=MYSQL_CONN_ID,
            sql=SQL,
            export_format='csv',
            field_delimiter='|',
            bucket=BUCKET,
            filename=CSV_FILENAME)

        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual(CSV_FILENAME.format(0), obj)
            self.assertEqual('text/csv', mime_type)
            with open(tmp_filename, 'rb') as f:
                self.assertEqual(b''.join(CSV_LINES_PIPE_DELIMITED), f.read())

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
            JSON_FILENAME.format(0): b''.join(NDJSON_LINES[:2]),
            JSON_FILENAME.format(1): NDJSON_LINES[2],
        }

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual('application/json', mime_type)
            with open(tmp_filename, 'rb') as f:
                self.assertEqual(expected_upload[obj], f.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MySqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            approx_max_file_size_bytes=len(expected_upload[JSON_FILENAME.format(0)]))
        op.execute(None)

    @mock.patch('airflow.contrib.operators.mysql_to_gcs.MySqlHook')
    @mock.patch('airflow.contrib.operators.mysql_to_gcs.GoogleCloudStorageHook')
    def test_schema_file(self, gcs_hook_mock_class, mysql_hook_mock_class):
        """Test writing schema files."""
        mysql_hook_mock = mysql_hook_mock_class.return_value
        mysql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mysql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type):
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, 'rb') as f:
                    self.assertEqual(b''.join(SCHEMA_JSON), f.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MySqlToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            schema_filename=SCHEMA_FILENAME)
        op.execute(None)

        # once for the file and once for the schema
        self.assertEqual(2, gcs_hook_mock.upload.call_count)
