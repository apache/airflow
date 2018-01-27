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

from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PY3 = sys.version_info[0] == 3

TASK_ID = 'test-postgres-to-gcs'
POSTGRES_CONN_ID = 'postgres_conn_test'
SQL = 'select 1'
BUCKET = 'gs://test'
FILENAME = 'test_{}.ndjson'
# we expect the psycopg cursor to return encoded strs in py2 and decoded in py3
if PY3:
    ROWS = [('mock_row_content_1', 42), ('mock_row_content_2', 43), ('mock_row_content_3', 44)]
    CURSOR_DESCRIPTION = (('some_str', 0), ('some_num', 1005))
else:
    ROWS = [(b'mock_row_content_1', 42), (b'mock_row_content_2', 43), (b'mock_row_content_3', 44)]
    CURSOR_DESCRIPTION = ((b'some_str', 0), (b'some_num', 1005))
NDJSON_LINES = [
    b'{"some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_num": 44, "some_str": "mock_row_content_3"}\n'
]
SCHEMA_FILENAME = 'schema_test.json'
SCHEMA_JSON = b'[{"mode": "NULLABLE", "name": "some_str", "type": "STRING"}, {"mode": "REPEATED", "name": "some_num", "type": "INTEGER"}]'


class PostgresToGoogleCloudStorageOperatorTest(unittest.TestCase):
    def test_init(self):
        """Test PostgresToGoogleCloudStorageOperator instance is properly initialized."""
        op = PostgresToGoogleCloudStorageOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=FILENAME)
        self.assertEqual(op.task_id, TASK_ID)
        self.assertEqual(op.sql, SQL)
        self.assertEqual(op.bucket, BUCKET)
        self.assertEqual(op.filename, FILENAME)

    @mock.patch('airflow.contrib.operators.postgres_to_gcs_operator.PostgresHook')
    @mock.patch('airflow.contrib.operators.postgres_to_gcs_operator.GoogleCloudStorageHook')
    def test_exec_success(self, gcs_hook_mock_class, pg_hook_mock_class):
        """Test the execute function in case where the run is successful."""
        op = PostgresToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME)

        pg_hook_mock = pg_hook_mock_class.return_value
        pg_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        pg_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, content_type):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual(FILENAME.format(0), obj)
            self.assertEqual('application/json', content_type)
            with open(tmp_filename, 'rb') as f:
                self.assertEqual(b''.join(NDJSON_LINES), f.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        pg_hook_mock_class.assert_called_once_with(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL, None)

    @mock.patch('airflow.contrib.operators.postgres_to_gcs_operator.PostgresHook')
    @mock.patch('airflow.contrib.operators.postgres_to_gcs_operator.GoogleCloudStorageHook')
    def test_file_splitting(self, gcs_hook_mock_class, pg_hook_mock_class):
        """Test that ndjson is split by approx_max_file_size_bytes param."""
        pg_hook_mock = pg_hook_mock_class.return_value
        pg_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        pg_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

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

        op = PostgresToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            approx_max_file_size_bytes=len(expected_upload[FILENAME.format(0)]))
        op.execute(None)

    @mock.patch('airflow.contrib.operators.postgres_to_gcs_operator.PostgresHook')
    @mock.patch('airflow.contrib.operators.postgres_to_gcs_operator.GoogleCloudStorageHook')
    def test_schema_file(self, gcs_hook_mock_class, pg_hook_mock_class):
        """Test writing schema files."""
        pg_hook_mock = pg_hook_mock_class.return_value
        pg_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        pg_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, content_type):
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, 'rb') as f:
                    self.assertEqual(SCHEMA_JSON, f.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = PostgresToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            schema_filename=SCHEMA_FILENAME)
        op.execute(None)

        # once for the file and once for the schema
        self.assertEqual(2, gcs_hook_mock.upload.call_count)
