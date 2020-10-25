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

import unittest

from unittest import mock

from airflow import PY38

if not PY38:
    from airflow.providers.google.cloud.transfers.mssql_to_gcs import MSSQLToGCSOperator

TASK_ID = 'test-mssql-to-gcs'
MSSQL_CONN_ID = 'mssql_conn_test'
SQL = 'select 1'
BUCKET = 'gs://test'
JSON_FILENAME = 'test_{}.ndjson'
GZIP = False

ROWS = [('mock_row_content_1', 42), ('mock_row_content_2', 43), ('mock_row_content_3', 44)]
CURSOR_DESCRIPTION = (
    ('some_str', 0, None, None, None, None, None),
    ('some_num', 3, None, None, None, None, None),
)
NDJSON_LINES = [
    b'{"some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_num": 44, "some_str": "mock_row_content_3"}\n',
]
SCHEMA_FILENAME = 'schema_test.json'
SCHEMA_JSON = [
    b'[{"mode": "NULLABLE", "name": "some_str", "type": "STRING"}, ',
    b'{"mode": "NULLABLE", "name": "some_num", "type": "INTEGER"}]',
]


@unittest.skipIf(PY38, "Mssql package not available when Python >= 3.8.")
class TestMsSqlToGoogleCloudStorageOperator(unittest.TestCase):
    def test_init(self):
        """Test MySqlToGoogleCloudStorageOperator instance is properly initialized."""
        op = MSSQLToGCSOperator(task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME)
        self.assertEqual(op.task_id, TASK_ID)
        self.assertEqual(op.sql, SQL)
        self.assertEqual(op.bucket, BUCKET)
        self.assertEqual(op.filename, JSON_FILENAME)

    @mock.patch('airflow.providers.google.cloud.transfers.mssql_to_gcs.MsSqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_exec_success_json(self, gcs_hook_mock_class, mssql_hook_mock_class):
        """Test successful run of execute function for JSON"""
        op = MSSQLToGCSOperator(
            task_id=TASK_ID, mssql_conn_id=MSSQL_CONN_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME
        )

        mssql_hook_mock = mssql_hook_mock_class.return_value
        mssql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mssql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual(JSON_FILENAME.format(0), obj)
            self.assertEqual('application/json', mime_type)
            self.assertEqual(GZIP, gzip)
            with open(tmp_filename, 'rb') as file:
                self.assertEqual(b''.join(NDJSON_LINES), file.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mssql_hook_mock_class.assert_called_once_with(mssql_conn_id=MSSQL_CONN_ID)
        mssql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch('airflow.providers.google.cloud.transfers.mssql_to_gcs.MsSqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_file_splitting(self, gcs_hook_mock_class, mssql_hook_mock_class):
        """Test that ndjson is split by approx_max_file_size_bytes param."""
        mssql_hook_mock = mssql_hook_mock_class.return_value
        mssql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mssql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value
        expected_upload = {
            JSON_FILENAME.format(0): b''.join(NDJSON_LINES[:2]),
            JSON_FILENAME.format(1): NDJSON_LINES[2],
        }

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual('application/json', mime_type)
            self.assertEqual(GZIP, gzip)
            with open(tmp_filename, 'rb') as file:
                self.assertEqual(expected_upload[obj], file.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MSSQLToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            approx_max_file_size_bytes=len(expected_upload[JSON_FILENAME.format(0)]),
        )
        op.execute(None)

    @mock.patch('airflow.providers.google.cloud.transfers.mssql_to_gcs.MsSqlHook')
    @mock.patch('airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook')
    def test_schema_file(self, gcs_hook_mock_class, mssql_hook_mock_class):
        """Test writing schema files."""
        mssql_hook_mock = mssql_hook_mock_class.return_value
        mssql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mssql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip):  # pylint: disable=unused-argument
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, 'rb') as file:
                    self.assertEqual(b''.join(SCHEMA_JSON), file.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MSSQLToGCSOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME, schema_filename=SCHEMA_FILENAME
        )
        op.execute(None)

        # once for the file and once for the schema
        self.assertEqual(2, gcs_hook_mock.upload.call_count)
