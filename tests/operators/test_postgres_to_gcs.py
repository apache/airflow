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

import unittest

import pytest
from mock import patch

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

TABLES = {'postgres_to_gcs_operator', 'postgres_to_gcs_operator_empty'}

TASK_ID = 'test-postgres-to-gcs'
POSTGRES_CONN_ID = 'postgres_default'
SQL = 'SELECT * FROM postgres_to_gcs_operator'
BUCKET = 'gs://test'
FILENAME = 'test_{}.ndjson'

NDJSON_LINES = [
    b'{"some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_num": 44, "some_str": "mock_row_content_3"}\n'
]
SCHEMA_FILENAME = 'schema_test.json'
SCHEMA_JSON = b'[{"mode": "NULLABLE", "name": "some_str", "type": "STRING"}, ' \
              b'{"mode": "NULLABLE", "name": "some_num", "type": "INTEGER"}]'


@pytest.mark.backend("postgres")
class TestPostgresToGoogleCloudStorageOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        postgres = PostgresHook()
        with postgres.get_conn() as conn:
            with conn.cursor() as cur:
                for table in TABLES:
                    cur.execute("DROP TABLE IF EXISTS {} CASCADE;".format(table))
                    cur.execute("CREATE TABLE {}(some_str varchar, some_num integer);"
                                .format(table))

                cur.execute(
                    "INSERT INTO postgres_to_gcs_operator VALUES(%s, %s);",
                    ('mock_row_content_1', 42)
                )
                cur.execute(
                    "INSERT INTO postgres_to_gcs_operator VALUES(%s, %s);",
                    ('mock_row_content_2', 43)
                )
                cur.execute(
                    "INSERT INTO postgres_to_gcs_operator VALUES(%s, %s);",
                    ('mock_row_content_3', 44)
                )

    @classmethod
    def tearDownClass(cls):
        postgres = PostgresHook()
        with postgres.get_conn() as conn:
            with conn.cursor() as cur:
                for table in TABLES:
                    cur.execute("DROP TABLE IF EXISTS {} CASCADE;".format(table))

    def test_init(self):
        """Test PostgresToGoogleCloudStorageOperator instance is properly initialized."""
        op = PostgresToGoogleCloudStorageOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=FILENAME)
        self.assertEqual(op.task_id, TASK_ID)
        self.assertEqual(op.sql, SQL)
        self.assertEqual(op.bucket, BUCKET)
        self.assertEqual(op.filename, FILENAME)

    @patch('airflow.operators.sql_to_gcs.GCSHook')
    def test_exec_success(self, gcs_hook_mock_class):
        """Test the execute function in case where the run is successful."""
        op = PostgresToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME)

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual(FILENAME.format(0), obj)
            self.assertEqual('application/json', mime_type)
            self.assertFalse(gzip)
            with open(tmp_filename, 'rb') as file:
                self.assertEqual(b''.join(NDJSON_LINES), file.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

    @patch('airflow.operators.sql_to_gcs.GCSHook')
    def test_file_splitting(self, gcs_hook_mock_class):
        """Test that ndjson is split by approx_max_file_size_bytes param."""

        gcs_hook_mock = gcs_hook_mock_class.return_value
        expected_upload = {
            FILENAME.format(0): b''.join(NDJSON_LINES[:2]),
            FILENAME.format(1): NDJSON_LINES[2],
        }

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip):
            self.assertEqual(BUCKET, bucket)
            self.assertEqual('application/json', mime_type)
            self.assertFalse(gzip)
            with open(tmp_filename, 'rb') as file:
                self.assertEqual(expected_upload[obj], file.read())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = PostgresToGoogleCloudStorageOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            approx_max_file_size_bytes=len(expected_upload[FILENAME.format(0)]))
        op.execute(None)

    @patch('airflow.operators.sql_to_gcs.GCSHook')
    def test_schema_file(self, gcs_hook_mock_class):
        """Test writing schema files."""

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip):  # pylint: disable=unused-argument
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, 'rb') as file:
                    self.assertEqual(SCHEMA_JSON, file.read())

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
