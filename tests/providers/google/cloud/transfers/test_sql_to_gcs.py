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

import json
import unittest
from unittest.mock import Mock

from unittest import mock
import unicodecsv as csv

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator

SQL = "SELECT * FROM test_table"
BUCKET = "TEST-BUCKET-1"
FILENAME = "test_results.csv"
TASK_ID = "TEST_TASK_ID"
SCHEMA = [
    {"name": "column_a", "type": "3"},
    {"name": "column_b", "type": "253"},
    {"name": "column_c", "type": "10"},
]
COLUMNS = ["column_a", "column_b", "column_c"]
ROW = ["convert_type_return_value", "convert_type_return_value", "convert_type_return_value"]
TMP_FILE_NAME = "temp-file"
INPUT_DATA = [
    ["101", "school", "2015-01-01"],
    ["102", "business", "2017-05-24"],
    ["103", "non-profit", "2018-10-01"],
]
OUTPUT_DATA = json.dumps(
    {
        "column_a": "convert_type_return_value",
        "column_b": "convert_type_return_value",
        "column_c": "convert_type_return_value",
    }
).encode("utf-8")
SCHEMA_FILE = "schema_file.json"
APP_JSON = "application/json"


class DummySQLToGCSOperator(BaseSQLToGCSOperator):
    def field_to_bigquery(self, field):
        pass

    def convert_type(self, value, schema_type):
        pass

    def query(self):
        pass


class TestBaseSQLToGCSOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.NamedTemporaryFile")
    @mock.patch.object(csv.writer, "writerow")
    @mock.patch.object(GCSHook, "upload")
    @mock.patch.object(DummySQLToGCSOperator, "query")
    @mock.patch.object(DummySQLToGCSOperator, "field_to_bigquery")
    @mock.patch.object(DummySQLToGCSOperator, "convert_type")
    def test_exec(
        self, mock_convert_type, mock_field_to_bigquery, mock_query, mock_upload, mock_writerow, mock_tempfile
    ):
        cursor_mock = Mock()
        cursor_mock.description = [("column_a", "3"), ("column_b", "253"), ("column_c", "10")]
        cursor_mock.__iter__ = Mock(return_value=iter(INPUT_DATA))
        mock_query.return_value = cursor_mock
        mock_convert_type.return_value = "convert_type_return_value"

        mock_file = Mock()

        mock_tell = Mock()
        mock_tell.return_value = 3
        mock_file.tell = mock_tell

        mock_flush = Mock()
        mock_file.flush = mock_flush

        mock_close = Mock()
        mock_file.close = mock_close

        mock_file.name = TMP_FILE_NAME

        mock_write = Mock()
        mock_file.write = mock_write

        mock_tempfile.return_value = mock_file

        operator = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            schema_filename=SCHEMA_FILE,
            approx_max_file_size_bytes=1,
            export_format="csv",
            gzip=True,
            schema=SCHEMA,
            google_cloud_storage_conn_id='google_cloud_default',
        )
        operator.execute(context=dict())

        mock_query.assert_called_once()
        mock_writerow.assert_has_calls(
            [
                mock.call(COLUMNS),
                mock.call(ROW),
                mock.call(COLUMNS),
                mock.call(ROW),
                mock.call(COLUMNS),
                mock.call(ROW),
                mock.call(COLUMNS),
            ]
        )
        mock_flush.assert_has_calls([mock.call(), mock.call(), mock.call(), mock.call(), mock.call()])
        csv_call = mock.call(BUCKET, FILENAME, TMP_FILE_NAME, mime_type='text/csv', gzip=True)
        json_call = mock.call(BUCKET, SCHEMA_FILE, TMP_FILE_NAME, mime_type=APP_JSON, gzip=False)
        upload_calls = [csv_call, csv_call, csv_call, json_call]
        mock_upload.assert_has_calls(upload_calls)
        mock_close.assert_has_calls([mock.call(), mock.call(), mock.call(), mock.call(), mock.call()])

        mock_query.reset_mock()
        mock_flush.reset_mock()
        mock_upload.reset_mock()
        mock_close.reset_mock()
        cursor_mock.reset_mock()

        cursor_mock.__iter__ = Mock(return_value=iter(INPUT_DATA))

        operator = DummySQLToGCSOperator(
            sql=SQL, bucket=BUCKET, filename=FILENAME, task_id=TASK_ID, export_format="json", schema=SCHEMA
        )
        operator.execute(context=dict())

        mock_query.assert_called_once()
        mock_write.assert_has_calls(
            [
                mock.call(OUTPUT_DATA),
                mock.call(b"\n"),
                mock.call(OUTPUT_DATA),
                mock.call(b"\n"),
                mock.call(OUTPUT_DATA),
                mock.call(b"\n"),
            ]
        )
        mock_flush.assert_called_once()
        mock_upload.assert_called_once_with(BUCKET, FILENAME, TMP_FILE_NAME, mime_type=APP_JSON, gzip=False)
        mock_close.assert_called_once()

        # Test null marker
        cursor_mock.__iter__ = Mock(return_value=iter(INPUT_DATA))
        mock_convert_type.return_value = None

        operator = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            export_format="csv",
            null_marker="NULL",
        )
        operator.execute(context=dict())

        mock_writerow.assert_has_calls(
            [
                mock.call(COLUMNS),
                mock.call(["NULL", "NULL", "NULL"]),
                mock.call(["NULL", "NULL", "NULL"]),
                mock.call(["NULL", "NULL", "NULL"]),
            ]
        )
