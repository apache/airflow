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
from __future__ import annotations

from unittest.mock import patch

import pytest

from airflow.providers.google.cloud.transfers.trino_to_gcs import TrinoToGCSOperator

TASK_ID = "test-trino-to-gcs"
TRINO_CONN_ID = "my-trino-conn"
GCP_CONN_ID = "my-gcp-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
SQL = "SELECT * FROM memory.default.test_multiple_types"
BUCKET = "gs://test"
FILENAME = "test_{}.ndjson"

NDJSON_LINES = [
    b'{"some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_num": 44, "some_str": "mock_row_content_3"}\n',
]
CSV_LINES = [
    b"some_num,some_str\r\n",
    b"42,mock_row_content_1\r\n",
    b"43,mock_row_content_2\r\n",
    b"44,mock_row_content_3\r\n",
]
SCHEMA_FILENAME = "schema_test.json"
SCHEMA_JSON = b'[{"name": "some_num", "type": "INT64"}, {"name": "some_str", "type": "STRING"}]'


@pytest.mark.integration("trino")
class TestTrinoToGCSOperator:
    def test_init(self):
        """Test TrinoToGCSOperator instance is properly initialized."""
        op = TrinoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        assert op.task_id == TASK_ID
        assert op.sql == SQL
        assert op.bucket == BUCKET
        assert op.filename == FILENAME
        assert op.impersonation_chain == IMPERSONATION_CHAIN

    @patch("airflow.providers.google.cloud.transfers.trino_to_gcs.TrinoHook")
    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_save_as_json(self, mock_gcs_hook, mock_trino_hook):
        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            assert BUCKET == bucket
            assert FILENAME.format(0) == obj
            assert "application/json" == mime_type
            assert not gzip
            with open(tmp_filename, "rb") as file:
                assert b"".join(NDJSON_LINES) == file.read()

        mock_gcs_hook.return_value.upload.side_effect = _assert_upload

        mock_cursor = mock_trino_hook.return_value.get_conn.return_value.cursor

        mock_cursor.return_value.description = [
            ("some_num", "INTEGER", None, None, None, None, None),
            ("some_str", "VARCHAR", None, None, None, None, None),
        ]

        mock_cursor.return_value.fetchone.side_effect = [
            [42, "mock_row_content_1"],
            [43, "mock_row_content_2"],
            [44, "mock_row_content_3"],
            None,
        ]

        op = TrinoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            trino_conn_id=TRINO_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(None)

        mock_trino_hook.assert_called_once_with(trino_conn_id=TRINO_CONN_ID)
        mock_gcs_hook.assert_called_once_with(
            delegate_to=None,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        mock_gcs_hook.return_value.upload.assert_called()

    @patch("airflow.providers.google.cloud.transfers.trino_to_gcs.TrinoHook")
    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_save_as_json_with_file_splitting(self, mock_gcs_hook, mock_trino_hook):
        """Test that ndjson is split by approx_max_file_size_bytes param."""

        expected_upload = {
            FILENAME.format(0): b"".join(NDJSON_LINES[:2]),
            FILENAME.format(1): NDJSON_LINES[2],
        }

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            assert BUCKET == bucket
            assert "application/json" == mime_type
            assert not gzip
            with open(tmp_filename, "rb") as file:
                assert expected_upload[obj] == file.read()

        mock_gcs_hook.return_value.upload.side_effect = _assert_upload

        mock_cursor = mock_trino_hook.return_value.get_conn.return_value.cursor

        mock_cursor.return_value.description = [
            ("some_num", "INTEGER", None, None, None, None, None),
            ("some_str", "VARCHAR(20)", None, None, None, None, None),
        ]

        mock_cursor.return_value.fetchone.side_effect = [
            [42, "mock_row_content_1"],
            [43, "mock_row_content_2"],
            [44, "mock_row_content_3"],
            None,
        ]

        op = TrinoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            approx_max_file_size_bytes=len(expected_upload[FILENAME.format(0)]),
        )

        op.execute(None)

        mock_gcs_hook.return_value.upload.assert_called()

    @patch("airflow.providers.google.cloud.transfers.trino_to_gcs.TrinoHook")
    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_save_as_json_with_schema_file(self, mock_gcs_hook, mock_trino_hook):
        """Test writing schema files."""

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, "rb") as file:
                    assert SCHEMA_JSON == file.read()

        mock_gcs_hook.return_value.upload.side_effect = _assert_upload

        mock_cursor = mock_trino_hook.return_value.get_conn.return_value.cursor

        mock_cursor.return_value.description = [
            ("some_num", "INTEGER", None, None, None, None, None),
            ("some_str", "VARCHAR", None, None, None, None, None),
        ]

        mock_cursor.return_value.fetchone.side_effect = [
            [42, "mock_row_content_1"],
            [43, "mock_row_content_2"],
            [44, "mock_row_content_3"],
            None,
        ]

        op = TrinoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            schema_filename=SCHEMA_FILENAME,
            export_format="csv",
            trino_conn_id=TRINO_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
        )
        op.execute(None)

        # once for the file and once for the schema
        assert 2 == mock_gcs_hook.return_value.upload.call_count

    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    @patch("airflow.providers.google.cloud.transfers.trino_to_gcs.TrinoHook")
    def test_save_as_csv(self, mock_trino_hook, mock_gcs_hook):
        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            assert BUCKET == bucket
            assert FILENAME.format(0) == obj
            assert "text/csv" == mime_type
            assert not gzip
            with open(tmp_filename, "rb") as file:
                assert b"".join(CSV_LINES) == file.read()

        mock_gcs_hook.return_value.upload.side_effect = _assert_upload

        mock_cursor = mock_trino_hook.return_value.get_conn.return_value.cursor

        mock_cursor.return_value.description = [
            ("some_num", "INTEGER", None, None, None, None, None),
            ("some_str", "VARCHAR", None, None, None, None, None),
        ]

        mock_cursor.return_value.fetchone.side_effect = [
            [42, "mock_row_content_1"],
            [43, "mock_row_content_2"],
            [44, "mock_row_content_3"],
            None,
        ]

        op = TrinoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            export_format="csv",
            trino_conn_id=TRINO_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(None)

        mock_gcs_hook.return_value.upload.assert_called()

        mock_trino_hook.assert_called_once_with(trino_conn_id=TRINO_CONN_ID)
        mock_gcs_hook.assert_called_once_with(
            delegate_to=None,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

    @patch("airflow.providers.google.cloud.transfers.trino_to_gcs.TrinoHook")
    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_save_as_csv_with_file_splitting(self, mock_gcs_hook, mock_trino_hook):
        """Test that csv is split by approx_max_file_size_bytes param."""

        expected_upload = {
            FILENAME.format(0): b"".join(CSV_LINES[:3]),
            FILENAME.format(1): b"".join([CSV_LINES[0], CSV_LINES[3]]),
        }

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            assert BUCKET == bucket
            assert "text/csv" == mime_type
            assert not gzip
            with open(tmp_filename, "rb") as file:
                assert expected_upload[obj] == file.read()

        mock_gcs_hook.return_value.upload.side_effect = _assert_upload

        mock_cursor = mock_trino_hook.return_value.get_conn.return_value.cursor

        mock_cursor.return_value.description = [
            ("some_num", "INTEGER", None, None, None, None, None),
            ("some_str", "VARCHAR(20)", None, None, None, None, None),
        ]

        mock_cursor.return_value.fetchone.side_effect = [
            [42, "mock_row_content_1"],
            [43, "mock_row_content_2"],
            [44, "mock_row_content_3"],
            None,
        ]

        op = TrinoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            approx_max_file_size_bytes=len(expected_upload[FILENAME.format(0)]),
            export_format="csv",
        )

        op.execute(None)

        mock_gcs_hook.return_value.upload.assert_called()

    @patch("airflow.providers.google.cloud.transfers.trino_to_gcs.TrinoHook")
    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_save_as_csv_with_schema_file(self, mock_gcs_hook, mock_trino_hook):
        """Test writing schema files."""

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, "rb") as file:
                    assert SCHEMA_JSON == file.read()

        mock_gcs_hook.return_value.upload.side_effect = _assert_upload

        mock_cursor = mock_trino_hook.return_value.get_conn.return_value.cursor

        mock_cursor.return_value.description = [
            ("some_num", "INTEGER", None, None, None, None, None),
            ("some_str", "VARCHAR", None, None, None, None, None),
        ]

        mock_cursor.return_value.fetchone.side_effect = [
            [42, "mock_row_content_1"],
            [43, "mock_row_content_2"],
            [44, "mock_row_content_3"],
            None,
        ]

        op = TrinoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            schema_filename=SCHEMA_FILENAME,
            export_format="csv",
        )
        op.execute(None)

        # once for the file and once for the schema
        assert 2 == mock_gcs_hook.return_value.upload.call_count
