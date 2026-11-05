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

from itertools import chain, repeat
from unittest.mock import MagicMock, patch

import pytest
from prestodb.dbapi import Cursor as PrestoCursor

from airflow.providers.google.cloud.transfers.presto_to_gcs import (
    PrestoToGCSOperator,
    _PrestoToGCSPrestoCursorAdapter,
)

TASK_ID = "test-presto-to-gcs"
PRESTO_CONN_ID = "my-presto-conn"
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

ROWS = [
    [42, "mock_row_content_1"],
    [43, "mock_row_content_2"],
    [44, "mock_row_content_3"],
]


def _setup_mock_cursor(mock_presto_hook, str_type="VARCHAR"):
    mock_cursor = mock_presto_hook.return_value.get_conn.return_value.cursor
    mock_cursor.return_value.description = [
        ("some_num", "INTEGER", None, None, None, None, None),
        ("some_str", str_type, None, None, None, None, None),
    ]
    mock_cursor.return_value.fetchone.side_effect = [*ROWS, None]
    return mock_cursor


class TestPrestoToGCSPrestoCursorAdapter:
    @pytest.fixture
    def cursor(self):
        cursor = MagicMock(spec=PrestoCursor)
        cursor.fetchone.side_effect = chain(ROWS, repeat(None))
        cursor.arraysize = 2
        return cursor

    def test_description_peeks_first_row_without_consuming_it(self, cursor):
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)

        assert adapter.description is cursor.description
        assert adapter.initialized
        assert cursor.fetchone.call_count == 1
        # The row peeked to load the description is buffered, so fetching it does not call the cursor.
        assert adapter.fetchone() == ROWS[0]
        assert cursor.fetchone.call_count == 1

    def test_description_does_not_peek_twice(self, cursor):
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)

        adapter.description
        adapter.description

        assert cursor.fetchone.call_count == 1

    def test_peekone(self, cursor):
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)

        assert adapter.peekone() == ROWS[0]
        assert adapter.fetchone() == ROWS[0]
        assert adapter.fetchone() == ROWS[1]

    def test_fetchmany_uses_arraysize_by_default(self, cursor):
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)

        assert adapter.fetchmany() == ROWS[:2]
        assert adapter.fetchmany() == ROWS[2:]
        assert adapter.fetchmany() == []

    def test_fetchmany_with_size(self, cursor):
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)

        assert adapter.fetchmany(5) == ROWS

    def test_iteration(self, cursor):
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)

        assert iter(adapter) is adapter
        assert list(adapter) == ROWS

    @pytest.mark.parametrize("method", ["execute", "executemany"])
    def test_execute_drops_row_peeked_from_previous_query(self, cursor, method):
        """A row peeked before running a new query must not be returned as a row of the new query."""
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)
        assert adapter.peekone() == ROWS[0]

        result = getattr(adapter, method)(SQL)

        assert result is getattr(cursor, method).return_value
        getattr(cursor, method).assert_called_once_with(SQL)
        assert not adapter.initialized
        # The next row comes from the cursor, not from the buffer.
        assert adapter.fetchone() == ROWS[1]

    def test_rowcount_and_close_delegate_to_cursor(self, cursor):
        adapter = _PrestoToGCSPrestoCursorAdapter(cursor)

        assert adapter.rowcount is cursor.rowcount
        adapter.close()
        cursor.close.assert_called_once_with()


class TestPrestoToGCSOperator:
    def test_init(self):
        """Test PrestoToGCSOperator instance is properly initialized."""
        op = PrestoToGCSOperator(
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
        assert op.presto_conn_id == "presto_default"
        assert "presto_conn_id" in op.template_fields

    @pytest.mark.parametrize(
        ("field_type", "expected_type"),
        [
            ("integer", "INT64"),
            ("BOOLEAN", "BOOL"),
            ("DECIMAL(10, 2)", "NUMERIC"),
            ("VARCHAR(20)", "STRING"),
            ("TIMESTAMP", "TIMESTAMP"),
            ("TIMESTAMP WITH TIME ZONE", "STRING"),
            ("ARRAY(INTEGER)", "STRING"),
        ],
    )
    def test_field_to_bigquery(self, field_type, expected_type):
        op = PrestoToGCSOperator(task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=FILENAME)

        assert op.field_to_bigquery(("col", field_type)) == {"name": "col", "type": expected_type}

    def test_convert_type_returns_value_unchanged(self):
        """
        The base operator passes every value it writes through ``convert_type``.

        Presto values need no conversion, so they are written as the cursor returns them.
        """
        op = PrestoToGCSOperator(task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=FILENAME)
        value = object()

        assert op.convert_type(value, "STRING") is value

    @pytest.mark.parametrize(
        ("export_format", "expected_mime_type", "expected_content"),
        [
            pytest.param("json", "application/json", b"".join(NDJSON_LINES), id="json"),
            pytest.param("csv", "text/csv", b"".join(CSV_LINES), id="csv"),
        ],
    )
    @patch("airflow.providers.google.cloud.transfers.presto_to_gcs.PrestoHook", autospec=True)
    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook", autospec=True)
    def test_save(self, mock_gcs_hook, mock_presto_hook, export_format, expected_mime_type, expected_content):
        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            assert bucket == BUCKET
            assert FILENAME.format(0) == obj
            assert mime_type == expected_mime_type
            assert not gzip
            with open(tmp_filename, "rb") as file:
                assert expected_content == file.read()

        mock_gcs_hook.return_value.upload.side_effect = _assert_upload
        mock_cursor = _setup_mock_cursor(mock_presto_hook)

        op = PrestoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            export_format=export_format,
            presto_conn_id=PRESTO_CONN_ID,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        op.execute(None)

        mock_presto_hook.assert_called_once_with(presto_conn_id=PRESTO_CONN_ID)
        mock_cursor.return_value.execute.assert_called_once_with(SQL)
        mock_gcs_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_gcs_hook.return_value.upload.assert_called_once()

    @pytest.mark.parametrize(
        ("export_format", "expected_mime_type", "header", "lines"),
        [
            pytest.param("json", "application/json", b"", NDJSON_LINES, id="json"),
            pytest.param("csv", "text/csv", CSV_LINES[0], CSV_LINES[1:], id="csv"),
        ],
    )
    @pytest.mark.parametrize(
        ("size_offset", "splits"),
        [
            pytest.param(-1, True, id="limit-below-two-rows"),
            pytest.param(0, True, id="limit-at-two-rows"),
            pytest.param(1, False, id="limit-above-two-rows"),
        ],
    )
    @patch("airflow.providers.google.cloud.transfers.presto_to_gcs.PrestoHook", autospec=True)
    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook", autospec=True)
    def test_save_split_by_file_size(
        self,
        mock_gcs_hook,
        mock_presto_hook,
        size_offset,
        splits,
        export_format,
        expected_mime_type,
        header,
        lines,
    ):
        """A new file is started once the current one reaches ``approx_max_file_size_bytes``."""
        two_rows = header + lines[0] + lines[1]
        if splits:
            expected_upload = {FILENAME.format(0): two_rows, FILENAME.format(1): header + lines[2]}
        else:
            expected_upload = {FILENAME.format(0): two_rows + lines[2]}

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            assert bucket == BUCKET
            assert mime_type == expected_mime_type
            assert not gzip
            with open(tmp_filename, "rb") as file:
                assert expected_upload[obj] == file.read()

        mock_gcs_hook.return_value.upload.side_effect = _assert_upload
        _setup_mock_cursor(mock_presto_hook, str_type="VARCHAR(20)")

        op = PrestoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            approx_max_file_size_bytes=len(two_rows) + size_offset,
            export_format=export_format,
        )

        op.execute(None)

        assert mock_gcs_hook.return_value.upload.call_count == len(expected_upload)

    @pytest.mark.parametrize("export_format", ["json", "csv"])
    @patch("airflow.providers.google.cloud.transfers.presto_to_gcs.PrestoHook", autospec=True)
    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook", autospec=True)
    def test_save_schema_file(self, mock_gcs_hook, mock_presto_hook, export_format):
        """Test writing schema files."""

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, "rb") as file:
                    assert file.read() == SCHEMA_JSON

        mock_gcs_hook.return_value.upload.side_effect = _assert_upload
        _setup_mock_cursor(mock_presto_hook)

        op = PrestoToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            schema_filename=SCHEMA_FILENAME,
            export_format=export_format,
        )
        op.execute(None)

        # once for the file and once for the schema
        assert mock_gcs_hook.return_value.upload.call_count == 2
