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

import json
from unittest import mock
from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator

SQL = "SELECT * FROM test_table"
BUCKET = "TEST-BUCKET-1"
FILENAME = "test_results_{}.csv"
TASK_ID = "TEST_TASK_ID"
SCHEMA = [
    {"name": "column_a", "type": "3"},
    {"name": "column_b", "type": "253"},
    {"name": "column_c", "type": "10"},
]
COLUMNS = ["column_a", "column_b", "column_c"]
ROW = ["convert_type_return_value", "convert_type_return_value", "convert_type_return_value"]
CURSOR_DESCRIPTION = [
    ("column_a", "3", 0, 0, 0, 0, False),
    ("column_b", "253", 0, 0, 0, 0, False),
    ("column_c", "10", 0, 0, 0, 0, False),
]
TMP_FILE_NAME = "temp-file"
EMPTY_INPUT_DATA: list[str] = []
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

OUTPUT_DF = pd.DataFrame([["convert_type_return_value"] * 3] * 3, columns=COLUMNS)

EXCLUDE_COLUMNS = set("column_c")
PARTITION_COLUMNS = ["column_b", "column_c"]
NEW_COLUMNS = [c for c in COLUMNS if c not in EXCLUDE_COLUMNS]
OUTPUT_DF_WITH_EXCLUDE_COLUMNS = pd.DataFrame(
    [["convert_type_return_value"] * len(NEW_COLUMNS)] * 3, columns=NEW_COLUMNS
)


class DummySQLToGCSOperator(BaseSQLToGCSOperator):
    def field_to_bigquery(self, field) -> dict[str, str]:
        return {
            "name": field[0],
            "type": "STRING",
            "mode": "NULLABLE",
        }

    def convert_type(self, value, schema_type, stringify_dict):
        return "convert_type_return_value"

    def query(self):
        pass


class TestBaseSQLToGCSOperator:
    @pytest.mark.db_test
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.NamedTemporaryFile")
    @mock.patch("csv.writer")
    @mock.patch.object(GCSHook, "upload")
    @mock.patch.object(DummySQLToGCSOperator, "query")
    @mock.patch.object(DummySQLToGCSOperator, "convert_type")
    def test_exec(self, mock_convert_type, mock_query, mock_upload, mock_writer, mock_tempfile):
        cursor_mock = Mock()
        cursor_mock.description = CURSOR_DESCRIPTION
        cursor_mock.__iter__ = Mock(return_value=iter(INPUT_DATA))
        mock_query.return_value = cursor_mock
        mock_convert_type.return_value = "convert_type_return_value"

        mock_file = mock_tempfile.return_value
        mock_file.tell.return_value = 3
        mock_file.name = TMP_FILE_NAME

        # Test CSV
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
            gcp_conn_id="google_cloud_default",
            upload_metadata=True,
        )
        result = operator.execute(context=dict())

        assert result == {
            "bucket": "TEST-BUCKET-1",
            "total_row_count": 3,
            "total_files": 3,
            "files": [
                {"file_name": "test_results_0.csv", "file_mime_type": "text/csv", "file_row_count": 1},
                {"file_name": "test_results_1.csv", "file_mime_type": "text/csv", "file_row_count": 1},
                {"file_name": "test_results_2.csv", "file_mime_type": "text/csv", "file_row_count": 1},
            ],
        }

        mock_query.assert_called_once()
        assert mock_writer.return_value.writerow.call_args_list == [
            mock.call(COLUMNS),
            mock.call(ROW),
            mock.call(COLUMNS),
            mock.call(ROW),
            mock.call(COLUMNS),
            mock.call(ROW),
            mock.call(COLUMNS),
        ]
        mock_file.flush.assert_has_calls([mock.call(), mock.call(), mock.call(), mock.call()])
        csv_calls = []
        for i in range(3):
            csv_calls.append(
                mock.call(
                    BUCKET,
                    FILENAME.format(i),
                    TMP_FILE_NAME,
                    mime_type="text/csv",
                    gzip=True,
                    metadata={"row_count": 1},
                )
            )
        json_call = mock.call(
            BUCKET, SCHEMA_FILE, TMP_FILE_NAME, mime_type=APP_JSON, gzip=False, metadata=None
        )
        upload_calls = [json_call, csv_calls[0], csv_calls[1], csv_calls[2]]
        mock_upload.assert_has_calls(upload_calls)
        mock_file.close.assert_has_calls([mock.call(), mock.call(), mock.call(), mock.call()])

        mock_query.reset_mock()
        mock_file.flush.reset_mock()
        mock_upload.reset_mock()
        mock_file.close.reset_mock()
        cursor_mock.reset_mock()

        cursor_mock.__iter__ = Mock(return_value=iter(INPUT_DATA))

        # Test JSON
        operator = DummySQLToGCSOperator(
            sql=SQL, bucket=BUCKET, filename=FILENAME, task_id=TASK_ID, export_format="json", schema=SCHEMA
        )
        result = operator.execute(context=dict())

        assert result == {
            "bucket": "TEST-BUCKET-1",
            "total_row_count": 3,
            "total_files": 1,
            "files": [
                {"file_name": "test_results_0.csv", "file_mime_type": "application/json", "file_row_count": 3}
            ],
        }

        mock_query.assert_called_once()
        mock_file.write.call_args_list == [
            mock.call(OUTPUT_DATA),
            mock.call(b"\n"),
            mock.call(OUTPUT_DATA),
            mock.call(b"\n"),
            mock.call(OUTPUT_DATA),
            mock.call(b"\n"),
        ]
        mock_upload.assert_called_once_with(
            BUCKET, FILENAME.format(0), TMP_FILE_NAME, mime_type=APP_JSON, gzip=False, metadata=None
        )
        mock_file.close.assert_called_once()

        mock_query.reset_mock()
        mock_file.flush.reset_mock()
        mock_upload.reset_mock()
        mock_file.close.reset_mock()
        mock_file.write.reset_mock()
        cursor_mock.reset_mock()

        cursor_mock.__iter__ = Mock(return_value=iter(INPUT_DATA))

        # Test Metadata Upload
        operator = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            export_format="json",
            schema=SCHEMA,
            upload_metadata=True,
        )
        result = operator.execute(context=dict())

        assert result == {
            "bucket": "TEST-BUCKET-1",
            "total_row_count": 3,
            "total_files": 1,
            "files": [
                {"file_name": "test_results_0.csv", "file_mime_type": "application/json", "file_row_count": 3}
            ],
        }

        mock_query.assert_called_once()
        mock_file.write.call_args_list == [
            mock.call(OUTPUT_DATA),
            mock.call(b"\n"),
            mock.call(OUTPUT_DATA),
            mock.call(b"\n"),
            mock.call(OUTPUT_DATA),
            mock.call(b"\n"),
        ]

        mock_file.flush.assert_called_once()
        mock_upload.assert_called_once_with(
            BUCKET,
            FILENAME.format(0),
            TMP_FILE_NAME,
            mime_type=APP_JSON,
            gzip=False,
            metadata={"row_count": 3},
        )
        mock_file.close.assert_called_once()

        mock_query.reset_mock()
        mock_file.flush.reset_mock()
        mock_upload.reset_mock()
        mock_file.close.reset_mock()
        cursor_mock.reset_mock()

        cursor_mock.__iter__ = Mock(return_value=iter(INPUT_DATA))

        # Test parquet
        operator = DummySQLToGCSOperator(
            sql=SQL, bucket=BUCKET, filename=FILENAME, task_id=TASK_ID, export_format="parquet", schema=SCHEMA
        )
        result = operator.execute(context=dict())

        assert result == {
            "bucket": "TEST-BUCKET-1",
            "total_row_count": 3,
            "total_files": 1,
            "files": [
                {
                    "file_name": "test_results_0.csv",
                    "file_mime_type": "application/octet-stream",
                    "file_row_count": 3,
                }
            ],
        }

        mock_query.assert_called_once()
        mock_file.flush.assert_called_once()
        mock_upload.assert_called_once_with(
            BUCKET,
            FILENAME.format(0),
            TMP_FILE_NAME,
            mime_type="application/octet-stream",
            gzip=False,
            metadata=None,
        )
        mock_file.close.assert_called_once()

        mock_query.reset_mock()
        mock_file.flush.reset_mock()
        mock_upload.reset_mock()
        mock_file.close.reset_mock()
        cursor_mock.reset_mock()

        cursor_mock.__iter__ = Mock(return_value=iter(INPUT_DATA))

        # Test partition columns
        operator = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            export_format="parquet",
            schema=SCHEMA,
            partition_columns=PARTITION_COLUMNS,
        )
        result = operator.execute(context=dict())

        assert result == {
            "bucket": "TEST-BUCKET-1",
            "total_row_count": 3,
            "total_files": 3,
            "files": [
                {
                    "file_name": "test_results_0.csv",
                    "file_mime_type": "application/octet-stream",
                    "file_row_count": 1,
                },
                {
                    "file_name": "test_results_1.csv",
                    "file_mime_type": "application/octet-stream",
                    "file_row_count": 1,
                },
                {
                    "file_name": "test_results_2.csv",
                    "file_mime_type": "application/octet-stream",
                    "file_row_count": 1,
                },
            ],
        }

        mock_query.assert_called_once()
        assert mock_file.flush.call_count == 3
        assert mock_file.close.call_count == 3
        mock_upload.assert_has_calls(
            [
                mock.call(
                    BUCKET,
                    f"column_b={row[1]}/column_c={row[2]}/test_results_{i}.csv",
                    TMP_FILE_NAME,
                    mime_type="application/octet-stream",
                    gzip=False,
                    metadata=None,
                )
                for i, row in enumerate(INPUT_DATA)
            ]
        )

        mock_query.reset_mock()
        mock_file.flush.reset_mock()
        mock_upload.reset_mock()
        mock_file.close.reset_mock()
        cursor_mock.reset_mock()

        cursor_mock.__iter__ = Mock(return_value=iter(INPUT_DATA))

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
        result = operator.execute(context=dict())

        assert result == {
            "bucket": "TEST-BUCKET-1",
            "total_row_count": 3,
            "total_files": 1,
            "files": [{"file_name": "test_results_0.csv", "file_mime_type": "text/csv", "file_row_count": 3}],
        }

        mock_writer.return_value.writerow.assert_has_calls(
            [
                mock.call(COLUMNS),
                mock.call(["NULL", "NULL", "NULL"]),
                mock.call(["NULL", "NULL", "NULL"]),
                mock.call(["NULL", "NULL", "NULL"]),
            ]
        )

    def test__write_local_data_files_csv(self):
        op = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            schema_filename=SCHEMA_FILE,
            export_format="csv",
            gzip=False,
            schema=SCHEMA,
            gcp_conn_id="google_cloud_default",
        )
        cursor = MagicMock()
        cursor.__iter__.return_value = INPUT_DATA
        cursor.description = CURSOR_DESCRIPTION

        files = op._write_local_data_files(cursor)
        file = next(files)["file_handle"]
        file.flush()
        df = pd.read_csv(file.name)
        assert df.equals(OUTPUT_DF)

    def test__write_local_data_files_json(self):
        op = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            schema_filename=SCHEMA_FILE,
            export_format="json",
            gzip=False,
            schema=SCHEMA,
            gcp_conn_id="google_cloud_default",
        )
        cursor = MagicMock()
        cursor.__iter__.return_value = INPUT_DATA
        cursor.description = CURSOR_DESCRIPTION

        files = op._write_local_data_files(cursor)
        file = next(files)["file_handle"]
        file.flush()
        df = pd.read_json(file.name, orient="records", lines=True)
        assert df.equals(OUTPUT_DF)

    def test__write_local_data_files_parquet(self):
        op = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            schema_filename=SCHEMA_FILE,
            export_format="parquet",
            gzip=False,
            schema=SCHEMA,
            gcp_conn_id="google_cloud_default",
        )
        cursor = MagicMock()
        cursor.__iter__.return_value = INPUT_DATA
        cursor.description = CURSOR_DESCRIPTION

        files = op._write_local_data_files(cursor)
        file = next(files)["file_handle"]
        file.flush()
        df = pd.read_parquet(file.name)
        assert df.equals(OUTPUT_DF)

    def test__write_local_data_files_parquet_with_row_size(self):
        import math

        import pyarrow.parquet as pq

        op = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            schema_filename=SCHEMA_FILE,
            export_format="parquet",
            gzip=False,
            schema=SCHEMA,
            gcp_conn_id="google_cloud_default",
            parquet_row_group_size=8,
        )
        input_data = INPUT_DATA * 10
        output_df = pd.DataFrame([["convert_type_return_value"] * 3] * 30, columns=COLUMNS)

        cursor = MagicMock()
        cursor.__iter__.return_value = input_data
        cursor.description = CURSOR_DESCRIPTION

        files = op._write_local_data_files(cursor)
        file = next(files)["file_handle"]
        file.flush()
        df = pd.read_parquet(file.name)
        assert df.equals(output_df)
        parquet_file = pq.ParquetFile(file.name)
        assert parquet_file.num_row_groups == math.ceil((len(INPUT_DATA) * 10) / op.parquet_row_group_size)
        tolerance = 1
        for i in range(parquet_file.num_row_groups):
            row_group_size = parquet_file.metadata.row_group(i).num_rows
            assert row_group_size == op.parquet_row_group_size or (tolerance := tolerance - 1) >= 0

    def test__write_local_data_files_json_with_exclude_columns(self):
        op = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            schema_filename=SCHEMA_FILE,
            export_format="json",
            gzip=False,
            schema=SCHEMA,
            gcp_conn_id="google_cloud_default",
            exclude_columns=EXCLUDE_COLUMNS,
        )
        cursor = MagicMock()
        cursor.__iter__.return_value = INPUT_DATA
        cursor.description = CURSOR_DESCRIPTION

        files = op._write_local_data_files(cursor)
        file = next(files)["file_handle"]
        file.flush()
        df = pd.read_json(file.name, orient="records", lines=True)
        assert df.equals(OUTPUT_DF_WITH_EXCLUDE_COLUMNS)

    def test__write_local_data_files_parquet_with_partition_columns(self):
        op = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            schema_filename=SCHEMA_FILE,
            export_format="parquet",
            gzip=False,
            schema=SCHEMA,
            gcp_conn_id="google_cloud_default",
            partition_columns=PARTITION_COLUMNS,
        )
        cursor = MagicMock()
        cursor.__iter__.return_value = INPUT_DATA
        cursor.description = CURSOR_DESCRIPTION

        local_data_files = op._write_local_data_files(cursor)
        concat_dfs = []
        for local_data_file in local_data_files:
            file = local_data_file["file_handle"]
            file.flush()
            df = pd.read_parquet(file.name)
            concat_dfs.append(df)

        concat_df = pd.concat(concat_dfs, ignore_index=True)
        assert concat_df.equals(OUTPUT_DF)

    def test__write_local_data_files_csv_does_not_write_on_empty_rows(self):
        op = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            schema_filename=SCHEMA_FILE,
            export_format="csv",
            gzip=False,
            schema=SCHEMA,
            gcp_conn_id="google_cloud_default",
        )
        cursor = MagicMock()
        cursor.__iter__.return_value = EMPTY_INPUT_DATA
        cursor.description = CURSOR_DESCRIPTION

        files = op._write_local_data_files(cursor)
        # Raises StopIteration when next is called because generator returns no files
        with pytest.raises(StopIteration):
            next(files)

    def test__write_local_data_files_csv_writes_empty_file_with_write_on_empty(self):
        op = DummySQLToGCSOperator(
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            task_id=TASK_ID,
            schema_filename=SCHEMA_FILE,
            export_format="csv",
            gzip=False,
            schema=SCHEMA,
            gcp_conn_id="google_cloud_default",
            write_on_empty=True,
        )
        cursor = MagicMock()
        cursor.__iter__.return_value = EMPTY_INPUT_DATA
        cursor.description = CURSOR_DESCRIPTION

        files = op._write_local_data_files(cursor)
        file = next(files)["file_handle"]
        file.flush()

        df = pd.read_csv(file.name)
        assert len(df.index) == 0
