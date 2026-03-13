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

import datetime
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.openlineage.facet import (
    OutputDataset,
    SchemaDatasetFacetFields,
)
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.google.cloud.transfers.mssql_to_gcs import MSSQLToGCSOperator

TASK_ID = "test-mssql-to-gcs"
MSSQL_CONN_ID = "mssql_conn_test"
SQL = "select 1"
BUCKET = "gs://test"
JSON_FILENAME = "test_{}.ndjson"
PARQUET_FILENAME = "test_{}.parquet"
GZIP = False

ROWS = [
    ("mock_row_content_1", 42, True, True),
    ("mock_row_content_2", 43, False, False),
    ("mock_row_content_3", 44, True, True),
]
CURSOR_DESCRIPTION = (
    ("some_str", 0, None, None, None, None, None),
    ("some_num", 3, None, None, None, None, None),
    ("some_binary", 2, None, None, None, None, None),
    ("some_bit", 3, None, None, None, None, None),
)
NDJSON_LINES = [
    b'{"some_binary": true, "some_bit": true, "some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_binary": false, "some_bit": false, "some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_binary": true, "some_bit": true, "some_num": 44, "some_str": "mock_row_content_3"}\n',
]
SCHEMA_FILENAME = "schema_test.json"
SCHEMA_JSON = [
    b'[{"mode": "NULLABLE", "name": "some_str", "type": "STRING"}, ',
    b'{"mode": "NULLABLE", "name": "some_num", "type": "INTEGER"}, ',
    b'{"mode": "NULLABLE", "name": "some_binary", "type": "BOOL"}, ',
    b'{"mode": "NULLABLE", "name": "some_bit", "type": "BOOL"}]',
]

SCHEMA_JSON_BIT_FIELDS = [
    b'[{"mode": "NULLABLE", "name": "some_str", "type": "STRING"}, ',
    b'{"mode": "NULLABLE", "name": "some_num", "type": "INTEGER"}, ',
    b'{"mode": "NULLABLE", "name": "some_binary", "type": "BOOL"}, ',
    b'{"mode": "NULLABLE", "name": "some_bit", "type": "INTEGER"}]',
]


class TestMsSqlToGoogleCloudStorageOperator:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("string", "string"),
            (32.9, 32.9),
            (-2, -2),
            (datetime.date(1970, 1, 2), "1970-01-02"),
            (datetime.date(1000, 1, 2), "1000-01-02"),
            (datetime.datetime(1970, 1, 1, 1, 0), "1970-01-01T01:00:00"),
            (datetime.time(hour=0, minute=0, second=0), "00:00:00"),
            (datetime.time(hour=23, minute=59, second=59), "23:59:59"),
        ],
    )
    def test_convert_type(self, value, expected):
        op = MSSQLToGCSOperator(
            task_id=TASK_ID,
            mssql_conn_id=MSSQL_CONN_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )
        assert op.convert_type(value, None) == expected

    def test_init(self):
        """Test MySqlToGoogleCloudStorageOperator instance is properly initialized."""
        op = MSSQLToGCSOperator(task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME)
        assert op.task_id == TASK_ID
        assert op.sql == SQL
        assert op.bucket == BUCKET
        assert op.filename == JSON_FILENAME

    @mock.patch("airflow.providers.google.cloud.transfers.mssql_to_gcs.MsSqlHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_exec_success_json(self, gcs_hook_mock_class, mssql_hook_mock_class):
        """Test successful run of execute function for JSON"""
        op = MSSQLToGCSOperator(
            task_id=TASK_ID, mssql_conn_id=MSSQL_CONN_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME
        )

        mssql_hook_mock = mssql_hook_mock_class.return_value
        mssql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mssql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False, metadata=None):
            assert bucket == BUCKET
            assert JSON_FILENAME.format(0) == obj
            assert mime_type == "application/json"
            assert gzip == GZIP
            with open(tmp_filename, "rb") as file:
                assert b"".join(NDJSON_LINES) == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        mssql_hook_mock_class.assert_called_once_with(mssql_conn_id=MSSQL_CONN_ID)
        mssql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch("airflow.providers.google.cloud.transfers.mssql_to_gcs.MsSqlHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_file_splitting(self, gcs_hook_mock_class, mssql_hook_mock_class):
        """Test that ndjson is split by approx_max_file_size_bytes param."""
        mssql_hook_mock = mssql_hook_mock_class.return_value
        mssql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mssql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value
        expected_upload = {
            JSON_FILENAME.format(0): b"".join(NDJSON_LINES[:2]),
            JSON_FILENAME.format(1): NDJSON_LINES[2],
        }

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False, metadata=None):
            assert bucket == BUCKET
            assert mime_type == "application/json"
            assert gzip == GZIP
            with open(tmp_filename, "rb") as file:
                assert expected_upload[obj] == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MSSQLToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            approx_max_file_size_bytes=len(expected_upload[JSON_FILENAME.format(0)]),
        )
        op.execute(None)

    @mock.patch("airflow.providers.google.cloud.transfers.mssql_to_gcs.MsSqlHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    @pytest.mark.parametrize(
        ("bit_fields", "schema_json"),
        [(None, SCHEMA_JSON), (["bit_fields", SCHEMA_JSON_BIT_FIELDS])],
    )
    def test_schema_file(self, gcs_hook_mock_class, mssql_hook_mock_class, bit_fields, schema_json):
        """Test writing schema files."""
        mssql_hook_mock = mssql_hook_mock_class.return_value
        mssql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mssql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, "rb") as file:
                    assert b"".join(SCHEMA_JSON) == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = MSSQLToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            schema_filename=SCHEMA_FILENAME,
            bit_fields=["some_bit"],
        )
        op.execute(None)

        # once for the file and once for the schema
        assert gcs_hook_mock.upload.call_count == 2

    @pytest.mark.parametrize(
        ("connection_port", "default_port", "expected_port"),
        [(None, 4321, 4321), (1234, None, 1234), (1234, 4321, 1234)],
    )
    def test_execute_openlineage_events(self, connection_port, default_port, expected_port):
        class DBApiHookForTests(DbApiHook):
            conn_name_attr = "sql_default"
            get_conn = mock.MagicMock(name="conn")
            get_connection = mock.MagicMock()

            def get_openlineage_database_info(self, connection):
                from airflow.providers.openlineage.sqlparser import DatabaseInfo

                return DatabaseInfo(
                    scheme="sqlscheme",
                    authority=DbApiHook.get_openlineage_authority_part(connection, default_port=default_port),
                )

        dbapi_hook = DBApiHookForTests()

        class MSSQLToGCSOperatorForTest(MSSQLToGCSOperator):
            @property
            def db_hook(self):
                return dbapi_hook

        sql = """SELECT a,b,c from my_db.my_table"""
        op = MSSQLToGCSOperatorForTest(task_id=TASK_ID, sql=sql, bucket="bucket", filename="dir/file{}.csv")
        DB_SCHEMA_NAME = "PUBLIC"
        rows = [
            (DB_SCHEMA_NAME, "popular_orders_day_of_week", "order_day_of_week", 1, "varchar"),
            (DB_SCHEMA_NAME, "popular_orders_day_of_week", "order_placed_on", 2, "timestamp"),
            (DB_SCHEMA_NAME, "popular_orders_day_of_week", "orders_placed", 3, "int4"),
        ]
        dbapi_hook.get_connection.return_value = Connection(
            conn_id="sql_default", conn_type="mssql", host="host", port=connection_port
        )
        dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

        lineage = op.get_openlineage_facets_on_start()
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].namespace == f"sqlscheme://host:{expected_port}"
        assert lineage.inputs[0].name == "PUBLIC.popular_orders_day_of_week"
        assert len(lineage.inputs[0].facets) == 1
        assert lineage.inputs[0].facets["schema"].fields == [
            SchemaDatasetFacetFields(name="order_day_of_week", type="varchar"),
            SchemaDatasetFacetFields(name="order_placed_on", type="timestamp"),
            SchemaDatasetFacetFields(name="orders_placed", type="int4"),
        ]
        assert lineage.outputs == [
            OutputDataset(
                namespace="gs://bucket",
                name="dir",
            )
        ]

        assert len(lineage.job_facets) == 1
        assert lineage.job_facets["sql"].query == sql
        assert lineage.run_facets == {}

    @mock.patch("airflow.providers.google.cloud.transfers.mssql_to_gcs.MsSqlHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_bit_to_boolean_field_conversion(self, gcs_hook_mock_class, mssql_hook_mock_class):
        """Test successful run of execute function for Parquet format with boolean fields.

        This test verifies that MSSQL tables with columns of type "BIT" can exported
        using the bit_fields parameter, resulting in boolean fields in the Parquet file.
        """
        import pyarrow

        op = MSSQLToGCSOperator(
            task_id=TASK_ID,
            mssql_conn_id=MSSQL_CONN_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=PARQUET_FILENAME,
            export_format="parquet",
            bit_fields=["some_binary", "some_bit"],
        )

        mssql_hook_mock = mssql_hook_mock_class.return_value
        mssql_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        mssql_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        upload_called = False

        def _assert_upload(bucket, obj, tmp_filename, mime_type=None, gzip=False, metadata=None):
            nonlocal upload_called
            upload_called = True
            assert bucket == BUCKET
            assert obj == PARQUET_FILENAME.format(0)
            assert mime_type == "application/octet-stream"
            assert gzip == GZIP

            parquet_file = pyarrow.parquet.ParquetFile(tmp_filename)
            schema = parquet_file.schema_arrow
            # Verify that bit fields are mapped to boolean type in parquet schema
            assert schema.field("some_binary").type.equals(pyarrow.bool_())
            assert schema.field("some_bit").type.equals(pyarrow.bool_())

        gcs_hook_mock.upload.side_effect = _assert_upload

        op.execute(None)

        assert upload_called, "Expected upload to be called"
        mssql_hook_mock_class.assert_called_once_with(mssql_conn_id=MSSQL_CONN_ID)
        mssql_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)
