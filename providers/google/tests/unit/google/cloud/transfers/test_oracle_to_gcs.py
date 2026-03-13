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

from unittest import mock
from unittest.mock import MagicMock

import oracledb
import pytest

from airflow.models import Connection
from airflow.providers.common.compat.openlineage.facet import (
    OutputDataset,
    SchemaDatasetFacetFields,
)
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator

TASK_ID = "test-oracle-to-gcs"
ORACLE_CONN_ID = "oracle_conn_test"
SQL = "select 1"
BUCKET = "gs://test"
JSON_FILENAME = "test_{}.ndjson"
GZIP = False

ROWS = [("mock_row_content_1", 42), ("mock_row_content_2", 43), ("mock_row_content_3", 44)]
CURSOR_DESCRIPTION = (
    ("some_str", oracledb.DB_TYPE_VARCHAR, None, None, None, None, None),
    ("some_num", oracledb.DB_TYPE_NUMBER, None, None, None, None, None),
)
NDJSON_LINES = [
    b'{"some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_num": 44, "some_str": "mock_row_content_3"}\n',
]
SCHEMA_FILENAME = "schema_test.json"
SCHEMA_JSON = [
    b'[{"mode": "NULLABLE", "name": "some_str", "type": "STRING"}, ',
    b'{"mode": "NULLABLE", "name": "some_num", "type": "NUMERIC"}]',
]


class TestOracleToGoogleCloudStorageOperator:
    def test_init(self):
        """Test OracleToGoogleCloudStorageOperator instance is properly initialized."""
        op = OracleToGCSOperator(task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME)
        assert op.task_id == TASK_ID
        assert op.sql == SQL
        assert op.bucket == BUCKET
        assert op.filename == JSON_FILENAME

    @mock.patch("airflow.providers.google.cloud.transfers.oracle_to_gcs.OracleHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_exec_success_json(self, gcs_hook_mock_class, oracle_hook_mock_class):
        """Test successful run of execute function for JSON"""
        op = OracleToGCSOperator(
            task_id=TASK_ID, oracle_conn_id=ORACLE_CONN_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME
        )

        oracle_hook_mock = oracle_hook_mock_class.return_value
        oracle_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        oracle_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

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

        oracle_hook_mock_class.assert_called_once_with(oracle_conn_id=ORACLE_CONN_ID)
        oracle_hook_mock.get_conn().cursor().execute.assert_called_once_with(SQL)

    @mock.patch("airflow.providers.google.cloud.transfers.oracle_to_gcs.OracleHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_file_splitting(self, gcs_hook_mock_class, oracle_hook_mock_class):
        """Test that ndjson is split by approx_max_file_size_bytes param."""
        oracle_hook_mock = oracle_hook_mock_class.return_value
        oracle_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        oracle_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

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

        op = OracleToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=JSON_FILENAME,
            approx_max_file_size_bytes=len(expected_upload[JSON_FILENAME.format(0)]),
        )
        op.execute(None)

    @mock.patch("airflow.providers.google.cloud.transfers.oracle_to_gcs.OracleHook")
    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_schema_file(self, gcs_hook_mock_class, oracle_hook_mock_class):
        """Test writing schema files."""
        oracle_hook_mock = oracle_hook_mock_class.return_value
        oracle_hook_mock.get_conn().cursor().__iter__.return_value = iter(ROWS)
        oracle_hook_mock.get_conn().cursor().description = CURSOR_DESCRIPTION

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, "rb") as file:
                    assert b"".join(SCHEMA_JSON) == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = OracleToGCSOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=JSON_FILENAME, schema_filename=SCHEMA_FILENAME
        )
        op.execute(None)

        # once for the file and once for the schema
        assert gcs_hook_mock.upload.call_count == 2

    @pytest.mark.parametrize(
        ("input_service_name", "input_sid", "connection_port", "default_port", "expected_port"),
        [
            ("ServiceName", None, None, 1521, 1521),
            (None, "SID", None, 1521, 1521),
            (None, "SID", 1234, None, 1234),
            (None, "SID", 1234, 1521, 1234),
        ],
    )
    def test_execute_openlineage_events(
        self, input_service_name, input_sid, connection_port, default_port, expected_port
    ):
        class DBApiHookForTests(DbApiHook):
            conn_name_attr = "sql_default"
            get_conn = MagicMock(name="conn")
            get_connection = MagicMock()
            service_name = input_service_name
            sid = input_sid

            def get_openlineage_database_info(self, connection):
                from airflow.providers.openlineage.sqlparser import DatabaseInfo

                return DatabaseInfo(
                    scheme="oracle",
                    authority=DbApiHook.get_openlineage_authority_part(connection, default_port=default_port),
                )

        dbapi_hook = DBApiHookForTests()

        class OracleToGCSOperatorForTest(OracleToGCSOperator):
            @property
            def db_hook(self):
                return dbapi_hook

        sql = """SELECT employee_id, first_name FROM hr.employees"""
        op = OracleToGCSOperatorForTest(task_id=TASK_ID, sql=sql, bucket="bucket", filename="dir/file{}.csv")
        DB_SCHEMA_NAME = "HR"
        rows = [
            (DB_SCHEMA_NAME, "employees", "employee_id", 1, "NUMBER"),
            (DB_SCHEMA_NAME, "employees", "first_name", 2, "VARCHAR2"),
            (DB_SCHEMA_NAME, "employees", "age", 3, "NUMBER"),
        ]
        dbapi_hook.get_connection.return_value = Connection(
            conn_id="sql_default", conn_type="oracle", host="host", port=connection_port
        )
        dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

        lineage = op.get_openlineage_facets_on_start()
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].namespace == f"oracle://host:{expected_port}"
        assert lineage.inputs[0].name == f"{input_service_name or input_sid}.HR.employees"
        assert len(lineage.inputs[0].facets) == 1
        assert lineage.inputs[0].facets["schema"].fields == [
            SchemaDatasetFacetFields(name="employee_id", type="NUMBER"),
            SchemaDatasetFacetFields(name="first_name", type="VARCHAR2"),
            SchemaDatasetFacetFields(name="age", type="NUMBER"),
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
