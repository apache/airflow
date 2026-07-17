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
from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.common.compat.openlineage.facet import (
    OutputDataset,
    SchemaDatasetFacetFields,
)
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

TABLES = {"postgres_to_gcs_operator", "postgres_to_gcs_operator_empty"}

TASK_ID = "test-postgres-to-gcs"
LONG_TASK_ID = "t" * 100
POSTGRES_CONN_ID = "postgres_default"
SQL = "SELECT * FROM postgres_to_gcs_operator"
BUCKET = "gs://test"
FILENAME = "test_{}.ndjson"

NDJSON_LINES = [
    b'{"some_json": {"firtname": "John", "lastname": "Smith", "nested_dict": {"a": null, "b": "something"}}, "some_num": 42, "some_str": "mock_row_content_1"}\n',
    b'{"some_json": {}, "some_num": 43, "some_str": "mock_row_content_2"}\n',
    b'{"some_json": {}, "some_num": 44, "some_str": "mock_row_content_3"}\n',
]
SCHEMA_FILENAME = "schema_test.json"
SCHEMA_JSON = (
    b'[{"mode": "NULLABLE", "name": "some_str", "type": "STRING"}, '
    b'{"mode": "NULLABLE", "name": "some_num", "type": "INTEGER"}, '
    b'{"mode": "NULLABLE", "name": "some_json", "type": "STRING"}]'
)


@pytest.mark.backend("postgres")
class TestPostgresToGoogleCloudStorageOperator:
    @classmethod
    def setup_class(cls):
        postgres = PostgresHook()
        with postgres.get_conn() as conn:
            with conn.cursor() as cur:
                for table in TABLES:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
                    cur.execute(f"CREATE TABLE {table}(some_str varchar, some_num integer, some_json json);")

                cur.execute(
                    "INSERT INTO postgres_to_gcs_operator VALUES(%s, %s, %s);",
                    (
                        "mock_row_content_1",
                        42,
                        '{"lastname": "Smith", "firtname": "John", \
                          "nested_dict": {"a": null, "b": "something"}}',
                    ),
                )
                cur.execute(
                    "INSERT INTO postgres_to_gcs_operator VALUES(%s, %s, %s);",
                    ("mock_row_content_2", 43, "{}"),
                )
                cur.execute(
                    "INSERT INTO postgres_to_gcs_operator VALUES(%s, %s, %s);",
                    ("mock_row_content_3", 44, "{}"),
                )

    @classmethod
    def teardown_class(cls):
        postgres = PostgresHook()
        with postgres.get_conn() as conn:
            with conn.cursor() as cur:
                for table in TABLES:
                    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

    def test_init(self):
        """Test PostgresToGoogleCloudStorageOperator instance is properly initialized."""
        op = PostgresToGCSOperator(task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=FILENAME)
        assert op.task_id == TASK_ID
        assert op.sql == SQL
        assert op.bucket == BUCKET
        assert op.filename == FILENAME

    def _assert_uploaded_file_content(self, bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
        assert bucket == BUCKET
        assert FILENAME.format(0) == obj
        assert mime_type == "application/json"
        assert not gzip
        with open(tmp_filename, "rb") as file:
            assert b"".join(NDJSON_LINES) == file.read()

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("string", "string"),
            (32.9, 32.9),
            (-2, -2),
            (datetime.date(1970, 1, 2), "1970-01-02"),
            (datetime.date(1000, 1, 2), "1000-01-02"),
            (datetime.datetime(1970, 1, 1, 1, 0, tzinfo=None), "1970-01-01T01:00:00"),
            (
                datetime.datetime(2022, 1, 1, 2, 0, tzinfo=datetime.timezone.utc),
                1641002400.0,
            ),
            (datetime.time(hour=0, minute=0, second=0), "0:00:00"),
            (datetime.time(hour=23, minute=59, second=59), "23:59:59"),
        ],
    )
    def test_convert_type(self, value, expected):
        op = PostgresToGCSOperator(
            task_id=TASK_ID, postgres_conn_id=POSTGRES_CONN_ID, sql=SQL, bucket=BUCKET, filename=FILENAME
        )
        assert op.convert_type(value, None) == expected

    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_exec_success(self, gcs_hook_mock_class):
        """Test the execute function in case where the run is successful."""
        op = PostgresToGCSOperator(
            task_id=TASK_ID, postgres_conn_id=POSTGRES_CONN_ID, sql=SQL, bucket=BUCKET, filename=FILENAME
        )

        gcs_hook_mock = gcs_hook_mock_class.return_value
        gcs_hook_mock.upload.side_effect = self._assert_uploaded_file_content
        op.execute(None)

    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_exec_success_server_side_cursor(self, gcs_hook_mock_class):
        """Test the execute in case where the run is successful while using server side cursor."""
        op = PostgresToGCSOperator(
            task_id=TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            use_server_side_cursor=True,
            cursor_itersize=100,
        )
        gcs_hook_mock = gcs_hook_mock_class.return_value
        gcs_hook_mock.upload.side_effect = self._assert_uploaded_file_content
        op.execute(None)

    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_exec_success_server_side_cursor_unique_name(self, gcs_hook_mock_class):
        """
        Test that the server side cursor unique name generator is successful
        with a task id that surpasses postgres identifier limit.
        """
        op = PostgresToGCSOperator(
            task_id=LONG_TASK_ID,
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            use_server_side_cursor=True,
            cursor_itersize=100,
        )
        gcs_hook_mock = gcs_hook_mock_class.return_value
        gcs_hook_mock.upload.side_effect = self._assert_uploaded_file_content
        op.execute(None)

    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_file_splitting(self, gcs_hook_mock_class):
        """Test that ndjson is split by approx_max_file_size_bytes param."""

        gcs_hook_mock = gcs_hook_mock_class.return_value
        expected_upload = {
            FILENAME.format(0): b"".join(NDJSON_LINES[:2]),
            FILENAME.format(1): NDJSON_LINES[2],
        }

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            assert bucket == BUCKET
            assert mime_type == "application/json"
            assert not gzip
            with open(tmp_filename, "rb") as file:
                assert expected_upload[obj] == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = PostgresToGCSOperator(
            task_id=TASK_ID,
            sql=SQL,
            bucket=BUCKET,
            filename=FILENAME,
            approx_max_file_size_bytes=len(expected_upload[FILENAME.format(0)]),
        )
        op.execute(None)

    @patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_schema_file(self, gcs_hook_mock_class):
        """Test writing schema files."""

        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(bucket, obj, tmp_filename, mime_type, gzip, metadata=None):
            if obj == SCHEMA_FILENAME:
                with open(tmp_filename, "rb") as file:
                    assert file.read() == SCHEMA_JSON

        gcs_hook_mock.upload.side_effect = _assert_upload

        op = PostgresToGCSOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=FILENAME, schema_filename=SCHEMA_FILENAME
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
            get_conn = MagicMock(name="conn")
            get_connection = MagicMock()
            database = None

            def get_openlineage_database_info(self, connection):
                from airflow.providers.openlineage.sqlparser import DatabaseInfo

                return DatabaseInfo(
                    scheme="sqlscheme",
                    authority=DbApiHook.get_openlineage_authority_part(connection, default_port=default_port),
                )

        dbapi_hook = DBApiHookForTests()

        class PostgresToGCSOperatorForTest(PostgresToGCSOperator):
            @property
            def db_hook(self):
                return dbapi_hook

        sql = """SELECT a,b,c from my_db.my_table"""
        op = PostgresToGCSOperatorForTest(
            task_id=TASK_ID, sql=sql, bucket="bucket", filename="dir/file{}.csv"
        )
        DB_SCHEMA_NAME = "PUBLIC"
        rows = [
            (DB_SCHEMA_NAME, "popular_orders_day_of_week", "order_day_of_week", 1, "varchar"),
            (DB_SCHEMA_NAME, "popular_orders_day_of_week", "order_placed_on", 2, "timestamp"),
            (DB_SCHEMA_NAME, "popular_orders_day_of_week", "orders_placed", 3, "int4"),
        ]
        dbapi_hook.get_connection.return_value = Connection(
            conn_id="sql_default", conn_type="postgresql", host="host", port=connection_port
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
