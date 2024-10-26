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
import decimal
from unittest import mock

import pytest

from airflow.providers.google.cloud.transfers.bigquery_sql_to_gcs import BigQuerySqlToGCSOperator

TASK_ID = "test-bq-sql-to-gcs"
TEST_SQL = "SELECT 1"
TEST_BUCKET = "test-bucket"
TEST_FILE_NAME = "file_name"

class TestBigQuerySqlToGCSOperator:
    def test_init_defaults(self):
        """Test BigQuerySqlToGCSOperator instance is properly initialized with defaults."""
        op = BigQuerySqlToGCSOperator(
            task_id=TASK_ID,
            sql=TEST_SQL,
            bucket=TEST_BUCKET,
            filename=TEST_FILE_NAME,
            export_format="CSV",
        )
        assert op.task_id == TASK_ID
        assert op.sql == TEST_SQL
        assert op.bucket == TEST_BUCKET
        assert op.filename == TEST_FILE_NAME
        assert op.export_format == "csv"
        assert op.use_legacy_sql == False
        assert op.sql_gcp_conn_id == "bigquery_default"
        assert op.override_schema_from_cursor == False
        assert op.custom_value_transform_delegate is None
        assert op.custom_field_to_bigquer_delegate is None

    def test_init_custom(self):
        """Test BigQuerySqlToGCSOperator instance is properly initialized with custom settings."""
        use_legacy_sql = True
        sql_gcp_conn_id = "mock sql_gcp_conn_id"
        override_schema_from_cursor = True
        custom_value_transform_delegate = "mock"
        custom_field_to_bigquer_delegate = "mock"
        op = BigQuerySqlToGCSOperator(
            task_id=TASK_ID,
            sql=TEST_SQL,
            bucket=TEST_BUCKET,
            filename=TEST_FILE_NAME,
            export_format="CSV",
            use_legacy_sql=use_legacy_sql,
            sql_gcp_conn_id=sql_gcp_conn_id,
            override_schema_from_cursor=override_schema_from_cursor,
            custom_value_transform_delegate=custom_value_transform_delegate,
        )
        assert op.task_id == TASK_ID
        assert op.sql == TEST_SQL
        assert op.bucket == TEST_BUCKET
        assert op.filename == TEST_FILE_NAME
        assert op.export_format == "csv"
        assert op.use_legacy_sql == use_legacy_sql
        assert op.sql_gcp_conn_id == sql_gcp_conn_id
        assert op.override_schema_from_cursor == override_schema_from_cursor
        assert op.custom_value_transform_delegate == custom_value_transform_delegate
        assert op.custom_field_to_bigquer_delegate == custom_field_to_bigquer_delegate

    @pytest.mark.parametrize(
        "value, field, expected",
        [
            (("n", "t", None, None, None, None, True), {"name":"n", "type": "t", "mode": "NULLABLE"}),
            (("n", "t", None, None, None, None, False), {"name":"n", "type": "t", "mode": "REQUIRED"}),
            (("n", None, None, None, None, None, True), {"name":"n", "type": "STRING", "mode": "NULLABLE"}),
            (("n", None, None, None, None, None, False), {"name":"n", "type": "STRING", "mode": "REQUIRED"}),
        ],
    )
    def test_field_to_bigquery_default(self, field, expected):
        """Tests passing 0's field as value, 1 as type (STRING if None) and 6th as nullable or not."""
        op = BigQuerySqlToGCSOperator(
            task_id=TASK_ID,
            sql=TEST_SQL,
            bucket=TEST_BUCKET,
            filename=TEST_FILE_NAME,
            export_format="CSV",
        )
        result = op.field_to_bigquery(field)
        assert result == expected

    def test_field_to_bigquery_custom(self):
        """Tests redirecting to custom convertation of field to BQ schema."""
        call_count = 0
        mock_result = "mock result"
        def custom(_):
            call_count += 1
            return mock_result
        op = BigQuerySqlToGCSOperator(
            task_id=TASK_ID,
            sql=TEST_SQL,
            bucket=TEST_BUCKET,
            filename=TEST_FILE_NAME,
            export_format="CSV",
            custom_field_to_bigquer_delegate=custom,
        )
        result = op.field_to_bigquery(("n", "t", None, None, None, None, True))
        assert result == mock_result
        assert call_count == 1

    @pytest.mark.parametrize(
        "value, schema_type, expected",
        [
            (None, "mock type", None),
            ("mock value", None, "mock value"),
            ("mock value", None, "mock value"),
            (True, "BOOLEAN", "true"),
            (False, "BOOLEAN", "false"),
            ("True", "BOOLEAN", "True"),
            ("False", "BOOLEAN", "False"),
            ("mock value", "BOOLEAN", "mock value"),
            (4511797920.000, "TIMESTAMP", "2112-12-21 21:12:00.000000 UTC"),
            ("4511797920.000", "TIMESTAMP", "4511797920.000"),
            ("mock value", "mock type", "mock value"),
        ],
    )
    def test_convert_type_default(self, value, schema_type, expected):
        """Tests correctness of of default value rendering."""
        op = BigQuerySqlToGCSOperator(
            task_id=TASK_ID,
            sql=TEST_SQL,
            bucket=TEST_BUCKET,
            filename=TEST_FILE_NAME,
            export_format="CSV",
        )
        result = op.convert_type(value, schema_type)
        assert result == expected

    def test_convert_type_custom(self):
        """Tests redirecting to custom convertation of type."""
        call_count = 0
        mock_result = "mock result"
        def custom(**_):
            call_count += 1
            return mock_result
        op = BigQuerySqlToGCSOperator(
            task_id=TASK_ID,
            sql=TEST_SQL,
            bucket=TEST_BUCKET,
            filename=TEST_FILE_NAME,
            export_format="CSV",
            custom_value_transform_delegate=custom,
        )
        result = op.convert_type("mock value", "mock type")
        assert result == mock_result
        assert call_count == 1
