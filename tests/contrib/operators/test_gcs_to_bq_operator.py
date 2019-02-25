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
from datetime import datetime

from airflow.contrib.operators.gcs_to_bq import \
    GoogleCloudStorageToBigQueryOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

LOAD_DAG_ID = "load_dag_id"
TASK_ID = "test_task_id"
DEFAULT_DATE = datetime(2017, 1, 1)

BUCKET = "test-bucket"
SOURCE_OBJECT = "test.data"
BQ_DESTINATION = "test-project:test_dataset.test_table"
BQ_SCHEMA = [
    {"mode": "NULLABLE", "name": "firstname", "type": "STRING"},
    {"mode": "NULLABLE", "name": "lastname", "type": "STRING"},
]
KWARGS = {
    "autodetect": False,
    "cluster_fields": None,
    "destination_project_dataset_table": BQ_DESTINATION,
    "skip_leading_rows": 0,
    "write_disposition": "WRITE_EMPTY",
    "allow_jagged_rows": False,
    "allow_quoted_newlines": False,
    "create_disposition": "CREATE_IF_NEEDED",
    "field_delimiter": ",",
    "ignore_unknown_values": False,
    "max_bad_records": 0,
    "quote_character": None,
    "schema_update_options": (),
    "source_format": "CSV",
    "src_fmt_configs": {},
    "time_partitioning": {},
}


class GoogleCloudStorageToBigQueryOperatorTest(unittest.TestCase):
    @mock.patch("airflow.contrib.operators.gcs_to_bq.BigQueryHook")
    def test_execute(self, mock_hook):
        gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id=TASK_ID,
            bucket=BUCKET,
            source_objects=SOURCE_OBJECT,
            schema_fields=BQ_SCHEMA,
            destination_project_dataset_table=BQ_DESTINATION,
        )
        gcs_to_bq.bq_hook = mock_hook
        gcs_to_bq.execute(None)

        source_uris = [f"gs://{BUCKET}/{SOURCE_OBJECT}"]
        mock_hook.return_value.get_conn().cursor().run_load.assert_called_once_with(
            source_uris=source_uris, schema_fields=BQ_SCHEMA, **KWARGS
        )

    @mock.patch("airflow.contrib.operators.gcs_to_bq.BigQueryHook")
    def test_execute_single_source_in_list(self, mock_hook):
        gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id=TASK_ID,
            bucket=BUCKET,
            source_objects=[SOURCE_OBJECT],
            schema_fields=BQ_SCHEMA,
            destination_project_dataset_table=BQ_DESTINATION,
        )
        gcs_to_bq.bq_hook = mock_hook
        gcs_to_bq.execute(None)

        source_uris = [f"gs://{BUCKET}/{SOURCE_OBJECT}"]
        mock_hook.return_value.get_conn().cursor().run_load.assert_called_once_with(
            source_uris=source_uris, schema_fields=BQ_SCHEMA, **KWARGS
        )

    @mock.patch("airflow.contrib.operators.gcs_to_bq.BigQueryHook")
    def test_execute_single_source_in_list_as_str(self, mock_hook):
        gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id=TASK_ID,
            bucket=BUCKET,
            source_objects=f"['{SOURCE_OBJECT}']",
            schema_fields=BQ_SCHEMA,
            destination_project_dataset_table=BQ_DESTINATION,
        )
        gcs_to_bq.bq_hook = mock_hook
        gcs_to_bq.execute(None)

        source_uris = [f"gs://{BUCKET}/{SOURCE_OBJECT}"]
        mock_hook.return_value.get_conn().cursor().run_load.assert_called_once_with(
            source_uris=source_uris, schema_fields=BQ_SCHEMA, **KWARGS
        )

    @mock.patch("airflow.contrib.operators.gcs_to_bq.BigQueryHook")
    def test_execute_multiple_sources_in_list(self, mock_hook):
        gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id=TASK_ID,
            bucket=BUCKET,
            source_objects=[SOURCE_OBJECT, SOURCE_OBJECT],
            schema_fields=BQ_SCHEMA,
            destination_project_dataset_table=BQ_DESTINATION,
        )
        gcs_to_bq.bq_hook = mock_hook
        gcs_to_bq.execute(None)

        source_uris = [
            f"gs://{BUCKET}/{SOURCE_OBJECT}",
            f"gs://{BUCKET}/{SOURCE_OBJECT}",
        ]
        mock_hook.return_value.get_conn().cursor().run_load.assert_called_once_with(
            source_uris=source_uris, schema_fields=BQ_SCHEMA, **KWARGS
        )

    @mock.patch("airflow.contrib.operators.gcs_to_bq.BigQueryHook")
    def test_execute_multiple_sources_in_list_as_str(self, mock_hook):
        gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id=TASK_ID,
            bucket=BUCKET,
            source_objects=f"['{SOURCE_OBJECT}', '{SOURCE_OBJECT}']",
            schema_fields=BQ_SCHEMA,
            destination_project_dataset_table=BQ_DESTINATION,
        )
        gcs_to_bq.bq_hook = mock_hook
        gcs_to_bq.execute(None)

        source_uris = [
            f"gs://{BUCKET}/{SOURCE_OBJECT}",
            f"gs://{BUCKET}/{SOURCE_OBJECT}",
        ]
        mock_hook.return_value.get_conn().cursor().run_load.assert_called_once_with(
            source_uris=source_uris, schema_fields=BQ_SCHEMA, **KWARGS
        )

    @mock.patch("airflow.contrib.operators.gcs_to_bq.BigQueryHook")
    def test_execute_bq_schema(self, mock_hook):
        gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id=TASK_ID,
            bucket=BUCKET,
            source_objects=SOURCE_OBJECT,
            schema_fields=BQ_SCHEMA,
            destination_project_dataset_table=BQ_DESTINATION,
        )
        gcs_to_bq.bq_hook = mock_hook
        gcs_to_bq.execute(None)

        source_uris = [f"gs://{BUCKET}/{SOURCE_OBJECT}"]
        mock_hook.return_value.get_conn().cursor().run_load.assert_called_once_with(
            source_uris=source_uris, schema_fields=BQ_SCHEMA, **KWARGS
        )

    @mock.patch("airflow.contrib.operators.gcs_to_bq.BigQueryHook")
    def test_execute_bq_schema_as_str(self, mock_hook):
        gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
            task_id=TASK_ID,
            bucket=BUCKET,
            source_objects=SOURCE_OBJECT,
            schema_fields=f"{BQ_SCHEMA}",
            destination_project_dataset_table=BQ_DESTINATION,
        )
        gcs_to_bq.bq_hook = mock_hook
        gcs_to_bq.execute(None)

        source_uris = [f"gs://{BUCKET}/{SOURCE_OBJECT}"]
        mock_hook.return_value.get_conn().cursor().run_load.assert_called_once_with(
            source_uris=source_uris, schema_fields=BQ_SCHEMA, **KWARGS
        )
