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

from airflow.providers.google.cloud.transfers.bigquery_to_mssql import BigQueryToMsSqlOperator

TASK_ID = "test-bq-create-table-operator"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
TEST_DAG_ID = "test-bigquery-operators"
TEST_PROJECT = "test-project"


class TestBigQueryToMsSqlOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_mssql.BigQueryTableLink")
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    def test_execute_good_request_to_bq(self, mock_hook, mock_link):
        destination_table = "table"
        operator = BigQueryToMsSqlOperator(
            task_id=TASK_ID,
            source_project_dataset_table=f"{TEST_PROJECT}.{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=destination_table,
            replace=False,
        )

        operator.execute(None)
        mock_hook.return_value.list_rows.assert_called_once_with(
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            max_results=1000,
            selected_fields=None,
            start_index=0,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_mssql.MsSqlHook")
    def test_get_sql_hook(self, mock_hook):
        hook_expected = mock_hook.return_value

        destination_table = "table"
        operator = BigQueryToMsSqlOperator(
            task_id=TASK_ID,
            source_project_dataset_table=f"{TEST_PROJECT}.{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=destination_table,
            replace=False,
        )

        hook_actual = operator.get_sql_hook()

        assert hook_actual == hook_expected
        mock_hook.assert_called_once_with(schema=operator.database, mssql_conn_id=operator.mssql_conn_id)

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_mssql.BigQueryTableLink")
    def test_persist_links(self, mock_link):
        mock_context = mock.MagicMock()

        destination_table = "table"
        operator = BigQueryToMsSqlOperator(
            task_id=TASK_ID,
            source_project_dataset_table=f"{TEST_PROJECT}.{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=destination_table,
            replace=False,
        )
        operator.persist_links(context=mock_context)

        mock_link.persist.assert_called_once_with(
            context=mock_context,
            task_instance=operator,
            dataset_id=TEST_DATASET,
            project_id=TEST_PROJECT,
            table_id=TEST_TABLE_ID,
        )
