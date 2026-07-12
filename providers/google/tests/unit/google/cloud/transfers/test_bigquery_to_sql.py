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

import pytest

from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.google.cloud.transfers.bigquery_to_sql import BigQueryToSqlBaseOperator

TASK_ID = "test-bq-to-sql-operator"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
TEST_PROJECT = "test-project"
TARGET_TABLE_NAME = "target_table"


class ConcreteBigQueryToSqlOperator(BigQueryToSqlBaseOperator):
    """Minimal concrete subclass so the abstract base operator can be instantiated."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql_hook = MagicMock(spec=DbApiHook)

    def get_sql_hook(self) -> DbApiHook:
        return self.sql_hook


class TestBigQueryToSqlBaseOperator:
    def test_init_parses_dataset_table(self):
        operator = ConcreteBigQueryToSqlOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TARGET_TABLE_NAME,
        )

        assert operator.dataset_id == TEST_DATASET
        assert operator.table_id == TEST_TABLE_ID

    @pytest.mark.parametrize("dataset_table", ["missing_dot", "too.many.dots"])
    def test_init_raises_exception_if_dataset_table_is_malformed(self, dataset_table):
        with pytest.raises(ValueError, match=f"Could not parse {dataset_table} as <dataset>.<table>"):
            ConcreteBigQueryToSqlOperator(
                task_id=TASK_ID,
                dataset_table=dataset_table,
                target_table_name=TARGET_TABLE_NAME,
            )

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    def test_bigquery_hook_created_with_operator_arguments(self, mock_bq_hook):
        operator = ConcreteBigQueryToSqlOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TARGET_TABLE_NAME,
            gcp_conn_id="google_cloud_test",
            location="europe-west3",
            impersonation_chain=["service-account@test.iam.gserviceaccount.com"],
        )

        assert operator.bigquery_hook is mock_bq_hook.return_value
        mock_bq_hook.assert_called_once_with(
            gcp_conn_id="google_cloud_test",
            location="europe-west3",
            impersonation_chain=["service-account@test.iam.gserviceaccount.com"],
        )

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    def test_execute_fetches_data_from_bigquery(self, mock_bq_hook):
        mock_bq_hook.return_value.list_rows.return_value = []
        operator = ConcreteBigQueryToSqlOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TARGET_TABLE_NAME,
            batch_size=500,
        )

        operator.execute(context=MagicMock())

        mock_bq_hook.return_value.list_rows.assert_called_once_with(
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            max_results=500,
            selected_fields=None,
            start_index=0,
        )
        operator.sql_hook.insert_rows.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.bigquery_get_data")
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    def test_execute_inserts_all_batches_into_sql_table(self, mock_bq_hook, mock_bigquery_get_data):
        first_batch = [("id1", "value1"), ("id2", "value2")]
        second_batch = [("id3", "value3")]
        mock_bigquery_get_data.return_value = iter([first_batch, second_batch])
        operator = ConcreteBigQueryToSqlOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TARGET_TABLE_NAME,
            selected_fields=["id", "value"],
            replace=True,
            batch_size=2,
        )

        operator.execute(context=MagicMock())

        mock_bigquery_get_data.assert_called_once_with(
            operator.log,
            TEST_DATASET,
            TEST_TABLE_ID,
            mock_bq_hook.return_value,
            2,
            ["id", "value"],
        )
        assert operator.sql_hook.insert_rows.call_args_list == [
            mock.call(
                table=TARGET_TABLE_NAME,
                rows=first_batch,
                target_fields=["id", "value"],
                replace=True,
                commit_every=2,
            ),
            mock.call(
                table=TARGET_TABLE_NAME,
                rows=second_batch,
                target_fields=["id", "value"],
                replace=True,
                commit_every=2,
            ),
        ]

    @mock.patch.object(ConcreteBigQueryToSqlOperator, "persist_links")
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.bigquery_get_data")
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    def test_execute_persists_links(self, mock_bq_hook, mock_bigquery_get_data, mock_persist_links):
        mock_bigquery_get_data.return_value = iter([])
        operator = ConcreteBigQueryToSqlOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TARGET_TABLE_NAME,
        )
        context = MagicMock()

        operator.execute(context=context)

        mock_persist_links.assert_called_once_with(context)

    def test_get_openlineage_facets_on_complete_returns_empty_lineage_when_table_fetch_fails(self):
        operator = ConcreteBigQueryToSqlOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TARGET_TABLE_NAME,
        )
        mock_bq_hook = MagicMock(project_id=TEST_PROJECT)
        mock_bq_hook.get_client.return_value.get_table.side_effect = RuntimeError("table not found")
        operator.bigquery_hook = mock_bq_hook

        result = operator.get_openlineage_facets_on_complete(None)

        assert result.inputs == []
        assert result.outputs == []

    def test_get_openlineage_facets_on_complete_returns_empty_lineage_without_database_info(self):
        operator = ConcreteBigQueryToSqlOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TARGET_TABLE_NAME,
        )
        mock_bq_hook = MagicMock(project_id=TEST_PROJECT)
        mock_table = MagicMock(schema=[], description=None, external_data_configuration=None)
        mock_bq_hook.get_client.return_value.get_table.return_value = mock_table
        operator.bigquery_hook = mock_bq_hook
        operator.sql_hook.get_openlineage_database_info.return_value = None

        result = operator.get_openlineage_facets_on_complete(None)

        assert result.inputs == []
        assert result.outputs == []
