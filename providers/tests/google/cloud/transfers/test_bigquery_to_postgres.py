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

import pytest

from airflow.providers.google.cloud.transfers.bigquery_to_postgres import (
    BigQueryToPostgresOperator,
)

TASK_ID = "test-bq-create-table-operator"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
TEST_DAG_ID = "test-bigquery-operators"
TEST_DESTINATION_TABLE = "table"


class TestBigQueryToPostgresOperator:
    @mock.patch(
        "airflow.providers.google.cloud.transfers.bigquery_to_postgres.BigQueryHook"
    )
    def test_execute_good_request_to_bq(self, mock_hook):
        operator = BigQueryToPostgresOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TEST_DESTINATION_TABLE,
            replace=False,
        )

        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.list_rows.assert_called_once_with(
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            max_results=1000,
            selected_fields=None,
            start_index=0,
        )

    @mock.patch(
        "airflow.providers.google.cloud.transfers.bigquery_to_postgres.BigQueryHook"
    )
    def test_execute_good_request_to_bq__with_replace(self, mock_hook):
        operator = BigQueryToPostgresOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TEST_DESTINATION_TABLE,
            replace=True,
            selected_fields=["col_1", "col_2"],
            replace_index=["col_1"],
        )

        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.list_rows.assert_called_once_with(
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            max_results=1000,
            selected_fields=["col_1", "col_2"],
            start_index=0,
        )

    @pytest.mark.parametrize(
        "selected_fields, replace_index",
        [(None, None), (["col_1, col_2"], None), (None, ["col_1"])],
    )
    def test_init_raises_exception_if_replace_is_true_and_missing_params(
        self, selected_fields, replace_index
    ):
        error_msg = "PostgreSQL ON CONFLICT upsert syntax requires column names and a unique index."
        with pytest.raises(ValueError, match=error_msg):
            _ = BigQueryToPostgresOperator(
                task_id=TASK_ID,
                dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
                target_table_name=TEST_DESTINATION_TABLE,
                replace=True,
                selected_fields=selected_fields,
                replace_index=replace_index,
            )
