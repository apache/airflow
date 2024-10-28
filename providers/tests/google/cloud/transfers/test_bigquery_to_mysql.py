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

from airflow.providers.google.cloud.transfers.bigquery_to_mysql import (
    BigQueryToMySqlOperator,
)

TASK_ID = "test-bq-create-table-operator"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
TEST_DAG_ID = "test-bigquery-operators"


class TestBigQueryToMySqlOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    def test_execute_good_request_to_bq(self, mock_hook):
        destination_table = "table"
        operator = BigQueryToMySqlOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
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
