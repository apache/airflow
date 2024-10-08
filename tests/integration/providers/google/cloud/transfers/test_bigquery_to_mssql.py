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

import os
from unittest import mock

import pytest
from google.cloud.bigquery import Row

from airflow.providers.google.cloud.transfers.bigquery_to_mssql import BigQueryToMsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

AIRFLOW_CONN_MSSQL_DEFAULT = "mssql://sa:airflow123@mssql:1433/"
TASK_ID = "test-bq-create-table-operator"
TEST_PROJECT_ID = "test-project"
TEST_DATASET = "test_dataset"
TEST_TABLE_ID = "Persons"
TEST_ROWS = [
    Row((0, "Airflow", "Apache", "1000 N West Street, Suite 1200", "Wilmington, NC, USA"), field_to_index={})
]


@pytest.mark.quarantined
@pytest.mark.integration("mssql")
class TestBigQueryToMsSqlOperator:
    def setup_method(self):
        os.environ["AIRFLOW_CONN_MSSQL_DEFAULT"] = AIRFLOW_CONN_MSSQL_DEFAULT
        mssql_hook = MsSqlHook()
        mssql_conn = mssql_hook.get_conn()
        mssql_hook.set_autocommit(mssql_conn, True)
        self.mssql_cursor = mssql_conn.cursor()
        self.mssql_cursor.execute(f"""CREATE TABLE {TEST_TABLE_ID} (
                            PersonID int,
                            LastName varchar(255),
                            FirstName varchar(255),
                            Address varchar(255),
                            City varchar(255)
                            )""")

    def teardown_method(self):
        self.mssql_cursor.execute(f"DROP TABLE {TEST_TABLE_ID}")

    @mock.patch(
        "airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook",
        return_value=mock.MagicMock(list_rows=mock.MagicMock(side_effect=[TEST_ROWS, []])),
    )
    def test_execute(self, mock_hook):
        operator = BigQueryToMsSqlOperator(
            task_id=TASK_ID,
            source_project_dataset_table=f"{TEST_PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TEST_TABLE_ID,
            selected_fields=["PersonID", "LastName", "FirstName", "Address", "City"],
            replace=False,
        )
        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.list_rows.assert_has_calls(
            [
                mock.call(
                    dataset_id=TEST_DATASET,
                    table_id=TEST_TABLE_ID,
                    max_results=1000,
                    selected_fields=["PersonID", "LastName", "FirstName", "Address", "City"],
                    start_index=0,
                ),
                mock.call(
                    dataset_id=TEST_DATASET,
                    table_id=TEST_TABLE_ID,
                    max_results=1000,
                    selected_fields=["PersonID", "LastName", "FirstName", "Address", "City"],
                    start_index=1000,
                ),
            ],
            any_order=False,
        )
        self.mssql_cursor.execute(f"SELECT * FROM {TEST_TABLE_ID}")
        res = self.mssql_cursor.fetchone()
        assert res == TEST_ROWS[0].values()
