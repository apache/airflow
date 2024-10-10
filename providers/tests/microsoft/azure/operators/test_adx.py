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
from azure.kusto.data._models import KustoResultTable

from airflow.models import DAG
from airflow.providers.microsoft.azure.hooks.adx import AzureDataExplorerHook
from airflow.providers.microsoft.azure.operators.adx import AzureDataExplorerQueryOperator
from airflow.utils import timezone
from airflow.utils.timezone import datetime

TEST_DAG_ID = "unit_tests"
DEFAULT_DATE = datetime(2019, 1, 1)

MOCK_DATA = {
    "task_id": "test_azure_data_explorer_query_operator",
    "query": "Logs | schema",
    "database": "Database",
    "options": {"option1": "option_value"},
}

MOCK_RESULT = KustoResultTable(
    json_table={
        "TableName": "getschema",
        "TableId": 1,
        "TableKind": "PrimaryResult",
        "Columns": [
            {"ColumnName": "Source", "ColumnType": "string", "DataType": "System.String"},
            {
                "ColumnName": "Timestamp",
                "ColumnType": "datetime",
                "DataType": "System.DateTime",
            },
        ],
        "Rows": [["hi", "2017-01-01T01:01:01.0000003Z"], ["hello", "2017-01-01T01:01:01.0000003Z"]],
    }
)


class MockResponse:
    primary_results = [MOCK_RESULT]


class TestAzureDataExplorerQueryOperator:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE, "provide_context": True}

        self.dag = DAG(TEST_DAG_ID + "test_schedule_dag_once", default_args=args, schedule="@once")
        self.operator = AzureDataExplorerQueryOperator(dag=self.dag, **MOCK_DATA)

    def test_init(self):
        assert self.operator.task_id == MOCK_DATA["task_id"]
        assert self.operator.query == MOCK_DATA["query"]
        assert self.operator.database == MOCK_DATA["database"]
        assert self.operator.azure_data_explorer_conn_id == "azure_data_explorer_default"

    @mock.patch.object(AzureDataExplorerHook, "run_query", return_value=MockResponse())
    @mock.patch.object(AzureDataExplorerHook, "get_conn")
    def test_run_query(self, mock_conn, mock_run_query):
        self.operator.execute(None)
        mock_run_query.assert_called_once_with(
            MOCK_DATA["query"], MOCK_DATA["database"], MOCK_DATA["options"]
        )


@pytest.mark.db_test
@mock.patch.object(AzureDataExplorerHook, "run_query", return_value=MockResponse())
@mock.patch.object(AzureDataExplorerHook, "get_conn")
def test_azure_data_explorer_query_operator_xcom_push_and_pull(
    mock_conn,
    mock_run_query,
    create_task_instance_of_operator,
):
    ti = create_task_instance_of_operator(
        AzureDataExplorerQueryOperator,
        dag_id="test_azure_data_explorer_query_operator_xcom_push_and_pull",
        execution_date=timezone.utcnow(),
        **MOCK_DATA,
    )
    ti.run()

    assert ti.xcom_pull(task_ids=MOCK_DATA["task_id"]) == str(MOCK_RESULT)
