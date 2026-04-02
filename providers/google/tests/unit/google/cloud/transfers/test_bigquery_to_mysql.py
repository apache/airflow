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

from airflow.providers.google.cloud.transfers.bigquery_to_mysql import BigQueryToMySqlOperator

TASK_ID = "test-bq-create-table-operator"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
TEST_DAG_ID = "test-bigquery-operators"
TEST_PROJECT = "test-project"


def _make_bq_table(schema_names: list[str]):
    class TableObj:
        def __init__(self, schema):
            self.schema = []
            for n in schema:
                field = MagicMock()
                field.name = n
                self.schema.append(field)
            self.description = "table description"
            self.external_data_configuration = None
            self.labels = {}
            self.num_rows = 0
            self.num_bytes = 0
            self.table_type = "TABLE"

    return TableObj(schema_names)


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

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_mysql.MySqlHook")
    def test_get_openlineage_facets_on_complete_no_selected_fields(self, mock_mysql_hook, mock_bq_hook):
        mock_bq_client = MagicMock()
        mock_bq_client.get_table.return_value = _make_bq_table(["id", "name", "value"])
        mock_bq_hook.get_client.return_value = mock_bq_client
        mock_bq_hook.return_value = mock_bq_hook

        db_info = MagicMock(scheme="mysql", authority="localhost:3306", database="mydb")
        mock_mysql_hook.get_openlineage_database_info.return_value = db_info
        mock_mysql_hook.return_value = mock_mysql_hook

        op = BigQueryToMySqlOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name="destination",
            selected_fields=None,
            database="mydb",
        )
        op.bigquery_hook = mock_bq_hook
        op.bigquery_hook.project_id = TEST_PROJECT
        op.mysql_hook = mock_mysql_hook
        context = mock.MagicMock()
        op.execute(context=context)

        result = op.get_openlineage_facets_on_complete(None)
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1

        input_ds = result.inputs[0]
        assert input_ds.namespace == "bigquery"
        assert input_ds.name == f"{TEST_PROJECT}.{TEST_DATASET}.{TEST_TABLE_ID}"
        assert "schema" in input_ds.facets
        schema_fields = [f.name for f in input_ds.facets["schema"].fields]
        assert set(schema_fields) == {"id", "name", "value"}

        output_ds = result.outputs[0]
        assert output_ds.namespace == "mysql://localhost:3306"
        assert output_ds.name == "mydb.destination"
        assert "columnLineage" in output_ds.facets
        col_lineage = output_ds.facets["columnLineage"]
        assert set(col_lineage.fields.keys()) == {"id", "name", "value"}

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_mysql.MySqlHook")
    def test_get_openlineage_facets_on_complete_selected_fields(self, mock_mysql_hook, mock_bq_hook):
        mock_bq_client = MagicMock()
        mock_bq_client.get_table.return_value = _make_bq_table(["id", "name", "value"])
        mock_bq_hook.get_client.return_value = mock_bq_client
        mock_bq_hook.return_value = mock_bq_hook

        db_info = MagicMock(scheme="mysql", authority="localhost:3306", database="mydb")
        mock_mysql_hook.get_openlineage_database_info.return_value = db_info
        mock_mysql_hook.return_value = mock_mysql_hook

        op = BigQueryToMySqlOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name="destination",
            selected_fields=["id", "name"],
            database="mydb",
        )
        op.bigquery_hook = mock_bq_hook
        op.bigquery_hook.project_id = TEST_PROJECT
        op.mysql_hook = mock_mysql_hook
        context = mock.MagicMock()
        op.execute(context=context)

        result = op.get_openlineage_facets_on_complete(None)
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1

        input_ds = result.inputs[0]
        assert input_ds.namespace == "bigquery"
        assert input_ds.name == f"{TEST_PROJECT}.{TEST_DATASET}.{TEST_TABLE_ID}"
        assert "schema" in input_ds.facets
        schema_fields = [f.name for f in input_ds.facets["schema"].fields]
        assert set(schema_fields) == {"id", "name"}

        output_ds = result.outputs[0]
        assert output_ds.namespace == "mysql://localhost:3306"
        assert output_ds.name == "mydb.destination"
        assert "columnLineage" in output_ds.facets
        col_lineage = output_ds.facets["columnLineage"]
        assert set(col_lineage.fields.keys()) == {"id", "name"}
