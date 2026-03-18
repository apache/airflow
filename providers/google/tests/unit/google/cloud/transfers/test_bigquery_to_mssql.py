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

from airflow.providers.google.cloud.transfers.bigquery_to_mssql import BigQueryToMsSqlOperator

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
            dataset_id=TEST_DATASET,
            project_id=TEST_PROJECT,
            table_id=TEST_TABLE_ID,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_mssql.MsSqlHook")
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    def test_get_openlineage_facets_on_complete_no_selected_fields(self, mock_bq_hook, mock_mssql_hook):
        mock_bq_client = MagicMock()
        table_obj = _make_bq_table(["id", "name", "value"])
        mock_bq_client.get_table.return_value = table_obj
        mock_bq_hook.get_client.return_value = mock_bq_client
        mock_bq_hook.return_value = mock_bq_hook

        db_info = MagicMock(scheme="mssql", authority="localhost:1433", database="mydb")
        mock_mssql_hook.get_openlineage_database_info.return_value = db_info
        mock_mssql_hook.get_openlineage_default_schema.return_value = "dbo"
        mock_mssql_hook.return_value = mock_mssql_hook

        op = BigQueryToMsSqlOperator(
            task_id="test",
            source_project_dataset_table="proj.dataset.table",
            target_table_name="dbo.destination",
            selected_fields=None,
            database="mydb",
        )
        op.bigquery_hook = mock_bq_hook
        op.mssql_hook = mock_mssql_hook
        context = mock.MagicMock()
        op.execute(context=context)

        result = op.get_openlineage_facets_on_complete(task_instance=MagicMock())
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1

        input_ds = result.inputs[0]
        assert input_ds.namespace == "bigquery"
        assert input_ds.name == "proj.dataset.table"

        assert "schema" in input_ds.facets
        schema_fields = [f.name for f in input_ds.facets["schema"].fields]
        assert set(schema_fields) == {"id", "name", "value"}

        output_ds = result.outputs[0]
        assert output_ds.namespace == "mssql://localhost:1433"
        assert output_ds.name == "mydb.dbo.destination"

        assert "columnLineage" in output_ds.facets
        col_lineage = output_ds.facets["columnLineage"]
        assert set(col_lineage.fields.keys()) == {"id", "name", "value"}

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_mssql.MsSqlHook")
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    def test_get_openlineage_facets_on_complete_selected_fields(self, mock_bq_hook, mock_mssql_hook):
        mock_bq_client = MagicMock()
        table_obj = _make_bq_table(["id", "name", "value"])
        mock_bq_client.get_table.return_value = table_obj
        mock_bq_hook.get_client.return_value = mock_bq_client
        mock_bq_hook.return_value = mock_bq_hook

        db_info = MagicMock(scheme="mssql", authority="server.example:1433", database="mydb")
        mock_mssql_hook.get_openlineage_database_info.return_value = db_info
        mock_mssql_hook.get_openlineage_default_schema.return_value = "dbo"
        mock_mssql_hook.return_value = mock_mssql_hook

        op = BigQueryToMsSqlOperator(
            task_id="test",
            source_project_dataset_table="proj.dataset.table",
            target_table_name="dbo.destination",
            selected_fields=["id", "name"],
            database="mydb",
        )
        op.bigquery_hook = mock_bq_hook
        op.mssql_hook = mock_mssql_hook
        context = mock.MagicMock()
        op.execute(context=context)

        result = op.get_openlineage_facets_on_complete(task_instance=MagicMock())
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1

        input_ds = result.inputs[0]
        assert input_ds.namespace == "bigquery"
        assert "schema" in input_ds.facets

        schema_fields = [f.name for f in input_ds.facets["schema"].fields]
        assert set(schema_fields) == {"id", "name"}

        output_ds = result.outputs[0]
        assert output_ds.namespace == "mssql://server.example:1433"
        assert output_ds.name == "mydb.dbo.destination"

        assert "columnLineage" in output_ds.facets
        col_lineage = output_ds.facets["columnLineage"]
        assert set(col_lineage.fields.keys()) == {"id", "name"}
