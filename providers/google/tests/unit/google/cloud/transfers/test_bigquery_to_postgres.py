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
from psycopg2.extras import Json

from airflow.providers.google.cloud.transfers.bigquery_to_postgres import BigQueryToPostgresOperator

TASK_ID = "test-bq-create-table-operator"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
TEST_DAG_ID = "test-bigquery-operators"
TEST_DESTINATION_TABLE = "table"
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


class TestBigQueryToPostgresOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_postgres.bigquery_get_data")
    @mock.patch.object(BigQueryToPostgresOperator, "bigquery_hook", new_callable=mock.PropertyMock)
    @mock.patch.object(BigQueryToPostgresOperator, "postgres_hook", new_callable=mock.PropertyMock)
    def test_execute_good_request_to_bq(self, mock_pg_hook, mock_bq_hook, mock_bigquery_get_data):
        operator = BigQueryToPostgresOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TEST_DESTINATION_TABLE,
            replace=False,
        )

        mock_bigquery_get_data.return_value = [[("row1", "val1")], [("row2", "val2")]]
        mock_pg = mock.MagicMock()
        mock_pg_hook.return_value = mock_pg
        mock_bq = mock.MagicMock()
        mock_bq.project_id = TEST_PROJECT
        mock_bq_hook.return_value = mock_bq

        operator.execute(context=mock.MagicMock())

        mock_bigquery_get_data.assert_called_once_with(
            operator.log,
            TEST_DATASET,
            TEST_TABLE_ID,
            mock_bq,
            operator.batch_size,
            operator.selected_fields,
        )
        assert mock_pg.insert_rows.call_count == 2

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_postgres.bigquery_get_data")
    @mock.patch.object(BigQueryToPostgresOperator, "bigquery_hook", new_callable=mock.PropertyMock)
    @mock.patch.object(BigQueryToPostgresOperator, "postgres_hook", new_callable=mock.PropertyMock)
    def test_execute_good_request_to_bq_with_replace(
        self,
        mock_pg_hook,
        mock_bq_hook,
        mock_bigquery_get_data,
    ):
        operator = BigQueryToPostgresOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TEST_DESTINATION_TABLE,
            replace=True,
            selected_fields=["col_1", "col_2"],
            replace_index=["col_1"],
        )

        mock_bigquery_get_data.return_value = [[("only_row", "val")]]
        mock_pg = mock.MagicMock()
        mock_pg_hook.return_value = mock_pg
        mock_bq = mock.MagicMock()
        mock_bq.project_id = TEST_PROJECT
        mock_bq_hook.return_value = mock_bq

        operator.execute(context=mock.MagicMock())

        mock_bigquery_get_data.assert_called_once_with(
            operator.log,
            TEST_DATASET,
            TEST_TABLE_ID,
            mock_bq,
            operator.batch_size,
            ["col_1", "col_2"],
        )
        mock_pg.insert_rows.assert_called_once_with(
            table=TEST_DESTINATION_TABLE,
            rows=[("only_row", "val")],
            target_fields=["col_1", "col_2"],
            replace=True,
            commit_every=operator.batch_size,
            replace_index=["col_1"],
        )

    @pytest.mark.parametrize(
        ("selected_fields", "replace_index"),
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

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id"
    )
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_postgres.register_adapter")
    def test_adapters_to_json_registered(self, mock_register_adapter, mock_get_creds, mock_get_client):
        mock_get_creds.return_value = (None, TEST_PROJECT)
        client = MagicMock()
        client.list_rows.return_value = []
        mock_get_client.return_value = client

        operator = BigQueryToPostgresOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name=TEST_DESTINATION_TABLE,
            replace=False,
        )
        operator.postgres_hook

        mock_register_adapter.assert_any_call(list, Json)
        mock_register_adapter.assert_any_call(dict, Json)

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_postgres.PostgresHook")
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    def test_get_openlineage_facets_on_complete_no_selected_fields(self, mock_bq_hook, mock_postgres_hook):
        mock_bq_client = MagicMock()
        mock_bq_client.get_table.return_value = _make_bq_table(["id", "name", "value"])
        mock_bq_hook.get_client.return_value = mock_bq_client
        mock_bq_hook.return_value = mock_bq_hook

        db_info = MagicMock(scheme="postgres", authority="localhost:5432", database="postgresdb")
        mock_postgres_hook.get_openlineage_database_info.return_value = db_info
        mock_postgres_hook.get_openlineage_default_schema.return_value = "postgres-schema"
        mock_postgres_hook.return_value = mock_postgres_hook

        op = BigQueryToPostgresOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name="destination",
            selected_fields=None,
            database="postgresdb",
        )
        op.bigquery_hook = mock_bq_hook
        op.bigquery_hook.project_id = TEST_PROJECT
        op.postgres_hook = mock_postgres_hook
        context = mock.MagicMock()
        op.execute(context=context)

        result = op.get_openlineage_facets_on_complete(task_instance=MagicMock())
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1

        input_ds = result.inputs[0]
        assert input_ds.namespace == "bigquery"
        assert input_ds.name == f"{TEST_PROJECT}.{TEST_DATASET}.{TEST_TABLE_ID}"
        assert "schema" in input_ds.facets
        schema_fields = [f.name for f in input_ds.facets["schema"].fields]
        assert set(schema_fields) == {"id", "name", "value"}

        output_ds = result.outputs[0]
        assert output_ds.namespace == "postgres://localhost:5432"
        assert output_ds.name == "postgresdb.postgres-schema.destination"

        assert "columnLineage" in output_ds.facets
        col_lineage = output_ds.facets["columnLineage"]
        assert set(col_lineage.fields.keys()) == {"id", "name", "value"}

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_postgres.PostgresHook")
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_sql.BigQueryHook")
    def test_get_openlineage_facets_on_complete_selected_fields(self, mock_bq_hook, mock_postgres_hook):
        mock_bq_client = MagicMock()
        mock_bq_client.get_table.return_value = _make_bq_table(["id", "name", "value"])
        mock_bq_hook.get_client.return_value = mock_bq_client
        mock_bq_hook.return_value = mock_bq_hook

        db_info = MagicMock(scheme="postgres", authority="localhost:5432", database="postgresdb")
        mock_postgres_hook.get_openlineage_database_info.return_value = db_info
        mock_postgres_hook.get_openlineage_default_schema.return_value = "postgres-schema"
        mock_postgres_hook.return_value = mock_postgres_hook

        op = BigQueryToPostgresOperator(
            task_id=TASK_ID,
            dataset_table=f"{TEST_DATASET}.{TEST_TABLE_ID}",
            target_table_name="destination",
            selected_fields=["id", "name"],
            database="postgresdb",
        )
        op.bigquery_hook = mock_bq_hook
        op.bigquery_hook.project_id = TEST_PROJECT
        op.postgres_hook = mock_postgres_hook
        context = mock.MagicMock()
        op.execute(context=context)

        result = op.get_openlineage_facets_on_complete(task_instance=MagicMock())
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1

        input_ds = result.inputs[0]
        assert input_ds.namespace == "bigquery"
        assert input_ds.name == f"{TEST_PROJECT}.{TEST_DATASET}.{TEST_TABLE_ID}"
        assert "schema" in input_ds.facets
        schema_fields = [f.name for f in input_ds.facets["schema"].fields]
        assert set(schema_fields) == {"id", "name"}

        output_ds = result.outputs[0]
        assert output_ds.namespace == "postgres://localhost:5432"
        assert output_ds.name == "postgresdb.postgres-schema.destination"
        assert "columnLineage" in output_ds.facets
        col_lineage = output_ds.facets["columnLineage"]
        assert set(col_lineage.fields.keys()) == {"id", "name"}
