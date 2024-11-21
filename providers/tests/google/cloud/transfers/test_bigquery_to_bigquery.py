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

from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from google.cloud.bigquery.table import Table

BQ_HOOK_PATH = "airflow.providers.google.cloud.transfers.bigquery_to_bigquery.BigQueryHook"
TASK_ID = "test-bq-create-table-operator"
TEST_GCP_PROJECT_ID = "test-project"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"

SOURCE_PROJECT_DATASET_TABLES = f"{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}"
DESTINATION_PROJECT_DATASET_TABLE = f"{TEST_GCP_PROJECT_ID}.{TEST_DATASET + '_new'}.{TEST_TABLE_ID}"
WRITE_DISPOSITION = "WRITE_EMPTY"
CREATE_DISPOSITION = "CREATE_IF_NEEDED"
LABELS = {"k1": "v1"}
ENCRYPTION_CONFIGURATION = {"key": "kk"}


def split_tablename_side_effect(*args, **kwargs):
    if kwargs["table_input"] == SOURCE_PROJECT_DATASET_TABLES:
        return (
            TEST_GCP_PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )
    elif kwargs["table_input"] == DESTINATION_PROJECT_DATASET_TABLE:
        return (
            TEST_GCP_PROJECT_ID,
            TEST_DATASET + "_new",
            TEST_TABLE_ID,
        )


def get_table_side_effect(*args, **kwargs):
    dataset_id = ""
    if args[0] == SOURCE_PROJECT_DATASET_TABLES:
        dataset_id = TEST_DATASET
    elif args[0] == DESTINATION_PROJECT_DATASET_TABLE:
        dataset_id = TEST_DATASET + '_new'
    return Table.from_api_repr(
    {
        "tableReference": {"projectId": TEST_GCP_PROJECT_ID, "datasetId": dataset_id, "tableId": TEST_TABLE_ID},
        "description": "table description",
        "schema": {
            "fields": [
                {"name": "field1", "type": "STRING", "description": "field1 description"},
                {"name": "field2", "type": "INTEGER", "description": "field2 description"},
            ]
        },
    }
)


class TestBigQueryToBigQueryOperator:
    @mock.patch(BQ_HOOK_PATH)
    def test_execute_without_location_should_execute_successfully(self, mock_hook):
        operator = BigQueryToBigQueryOperator(
            task_id=TASK_ID,
            source_project_dataset_tables=SOURCE_PROJECT_DATASET_TABLES,
            destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE,
            write_disposition=WRITE_DISPOSITION,
            create_disposition=CREATE_DISPOSITION,
            labels=LABELS,
            encryption_configuration=ENCRYPTION_CONFIGURATION,
        )

        mock_hook.return_value.split_tablename.side_effect = split_tablename_side_effect
        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.insert_job.assert_called_once_with(
            configuration={
                "copy": {
                    "createDisposition": CREATE_DISPOSITION,
                    "destinationEncryptionConfiguration": ENCRYPTION_CONFIGURATION,
                    "destinationTable": {
                        "datasetId": TEST_DATASET + "_new",
                        "projectId": TEST_GCP_PROJECT_ID,
                        "tableId": TEST_TABLE_ID,
                    },
                    "sourceTables": [
                        {
                            "datasetId": TEST_DATASET,
                            "projectId": TEST_GCP_PROJECT_ID,
                            "tableId": TEST_TABLE_ID,
                        },
                    ],
                    "writeDisposition": WRITE_DISPOSITION,
                },
                "labels": LABELS,
            },
            project_id=mock_hook.return_value.project_id,
        )

    @mock.patch(BQ_HOOK_PATH)
    def test_execute_single_regional_location_should_execute_successfully(self, mock_hook):
        location = "us-central1"

        operator = BigQueryToBigQueryOperator(
            task_id=TASK_ID,
            source_project_dataset_tables=SOURCE_PROJECT_DATASET_TABLES,
            destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE,
            write_disposition=WRITE_DISPOSITION,
            create_disposition=CREATE_DISPOSITION,
            labels=LABELS,
            encryption_configuration=ENCRYPTION_CONFIGURATION,
            location=location,
        )

        mock_hook.return_value.split_tablename.side_effect = split_tablename_side_effect
        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.get_job.assert_called_once_with(
            job_id=mock_hook.return_value.insert_job.return_value.job_id,
            location=location,
        )

    @mock.patch(BQ_HOOK_PATH)
    def test_get_openlineage_facets_on_complete(self, mock_hook):
        operator = BigQueryToBigQueryOperator(
            task_id=TASK_ID,
            source_project_dataset_tables=SOURCE_PROJECT_DATASET_TABLES,
            destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE,
        )
        mock_hook.return_value.split_tablename.side_effect = split_tablename_side_effect
        mock_hook.return_value.get_client.return_value.get_table.side_effect = get_table_side_effect

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].name == SOURCE_PROJECT_DATASET_TABLES
        assert lineage.inputs[0].namespace == "bigquery"
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].name == DESTINATION_PROJECT_DATASET_TABLE
        assert lineage.outputs[0].namespace == "bigquery"
