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

from google.cloud.bigquery import Table

from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    Dataset,
    DocumentationDatasetFacet,
    ExternalQueryRunFacet,
    Fields,
    InputField,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator

BQ_HOOK_PATH = "airflow.providers.google.cloud.transfers.bigquery_to_bigquery.BigQueryHook"
TASK_ID = "test-bq-to-bq-operator"
TEST_GCP_PROJECT_ID = "test-project"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"

SOURCE_PROJECT_DATASET_TABLE = f"{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}"
SOURCE_PROJECT_DATASET_TABLE2 = f"{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}-2"
DESTINATION_PROJECT_DATASET_TABLE = f"{TEST_GCP_PROJECT_ID}.{TEST_DATASET}_new.{TEST_TABLE_ID}"
WRITE_DISPOSITION = "WRITE_EMPTY"
CREATE_DISPOSITION = "CREATE_IF_NEEDED"
LABELS = {"k1": "v1"}
ENCRYPTION_CONFIGURATION = {"key": "kk"}


def split_tablename_side_effect(*args, **kwargs):
    if kwargs["table_input"] == SOURCE_PROJECT_DATASET_TABLE:
        return (
            TEST_GCP_PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )
    if kwargs["table_input"] == SOURCE_PROJECT_DATASET_TABLE2:
        return (
            TEST_GCP_PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID + "-2",
        )
    if kwargs["table_input"] == DESTINATION_PROJECT_DATASET_TABLE:
        return (
            TEST_GCP_PROJECT_ID,
            TEST_DATASET + "_new",
            TEST_TABLE_ID,
        )


class TestBigQueryToBigQueryOperator:
    @mock.patch(BQ_HOOK_PATH)
    def test_execute_without_location_should_execute_successfully(self, mock_hook):
        operator = BigQueryToBigQueryOperator(
            task_id=TASK_ID,
            source_project_dataset_tables=SOURCE_PROJECT_DATASET_TABLE,
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
            source_project_dataset_tables=SOURCE_PROJECT_DATASET_TABLE,
            destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE,
            write_disposition=WRITE_DISPOSITION,
            create_disposition=CREATE_DISPOSITION,
            labels=LABELS,
            encryption_configuration=ENCRYPTION_CONFIGURATION,
            location=location,
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
    def test_get_openlineage_facets_on_complete_single_source_table(self, mock_hook):
        location = "us-central1"

        operator = BigQueryToBigQueryOperator(
            task_id=TASK_ID,
            source_project_dataset_tables=SOURCE_PROJECT_DATASET_TABLE,
            destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE,
            write_disposition=WRITE_DISPOSITION,
            create_disposition=CREATE_DISPOSITION,
            labels=LABELS,
            encryption_configuration=ENCRYPTION_CONFIGURATION,
            location=location,
        )

        source_table_api_repr = {
            "tableReference": {
                "projectId": TEST_GCP_PROJECT_ID,
                "datasetId": TEST_DATASET,
                "tableId": TEST_TABLE_ID,
            },
            "description": "Table description.",
            "schema": {
                "fields": [
                    {"name": "field1", "type": "STRING"},
                    {"name": "field2", "type": "INTEGER"},
                ]
            },
        }
        dest_table_api_repr = {**source_table_api_repr}
        dest_table_api_repr["tableReference"]["datasetId"] = TEST_DATASET + "_new"
        mock_table_data = {
            SOURCE_PROJECT_DATASET_TABLE: Table.from_api_repr(source_table_api_repr),
            DESTINATION_PROJECT_DATASET_TABLE: Table.from_api_repr(dest_table_api_repr),
        }

        mock_hook.return_value.insert_job.return_value.to_api_repr.return_value = {
            "jobReference": {
                "projectId": TEST_GCP_PROJECT_ID,
                "jobId": "actual_job_id",
                "location": location,
            },
            "configuration": {
                "copy": {
                    "sourceTables": [
                        {
                            "projectId": TEST_GCP_PROJECT_ID,
                            "datasetId": TEST_DATASET,
                            "tableId": TEST_TABLE_ID,
                        },
                    ],
                    "destinationTable": {
                        "projectId": TEST_GCP_PROJECT_ID,
                        "datasetId": TEST_DATASET + "_new",
                        "tableId": TEST_TABLE_ID,
                    },
                }
            },
        }
        mock_hook.return_value.split_tablename.side_effect = split_tablename_side_effect
        mock_hook.return_value.get_client.return_value.get_table.side_effect = (
            lambda table_id: mock_table_data[table_id]
        )

        operator.execute(context=mock.MagicMock())
        result = operator.get_openlineage_facets_on_complete(None)

        assert result.job_facets == {}
        assert result.run_facets == {
            "externalQuery": ExternalQueryRunFacet(externalQueryId="actual_job_id", source="bigquery")
        }
        assert len(result.inputs) == 1
        assert result.inputs[0] == Dataset(
            namespace="bigquery",
            name=SOURCE_PROJECT_DATASET_TABLE,
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="field1", type="STRING"),
                        SchemaDatasetFacetFields(name="field2", type="INTEGER"),
                    ]
                ),
                "documentation": DocumentationDatasetFacet("Table description."),
            },
        )
        assert len(result.outputs) == 1
        assert result.outputs[0] == Dataset(
            namespace="bigquery",
            name=DESTINATION_PROJECT_DATASET_TABLE,
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="field1", type="STRING"),
                        SchemaDatasetFacetFields(name="field2", type="INTEGER"),
                    ]
                ),
                "documentation": DocumentationDatasetFacet("Table description."),
                "columnLineage": ColumnLineageDatasetFacet(
                    fields={
                        "field1": Fields(
                            inputFields=[
                                InputField(
                                    namespace="bigquery",
                                    name=SOURCE_PROJECT_DATASET_TABLE,
                                    field="field1",
                                    transformations=[],
                                )
                            ],
                            transformationDescription="identical",
                            transformationType="IDENTITY",
                        ),
                        "field2": Fields(
                            inputFields=[
                                InputField(
                                    namespace="bigquery",
                                    name=SOURCE_PROJECT_DATASET_TABLE,
                                    field="field2",
                                    transformations=[],
                                )
                            ],
                            transformationDescription="identical",
                            transformationType="IDENTITY",
                        ),
                    },
                    dataset=[],
                ),
            },
        )

    @mock.patch(BQ_HOOK_PATH)
    def test_get_openlineage_facets_on_complete_multiple_source_tables(self, mock_hook):
        location = "us-central1"

        operator = BigQueryToBigQueryOperator(
            task_id=TASK_ID,
            source_project_dataset_tables=[
                SOURCE_PROJECT_DATASET_TABLE,
                SOURCE_PROJECT_DATASET_TABLE2,
            ],
            destination_project_dataset_table=DESTINATION_PROJECT_DATASET_TABLE,
            write_disposition=WRITE_DISPOSITION,
            create_disposition=CREATE_DISPOSITION,
            labels=LABELS,
            encryption_configuration=ENCRYPTION_CONFIGURATION,
            location=location,
        )

        source_table_repr = {
            "tableReference": {
                "projectId": TEST_GCP_PROJECT_ID,
                "datasetId": TEST_DATASET,
                "tableId": TEST_TABLE_ID,
            },
            "schema": {
                "fields": [
                    {"name": "field1", "type": "STRING"},
                ]
            },
        }
        source_table_repr2 = {**source_table_repr}
        source_table_repr2["tableReference"]["tableId"] = TEST_TABLE_ID + "-2"
        dest_table_api_repr = {
            "tableReference": {
                "projectId": TEST_GCP_PROJECT_ID,
                "datasetId": TEST_DATASET + "_new",
                "tableId": TEST_TABLE_ID,
            },
            "schema": {
                "fields": [
                    {"name": "field1", "type": "STRING"},
                    {"name": "field2", "type": "INTEGER"},
                ]
            },
        }
        mock_table_data = {
            SOURCE_PROJECT_DATASET_TABLE: Table.from_api_repr(source_table_repr),
            SOURCE_PROJECT_DATASET_TABLE2: Table.from_api_repr(source_table_repr2),
            DESTINATION_PROJECT_DATASET_TABLE: Table.from_api_repr(dest_table_api_repr),
        }

        mock_hook.return_value.insert_job.return_value.to_api_repr.return_value = {
            "jobReference": {
                "projectId": TEST_GCP_PROJECT_ID,
                "jobId": "actual_job_id",
                "location": location,
            },
            "configuration": {
                "copy": {
                    "sourceTables": [
                        {
                            "projectId": TEST_GCP_PROJECT_ID,
                            "datasetId": TEST_DATASET,
                            "tableId": TEST_TABLE_ID,
                        },
                        {
                            "projectId": TEST_GCP_PROJECT_ID,
                            "datasetId": TEST_DATASET,
                            "tableId": TEST_TABLE_ID + "-2",
                        },
                    ],
                    "destinationTable": {
                        "projectId": TEST_GCP_PROJECT_ID,
                        "datasetId": TEST_DATASET + "_new",
                        "tableId": TEST_TABLE_ID,
                    },
                }
            },
        }
        mock_hook.return_value.split_tablename.side_effect = split_tablename_side_effect
        mock_hook.return_value.get_client.return_value.get_table.side_effect = (
            lambda table_id: mock_table_data[table_id]
        )
        operator.execute(context=mock.MagicMock())
        result = operator.get_openlineage_facets_on_complete(None)
        assert result.job_facets == {}
        assert result.run_facets == {
            "externalQuery": ExternalQueryRunFacet(externalQueryId="actual_job_id", source="bigquery")
        }
        assert len(result.inputs) == 2
        assert result.inputs[0] == Dataset(
            namespace="bigquery",
            name=SOURCE_PROJECT_DATASET_TABLE,
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="field1", type="STRING"),
                    ]
                )
            },
        )
        assert result.inputs[1] == Dataset(
            namespace="bigquery",
            name=SOURCE_PROJECT_DATASET_TABLE2,
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="field1", type="STRING"),
                    ]
                )
            },
        )
        assert len(result.outputs) == 1
        assert result.outputs[0] == Dataset(
            namespace="bigquery",
            name=DESTINATION_PROJECT_DATASET_TABLE,
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="field1", type="STRING"),
                        SchemaDatasetFacetFields(name="field2", type="INTEGER"),
                    ]
                ),
                "columnLineage": ColumnLineageDatasetFacet(
                    fields={
                        "field1": Fields(
                            inputFields=[
                                InputField(
                                    namespace="bigquery",
                                    name=SOURCE_PROJECT_DATASET_TABLE,
                                    field="field1",
                                    transformations=[],
                                ),
                                InputField(
                                    namespace="bigquery",
                                    name=SOURCE_PROJECT_DATASET_TABLE2,
                                    field="field1",
                                    transformations=[],
                                ),
                            ],
                            transformationDescription="identical",
                            transformationType="IDENTITY",
                        )
                    },
                    dataset=[],
                ),
            },
        )
