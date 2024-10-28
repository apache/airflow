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
from google.cloud.bigquery.retry import DEFAULT_RETRY
from google.cloud.bigquery.table import Table

from airflow.exceptions import TaskDeferred
from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    Dataset,
    DocumentationDatasetFacet,
    ExternalQueryRunFacet,
    Fields,
    Identifier,
    InputField,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
    SymlinksDatasetFacet,
)
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.triggers.bigquery import BigQueryInsertJobTrigger

TASK_ID = "test-bq-create-table-operator"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
PROJECT_ID = "test-project-id"
JOB_PROJECT_ID = "job-project-id"
TEST_BUCKET = "test-bucket"
TEST_FOLDER = "test-folder"
TEST_OBJECT_NO_WILDCARD = "file.extension"
TEST_OBJECT_WILDCARD = "file_*.extension"
TEST_TABLE_API_REPR = {
    "tableReference": {
        "projectId": PROJECT_ID,
        "datasetId": TEST_DATASET,
        "tableId": TEST_TABLE_ID,
    },
    "description": "Table description.",
    "schema": {
        "fields": [
            {"name": "field1", "type": "STRING", "description": "field1 description"},
            {"name": "field2", "type": "INTEGER"},
        ]
    },
}
TEST_TABLE: Table = Table.from_api_repr(TEST_TABLE_API_REPR)
TEST_EMPTY_TABLE_API_REPR = {
    "tableReference": {
        "projectId": PROJECT_ID,
        "datasetId": TEST_DATASET,
        "tableId": TEST_TABLE_ID,
    }
}
TEST_EMPTY_TABLE: Table = Table.from_api_repr(TEST_EMPTY_TABLE_API_REPR)


class TestBigQueryToGCSOperator:
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_execute(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}:{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_cloud_storage_uris = ["gs://some-bucket/some-file.txt"]
        compression = "NONE"
        export_format = "CSV"
        field_delimiter = ","
        print_header = True
        labels = {"k1": "v1"}
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        expected_configuration = {
            "extract": {
                "sourceTable": {
                    "projectId": "test-project-id",
                    "datasetId": "test-dataset",
                    "tableId": "test-table-id",
                },
                "compression": "NONE",
                "destinationUris": ["gs://some-bucket/some-file.txt"],
                "destinationFormat": "CSV",
                "fieldDelimiter": ",",
                "printHeader": True,
            },
            "labels": {"k1": "v1"},
        }

        mock_hook.return_value.split_tablename.return_value = (
            PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.insert_job.return_value = MagicMock(
            job_id="real_job_id", error_result=False
        )
        mock_hook.return_value.project_id = JOB_PROJECT_ID

        operator = BigQueryToGCSOperator(
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
            compression=compression,
            export_format=export_format,
            field_delimiter=field_delimiter,
            print_header=print_header,
            labels=labels,
            project_id=JOB_PROJECT_ID,
        )
        operator.execute(context=mock.MagicMock())

        mock_hook.return_value.insert_job.assert_called_once_with(
            job_id="123456_hash",
            configuration=expected_configuration,
            project_id=JOB_PROJECT_ID,
            location=None,
            timeout=None,
            retry=DEFAULT_RETRY,
            nowait=False,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_execute_deferrable_mode(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}:{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_cloud_storage_uris = ["gs://some-bucket/some-file.txt"]
        compression = "NONE"
        export_format = "CSV"
        field_delimiter = ","
        print_header = True
        labels = {"k1": "v1"}
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        expected_configuration = {
            "extract": {
                "sourceTable": {
                    "projectId": "test-project-id",
                    "datasetId": "test-dataset",
                    "tableId": "test-table-id",
                },
                "compression": "NONE",
                "destinationUris": ["gs://some-bucket/some-file.txt"],
                "destinationFormat": "CSV",
                "fieldDelimiter": ",",
                "printHeader": True,
            },
            "labels": {"k1": "v1"},
        }

        mock_hook.return_value.split_tablename.return_value = (
            PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.insert_job.return_value = MagicMock(
            job_id="real_job_id", error_result=False
        )
        mock_hook.return_value.project_id = JOB_PROJECT_ID

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
            compression=compression,
            export_format=export_format,
            field_delimiter=field_delimiter,
            print_header=print_header,
            labels=labels,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context=mock.MagicMock())

        assert isinstance(
            exc.value.trigger, BigQueryInsertJobTrigger
        ), "Trigger is not a BigQueryInsertJobTrigger"

        mock_hook.return_value.insert_job.assert_called_once_with(
            configuration=expected_configuration,
            job_id="123456_hash",
            project_id=JOB_PROJECT_ID,
            location=None,
            timeout=None,
            retry=DEFAULT_RETRY,
            nowait=True,
        )

    def test_execute_complete_reassigns_job_id(self):
        """Assert that we use job_id from event after deferral."""

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}",
            destination_cloud_storage_uris=[f"gs://{TEST_BUCKET}/{TEST_FOLDER}/"],
            deferrable=True,
            job_id=None,
        )
        job_id = "123456"

        assert operator.job_id is None
        operator.execute_complete(
            context=MagicMock(),
            event={"status": "success", "message": "Job completed", "job_id": job_id},
        )
        assert operator.job_id == job_id

    @pytest.mark.parametrize(
        ("gcs_uri", "expected_dataset_name"),
        (
            (
                f"gs://{TEST_BUCKET}/{TEST_FOLDER}/{TEST_OBJECT_NO_WILDCARD}",
                f"{TEST_FOLDER}/{TEST_OBJECT_NO_WILDCARD}",
            ),
            (f"gs://{TEST_BUCKET}/{TEST_OBJECT_NO_WILDCARD}", TEST_OBJECT_NO_WILDCARD),
            (f"gs://{TEST_BUCKET}/{TEST_FOLDER}/{TEST_OBJECT_WILDCARD}", TEST_FOLDER),
            (f"gs://{TEST_BUCKET}/{TEST_OBJECT_WILDCARD}", "/"),
            (f"gs://{TEST_BUCKET}/{TEST_FOLDER}/*", TEST_FOLDER),
        ),
    )
    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_get_openlineage_facets_on_complete_gcs_dataset_name(
        self, mock_hook, gcs_uri, expected_dataset_name
    ):
        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}",
            destination_cloud_storage_uris=[gcs_uri],
        )

        mock_hook.return_value.split_tablename.return_value = (
            PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )
        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].name == expected_dataset_name

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_get_openlineage_facets_on_complete_gcs_multiple_uris(self, mock_hook):
        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}",
            destination_cloud_storage_uris=[
                f"gs://{TEST_BUCKET}1/{TEST_FOLDER}1/{TEST_OBJECT_NO_WILDCARD}",
                f"gs://{TEST_BUCKET}2/{TEST_FOLDER}2/{TEST_OBJECT_WILDCARD}",
                f"gs://{TEST_BUCKET}3/{TEST_OBJECT_NO_WILDCARD}",
                f"gs://{TEST_BUCKET}4/{TEST_OBJECT_WILDCARD}",
            ],
        )

        mock_hook.return_value.split_tablename.return_value = (
            PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )
        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.outputs) == 4
        assert lineage.outputs[0].name == f"{TEST_FOLDER}1/{TEST_OBJECT_NO_WILDCARD}"
        assert lineage.outputs[1].name == f"{TEST_FOLDER}2"
        assert lineage.outputs[2].name == TEST_OBJECT_NO_WILDCARD
        assert lineage.outputs[3].name == "/"

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_get_openlineage_facets_on_complete_bq_dataset(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}"

        expected_input_dataset_facets = {
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaDatasetFacetFields(
                        name="field1", type="STRING", description="field1 description"
                    ),
                    SchemaDatasetFacetFields(name="field2", type="INTEGER"),
                ]
            ),
            "documentation": DocumentationDatasetFacet(description="Table description."),
        }

        mock_hook.return_value.split_tablename.return_value = (
            PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )
        mock_hook.return_value.get_client.return_value.get_table.return_value = TEST_TABLE

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=["gs://bucket/file"],
        )
        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0] == Dataset(
            namespace="bigquery",
            name=source_project_dataset_table,
            facets=expected_input_dataset_facets,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_get_openlineage_facets_on_complete_bq_dataset_empty_table(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}"

        expected_input_dataset_facets = {
            "schema": SchemaDatasetFacet(fields=[]),
            "documentation": DocumentationDatasetFacet(description=""),
        }

        mock_hook.return_value.split_tablename.return_value = (
            PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )
        mock_hook.return_value.get_client.return_value.get_table.return_value = (
            TEST_EMPTY_TABLE
        )

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=["gs://bucket/file"],
        )
        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0] == Dataset(
            namespace="bigquery",
            name=source_project_dataset_table,
            facets=expected_input_dataset_facets,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_get_openlineage_facets_on_complete_gcs_no_wildcard_empty_table(
        self, mock_hook
    ):
        source_project_dataset_table = f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_cloud_storage_uris = [
            f"gs://{TEST_BUCKET}/{TEST_FOLDER}/{TEST_OBJECT_NO_WILDCARD}"
        ]
        real_job_id = "123456_hash"
        bq_namespace = "bigquery"

        expected_input_facets = {
            "schema": SchemaDatasetFacet(fields=[]),
            "documentation": DocumentationDatasetFacet(description=""),
        }

        expected_output_facets = {
            "schema": SchemaDatasetFacet(fields=[]),
            "columnLineage": ColumnLineageDatasetFacet(fields={}),
        }

        mock_hook.return_value.split_tablename.return_value = (
            PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )
        mock_hook.return_value.insert_job.return_value = MagicMock(
            job_id=real_job_id, error_result=False
        )
        mock_hook.return_value.get_client.return_value.get_table.return_value = (
            TEST_EMPTY_TABLE
        )

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0] == Dataset(
            namespace=bq_namespace,
            name=source_project_dataset_table,
            facets=expected_input_facets,
        )
        assert lineage.outputs[0] == Dataset(
            namespace=f"gs://{TEST_BUCKET}",
            name=f"{TEST_FOLDER}/{TEST_OBJECT_NO_WILDCARD}",
            facets=expected_output_facets,
        )
        assert lineage.run_facets == {
            "externalQuery": ExternalQueryRunFacet(
                externalQueryId=real_job_id, source=bq_namespace
            )
        }
        assert lineage.job_facets == {}

    @mock.patch("airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryHook")
    def test_get_openlineage_facets_on_complete_gcs_wildcard_full_table(self, mock_hook):
        source_project_dataset_table = f"{PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_cloud_storage_uris = [
            f"gs://{TEST_BUCKET}/{TEST_FOLDER}/{TEST_OBJECT_WILDCARD}"
        ]
        real_job_id = "123456_hash"
        bq_namespace = "bigquery"

        schema_facet = SchemaDatasetFacet(
            fields=[
                SchemaDatasetFacetFields(
                    name="field1", type="STRING", description="field1 description"
                ),
                SchemaDatasetFacetFields(name="field2", type="INTEGER"),
            ]
        )
        expected_input_facets = {
            "schema": schema_facet,
            "documentation": DocumentationDatasetFacet(description="Table description."),
        }

        expected_output_facets = {
            "schema": schema_facet,
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "field1": Fields(
                        inputFields=[
                            InputField(
                                namespace=bq_namespace,
                                name=source_project_dataset_table,
                                field="field1",
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                    "field2": Fields(
                        inputFields=[
                            InputField(
                                namespace=bq_namespace,
                                name=source_project_dataset_table,
                                field="field2",
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                }
            ),
            "symlink": SymlinksDatasetFacet(
                identifiers=[
                    Identifier(
                        namespace=f"gs://{TEST_BUCKET}",
                        name=f"{TEST_FOLDER}/{TEST_OBJECT_WILDCARD}",
                        type="file",
                    )
                ]
            ),
        }

        mock_hook.return_value.split_tablename.return_value = (
            PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )
        mock_hook.return_value.insert_job.return_value = MagicMock(
            job_id=real_job_id, error_result=False
        )
        mock_hook.return_value.get_client.return_value.get_table.return_value = TEST_TABLE

        operator = BigQueryToGCSOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert len(lineage.outputs) == 1
        assert lineage.inputs[0] == Dataset(
            namespace=bq_namespace,
            name=source_project_dataset_table,
            facets=expected_input_facets,
        )
        assert lineage.outputs[0] == Dataset(
            namespace=f"gs://{TEST_BUCKET}",
            name=TEST_FOLDER,
            facets=expected_output_facets,
        )
        assert lineage.run_facets == {
            "externalQuery": ExternalQueryRunFacet(
                externalQueryId=real_job_id, source=bq_namespace
            )
        }
        assert lineage.job_facets == {}
