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

import functools
import json
from unittest import mock
from unittest.mock import MagicMock, call

import pytest
from google.cloud.bigquery import DEFAULT_RETRY, Table
from google.cloud.exceptions import Conflict
from sqlalchemy import select

from airflow.models.trigger import Trigger
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
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.state import TaskInstanceState

TASK_ID = "test-gcs-to-bq-operator"
TEST_EXPLICIT_DEST = "test-project.dataset.table"
TEST_BUCKET = "test-bucket"
TEST_FOLDER = "test-folder"
TEST_OBJECT_NO_WILDCARD = "file.extension"
TEST_OBJECT_WILDCARD = "file_*.extension"
PROJECT_ID = "test-project"
DATASET = "dataset"
TABLE = "table"
JOB_PROJECT_ID = "job-project-id"
WRITE_DISPOSITION = "WRITE_TRUNCATE"
MAX_ID_KEY = "id"
TEST_DATASET_LOCATION = "US"
SCHEMA_FIELDS = [
    {"name": "id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
]
SCHEMA_FIELDS_INT = [
    {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
]
SCHEMA_BUCKET = "test-schema-bucket"
SCHEMA_OBJECT = "test/schema/schema.json"
TEST_SOURCE_OBJECTS_LIST = ["test/objects/test.csv"]
TEST_SOURCE_OBJECTS = "test/objects/test.csv"
TEST_SOURCE_OBJECTS_JSON = "test/objects/test.json"
LABELS = {"k1": "v1"}
DESCRIPTION = "Test Description"
TEST_TABLE: Table = Table.from_api_repr(
    {
        "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
        "description": DESCRIPTION,
        "schema": {
            "fields": [
                {"name": "field1", "type": "STRING", "description": "field1 description"},
                {"name": "field2", "type": "INTEGER"},
            ]
        },
    }
)
TEST_EMPTY_TABLE: Table = Table.from_api_repr(
    {"tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE}}
)
job_id = "123456"
hash_ = "hash"
REAL_JOB_ID = f"{job_id}_{hash_}"

GCS_TO_BQ_PATH = "airflow.providers.google.cloud.transfers.gcs_to_bigquery.{}"


class TestGCSToBigQueryOperator:
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_max_value_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_job.return_value.result.return_value = ("1",)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            max_id_key=MAX_ID_KEY,
            external_table=True,
            project_id=JOB_PROJECT_ID,
        )

        result = operator.execute(context=MagicMock())

        assert result == "1"
        hook.return_value.create_table.assert_called_once_with(
            exists_ok=True,
            location=None,
            project_id=JOB_PROJECT_ID,
            dataset_id=DATASET,
            table_id=TABLE,
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": {},
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "CSV",
                    "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                    "compression": "NONE",
                    "ignoreUnknownValues": False,
                    "csvOptions": {
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                        "encoding": "UTF-8",
                    },
                    "schema": {"fields": SCHEMA_FIELDS},
                },
            },
        )
        hook.return_value.insert_job.assert_called_once_with(
            configuration={
                "query": {
                    "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                    "useLegacySql": False,
                    "schemaUpdateOptions": [],
                }
            },
            project_id=JOB_PROJECT_ID,
        )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_external_table_explicitly_passes_dataset_and_table_ids(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)

        def _validate_create_table(**kwargs):
            assert kwargs["dataset_id"] == DATASET
            assert kwargs["table_id"] == TABLE

        hook.return_value.create_table.side_effect = _validate_create_table

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            schema_fields=SCHEMA_FIELDS,
            external_table=True,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_max_value_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_job.return_value.result.return_value = ("1",)

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            schema_fields=SCHEMA_FIELDS,
            max_id_key=MAX_ID_KEY,
            write_disposition=WRITE_DISPOSITION,
            external_table=False,
            project_id=JOB_PROJECT_ID,
        )

        result = operator.execute(context=MagicMock())
        assert result == "1"

        calls = [
            call(
                configuration={
                    "load": {
                        "autodetect": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "destinationTable": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        "sourceFormat": "CSV",
                        "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        "writeDisposition": WRITE_DISPOSITION,
                        "ignoreUnknownValues": False,
                        "schema": {"fields": SCHEMA_FIELDS},
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "encoding": "UTF-8",
                    }
                },
                job_id=REAL_JOB_ID,
                location=None,
                nowait=True,
                project_id=JOB_PROJECT_ID,
                retry=DEFAULT_RETRY,
                timeout=None,
            ),
            call(
                configuration={
                    "query": {
                        "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                        "useLegacySql": False,
                        "schemaUpdateOptions": [],
                    }
                },
                project_id=JOB_PROJECT_ID,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_two_partitionings_should_fail(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        with pytest.raises(
            ValueError, match=r"Only one of time_partitioning or range_partitioning can be set."
        ):
            GCSToBigQueryOperator(
                task_id=TASK_ID,
                bucket=TEST_BUCKET,
                source_objects=TEST_SOURCE_OBJECTS,
                destination_project_dataset_table=TEST_EXPLICIT_DEST,
                schema_fields=SCHEMA_FIELDS,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=False,
                project_id=JOB_PROJECT_ID,
                time_partitioning={"field": "created", "type": "DAY"},
                range_partitioning={"field": "grade", "range": {"start": 0, "end": 100, "interval": 20}},
            )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_max_value_should_throw_ex_when_query_returns_no_rows(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            schema_fields=SCHEMA_FIELDS,
            max_id_key=MAX_ID_KEY,
            write_disposition=WRITE_DISPOSITION,
            external_table=False,
            project_id=JOB_PROJECT_ID,
        )
        with pytest.raises(RuntimeError, match=r"returned no rows!"):
            operator.execute(context=MagicMock())

        calls = [
            call(
                configuration={
                    "load": {
                        "autodetect": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "destinationTable": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        "sourceFormat": "CSV",
                        "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        "writeDisposition": "WRITE_TRUNCATE",
                        "ignoreUnknownValues": False,
                        "schema": {"fields": SCHEMA_FIELDS},
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "encoding": "UTF-8",
                    }
                },
                job_id=REAL_JOB_ID,
                location=None,
                nowait=True,
                project_id=JOB_PROJECT_ID,
                retry=DEFAULT_RETRY,
                timeout=None,
            ),
            call(
                configuration={
                    "query": {
                        "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                        "useLegacySql": False,
                        "schemaUpdateOptions": [],
                    }
                },
                project_id=JOB_PROJECT_ID,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_labels_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            schema_fields=SCHEMA_FIELDS,
            write_disposition=WRITE_DISPOSITION,
            external_table=True,
            labels=LABELS,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())
        hook.return_value.create_table.assert_called_once_with(
            exists_ok=True,
            location=None,
            project_id=JOB_PROJECT_ID,
            dataset_id=DATASET,
            table_id=TABLE,
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": LABELS,
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "CSV",
                    "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                    "compression": "NONE",
                    "ignoreUnknownValues": False,
                    "csvOptions": {
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                        "encoding": "UTF-8",
                    },
                    "schema": {"fields": SCHEMA_FIELDS},
                },
            },
        )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_labels_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            external_table=False,
            labels=LABELS,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())
        calls = [
            call(
                configuration={
                    "load": {
                        "autodetect": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "destinationTable": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        "sourceFormat": "CSV",
                        "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        "writeDisposition": "WRITE_TRUNCATE",
                        "ignoreUnknownValues": False,
                        "schema": {"fields": SCHEMA_FIELDS},
                        "destinationTableProperties": {"labels": LABELS},
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "encoding": "UTF-8",
                    }
                },
                job_id=REAL_JOB_ID,
                location=None,
                nowait=True,
                project_id=JOB_PROJECT_ID,
                retry=DEFAULT_RETRY,
                timeout=None,
            )
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_description_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            description=DESCRIPTION,
            external_table=True,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())
        hook.return_value.create_table.assert_called_once_with(
            exists_ok=True,
            location=None,
            project_id=JOB_PROJECT_ID,
            dataset_id=DATASET,
            table_id=TABLE,
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": {},
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "CSV",
                    "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                    "compression": "NONE",
                    "ignoreUnknownValues": False,
                    "csvOptions": {
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                        "encoding": "UTF-8",
                    },
                    "schema": {"fields": SCHEMA_FIELDS},
                },
                "description": DESCRIPTION,
            },
        )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_description_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            schema_fields=SCHEMA_FIELDS,
            write_disposition=WRITE_DISPOSITION,
            external_table=False,
            description=DESCRIPTION,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())
        calls = [
            call(
                configuration={
                    "load": dict(
                        autodetect=True,
                        createDisposition="CREATE_IF_NEEDED",
                        destinationTable={"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        destinationTableProperties={
                            "description": DESCRIPTION,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        quote=None,
                        fieldDelimiter=",",
                    ),
                },
                project_id=JOB_PROJECT_ID,
                location=None,
                job_id=REAL_JOB_ID,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
        ]
        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_source_objs_as_list_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            write_disposition=WRITE_DISPOSITION,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            external_table=True,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())

        hook.return_value.create_table.assert_called_once_with(
            exists_ok=True,
            location=None,
            project_id=JOB_PROJECT_ID,
            dataset_id=DATASET,
            table_id=TABLE,
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": {},
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "CSV",
                    "sourceUris": [
                        f"gs://{TEST_BUCKET}/{source_object}" for source_object in TEST_SOURCE_OBJECTS_LIST
                    ],
                    "compression": "NONE",
                    "ignoreUnknownValues": False,
                    "csvOptions": {
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                        "encoding": "UTF-8",
                    },
                    "schema": {"fields": SCHEMA_FIELDS},
                },
            },
        )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_source_objs_as_list_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            write_disposition=WRITE_DISPOSITION,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            external_table=False,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())

        calls = [
            call(
                configuration={
                    "load": {
                        "autodetect": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "destinationTable": {
                            "projectId": "test-project",
                            "datasetId": "dataset",
                            "tableId": "table",
                        },
                        "sourceFormat": "CSV",
                        "sourceUris": ["gs://test-bucket/test/objects/test.csv"],
                        "writeDisposition": "WRITE_TRUNCATE",
                        "ignoreUnknownValues": False,
                        "schema": {"fields": SCHEMA_FIELDS},
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "encoding": "UTF-8",
                    }
                },
                job_id=REAL_JOB_ID,
                location=None,
                nowait=True,
                project_id=JOB_PROJECT_ID,
                retry=DEFAULT_RETRY,
                timeout=None,
            )
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_source_objs_as_string_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            write_disposition=WRITE_DISPOSITION,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            external_table=True,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())

        hook.return_value.create_table.assert_called_once_with(
            exists_ok=True,
            location=None,
            project_id=JOB_PROJECT_ID,
            dataset_id=DATASET,
            table_id=TABLE,
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": {},
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "CSV",
                    "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                    "compression": "NONE",
                    "ignoreUnknownValues": False,
                    "csvOptions": {
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                        "encoding": "UTF-8",
                    },
                    "schema": {"fields": SCHEMA_FIELDS},
                },
            },
        )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_source_objs_as_string_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            external_table=False,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())

        calls = [
            call(
                configuration={
                    "load": {
                        "autodetect": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "destinationTable": {
                            "projectId": PROJECT_ID,
                            "datasetId": DATASET,
                            "tableId": "table",
                        },
                        "sourceFormat": "CSV",
                        "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        "writeDisposition": "WRITE_TRUNCATE",
                        "ignoreUnknownValues": False,
                        "schema": {"fields": SCHEMA_FIELDS},
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "encoding": "UTF-8",
                    }
                },
                job_id=REAL_JOB_ID,
                location=None,
                nowait=True,
                project_id=JOB_PROJECT_ID,
                retry=DEFAULT_RETRY,
                timeout=None,
            )
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch(GCS_TO_BQ_PATH.format("GCSHook"))
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_schema_obj_external_table_should_execute_successfully(self, bq_hook, gcs_hook):
        bq_hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        bq_hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        bq_hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        gcs_hook.return_value.download.return_value = bytes(json.dumps(SCHEMA_FIELDS), "utf-8")
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_object_bucket=SCHEMA_BUCKET,
            schema_object=SCHEMA_OBJECT,
            write_disposition=WRITE_DISPOSITION,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            external_table=True,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())

        bq_hook.return_value.create_table.assert_called_once_with(
            exists_ok=True,
            location=None,
            project_id=JOB_PROJECT_ID,
            dataset_id=DATASET,
            table_id=TABLE,
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": {},
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "CSV",
                    "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                    "compression": "NONE",
                    "ignoreUnknownValues": False,
                    "csvOptions": {
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                        "encoding": "UTF-8",
                    },
                    "schema": {"fields": SCHEMA_FIELDS},
                },
            },
        )
        gcs_hook.return_value.download.assert_called_once_with(SCHEMA_BUCKET, SCHEMA_OBJECT)

    @mock.patch(GCS_TO_BQ_PATH.format("GCSHook"))
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_schema_obj_without_external_table_should_execute_successfully(self, bq_hook, gcs_hook):
        bq_hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        bq_hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        bq_hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        gcs_hook.return_value.download.return_value = bytes(json.dumps(SCHEMA_FIELDS), "utf-8")

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_object_bucket=SCHEMA_BUCKET,
            schema_object=SCHEMA_OBJECT,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            external_table=False,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())

        calls = [
            call(
                configuration={
                    "load": {
                        "autodetect": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "destinationTable": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        "sourceFormat": "CSV",
                        "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        "writeDisposition": "WRITE_TRUNCATE",
                        "ignoreUnknownValues": False,
                        "schema": {"fields": SCHEMA_FIELDS},
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "encoding": "UTF-8",
                    }
                },
                project_id=JOB_PROJECT_ID,
                location=None,
                job_id=REAL_JOB_ID,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            )
        ]

        bq_hook.return_value.insert_job.assert_has_calls(calls)
        gcs_hook.return_value.download.assert_called_once_with(SCHEMA_BUCKET, SCHEMA_OBJECT)

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_autodetect_none_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            write_disposition=WRITE_DISPOSITION,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            external_table=True,
            autodetect=None,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())

        hook.return_value.create_table.assert_called_once_with(
            exists_ok=True,
            location=None,
            project_id=JOB_PROJECT_ID,
            dataset_id=DATASET,
            table_id=TABLE,
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": {},
                "externalDataConfiguration": {
                    "autodetect": None,
                    "sourceFormat": "CSV",
                    "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                    "compression": "NONE",
                    "ignoreUnknownValues": False,
                    "csvOptions": {
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                        "encoding": "UTF-8",
                    },
                },
            },
        )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_autodetect_none_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            autodetect=None,
            external_table=False,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())

        calls = [
            call(
                configuration={
                    "load": {
                        "autodetect": None,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "destinationTable": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        "sourceFormat": "CSV",
                        "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        "writeDisposition": "WRITE_TRUNCATE",
                        "ignoreUnknownValues": False,
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "encoding": "UTF-8",
                    }
                },
                project_id=JOB_PROJECT_ID,
                location=None,
                job_id=REAL_JOB_ID,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            )
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_execute_should_throw_ex_when_no_bucket_specified(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        with pytest.raises((TypeError, AirflowException), match=r"missing keyword argument 'bucket'"):
            GCSToBigQueryOperator(
                task_id=TASK_ID,
                source_objects=TEST_SOURCE_OBJECTS,
                destination_project_dataset_table=TEST_EXPLICIT_DEST,
                schema_fields=SCHEMA_FIELDS,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=False,
                project_id=JOB_PROJECT_ID,
            )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_execute_should_throw_ex_when_no_source_objects_specified(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        with pytest.raises((TypeError, AirflowException), match=r"missing keyword argument 'source_objects'"):
            GCSToBigQueryOperator(
                task_id=TASK_ID,
                destination_project_dataset_table=TEST_EXPLICIT_DEST,
                schema_fields=SCHEMA_FIELDS,
                bucket=TEST_BUCKET,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=False,
                project_id=JOB_PROJECT_ID,
            )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_execute_should_throw_ex_when_no_destination_project_dataset_table_specified(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        with pytest.raises(
            (TypeError, AirflowException),
            match=r"missing keyword argument 'destination_project_dataset_table'",
        ):
            GCSToBigQueryOperator(
                task_id=TASK_ID,
                schema_fields=SCHEMA_FIELDS,
                bucket=TEST_BUCKET,
                source_objects=TEST_SOURCE_OBJECTS,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=False,
                project_id=JOB_PROJECT_ID,
            )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_source_format_check_should_throw_ex_when_incorrect_source_type(
        self,
        hook,
    ):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_job.return_value.result.return_value = ("1",)
        with pytest.raises(
            ValueError,
            match=r"is not a valid source format.",
        ):
            GCSToBigQueryOperator(
                task_id=TASK_ID,
                bucket=TEST_BUCKET,
                source_objects=TEST_SOURCE_OBJECTS,
                destination_project_dataset_table=TEST_EXPLICIT_DEST,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=False,
                autodetect=False,
                source_format="incorrect",
                project_id=JOB_PROJECT_ID,
            )

    @mock.patch(GCS_TO_BQ_PATH.format("GCSHook"))
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_schema_fields_integer_scanner_external_table_should_execute_successfully(
        self, bq_hook, gcs_hook
    ):
        """
        Check detection of schema fields if schema_fields parameter is not
        specified and fields are read from source objects correctly by BigQuery if at least
        one field includes non-string value.
        """
        bq_hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        bq_hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        bq_hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        bq_hook.return_value.get_job.return_value.result.return_value = ("1",)
        gcs_hook.return_value.download.return_value = b"id,name\r\n1,Anna"

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            max_id_key=MAX_ID_KEY,
            external_table=True,
            autodetect=True,
            project_id=JOB_PROJECT_ID,
        )

        result = operator.execute(context=MagicMock())

        assert result == "1"
        bq_hook.return_value.create_table.assert_called_once_with(
            exists_ok=True,
            location=None,
            project_id=JOB_PROJECT_ID,
            dataset_id=DATASET,
            table_id=TABLE,
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": {},
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "CSV",
                    "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                    "compression": "NONE",
                    "ignoreUnknownValues": False,
                    "csvOptions": {
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                        "encoding": "UTF-8",
                    },
                },
            },
        )
        bq_hook.return_value.insert_job.assert_called_once_with(
            configuration={
                "query": {
                    "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                    "useLegacySql": False,
                    "schemaUpdateOptions": [],
                }
            },
            project_id=JOB_PROJECT_ID,
        )

    @mock.patch(GCS_TO_BQ_PATH.format("GCSHook"))
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_schema_fields_integer_scanner_without_external_table_should_execute_successfully(
        self, bq_hook, gcs_hook
    ):
        """
        Check detection of schema fields if schema_fields parameter is not
        specified and fields are read from source objects correctly by BigQuery if at least
        one field includes non-string value.
        """
        bq_hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        bq_hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        bq_hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        bq_hook.return_value.get_job.return_value.result.return_value = ("1",)
        gcs_hook.return_value.download.return_value = b"id,name\r\n1,Anna"

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            max_id_key=MAX_ID_KEY,
            external_table=False,
            autodetect=True,
            project_id=JOB_PROJECT_ID,
        )

        result = operator.execute(context=MagicMock())

        assert result == "1"
        calls = [
            call(
                configuration={
                    "load": {
                        "autodetect": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "destinationTable": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        "sourceFormat": "CSV",
                        "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        "writeDisposition": WRITE_DISPOSITION,
                        "ignoreUnknownValues": False,
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "encoding": "UTF-8",
                    }
                },
                job_id=REAL_JOB_ID,
                location=None,
                nowait=True,
                project_id=JOB_PROJECT_ID,
                retry=DEFAULT_RETRY,
                timeout=None,
            ),
            call(
                configuration={
                    "query": {
                        "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                        "useLegacySql": False,
                        "schemaUpdateOptions": [],
                    }
                },
                project_id=JOB_PROJECT_ID,
            ),
        ]

        bq_hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_schema_fields_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_job.return_value.result.return_value = ("1",)

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS_INT,
            external_table=False,
            autodetect=True,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())
        calls = [
            call(
                configuration={
                    "load": {
                        "autodetect": True,
                        "createDisposition": "CREATE_IF_NEEDED",
                        "destinationTable": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        "sourceFormat": "CSV",
                        "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        "writeDisposition": WRITE_DISPOSITION,
                        "ignoreUnknownValues": False,
                        "schema": {"fields": SCHEMA_FIELDS_INT},
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "encoding": "UTF-8",
                    }
                },
                job_id=REAL_JOB_ID,
                location=None,
                nowait=True,
                project_id=JOB_PROJECT_ID,
                retry=DEFAULT_RETRY,
                timeout=None,
            )
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_schema_fields_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_job.return_value.result.return_value = ("1",)

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS_INT,
            external_table=True,
            autodetect=True,
            project_id=JOB_PROJECT_ID,
        )

        operator.execute(context=MagicMock())
        hook.return_value.create_table.assert_called_once_with(
            exists_ok=True,
            location=None,
            project_id=JOB_PROJECT_ID,
            dataset_id=DATASET,
            table_id=TABLE,
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": {},
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "CSV",
                    "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                    "compression": "NONE",
                    "ignoreUnknownValues": False,
                    "csvOptions": {
                        "skipLeadingRows": None,
                        "fieldDelimiter": ",",
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                        "encoding": "UTF-8",
                    },
                    "schema": {"fields": SCHEMA_FIELDS_INT},
                },
            },
        )

    @pytest.mark.parametrize(
        ("source_object", "expected_dataset_name"),
        (
            (f"{TEST_FOLDER}/{TEST_OBJECT_NO_WILDCARD}", f"{TEST_FOLDER}/{TEST_OBJECT_NO_WILDCARD}"),
            (TEST_OBJECT_NO_WILDCARD, TEST_OBJECT_NO_WILDCARD),
            (f"{TEST_FOLDER}/{TEST_OBJECT_WILDCARD}", TEST_FOLDER),
            (f"{TEST_OBJECT_WILDCARD}", "/"),
            (f"{TEST_FOLDER}/*", TEST_FOLDER),
        ),
    )
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_get_openlineage_facets_on_complete_gcs_dataset_name(
        self, hook, source_object, expected_dataset_name
    ):
        hook.return_value.insert_job.return_value = MagicMock(job_id=REAL_JOB_ID, error_result=False)
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=[source_object],
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
        )

        expected_symlink = SymlinksDatasetFacet(
            identifiers=[
                Identifier(
                    namespace=f"gs://{TEST_BUCKET}",
                    name=source_object,
                    type="file",
                )
            ]
        )
        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].name == expected_dataset_name
        if "*" in source_object:
            assert lineage.inputs[0].facets.get("symlink")
            assert lineage.inputs[0].facets.get("symlink") == expected_symlink

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_get_openlineage_facets_on_complete_gcs_multiple_uris(self, hook):
        hook.return_value.insert_job.return_value = MagicMock(job_id=REAL_JOB_ID, error_result=False)
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=[
                TEST_OBJECT_NO_WILDCARD,
                TEST_OBJECT_WILDCARD,
                f"{TEST_FOLDER}1/{TEST_OBJECT_NO_WILDCARD}",
                f"{TEST_FOLDER}2/{TEST_OBJECT_WILDCARD}",
            ],
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 4
        assert lineage.inputs[0].name == TEST_OBJECT_NO_WILDCARD
        assert lineage.inputs[1].name == "/"
        assert lineage.inputs[1].facets.get("symlink") == SymlinksDatasetFacet(
            identifiers=[
                Identifier(
                    namespace=f"gs://{TEST_BUCKET}",
                    name=TEST_OBJECT_WILDCARD,
                    type="file",
                )
            ]
        )
        assert lineage.inputs[2].name == f"{TEST_FOLDER}1/{TEST_OBJECT_NO_WILDCARD}"
        assert lineage.inputs[3].name == f"{TEST_FOLDER}2"
        assert lineage.inputs[3].facets.get("symlink") == SymlinksDatasetFacet(
            identifiers=[
                Identifier(
                    namespace=f"gs://{TEST_BUCKET}",
                    name=f"{TEST_FOLDER}2/{TEST_OBJECT_WILDCARD}",
                    type="file",
                )
            ]
        )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_get_openlineage_facets_on_complete_bq_dataset(self, hook):
        hook.return_value.insert_job.return_value = MagicMock(job_id=REAL_JOB_ID, error_result=False)
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_client.return_value.get_table.return_value = TEST_TABLE

        expected_output_dataset_facets = {
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaDatasetFacetFields(name="field1", type="STRING", description="field1 description"),
                    SchemaDatasetFacetFields(name="field2", type="INTEGER"),
                ]
            ),
            "documentation": DocumentationDatasetFacet(description="Test Description"),
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "field1": Fields(
                        inputFields=[
                            InputField(
                                namespace=f"gs://{TEST_BUCKET}", name=TEST_OBJECT_NO_WILDCARD, field="field1"
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                    "field2": Fields(
                        inputFields=[
                            InputField(
                                namespace=f"gs://{TEST_BUCKET}", name=TEST_OBJECT_NO_WILDCARD, field="field2"
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                }
            ),
        }

        operator = GCSToBigQueryOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=[TEST_OBJECT_NO_WILDCARD],
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0] == Dataset(
            namespace="bigquery",
            name=TEST_EXPLICIT_DEST,
            facets=expected_output_dataset_facets,
        )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_get_openlineage_facets_on_complete_bq_dataset_multiple_gcs_uris(self, hook):
        hook.return_value.insert_job.return_value = MagicMock(job_id=REAL_JOB_ID, error_result=False)
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_client.return_value.get_table.return_value = TEST_TABLE

        expected_output_dataset_facets = {
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaDatasetFacetFields(name="field1", type="STRING", description="field1 description"),
                    SchemaDatasetFacetFields(name="field2", type="INTEGER"),
                ]
            ),
            "documentation": DocumentationDatasetFacet(description="Test Description"),
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "field1": Fields(
                        inputFields=[
                            InputField(
                                namespace=f"gs://{TEST_BUCKET}", name=TEST_OBJECT_NO_WILDCARD, field="field1"
                            ),
                            InputField(namespace=f"gs://{TEST_BUCKET}", name="/", field="field1"),
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                    "field2": Fields(
                        inputFields=[
                            InputField(
                                namespace=f"gs://{TEST_BUCKET}", name=TEST_OBJECT_NO_WILDCARD, field="field2"
                            ),
                            InputField(namespace=f"gs://{TEST_BUCKET}", name="/", field="field2"),
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                }
            ),
        }

        operator = GCSToBigQueryOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=[TEST_OBJECT_NO_WILDCARD, TEST_OBJECT_WILDCARD],
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0] == Dataset(
            namespace="bigquery",
            name=TEST_EXPLICIT_DEST,
            facets=expected_output_dataset_facets,
        )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_get_openlineage_facets_on_complete_empty_table(self, hook):
        hook.return_value.insert_job.return_value = MagicMock(job_id=REAL_JOB_ID, error_result=False)
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_client.return_value.get_table.return_value = TEST_EMPTY_TABLE

        operator = GCSToBigQueryOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=[TEST_OBJECT_NO_WILDCARD, TEST_OBJECT_WILDCARD],
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 2
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0] == Dataset(
            namespace="bigquery",
            name=TEST_EXPLICIT_DEST,
            facets={},
        )
        assert lineage.inputs[0] == Dataset(
            namespace=f"gs://{TEST_BUCKET}",
            name=TEST_OBJECT_NO_WILDCARD,
            facets={},
        )
        assert lineage.inputs[1] == Dataset(
            namespace=f"gs://{TEST_BUCKET}",
            name="/",
            facets={
                "symlink": SymlinksDatasetFacet(
                    identifiers=[
                        Identifier(
                            namespace=f"gs://{TEST_BUCKET}",
                            name=TEST_OBJECT_WILDCARD,
                            type="file",
                        )
                    ]
                ),
            },
        )

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_get_openlineage_facets_on_complete_full_table_multiple_gcs_uris(self, hook):
        hook.return_value.insert_job.return_value = MagicMock(job_id=REAL_JOB_ID, error_result=False)
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_client.return_value.get_table.return_value = TEST_TABLE
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID

        schema_facet = SchemaDatasetFacet(
            fields=[
                SchemaDatasetFacetFields(name="field1", type="STRING", description="field1 description"),
                SchemaDatasetFacetFields(name="field2", type="INTEGER"),
            ]
        )

        expected_input_wildcard_dataset_facets = {
            "schema": schema_facet,
            "symlink": SymlinksDatasetFacet(
                identifiers=[
                    Identifier(
                        namespace=f"gs://{TEST_BUCKET}",
                        name=TEST_OBJECT_WILDCARD,
                        type="file",
                    )
                ]
            ),
        }
        expected_input_no_wildcard_dataset_facets = {"schema": schema_facet}

        expected_output_dataset_facets = {
            "schema": schema_facet,
            "documentation": DocumentationDatasetFacet(description="Test Description"),
            "columnLineage": ColumnLineageDatasetFacet(
                fields={
                    "field1": Fields(
                        inputFields=[
                            InputField(
                                namespace=f"gs://{TEST_BUCKET}", name=TEST_OBJECT_NO_WILDCARD, field="field1"
                            ),
                            InputField(namespace=f"gs://{TEST_BUCKET}", name="/", field="field1"),
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                    "field2": Fields(
                        inputFields=[
                            InputField(
                                namespace=f"gs://{TEST_BUCKET}", name=TEST_OBJECT_NO_WILDCARD, field="field2"
                            ),
                            InputField(namespace=f"gs://{TEST_BUCKET}", name="/", field="field2"),
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="identical",
                    ),
                }
            ),
        }

        operator = GCSToBigQueryOperator(
            project_id=JOB_PROJECT_ID,
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=[TEST_OBJECT_NO_WILDCARD, TEST_OBJECT_WILDCARD],
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
        )

        operator.execute(context=mock.MagicMock())

        lineage = operator.get_openlineage_facets_on_complete(None)
        assert len(lineage.inputs) == 2
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0] == Dataset(
            namespace="bigquery", name=TEST_EXPLICIT_DEST, facets=expected_output_dataset_facets
        )

        assert lineage.inputs[0] == Dataset(
            namespace=f"gs://{TEST_BUCKET}",
            name=TEST_OBJECT_NO_WILDCARD,
            facets=expected_input_no_wildcard_dataset_facets,
        )
        assert lineage.inputs[1] == Dataset(
            namespace=f"gs://{TEST_BUCKET}", name="/", facets=expected_input_wildcard_dataset_facets
        )
        assert lineage.run_facets == {
            "externalQuery": ExternalQueryRunFacet(externalQueryId=REAL_JOB_ID, source="bigquery")
        }
        assert lineage.job_facets == {}

        @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
        def test_external_table_should_accept_orc_source_format(self, hook):
            hook.return_value.insert_job.side_effect = [
                MagicMock(job_id=REAL_JOB_ID, error_result=False),
                REAL_JOB_ID,
            ]
            hook.return_value.generate_job_id.return_value = REAL_JOB_ID
            hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)

            operator = GCSToBigQueryOperator(
                task_id=TASK_ID,
                bucket=TEST_BUCKET,
                source_objects=TEST_SOURCE_OBJECTS,
                destination_project_dataset_table=TEST_EXPLICIT_DEST,
                schema_fields=SCHEMA_FIELDS,
                write_disposition=WRITE_DISPOSITION,
                external_table=True,
                project_id=JOB_PROJECT_ID,
                source_format="ORC",
            )

            operator.execute(context=MagicMock())

            hook.return_value.create_table.assert_called_once_with(
                exists_ok=True,
                location=None,
                project_id=JOB_PROJECT_ID,
                dataset_id=DATASET,
                table_id=TABLE,
                table_resource={
                    "tableReference": {
                        "projectId": PROJECT_ID,
                        "datasetId": DATASET,
                        "tableId": TABLE,
                    },
                    "externalDataConfiguration": {
                        "autodetect": True,
                        "sourceFormat": "ORC",
                        "sourceUris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        "compression": "NONE",
                        "ignoreUnknownValues": False,
                        "schema": {"fields": SCHEMA_FIELDS},
                    },
                },
            )


@pytest.fixture
def create_task_instance(create_task_instance_of_operator, session):
    return functools.partial(
        create_task_instance_of_operator,
        session=session,
        operator_class=GCSToBigQueryOperator,
        dag_id="adhoc_airflow",
    )


class TestAsyncGCSToBigQueryOperator:
    def _set_execute_complete(self, session, ti, **next_kwargs):
        ti.next_method = "execute_complete"
        ti.next_kwargs = next_kwargs
        session.flush()

    @pytest.mark.db_test
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_execute_without_external_table_async_should_execute_successfully(
        self, hook, create_task_instance, session
    ):
        """
        Asserts that a task is deferred and a BigQueryInsertJobTrigger will be fired
        when Operator is executed in deferrable.
        """
        hook.return_value.insert_job.return_value = MagicMock(job_id=REAL_JOB_ID, error_result=False)
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_job.return_value.result.return_value = ("1",)

        ti = create_task_instance(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            external_table=False,
            autodetect=True,
            deferrable=True,
            project_id=JOB_PROJECT_ID,
        )
        ti.run(session=session)

        assert ti.state == TaskInstanceState.DEFERRED
        trigger_cls = session.scalar(select(Trigger.classpath).where(Trigger.id == ti.trigger_id))
        assert trigger_cls == "airflow.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger"

    @pytest.mark.db_test
    def test_execute_without_external_table_async_should_throw_ex_when_event_status_error(
        self, create_task_instance, session
    ):
        """
        Tests that an AirflowException is raised in case of error event.
        """
        ti = create_task_instance(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            external_table=False,
            autodetect=True,
            deferrable=True,
            project_id=JOB_PROJECT_ID,
        )
        self._set_execute_complete(session, ti, event={"status": "error", "message": "test failure message"})

        with pytest.raises(AirflowException):
            ti.run()

    @pytest.mark.db_test
    def test_execute_logging_without_external_table_async_should_execute_successfully(
        self, caplog, create_task_instance, session
    ):
        """
        Asserts that logging occurs as expected.
        """
        ti = create_task_instance(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            external_table=False,
            autodetect=True,
            deferrable=True,
            project_id=JOB_PROJECT_ID,
        )
        self._set_execute_complete(
            session, ti, event={"status": "success", "message": "Job completed", "job_id": job_id}
        )

        with mock.patch.object(ti.task.log, "info") as mock_log_info:
            ti.run()
        mock_log_info.assert_called_with(
            "%s completed with response %s ", "test-gcs-to-bq-operator", "Job completed"
        )

    @pytest.mark.db_test
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_execute_without_external_table_generate_job_id_async_should_execute_successfully(
        self, hook, create_task_instance, session
    ):
        hook.return_value.insert_job.side_effect = Conflict("any")
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        job = MagicMock(
            job_id=REAL_JOB_ID,
            error_result=False,
            state="PENDING",
            done=lambda: False,
        )
        hook.return_value.get_job.return_value = job

        ti = create_task_instance(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            reattach_states={"PENDING"},
            external_table=False,
            autodetect=True,
            deferrable=True,
            project_id=JOB_PROJECT_ID,
        )

        ti.run(session=session)
        assert ti.state == TaskInstanceState.DEFERRED
        hook.return_value.generate_job_id.assert_called_once_with(
            job_id=None,
            dag_id="adhoc_airflow",
            task_id=TASK_ID,
            logical_date=None,
            run_after=hook.return_value.get_run_after_or_logical_date(),
            configuration={},
            force_rerun=True,
        )

    @pytest.mark.db_test
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_execute_without_external_table_reattach_async_should_execute_successfully(
        self, hook, create_task_instance, session
    ):
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID

        hook.return_value.insert_job.side_effect = Conflict("any")
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        job = MagicMock(
            job_id=REAL_JOB_ID,
            error_result=False,
            state="PENDING",
            done=lambda: False,
        )
        hook.return_value.get_job.return_value = job

        ti = create_task_instance(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            location=TEST_DATASET_LOCATION,
            reattach_states={"PENDING"},
            external_table=False,
            autodetect=True,
            deferrable=True,
            project_id=JOB_PROJECT_ID,
        )

        ti.run(session=session)
        assert ti.state == TaskInstanceState.DEFERRED
        hook.return_value.get_job.assert_called_once_with(
            location=TEST_DATASET_LOCATION,
            job_id=REAL_JOB_ID,
            project_id=JOB_PROJECT_ID,
        )

    @pytest.mark.db_test
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_execute_without_external_table_force_rerun_async_should_execute_successfully(
        self, hook, create_task_instance
    ):
        hook.return_value.generate_job_id.return_value = f"{job_id}_{hash_}"
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)

        hook.return_value.insert_job.side_effect = Conflict("any")
        job = MagicMock(
            job_id=REAL_JOB_ID,
            error_result=False,
            state="DONE",
            done=lambda: False,
        )
        hook.return_value.get_job.return_value = job

        ti = create_task_instance(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            location=TEST_DATASET_LOCATION,
            reattach_states={"PENDING"},
            external_table=False,
            autodetect=True,
            deferrable=True,
            project_id=JOB_PROJECT_ID,
        )

        with pytest.raises(AirflowException) as exc:
            ti.run()

        expected_exception_msg = (
            f"Job with id: {REAL_JOB_ID} already exists and is in {job.state} state. "
            f"If you want to force rerun it consider setting `force_rerun=True`."
            f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
        )

        assert str(exc.value) == expected_exception_msg

        hook.return_value.get_job.assert_called_once_with(
            location=TEST_DATASET_LOCATION,
            job_id=REAL_JOB_ID,
            project_id=JOB_PROJECT_ID,
        )

    @pytest.mark.db_test
    @mock.patch(GCS_TO_BQ_PATH.format("GCSHook"))
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_schema_fields_without_external_table_async_should_execute_successfully(
        self, bq_hook, gcs_hook, create_task_instance
    ):
        bq_hook.return_value.insert_job.return_value = MagicMock(job_id=REAL_JOB_ID, error_result=False)
        bq_hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        bq_hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        bq_hook.return_value.get_job.return_value.result.return_value = ("1",)
        gcs_hook.return_value.download.return_value = b"id,name\r\none,Anna"

        ti = create_task_instance(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            max_id_key=MAX_ID_KEY,
            external_table=False,
            autodetect=True,
            deferrable=True,
            project_id=JOB_PROJECT_ID,
        )
        ti.run()
        assert ti.state == TaskInstanceState.DEFERRED

        calls = [
            call(
                configuration={
                    "load": dict(
                        autodetect=True,
                        createDisposition="CREATE_IF_NEEDED",
                        destinationTable={
                            "projectId": PROJECT_ID,
                            "datasetId": DATASET,
                            "tableId": TABLE,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        fieldDelimiter=",",
                        schema={"fields": SCHEMA_FIELDS},
                        quote=None,
                    ),
                },
                project_id=JOB_PROJECT_ID,
                location=None,
                job_id=REAL_JOB_ID,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            )
        ]

        bq_hook.return_value.insert_job.assert_has_calls(calls)

    @pytest.mark.db_test
    @mock.patch(GCS_TO_BQ_PATH.format("GCSHook"))
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_schema_fields_int_without_external_table_async_should_execute_successfully(
        self, bq_hook, gcs_hook, create_task_instance
    ):
        bq_hook.return_value.insert_job.return_value = MagicMock(job_id=REAL_JOB_ID, error_result=False)
        bq_hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        bq_hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        bq_hook.return_value.get_job.return_value.result.return_value = ("1",)
        gcs_hook.return_value.download.return_value = b"id,name\r\n1,Anna"

        ti = create_task_instance(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            max_id_key=MAX_ID_KEY,
            external_table=False,
            autodetect=True,
            deferrable=True,
            project_id=JOB_PROJECT_ID,
        )
        ti.run()
        assert ti.state == TaskInstanceState.DEFERRED

        calls = [
            call(
                configuration={
                    "load": dict(
                        autodetect=True,
                        createDisposition="CREATE_IF_NEEDED",
                        destinationTable={
                            "projectId": PROJECT_ID,
                            "datasetId": DATASET,
                            "tableId": TABLE,
                        },
                        fieldDelimiter=",",
                        quote=None,
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={
                            "fields": [
                                {"mode": "NULLABLE", "name": "id", "type": "STRING"},
                                {"mode": "NULLABLE", "name": "name", "type": "STRING"},
                            ],
                        },
                    ),
                },
                project_id=JOB_PROJECT_ID,
                location=None,
                job_id=REAL_JOB_ID,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            )
        ]

        bq_hook.return_value.insert_job.assert_has_calls(calls)

    @pytest.mark.db_test
    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_execute_complete_reassigns_job_id(self, bq_hook, create_task_instance, session):
        """Assert that we use job_id from event after deferral."""
        bq_hook.return_value.split_tablename.return_value = "", "", ""
        ti = create_task_instance(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            deferrable=True,
            job_id=None,
        )

        generated_job_id = "123456"
        ti.next_method = "execute_complete"
        ti.next_kwargs = {
            "event": {"status": "success", "message": "Job completed", "job_id": generated_job_id},
        }
        session.flush()

        assert ti.task.job_id is None
        ti.run(session=session)
        assert ti.task.job_id == generated_job_id

    @mock.patch(GCS_TO_BQ_PATH.format("BigQueryHook"))
    def test_force_delete_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=REAL_JOB_ID, error_result=False),
            REAL_JOB_ID,
        ]
        hook.return_value.generate_job_id.return_value = REAL_JOB_ID
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_job.return_value.result.return_value = ("1",)

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS_INT,
            autodetect=True,
            project_id=JOB_PROJECT_ID,
            force_delete=True,
        )

        operator.execute(context=MagicMock())
        hook.return_value.delete_table.assert_called_once_with(table_id=TEST_EXPLICIT_DEST)
