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

import unittest
from unittest import mock
from unittest.mock import MagicMock, call

import pytest
from google.cloud.bigquery import DEFAULT_RETRY
from google.cloud.exceptions import Conflict

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.triggers.bigquery import BigQueryInsertJobTrigger
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

TASK_ID = "test-gcs-to-bq-operator"
TEST_EXPLICIT_DEST = "test-project.dataset.table"
TEST_BUCKET = "test-bucket"
PROJECT_ID = "test-project"
DATASET = "dataset"
TABLE = "table"
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
TEST_SOURCE_OBJECTS = ["test/objects/test.csv"]
TEST_SOURCE_OBJECTS_AS_STRING = "test/objects/test.csv"
LABELS = {"k1": "v1"}
DESCRIPTION = "Test Description"

job_id = "123456"
hash_ = "hash"
pytest.real_job_id = f"{job_id}_{hash_}"


class TestGCSToBigQueryOperator(unittest.TestCase):
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_max_value_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
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
        )

        result = operator.execute(context=MagicMock())

        assert result == "1"
        hook.return_value.create_empty_table.assert_called_once_with(
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": None,
                "description": None,
                "externalDataConfiguration": {
                    "source_uris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                    "source_format": "CSV",
                    "maxBadRecords": 0,
                    "autodetect": True,
                    "compression": "NONE",
                    "csvOptions": {
                        "fieldDelimeter": ",",
                        "skipLeadingRows": None,
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                    },
                },
                "location": None,
                "encryptionConfiguration": None,
                "schema": {"fields": SCHEMA_FIELDS},
            }
        )
        hook.return_value.insert_job.assert_called_once_with(
            configuration={
                "query": {
                    "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                    "useLegacySql": False,
                    "schemaUpdateOptions": [],
                }
            },
            project_id=hook.return_value.project_id,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_max_value_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
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
        )

        result = operator.execute(context=MagicMock())
        assert result == "1"

        calls = [
            call(
                configuration={
                    "load": dict(
                        autodetect=True,
                        createDisposition="CREATE_IF_NEEDED",
                        destinationTable={"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        destinationTableProperties={
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                    ),
                },
                project_id=hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
            call(
                configuration={
                    "query": {
                        "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                        "useLegacySql": False,
                        "schemaUpdateOptions": [],
                    }
                },
                project_id=hook.return_value.project_id,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_max_value_should_throw_ex_when_query_returns_no_rows(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        with pytest.raises(RuntimeError, match=r"returned no rows!"):
            operator = GCSToBigQueryOperator(
                task_id=TASK_ID,
                bucket=TEST_BUCKET,
                source_objects=TEST_SOURCE_OBJECTS,
                destination_project_dataset_table=TEST_EXPLICIT_DEST,
                schema_fields=SCHEMA_FIELDS,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=False,
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
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                    ),
                },
                project_id=hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
            call(
                configuration={
                    "query": {
                        "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                        "useLegacySql": False,
                        "schemaUpdateOptions": [],
                    }
                },
                project_id=hook.return_value.project_id,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_labels_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
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
        )

        operator.execute(context=MagicMock())
        hook.return_value.create_empty_table.assert_called_once_with(
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": LABELS,
                "description": None,
                "externalDataConfiguration": {
                    "source_uris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                    "source_format": "CSV",
                    "maxBadRecords": 0,
                    "autodetect": True,
                    "compression": "NONE",
                    "csvOptions": {
                        "fieldDelimeter": ",",
                        "skipLeadingRows": None,
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                    },
                },
                "location": None,
                "encryptionConfiguration": None,
                "schema": {"fields": SCHEMA_FIELDS},
            }
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_labels_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
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
                            "description": None,
                            "labels": LABELS,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                    ),
                },
                project_id=hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_description_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
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
        )

        operator.execute(context=MagicMock())
        hook.return_value.create_empty_table.assert_called_once_with(
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": None,
                "description": DESCRIPTION,
                "externalDataConfiguration": {
                    "source_uris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                    "source_format": "CSV",
                    "maxBadRecords": 0,
                    "autodetect": True,
                    "compression": "NONE",
                    "csvOptions": {
                        "fieldDelimeter": ",",
                        "skipLeadingRows": None,
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                    },
                },
                "location": None,
                "encryptionConfiguration": None,
                "schema": {"fields": SCHEMA_FIELDS},
            }
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_description_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
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
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                    ),
                },
                project_id=hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_source_objs_as_list_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            write_disposition=WRITE_DISPOSITION,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            external_table=True,
        )

        operator.execute(context=MagicMock())

        hook.return_value.create_empty_table.assert_called_once_with(
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": None,
                "description": None,
                "externalDataConfiguration": {
                    "source_uris": [
                        f"gs://{TEST_BUCKET}/{source_object}" for source_object in TEST_SOURCE_OBJECTS
                    ],
                    "source_format": "CSV",
                    "maxBadRecords": 0,
                    "autodetect": True,
                    "compression": "NONE",
                    "csvOptions": {
                        "fieldDelimeter": ",",
                        "skipLeadingRows": None,
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                    },
                },
                "location": None,
                "encryptionConfiguration": None,
                "schema": {"fields": SCHEMA_FIELDS},
            }
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_source_objs_as_list_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            write_disposition=WRITE_DISPOSITION,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            external_table=False,
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
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[
                            f"gs://{TEST_BUCKET}/{source_object}" for source_object in TEST_SOURCE_OBJECTS
                        ],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                    ),
                },
                project_id=hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_source_objs_as_string_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            write_disposition=WRITE_DISPOSITION,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            external_table=True,
        )

        operator.execute(context=MagicMock())

        hook.return_value.create_empty_table.assert_called_once_with(
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": None,
                "description": None,
                "externalDataConfiguration": {
                    "source_uris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                    "source_format": "CSV",
                    "maxBadRecords": 0,
                    "autodetect": True,
                    "compression": "NONE",
                    "csvOptions": {
                        "fieldDelimeter": ",",
                        "skipLeadingRows": None,
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                    },
                },
                "location": None,
                "encryptionConfiguration": None,
                "schema": {"fields": SCHEMA_FIELDS},
            }
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_source_objs_as_string_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            external_table=False,
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
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                    ),
                },
                project_id=hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_all_fields_should_be_present(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            external_table=False,
            field_delimiter=";",
            max_bad_records=13,
            quote_character="|",
            schema_update_options={"foo": "bar"},
            allow_jagged_rows=True,
            encryption_configuration={"bar": "baz"},
            cluster_fields=["field_1", "field_2"],
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
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        allowJaggedRows=True,
                        fieldDelimiter=";",
                        maxBadRecords=13,
                        quote="|",
                        schemaUpdateOptions={"foo": "bar"},
                        destinationEncryptionConfiguration={"bar": "baz"},
                        clustering={"fields": ["field_1", "field_2"]},
                    ),
                },
                project_id=hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_date_partitioned_explicit_setting_should_be_found(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            external_table=False,
            time_partitioning={"type": "DAY"},
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
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                        timePartitioning={"type": "DAY"},
                    ),
                },
                project_id=hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_date_partitioned_implied_in_table_name_should_be_found(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            schema_fields=SCHEMA_FIELDS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST + "$20221123",
            write_disposition=WRITE_DISPOSITION,
            external_table=False,
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
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                        timePartitioning={"type": "DAY"},
                    ),
                },
                project_id=hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_execute_should_throw_ex_when_no_bucket_specified(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        with pytest.raises(AirflowException, match=r"missing keyword argument 'bucket'"):
            operator = GCSToBigQueryOperator(
                task_id=TASK_ID,
                source_objects=TEST_SOURCE_OBJECTS,
                destination_project_dataset_table=TEST_EXPLICIT_DEST,
                schema_fields=SCHEMA_FIELDS,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=False,
            )
            operator.execute(context=MagicMock())

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_execute_should_throw_ex_when_no_source_objects_specified(self, hook):

        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        with pytest.raises(AirflowException, match=r"missing keyword argument 'source_objects'"):
            operator = GCSToBigQueryOperator(
                task_id=TASK_ID,
                destination_project_dataset_table=TEST_EXPLICIT_DEST,
                schema_fields=SCHEMA_FIELDS,
                bucket=TEST_BUCKET,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=False,
            )
            operator.execute(context=MagicMock())

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_execute_should_throw_ex_when_no_destination_project_dataset_table_specified(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        with pytest.raises(
            AirflowException, match=r"missing keyword argument 'destination_project_dataset_table'"
        ):
            operator = GCSToBigQueryOperator(
                task_id=TASK_ID,
                schema_fields=SCHEMA_FIELDS,
                bucket=TEST_BUCKET,
                source_objects=TEST_SOURCE_OBJECTS,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=False,
            )
            operator.execute(context=MagicMock())

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_schema_fields_scanner_external_table_should_execute_successfully(self, bq_hook, gcs_hook):
        """
        Check detection of schema fields if schema_fields parameter is not
        specified and fields are read from source objects correctly by the operator
        if all fields are characters. In this case operator searches for fields in source object
        and update configuration with constructed schema_fields.
        """
        bq_hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        bq_hook.return_value.generate_job_id.return_value = pytest.real_job_id
        bq_hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        bq_hook.return_value.get_job.return_value.result.return_value = ("1",)

        gcs_hook.return_value.download.return_value = b"id,name\r\none,Anna"

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            max_id_key=MAX_ID_KEY,
            write_disposition=WRITE_DISPOSITION,
            external_table=True,
            autodetect=True,
        )

        result = operator.execute(context=MagicMock())

        assert result == "1"
        bq_hook.return_value.create_empty_table.assert_called_once_with(
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": None,
                "description": None,
                "externalDataConfiguration": {
                    "source_uris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                    "source_format": "CSV",
                    "maxBadRecords": 0,
                    "autodetect": True,
                    "compression": "NONE",
                    "csvOptions": {
                        "fieldDelimeter": ",",
                        "skipLeadingRows": 1,
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                    },
                },
                "location": None,
                "encryptionConfiguration": None,
                "schema": {"fields": SCHEMA_FIELDS},
            }
        )
        bq_hook.return_value.insert_job.assert_called_once_with(
            configuration={
                "query": {
                    "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                    "useLegacySql": False,
                    "schemaUpdateOptions": [],
                }
            },
            project_id=bq_hook.return_value.project_id,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_schema_fields_scanner_without_external_table_should_execute_successfully(
        self, bq_hook, gcs_hook
    ):
        """
        Check detection of schema fields if schema_fields parameter is not
        specified and fields are read from source objects correctly by the operator
        if all fields are characters. In this case operator searches for fields in source object
        and update configuration with constructed schema_fields.
        """
        bq_hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        bq_hook.return_value.generate_job_id.return_value = pytest.real_job_id
        bq_hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        bq_hook.return_value.get_job.return_value.result.return_value = ("1",)

        gcs_hook.return_value.download.return_value = b"id,name\r\none,Anna"

        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            max_id_key=MAX_ID_KEY,
            external_table=False,
            autodetect=True,
        )

        result = operator.execute(context=MagicMock())

        assert result == "1"
        calls = [
            call(
                configuration={
                    "load": dict(
                        autodetect=True,
                        createDisposition="CREATE_IF_NEEDED",
                        destinationTable={"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        destinationTableProperties={
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=1,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                    ),
                },
                project_id=bq_hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
            call(
                configuration={
                    "query": {
                        "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                        "useLegacySql": False,
                        "schemaUpdateOptions": [],
                    }
                },
                project_id=bq_hook.return_value.project_id,
            ),
        ]

        bq_hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_schema_fields_scanner_external_table_should_throw_ex_when_autodetect_not_specified(
        self,
        hook,
    ):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_job.return_value.result.return_value = ("1",)

        with pytest.raises(RuntimeError, match=r"Table schema was not found."):
            operator = GCSToBigQueryOperator(
                task_id=TASK_ID,
                bucket=TEST_BUCKET,
                source_objects=TEST_SOURCE_OBJECTS,
                destination_project_dataset_table=TEST_EXPLICIT_DEST,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=True,
                autodetect=False,
            )
            operator.execute(context=MagicMock())

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_schema_fields_scanner_without_external_table_should_throw_ex_when_autodetect_not_specified(
        self,
        hook,
    ):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
        hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
        hook.return_value.get_job.return_value.result.return_value = ("1",)

        with pytest.raises(RuntimeError, match=r"Table schema was not found."):
            operator = GCSToBigQueryOperator(
                task_id=TASK_ID,
                bucket=TEST_BUCKET,
                source_objects=TEST_SOURCE_OBJECTS,
                destination_project_dataset_table=TEST_EXPLICIT_DEST,
                max_id_key=MAX_ID_KEY,
                write_disposition=WRITE_DISPOSITION,
                external_table=False,
                autodetect=False,
            )
            operator.execute(context=MagicMock())

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_schema_fields_integer_scanner_external_table_should_execute_successfully(
        self, bq_hook, gcs_hook
    ):
        """
        Check detection of schema fields if schema_fields parameter is not
        specified and fields are read from source objects correctly by BigQuery if at least
        one field includes non-string value.
        """
        bq_hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        bq_hook.return_value.generate_job_id.return_value = pytest.real_job_id
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
        )

        result = operator.execute(context=MagicMock())

        assert result == "1"
        bq_hook.return_value.create_empty_table.assert_called_once_with(
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": None,
                "description": None,
                "externalDataConfiguration": {
                    "source_uris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                    "source_format": "CSV",
                    "maxBadRecords": 0,
                    "autodetect": True,
                    "compression": "NONE",
                    "csvOptions": {
                        "fieldDelimeter": ",",
                        "skipLeadingRows": None,
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                    },
                },
                "location": None,
                "encryptionConfiguration": None,
            }
        )
        bq_hook.return_value.insert_job.assert_called_once_with(
            configuration={
                "query": {
                    "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                    "useLegacySql": False,
                    "schemaUpdateOptions": [],
                }
            },
            project_id=bq_hook.return_value.project_id,
        )

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSHook")
    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_schema_fields_integer_scanner_without_external_table_should_execute_successfully(
        self, bq_hook, gcs_hook
    ):
        """
        Check detection of schema fields if schema_fields parameter is not
        specified and fields are read from source objects correctly by BigQuery if at least
        one field includes non-string value.
        """
        bq_hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        bq_hook.return_value.generate_job_id.return_value = pytest.real_job_id
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
        )

        result = operator.execute(context=MagicMock())

        assert result == "1"
        calls = [
            call(
                configuration={
                    "load": dict(
                        autodetect=True,
                        createDisposition="CREATE_IF_NEEDED",
                        destinationTable={"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        destinationTableProperties={
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                    ),
                },
                project_id=bq_hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
            call(
                configuration={
                    "query": {
                        "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                        "useLegacySql": False,
                        "schemaUpdateOptions": [],
                    }
                },
                project_id=bq_hook.return_value.project_id,
            ),
        ]

        bq_hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_schema_fields_without_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
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
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS_INT},
                        allowJaggedRows=False,
                        fieldDelimiter=",",
                        maxBadRecords=0,
                        quote=None,
                        schemaUpdateOptions=(),
                    ),
                },
                project_id=hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
        ]

        hook.return_value.insert_job.assert_has_calls(calls)

    @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
    def test_schema_fields_external_table_should_execute_successfully(self, hook):
        hook.return_value.insert_job.side_effect = [
            MagicMock(job_id=pytest.real_job_id, error_result=False),
            pytest.real_job_id,
        ]
        hook.return_value.generate_job_id.return_value = pytest.real_job_id
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
        )

        operator.execute(context=MagicMock())
        hook.return_value.create_empty_table.assert_called_once_with(
            table_resource={
                "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                "labels": None,
                "description": None,
                "externalDataConfiguration": {
                    "source_uris": [f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                    "source_format": "CSV",
                    "maxBadRecords": 0,
                    "autodetect": True,
                    "compression": "NONE",
                    "csvOptions": {
                        "fieldDelimeter": ",",
                        "skipLeadingRows": None,
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                    },
                },
                "location": None,
                "encryptionConfiguration": None,
                "schema": {"fields": SCHEMA_FIELDS_INT},
            }
        )


@mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
def test_execute_without_external_table_async_should_execute_successfully(hook):
    """
    Asserts that a task is deferred and a BigQueryInsertJobTrigger will be fired
    when Operator is executed in deferrable.
    """
    hook.return_value.insert_job.return_value = MagicMock(job_id=pytest.real_job_id, error_result=False)
    hook.return_value.generate_job_id.return_value = pytest.real_job_id
    hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
    hook.return_value.get_job.return_value.result.return_value = ("1",)

    operator = GCSToBigQueryOperator(
        task_id=TASK_ID,
        bucket=TEST_BUCKET,
        source_objects=TEST_SOURCE_OBJECTS,
        destination_project_dataset_table=TEST_EXPLICIT_DEST,
        write_disposition=WRITE_DISPOSITION,
        schema_fields=SCHEMA_FIELDS,
        external_table=False,
        autodetect=True,
        deferrable=True,
    )

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(create_context(operator))

    assert isinstance(
        exc.value.trigger, BigQueryInsertJobTrigger
    ), "Trigger is not a BigQueryInsertJobTrigger"


def test_execute_without_external_table_async_should_throw_ex_when_event_status_error():
    """
    Tests that an AirflowException is raised in case of error event.
    """

    with pytest.raises(AirflowException):
        operator = GCSToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            write_disposition=WRITE_DISPOSITION,
            schema_fields=SCHEMA_FIELDS,
            external_table=False,
            autodetect=True,
            deferrable=True,
        )
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_execute_logging_without_external_table_async_should_execute_successfully():
    """
    Asserts that logging occurs as expected.
    """

    operator = GCSToBigQueryOperator(
        task_id=TASK_ID,
        bucket=TEST_BUCKET,
        source_objects=TEST_SOURCE_OBJECTS,
        destination_project_dataset_table=TEST_EXPLICIT_DEST,
        write_disposition=WRITE_DISPOSITION,
        schema_fields=SCHEMA_FIELDS,
        external_table=False,
        autodetect=True,
        deferrable=True,
    )
    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(
            context=create_context(operator),
            event={"status": "success", "message": "Job completed", "job_id": job_id},
        )
    mock_log_info.assert_called_with(
        "%s completed with response %s ", "test-gcs-to-bq-operator", "Job completed"
    )


@mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
def test_execute_without_external_table_generate_job_id_async_should_execute_successfully(hook):
    hook.return_value.insert_job.side_effect = Conflict("any")
    hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
    job = MagicMock(
        job_id=pytest.real_job_id,
        error_result=False,
        state="PENDING",
        done=lambda: False,
    )
    hook.return_value.get_job.return_value = job

    operator = GCSToBigQueryOperator(
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
    )

    with pytest.raises(TaskDeferred):
        operator.execute(create_context(operator))

    hook.return_value.generate_job_id.assert_called_once_with(
        job_id=None,
        dag_id="adhoc_airflow",
        task_id=TASK_ID,
        logical_date=datetime(2022, 1, 1, 0, 0),
        configuration={},
        force_rerun=True,
    )


@mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
def test_execute_without_external_table_reattach_async_should_execute_successfully(hook):
    hook.return_value.generate_job_id.return_value = pytest.real_job_id

    hook.return_value.insert_job.side_effect = Conflict("any")
    hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
    job = MagicMock(
        job_id=pytest.real_job_id,
        error_result=False,
        state="PENDING",
        done=lambda: False,
    )
    hook.return_value.get_job.return_value = job

    operator = GCSToBigQueryOperator(
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
    )

    with pytest.raises(TaskDeferred):
        operator.execute(create_context(operator))

    hook.return_value.get_job.assert_called_once_with(
        location=TEST_DATASET_LOCATION,
        job_id=pytest.real_job_id,
        project_id=hook.return_value.project_id,
    )

    job._begin.assert_called_once_with()


@mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
def test_execute_without_external_table_force_rerun_async_should_execute_successfully(hook):
    hook.return_value.generate_job_id.return_value = f"{job_id}_{hash_}"
    hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)

    hook.return_value.insert_job.side_effect = Conflict("any")
    job = MagicMock(
        job_id=pytest.real_job_id,
        error_result=False,
        state="DONE",
        done=lambda: False,
    )
    hook.return_value.get_job.return_value = job

    operator = GCSToBigQueryOperator(
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
    )

    with pytest.raises(AirflowException) as exc:
        operator.execute(create_context(operator))

    expected_exception_msg = (
        f"Job with id: {pytest.real_job_id} already exists and is in {job.state} state. "
        f"If you want to force rerun it consider setting `force_rerun=True`."
        f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
    )

    assert str(exc.value) == expected_exception_msg

    hook.return_value.get_job.assert_called_once_with(
        location=TEST_DATASET_LOCATION,
        job_id=pytest.real_job_id,
        project_id=hook.return_value.project_id,
    )


@mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSHook")
@mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
def test_schema_fields_without_external_table_async_should_execute_successfully(bq_hook, gcs_hook):
    bq_hook.return_value.insert_job.return_value = MagicMock(job_id=pytest.real_job_id, error_result=False)
    bq_hook.return_value.generate_job_id.return_value = pytest.real_job_id
    bq_hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
    bq_hook.return_value.get_job.return_value.result.return_value = ("1",)
    gcs_hook.return_value.download.return_value = b"id,name\r\none,Anna"

    operator = GCSToBigQueryOperator(
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
    )

    with pytest.raises(TaskDeferred):
        result = operator.execute(create_context(operator))
        assert result == "1"

        calls = [
            call(
                configuration={
                    "load": dict(
                        autodetect=True,
                        createDisposition="CREATE_IF_NEEDED",
                        destinationTable={"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        destinationTableProperties={
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                        schema={"fields": SCHEMA_FIELDS},
                    ),
                },
                project_id=bq_hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
            call(
                configuration={
                    "query": {
                        "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                        "useLegacySql": False,
                        "schemaUpdateOptions": [],
                    }
                },
                project_id=bq_hook.return_value.project_id,
            ),
        ]

        bq_hook.return_value.insert_job.assert_has_calls(calls)


@mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSHook")
@mock.patch("airflow.providers.google.cloud.transfers.gcs_to_bigquery.BigQueryHook")
def test_schema_fields_int_without_external_table_async_should_execute_successfully(bq_hook, gcs_hook):
    bq_hook.return_value.insert_job.return_value = MagicMock(job_id=pytest.real_job_id, error_result=False)
    bq_hook.return_value.generate_job_id.return_value = pytest.real_job_id
    bq_hook.return_value.split_tablename.return_value = (PROJECT_ID, DATASET, TABLE)
    bq_hook.return_value.get_job.return_value.result.return_value = ("1",)
    gcs_hook.return_value.download.return_value = b"id,name\r\n1,Anna"

    operator = GCSToBigQueryOperator(
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
    )

    with pytest.raises(TaskDeferred):
        result = operator.execute(create_context(operator))
        assert result == "1"

        calls = [
            call(
                configuration={
                    "load": dict(
                        autodetect=True,
                        createDisposition="CREATE_IF_NEEDED",
                        destinationTable={"projectId": PROJECT_ID, "datasetId": DATASET, "tableId": TABLE},
                        destinationTableProperties={
                            "description": None,
                            "labels": None,
                        },
                        sourceFormat="CSV",
                        skipLeadingRows=None,
                        sourceUris=[f"gs://{TEST_BUCKET}/{TEST_SOURCE_OBJECTS_AS_STRING}"],
                        writeDisposition=WRITE_DISPOSITION,
                        ignoreUnknownValues=False,
                        allowQuotedNewlines=False,
                        encoding="UTF-8",
                    ),
                },
                project_id=bq_hook.return_value.project_id,
                location=None,
                job_id=pytest.real_job_id,
                timeout=None,
                retry=DEFAULT_RETRY,
                nowait=True,
            ),
            call(
                configuration={
                    "query": {
                        "query": f"SELECT MAX({MAX_ID_KEY}) AS max_value FROM {TEST_EXPLICIT_DEST}",
                        "useLegacySql": False,
                        "schemaUpdateOptions": [],
                    }
                },
                project_id=bq_hook.return_value.project_id,
            ),
        ]

        bq_hook.return_value.insert_job.assert_has_calls(calls)


def create_context(task):
    dag = DAG(dag_id="dag")
    logical_date = datetime(2022, 1, 1, 0, 0, 0)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=logical_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, logical_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "logical_date": logical_date,
    }
