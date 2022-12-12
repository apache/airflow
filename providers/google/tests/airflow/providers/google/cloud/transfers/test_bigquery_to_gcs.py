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
from unittest.mock import MagicMock

import pytest
from google.cloud.bigquery.retry import DEFAULT_RETRY

from airflow.exceptions import TaskDeferred
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.triggers.bigquery import BigQueryInsertJobTrigger

TASK_ID = "test-bq-create-table-operator"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
PROJECT_ID = "test-project-id"


class TestBigQueryToGCSOperator(unittest.TestCase):
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

        mock_hook.return_value.split_tablename.return_value = (PROJECT_ID, TEST_DATASET, TEST_TABLE_ID)
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.insert_job.return_value = MagicMock(job_id="real_job_id", error_result=False)
        mock_hook.return_value.project_id = PROJECT_ID

        operator = BigQueryToGCSOperator(
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
            compression=compression,
            export_format=export_format,
            field_delimiter=field_delimiter,
            print_header=print_header,
            labels=labels,
        )
        operator.execute(context=mock.MagicMock())

        mock_hook.return_value.insert_job.assert_called_once_with(
            job_id="123456_hash",
            configuration=expected_configuration,
            project_id=PROJECT_ID,
            location=None,
            timeout=None,
            retry=DEFAULT_RETRY,
            nowait=True,
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

        mock_hook.return_value.split_tablename.return_value = (PROJECT_ID, TEST_DATASET, TEST_TABLE_ID)
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.insert_job.return_value = MagicMock(job_id="real_job_id", error_result=False)
        mock_hook.return_value.project_id = PROJECT_ID

        operator = BigQueryToGCSOperator(
            project_id=PROJECT_ID,
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
            project_id=PROJECT_ID,
            location=None,
            timeout=None,
            retry=DEFAULT_RETRY,
            nowait=True,
        )
