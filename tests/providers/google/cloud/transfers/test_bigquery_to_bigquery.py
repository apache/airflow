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

from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator

BQ_HOOK_PATH = "airflow.providers.google.cloud.transfers.bigquery_to_bigquery.BigQueryHook"
TASK_ID = "test-bq-create-table-operator"
TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"


class TestBigQueryToBigQueryOperator(unittest.TestCase):
    @mock.patch(BQ_HOOK_PATH)
    def test_execute_without_location_should_execute_successfully(self, mock_hook):
        source_project_dataset_tables = f"{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_project_dataset_table = f"{TEST_DATASET + '_new'}.{TEST_TABLE_ID}"
        write_disposition = "WRITE_EMPTY"
        create_disposition = "CREATE_IF_NEEDED"
        labels = {"k1": "v1"}
        encryption_configuration = {"key": "kk"}

        operator = BigQueryToBigQueryOperator(
            task_id=TASK_ID,
            source_project_dataset_tables=source_project_dataset_tables,
            destination_project_dataset_table=destination_project_dataset_table,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            labels=labels,
            encryption_configuration=encryption_configuration,
        )

        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.run_copy.assert_called_once_with(
            source_project_dataset_tables=source_project_dataset_tables,
            destination_project_dataset_table=destination_project_dataset_table,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            labels=labels,
            encryption_configuration=encryption_configuration,
        )

    @mock.patch(BQ_HOOK_PATH)
    def test_execute_single_regional_location_should_execute_successfully(self, mock_hook):
        source_project_dataset_tables = f"{TEST_DATASET}.{TEST_TABLE_ID}"
        destination_project_dataset_table = f"{TEST_DATASET + '_new'}.{TEST_TABLE_ID}"
        write_disposition = "WRITE_EMPTY"
        create_disposition = "CREATE_IF_NEEDED"
        labels = {"k1": "v1"}
        location = "us-central1"
        encryption_configuration = {"key": "kk"}
        mock_hook.return_value.run_copy.return_value = "job-id"

        operator = BigQueryToBigQueryOperator(
            task_id=TASK_ID,
            source_project_dataset_tables=source_project_dataset_tables,
            destination_project_dataset_table=destination_project_dataset_table,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            labels=labels,
            encryption_configuration=encryption_configuration,
            location=location,
        )

        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.get_job.assert_called_once_with(
            job_id="job-id",
            location=location,
        )
