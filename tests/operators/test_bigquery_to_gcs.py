# -*- coding: utf-8 -*-
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

import unittest

from airflow.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from tests.compat import mock

TASK_ID = 'test-bq-create-table-operator'
TEST_DATASET = 'test-dataset'
TEST_TABLE_ID = 'test-table-id'


class TestBigQueryToCloudStorageOperator(unittest.TestCase):
    @mock.patch('airflow.operators.bigquery_to_gcs.BigQueryHook')
    def test_execute(self, mock_hook):
        source_project_dataset_table = '{}.{}'.format(
            TEST_DATASET, TEST_TABLE_ID)
        destination_cloud_storage_uris = ['gs://some-bucket/some-file.txt']
        compression = 'NONE'
        export_format = 'CSV'
        field_delimiter = ','
        print_header = True
        labels = {'k1': 'v1'}

        operator = BigQueryToCloudStorageOperator(
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
            compression=compression,
            export_format=export_format,
            field_delimiter=field_delimiter,
            print_header=print_header,
            labels=labels
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_extract \
            .assert_called_once_with(
                source_project_dataset_table=source_project_dataset_table,
                destination_cloud_storage_uris=destination_cloud_storage_uris,
                compression=compression,
                export_format=export_format,
                field_delimiter=field_delimiter,
                print_header=print_header,
                labels=labels
            )
