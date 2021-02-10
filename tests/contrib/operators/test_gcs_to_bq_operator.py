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

from tests.compat import mock

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

TASK_ID = 'test-gcs-to-bq-operator'
TEST_EXPLICIT_DEST = 'test-project.dataset.table'
TEST_BUCKET = 'test-bucket'
MAX_ID_KEY = 'id'
TEST_SOURCE_OBJECTS = ['test/objects/*']


class TestGoogleCloudStorageToBigQueryOperator(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.gcs_to_bq.BigQueryHook')
    def test_execute_explicit_project_legacy(self, bq_hook):
        operator = GoogleCloudStorageToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            max_id_key=MAX_ID_KEY
        )

        # using legacy SQL
        bq_hook.return_value.get_conn.return_value.cursor.return_value.use_legacy_sql = True

        operator.execute(None)

        bq_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .execute \
            .assert_called_once_with("SELECT MAX(id) FROM [test-project.dataset.table]")

    @mock.patch('airflow.contrib.operators.gcs_to_bq.BigQueryHook')
    def test_execute_explicit_project(self, bq_hook):
        operator = GoogleCloudStorageToBigQueryOperator(
            task_id=TASK_ID,
            bucket=TEST_BUCKET,
            source_objects=TEST_SOURCE_OBJECTS,
            destination_project_dataset_table=TEST_EXPLICIT_DEST,
            max_id_key=MAX_ID_KEY
        )

        # using non-legacy SQL
        bq_hook.return_value.get_conn.return_value.cursor.return_value.use_legacy_sql = False

        operator.execute(None)

        bq_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .execute \
            .assert_called_once_with("SELECT MAX(id) FROM `test-project.dataset.table`")
