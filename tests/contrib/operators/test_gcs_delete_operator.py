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

from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from tests.compat import mock

TASK_ID = 'test-gcs-delete-operator'
TEST_BUCKET = 'test-bucket'
PREFIX = 'ab'
MOCK_FILES = ["a", "ab", "abc"]


class GoogleCloudStorageDeleteOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.gcs_delete_operator.GoogleCloudStorageHook')
    def test_delete_objects(self, mock_hook):
        operator = GoogleCloudStorageDeleteOperator(task_id=TASK_ID,
                                                    bucket_name=TEST_BUCKET,
                                                    objects=MOCK_FILES[0:2])

        operator.execute(None)
        mock_hook.return_value.list.assert_not_called()
        mock_hook.return_value.delete.assert_has_calls(
            calls=[
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[0]),
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[1])
            ],
            any_order=True
        )

    @mock.patch('airflow.contrib.operators.gcs_delete_operator.GoogleCloudStorageHook')
    def test_delete_prefix(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES[1:3]
        operator = GoogleCloudStorageDeleteOperator(task_id=TASK_ID,
                                                    bucket_name=TEST_BUCKET,
                                                    prefix=PREFIX)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            bucket_name=TEST_BUCKET, prefix=PREFIX
        )
        mock_hook.return_value.delete.assert_has_calls(
            calls=[
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[1]),
                mock.call(bucket_name=TEST_BUCKET, object_name=MOCK_FILES[2])
            ],
            any_order=True
        )
