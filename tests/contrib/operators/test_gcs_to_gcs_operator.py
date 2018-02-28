# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from airflow.contrib.operators.gcs_to_gcs import \
    GoogleCloudStorageToGoogleCloudStorageOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

TASK_ID = 'test-gcs-to-gcs-operator'
TEST_BUCKET = 'test-bucket'
DELIMITER = '.csv'
PREFIX = 'TEST'
SOURCE_OBJECT_1 = '*test_object'
SOURCE_OBJECT_2 = 'test_object*'
SOURCE_OBJECT_3 = 'test*object'
DESTINATION_BUCKET = 'archive'


class GoogleCloudStorageToCloudStorageOperatorTest(unittest.TestCase):
    """
    Tests the three use-cases for the wildcard operator. These are
    no_prefix: *test_object
    no_suffix: test_object*
    prefix_and_suffix: test*object
    """

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_no_prefix(self, mock_hook):
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_1,
            destination_bucket=DESTINATION_BUCKET)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="", delimiter="test_object"
        )

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_no_suffix(self, mock_hook):
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_2,
            destination_bucket=DESTINATION_BUCKET)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="test_object", delimiter=""
        )

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_prefix_and_suffix(self, mock_hook):
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_3,
            destination_bucket=DESTINATION_BUCKET)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="test", delimiter="object"
        )
