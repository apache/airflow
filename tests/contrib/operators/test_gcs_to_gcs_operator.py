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
SOURCE_OBJECT_4 = 'test_object*.txt'
DESTINATION_BUCKET = 'archive'
DESTINATION_OBJECT_PREFIX = 'foo/bar'
SOURCE_FILES_LIST = [
    'test_object/file1.txt',
    'test_object/file2.txt',
    'test_object/file3.json',
]


class GoogleCloudStorageToCloudStorageOperatorTest(unittest.TestCase):
    """
    Tests the three use-cases for the wildcard operator. These are
    no_prefix: *test_object
    no_suffix: test_object*
    prefix_and_suffix: test*object

    Also tests the destionation_object as prefix when the wildcard is used.
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

    # copy with wildcard

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_wildcard_with_destination_object(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_4,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PREFIX)

        operator.execute(None)
        mock_calls = [
            mock.call(TEST_BUCKET, 'test_object/file1.txt',
                      DESTINATION_BUCKET, 'foo/bar/file1.txt'),
            mock.call(TEST_BUCKET, 'test_object/file2.txt',
                      DESTINATION_BUCKET, 'foo/bar/file2.txt'),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls)

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_wildcard_with_destination_object_retained_prefix(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_4,
            destination_bucket=DESTINATION_BUCKET,
            destination_object='{}/{}'.format(DESTINATION_OBJECT_PREFIX,
                                              SOURCE_OBJECT_2[:-1])
        )

        operator.execute(None)
        mock_calls_retained = [
            mock.call(TEST_BUCKET, 'test_object/file1.txt',
                      DESTINATION_BUCKET, 'foo/bar/test_object/file1.txt'),
            mock.call(TEST_BUCKET, 'test_object/file2.txt',
                      DESTINATION_BUCKET, 'foo/bar/test_object/file2.txt'),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_retained)

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_wildcard_without_destination_object(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_4,
            destination_bucket=DESTINATION_BUCKET)

        operator.execute(None)
        mock_calls_none = [
            mock.call(TEST_BUCKET, 'test_object/file1.txt',
                      DESTINATION_BUCKET, 'test_object/file1.txt'),
            mock.call(TEST_BUCKET, 'test_object/file2.txt',
                      DESTINATION_BUCKET, 'test_object/file2.txt'),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_none)

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_wildcard_empty_destination_object(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_4,
            destination_bucket=DESTINATION_BUCKET,
            destination_object='')

        operator.execute(None)
        mock_calls_empty = [
            mock.call(TEST_BUCKET, 'test_object/file1.txt',
                      DESTINATION_BUCKET, '/file1.txt'),
            mock.call(TEST_BUCKET, 'test_object/file2.txt',
                      DESTINATION_BUCKET, '/file2.txt'),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_empty)
