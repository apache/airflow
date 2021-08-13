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
from datetime import datetime

import six

from airflow.contrib.operators.gcs_to_gcs import \
    GoogleCloudStorageToGoogleCloudStorageOperator, WILDCARD
from airflow.exceptions import AirflowException
from tests.compat import mock, patch

TASK_ID = 'test-gcs-to-gcs-operator'
TEST_BUCKET = 'test-bucket'
DELIMITER = '.csv'
PREFIX = 'TEST'
SOURCE_OBJECT_WILDCARD_PREFIX = '*test_object'
SOURCE_OBJECT_WILDCARD_SUFFIX = 'test_object*'
SOURCE_OBJECT_WILDCARD_MIDDLE = 'test*object'
SOURCE_OBJECT_WILDCARD_FILENAME = 'test_object*.txt'
SOURCE_OBJECT_NO_WILDCARD = 'test_object.txt'
SOURCE_OBJECT_MULTIPLE_WILDCARDS = 'csv/*/test_*.csv'
DESTINATION_BUCKET = 'archive'
DESTINATION_OBJECT_PREFIX = 'foo/bar'
SOURCE_FILES_LIST = [
    'test_object/file1.txt',
    'test_object/file2.txt',
    'test_object/file3.json',
]
MOD_TIME_1 = datetime(2016, 1, 1)


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
            source_object=SOURCE_OBJECT_WILDCARD_PREFIX,
            destination_bucket=DESTINATION_BUCKET)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="", delimiter="test_object"
        )

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_no_suffix(self, mock_hook):
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_SUFFIX,
            destination_bucket=DESTINATION_BUCKET)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="test_object", delimiter=""
        )

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_prefix_and_suffix(self, mock_hook):
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_MIDDLE,
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
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
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
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            destination_object='{}/{}'.format(DESTINATION_OBJECT_PREFIX,
                                              SOURCE_OBJECT_WILDCARD_SUFFIX[:-1])
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
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
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
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
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

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_last_modified_time(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            last_modified_time=None)

        operator.execute(None)
        mock_calls_none = [
            mock.call(TEST_BUCKET, 'test_object/file1.txt',
                      DESTINATION_BUCKET, 'test_object/file1.txt'),
            mock.call(TEST_BUCKET, 'test_object/file2.txt',
                      DESTINATION_BUCKET, 'test_object/file2.txt'),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_none)

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_wc_with_last_modified_time_with_all_true_cond(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        mock_hook.return_value.is_updated_after.side_effect = [True, True, True]
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            last_modified_time=MOD_TIME_1)

        operator.execute(None)
        mock_calls_none = [
            mock.call(TEST_BUCKET, 'test_object/file1.txt',
                      DESTINATION_BUCKET, 'test_object/file1.txt'),
            mock.call(TEST_BUCKET, 'test_object/file2.txt',
                      DESTINATION_BUCKET, 'test_object/file2.txt'),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_none)

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_wc_with_last_modified_time_with_one_true_cond(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        mock_hook.return_value.is_updated_after.side_effect = [True, False, False]
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            last_modified_time=MOD_TIME_1)

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET, 'test_object/file1.txt',
            DESTINATION_BUCKET, 'test_object/file1.txt')

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_wc_with_no_last_modified_time(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            last_modified_time=None)

        operator.execute(None)
        mock_calls_none = [
            mock.call(TEST_BUCKET, 'test_object/file1.txt',
                      DESTINATION_BUCKET, 'test_object/file1.txt'),
            mock.call(TEST_BUCKET, 'test_object/file2.txt',
                      DESTINATION_BUCKET, 'test_object/file2.txt'),
        ]
        mock_hook.return_value.rewrite.assert_has_calls(mock_calls_none)

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_no_prefix_with_last_modified_time_with_true_cond(self, mock_hook):
        mock_hook.return_value.is_updated_after.return_value = True
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=MOD_TIME_1)

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET, 'test_object.txt', DESTINATION_BUCKET, 'test_object.txt')

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_no_prefix_with_no_last_modified_time(self, mock_hook):
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=None)

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET, 'test_object.txt', DESTINATION_BUCKET, 'test_object.txt')

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_no_prefix_with_last_modified_time_with_false_cond(self, mock_hook):
        mock_hook.return_value.is_updated_after.return_value = False
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=MOD_TIME_1)

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_not_called()

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_more_than_1_wildcard(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_MULTIPLE_WILDCARDS,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PREFIX)

        total_wildcards = operator.source_object.count(WILDCARD)

        error_msg = "Only one wildcard '[*]' is allowed in source_object parameter. " \
                    "Found {}".format(total_wildcards)

        with six.assertRaisesRegex(self, AirflowException, error_msg):
            operator.execute(None)

    @mock.patch('airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageHook')
    def test_execute_with_empty_destination_bucket(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GoogleCloudStorageToGoogleCloudStorageOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=None,
            destination_object=DESTINATION_OBJECT_PREFIX)

        with patch.object(operator.log, 'warning') as mock_warn:
            operator.execute(None)
            mock_warn.assert_called_with(
                'destination_bucket is None. Defaulting it to source_bucket (%s)',
                TEST_BUCKET
            )
            self.assertEquals(operator.destination_bucket, operator.source_bucket)
