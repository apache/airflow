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

import mock

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.gcs_to_gcs import (
    WILDCARD, GCSSynchronizeBuckets, GCSToGCSOperator,
)

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


class TestGoogleCloudStorageToCloudStorageOperator(unittest.TestCase):
    """
    Tests the three use-cases for the wildcard operator. These are
    no_prefix: *test_object
    no_suffix: test_object*
    prefix_and_suffix: test*object

    Also tests the destination_object as prefix when the wildcard is used.
    """

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_no_prefix(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_PREFIX,
            destination_bucket=DESTINATION_BUCKET)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="", delimiter="test_object"
        )

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_no_suffix(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_SUFFIX,
            destination_bucket=DESTINATION_BUCKET)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="test_object", delimiter=""
        )

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_prefix_and_suffix(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_MIDDLE,
            destination_bucket=DESTINATION_BUCKET)

        operator.execute(None)
        mock_hook.return_value.list.assert_called_once_with(
            TEST_BUCKET, prefix="test", delimiter="object"
        )

    # copy with wildcard

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_wildcard_with_destination_object(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GCSToGCSOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_wildcard_with_destination_object_retained_prefix(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GCSToGCSOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_wildcard_without_destination_object(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GCSToGCSOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_wildcard_empty_destination_object(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GCSToGCSOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_last_modified_time(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GCSToGCSOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_wc_with_last_modified_time_with_all_true_cond(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        mock_hook.return_value.is_updated_after.side_effect = [True, True, True]
        operator = GCSToGCSOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_wc_with_last_modified_time_with_one_true_cond(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        mock_hook.return_value.is_updated_after.side_effect = [True, False, False]
        operator = GCSToGCSOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_WILDCARD_FILENAME,
            destination_bucket=DESTINATION_BUCKET,
            last_modified_time=MOD_TIME_1)

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET, 'test_object/file1.txt',
            DESTINATION_BUCKET, 'test_object/file1.txt')

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_wc_with_no_last_modified_time(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GCSToGCSOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_no_prefix_with_last_modified_time_with_true_cond(self, mock_hook):
        mock_hook.return_value.is_updated_after.return_value = True
        operator = GCSToGCSOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=MOD_TIME_1)

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET, 'test_object.txt', DESTINATION_BUCKET, 'test_object.txt')

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_no_prefix_with_no_last_modified_time(self, mock_hook):
        operator = GCSToGCSOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=None)

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_called_once_with(
            TEST_BUCKET, 'test_object.txt', DESTINATION_BUCKET, 'test_object.txt')

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_no_prefix_with_last_modified_time_with_false_cond(self, mock_hook):
        mock_hook.return_value.is_updated_after.return_value = False
        operator = GCSToGCSOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=SOURCE_OBJECT_NO_WILDCARD,
            last_modified_time=MOD_TIME_1)

        operator.execute(None)
        mock_hook.return_value.rewrite.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_more_than_1_wildcard(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GCSToGCSOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_MULTIPLE_WILDCARDS,
            destination_bucket=DESTINATION_BUCKET,
            destination_object=DESTINATION_OBJECT_PREFIX)

        total_wildcards = operator.source_object.count(WILDCARD)

        error_msg = "Only one wildcard '[*]' is allowed in source_object parameter. " \
                    "Found {}".format(total_wildcards)

        with self.assertRaisesRegex(AirflowException, error_msg):
            operator.execute(None)

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute_with_empty_destination_bucket(self, mock_hook):
        mock_hook.return_value.list.return_value = SOURCE_FILES_LIST
        operator = GCSToGCSOperator(
            task_id=TASK_ID, source_bucket=TEST_BUCKET,
            source_object=SOURCE_OBJECT_NO_WILDCARD,
            destination_bucket=None,
            destination_object=DESTINATION_OBJECT_PREFIX)

        with mock.patch.object(operator.log, 'warning') as mock_warn:
            operator.execute(None)
            mock_warn.assert_called_once_with(
                'destination_bucket is None. Defaulting it to source_bucket (%s)',
                TEST_BUCKET
            )
            self.assertEqual(operator.destination_bucket, operator.source_bucket)


class TestGoogleCloudStorageSync(unittest.TestCase):

    @mock.patch('airflow.providers.google.cloud.operators.gcs_to_gcs.GCSHook')
    def test_execute(self, mock_hook):
        task = GCSSynchronizeBuckets(
            task_id="task-id",
            source_bucket="SOURCE_BUCKET",
            destination_bucket="DESTINATION_BUCKET",
            source_object="SOURCE_OBJECT",
            destination_object="DESTINATION_OBJECT",
            recursive="RECURSIVE",
            delete_extra_files="DELETE_EXTRA_FILES",
            allow_overwrite="ALLOW_OVERWRITE",
            gcp_conn_id="GCP_CONN_ID",
            delegate_to="DELEGATE_TO",
        )
        task.execute({})
        mock_hook.assert_called_once_with(
            google_cloud_storage_conn_id='GCP_CONN_ID',
            delegate_to='DELEGATE_TO'
        )
        mock_hook.return_value.sync.assert_called_once_with(
            source_bucket='SOURCE_BUCKET',
            source_object='SOURCE_OBJECT',
            destination_bucket='DESTINATION_BUCKET',
            destination_object='DESTINATION_OBJECT',
            delete_extra_files='DELETE_EXTRA_FILES',
            recursive='RECURSIVE',
            allow_overwrite="ALLOW_OVERWRITE",
        )
