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
from tempfile import NamedTemporaryFile
from unittest import mock

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator

try:
    from moto import mock_s3
except ImportError:
    mock_s3 = None

TASK_ID = 'test-gcs-list-operator'
GCS_BUCKET = 'test-bucket'
DELIMITER = '.csv'
PREFIX = 'TEST'
S3_BUCKET = 's3://bucket/'
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
S3_ACL_POLICY = "private-read"


class TestGCSToS3Operator(unittest.TestCase):

    # Test1: incremental behaviour (just some files missing)
    @mock_s3
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook')
    def test_execute_incremental(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            operator = GCSToS3Operator(
                task_id=TASK_ID,
                bucket=GCS_BUCKET,
                prefix=PREFIX,
                delimiter=DELIMITER,
                dest_aws_conn_id="aws_default",
                dest_s3_key=S3_BUCKET,
                replace=False,
            )
            # create dest bucket
            hook = S3Hook(aws_conn_id='airflow_gcs_test')
            bucket = hook.get_bucket('bucket')
            bucket.create()
            bucket.put_object(Key=MOCK_FILES[0], Body=b'testing')

            # we expect all except first file in MOCK_FILES to be uploaded
            # and all the MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES[1:]) == sorted(uploaded_files)
            assert sorted(MOCK_FILES) == sorted(hook.list_keys('bucket', delimiter='/'))

    # Test2: All the files are already in origin and destination without replace
    @mock_s3
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook')
    def test_execute_without_replace(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            operator = GCSToS3Operator(
                task_id=TASK_ID,
                bucket=GCS_BUCKET,
                prefix=PREFIX,
                delimiter=DELIMITER,
                dest_aws_conn_id="aws_default",
                dest_s3_key=S3_BUCKET,
                replace=False,
            )
            # create dest bucket with all the files
            hook = S3Hook(aws_conn_id='airflow_gcs_test')
            bucket = hook.get_bucket('bucket')
            bucket.create()
            for mock_file in MOCK_FILES:
                bucket.put_object(Key=mock_file, Body=b'testing')

            # we expect nothing to be uploaded
            # and all the MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert [] == uploaded_files
            assert sorted(MOCK_FILES) == sorted(hook.list_keys('bucket', delimiter='/'))

    # Test3: There are no files in destination bucket
    @mock_s3
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook')
    def test_execute(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            operator = GCSToS3Operator(
                task_id=TASK_ID,
                bucket=GCS_BUCKET,
                prefix=PREFIX,
                delimiter=DELIMITER,
                dest_aws_conn_id="aws_default",
                dest_s3_key=S3_BUCKET,
                replace=False,
            )
            # create dest bucket without files
            hook = S3Hook(aws_conn_id='airflow_gcs_test')
            bucket = hook.get_bucket('bucket')
            bucket.create()

            # we expect all MOCK_FILES to be uploaded
            # and all MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES) == sorted(uploaded_files)
            assert sorted(MOCK_FILES) == sorted(hook.list_keys('bucket', delimiter='/'))

    # Test4: Destination and Origin are in sync but replace all files in destination
    @mock_s3
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook')
    def test_execute_with_replace(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            operator = GCSToS3Operator(
                task_id=TASK_ID,
                bucket=GCS_BUCKET,
                prefix=PREFIX,
                delimiter=DELIMITER,
                dest_aws_conn_id="aws_default",
                dest_s3_key=S3_BUCKET,
                replace=True,
            )
            # create dest bucket with all the files
            hook = S3Hook(aws_conn_id='airflow_gcs_test')
            bucket = hook.get_bucket('bucket')
            bucket.create()
            for mock_file in MOCK_FILES:
                bucket.put_object(Key=mock_file, Body=b'testing')

            # we expect all MOCK_FILES to be uploaded and replace the existing ones
            # and all MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES) == sorted(uploaded_files)
            assert sorted(MOCK_FILES) == sorted(hook.list_keys('bucket', delimiter='/'))

    # Test5: Incremental sync with replace
    @mock_s3
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook')
    def test_execute_incremental_with_replace(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            operator = GCSToS3Operator(
                task_id=TASK_ID,
                bucket=GCS_BUCKET,
                prefix=PREFIX,
                delimiter=DELIMITER,
                dest_aws_conn_id="aws_default",
                dest_s3_key=S3_BUCKET,
                replace=True,
            )
            # create dest bucket with just two files (the first two files in MOCK_FILES)
            hook = S3Hook(aws_conn_id='airflow_gcs_test')
            bucket = hook.get_bucket('bucket')
            bucket.create()
            for mock_file in MOCK_FILES[:2]:
                bucket.put_object(Key=mock_file, Body=b'testing')

            # we expect all the MOCK_FILES to be uploaded and replace the existing ones
            # and all MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES) == sorted(uploaded_files)
            assert sorted(MOCK_FILES) == sorted(hook.list_keys('bucket', delimiter='/'))

    @mock_s3
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook')
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.S3Hook')
    def test_execute_should_handle_with_default_dest_s3_extra_args(self, s3_mock_hook, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        mock_hook.return_value.download.return_value = b"testing"
        s3_mock_hook.return_value = mock.Mock()
        s3_mock_hook.parse_s3_url.return_value = mock.Mock()

        operator = GCSToS3Operator(
            task_id=TASK_ID,
            bucket=GCS_BUCKET,
            prefix=PREFIX,
            delimiter=DELIMITER,
            dest_aws_conn_id="aws_default",
            dest_s3_key=S3_BUCKET,
            replace=True,
        )
        operator.execute(None)
        s3_mock_hook.assert_called_once_with(aws_conn_id='aws_default', extra_args={}, verify=None)

    @mock_s3
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook')
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.S3Hook')
    def test_execute_should_pass_dest_s3_extra_args_to_s3_hook(self, s3_mock_hook, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name
            s3_mock_hook.return_value = mock.Mock()
            s3_mock_hook.parse_s3_url.return_value = mock.Mock()

            operator = GCSToS3Operator(
                task_id=TASK_ID,
                bucket=GCS_BUCKET,
                prefix=PREFIX,
                delimiter=DELIMITER,
                dest_aws_conn_id="aws_default",
                dest_s3_key=S3_BUCKET,
                replace=True,
                dest_s3_extra_args={
                    "ContentLanguage": "value",
                },
            )
            operator.execute(None)
            s3_mock_hook.assert_called_once_with(
                aws_conn_id='aws_default', extra_args={'ContentLanguage': 'value'}, verify=None
            )

    # Test6: s3_acl_policy parameter is set
    @mock_s3
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook')
    @mock.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file')
    def test_execute_with_s3_acl_policy(self, mock_load_file, mock_gcs_hook):
        mock_gcs_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_gcs_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            operator = GCSToS3Operator(
                task_id=TASK_ID,
                bucket=GCS_BUCKET,
                prefix=PREFIX,
                delimiter=DELIMITER,
                dest_aws_conn_id="aws_default",
                dest_s3_key=S3_BUCKET,
                replace=False,
                s3_acl_policy=S3_ACL_POLICY,
            )

            # Create dest bucket without files
            hook = S3Hook(aws_conn_id='airflow_gcs_test')
            bucket = hook.get_bucket('bucket')
            bucket.create()

            operator.execute(None)

            # Make sure the acl_policy parameter is passed to the upload method
            _, kwargs = mock_load_file.call_args
            assert kwargs['acl_policy'] == S3_ACL_POLICY

    @mock_s3
    @mock.patch('airflow.providers.amazon.aws.transfers.gcs_to_s3.GCSHook')
    def test_execute_without_keep_director_structure(self, mock_hook):
        mock_hook.return_value.list.return_value = MOCK_FILES
        with NamedTemporaryFile() as f:
            gcs_provide_file = mock_hook.return_value.provide_file
            gcs_provide_file.return_value.__enter__.return_value.name = f.name

            operator = GCSToS3Operator(
                task_id=TASK_ID,
                bucket=GCS_BUCKET,
                prefix=PREFIX,
                delimiter=DELIMITER,
                dest_aws_conn_id="aws_default",
                dest_s3_key=S3_BUCKET,
                replace=False,
                keep_directory_structure=False,
            )
            # create dest bucket with all the files
            hook = S3Hook(aws_conn_id='airflow_gcs_test')
            bucket = hook.get_bucket('bucket')
            bucket.create()

            # we expect all except first file in MOCK_FILES to be uploaded
            # and all the MOCK_FILES to be present at the S3 bucket
            uploaded_files = operator.execute(None)
            assert sorted(MOCK_FILES) == sorted(uploaded_files)
            assert hook.check_for_prefix(bucket_name='bucket', prefix=PREFIX + '/', delimiter='/') is True
