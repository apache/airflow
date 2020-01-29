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

import mock

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.gcs_to_s3 import GCSToS3Operator

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


class TestGCSToS3Operator(unittest.TestCase):

    # Test1: incremental behaviour (just some files missing)
    @mock_s3
    @mock.patch('airflow.providers.google.cloud.operators.gcs.GCSHook')
    @mock.patch('airflow.providers.amazon.aws.operators.gcs_to_s3.GCSHook')
    def test_execute_incremental(self, mock_hook, mock_hook2):
        mock_hook.return_value.list.return_value = MOCK_FILES
        mock_hook.return_value.download.return_value = b"testing"
        mock_hook2.return_value.list.return_value = MOCK_FILES

        operator = GCSToS3Operator(task_id=TASK_ID,
                                   bucket=GCS_BUCKET,
                                   prefix=PREFIX,
                                   delimiter=DELIMITER,
                                   dest_aws_conn_id=None,
                                   dest_s3_key=S3_BUCKET,
                                   replace=False)
        # create dest bucket
        hook = S3Hook(aws_conn_id=None)
        bucket = hook.get_bucket('bucket')
        bucket.create()
        bucket.put_object(Key=MOCK_FILES[0], Body=b'testing')

        # we expect all except first file in MOCK_FILES to be uploaded
        # and all the MOCK_FILES to be present at the S3 bucket
        uploaded_files = operator.execute(None)
        self.assertEqual(sorted(MOCK_FILES[1:]),
                         sorted(uploaded_files))
        self.assertEqual(sorted(MOCK_FILES),
                         sorted(hook.list_keys('bucket', delimiter='/')))

    # Test2: All the files are already in origin and destination without replace
    @mock_s3
    @mock.patch('airflow.providers.google.cloud.operators.gcs.GCSHook')
    @mock.patch('airflow.providers.amazon.aws.operators.gcs_to_s3.GCSHook')
    def test_execute_without_replace(self, mock_hook, mock_hook2):
        mock_hook.return_value.list.return_value = MOCK_FILES
        mock_hook.return_value.download.return_value = b"testing"
        mock_hook2.return_value.list.return_value = MOCK_FILES

        operator = GCSToS3Operator(task_id=TASK_ID,
                                   bucket=GCS_BUCKET,
                                   prefix=PREFIX,
                                   delimiter=DELIMITER,
                                   dest_aws_conn_id=None,
                                   dest_s3_key=S3_BUCKET,
                                   replace=False)
        # create dest bucket with all the files
        hook = S3Hook(aws_conn_id=None)
        bucket = hook.get_bucket('bucket')
        bucket.create()
        for mock_file in MOCK_FILES:
            bucket.put_object(Key=mock_file, Body=b'testing')

        # we expect nothing to be uploaded
        # and all the MOCK_FILES to be present at the S3 bucket
        uploaded_files = operator.execute(None)
        self.assertEqual([],
                         uploaded_files)
        self.assertEqual(sorted(MOCK_FILES),
                         sorted(hook.list_keys('bucket', delimiter='/')))

    # Test3: There are no files in destination bucket
    @mock_s3
    @mock.patch('airflow.providers.google.cloud.operators.gcs.GCSHook')
    @mock.patch('airflow.providers.amazon.aws.operators.gcs_to_s3.GCSHook')
    def test_execute(self, mock_hook, mock_hook2):
        mock_hook.return_value.list.return_value = MOCK_FILES
        mock_hook.return_value.download.return_value = b"testing"
        mock_hook2.return_value.list.return_value = MOCK_FILES

        operator = GCSToS3Operator(task_id=TASK_ID,
                                   bucket=GCS_BUCKET,
                                   prefix=PREFIX,
                                   delimiter=DELIMITER,
                                   dest_aws_conn_id=None,
                                   dest_s3_key=S3_BUCKET,
                                   replace=False)
        # create dest bucket without files
        hook = S3Hook(aws_conn_id=None)
        bucket = hook.get_bucket('bucket')
        bucket.create()

        # we expect all MOCK_FILES to be uploaded
        # and all MOCK_FILES to be present at the S3 bucket
        uploaded_files = operator.execute(None)
        self.assertEqual(sorted(MOCK_FILES),
                         sorted(uploaded_files))
        self.assertEqual(sorted(MOCK_FILES),
                         sorted(hook.list_keys('bucket', delimiter='/')))

    # Test4: Destination and Origin are in sync but replace all files in destination
    @mock_s3
    @mock.patch('airflow.providers.google.cloud.operators.gcs.GCSHook')
    @mock.patch('airflow.providers.amazon.aws.operators.gcs_to_s3.GCSHook')
    def test_execute_with_replace(self, mock_hook, mock_hook2):
        mock_hook.return_value.list.return_value = MOCK_FILES
        mock_hook.return_value.download.return_value = b"testing"
        mock_hook2.return_value.list.return_value = MOCK_FILES

        operator = GCSToS3Operator(task_id=TASK_ID,
                                   bucket=GCS_BUCKET,
                                   prefix=PREFIX,
                                   delimiter=DELIMITER,
                                   dest_aws_conn_id=None,
                                   dest_s3_key=S3_BUCKET,
                                   replace=True)
        # create dest bucket with all the files
        hook = S3Hook(aws_conn_id=None)
        bucket = hook.get_bucket('bucket')
        bucket.create()
        for mock_file in MOCK_FILES:
            bucket.put_object(Key=mock_file, Body=b'testing')

        # we expect all MOCK_FILES to be uploaded and replace the existing ones
        # and all MOCK_FILES to be present at the S3 bucket
        uploaded_files = operator.execute(None)
        self.assertEqual(sorted(MOCK_FILES),
                         sorted(uploaded_files))
        self.assertEqual(sorted(MOCK_FILES),
                         sorted(hook.list_keys('bucket', delimiter='/')))

    # Test5: Incremental sync with replace
    @mock_s3
    @mock.patch('airflow.providers.google.cloud.operators.gcs.GCSHook')
    @mock.patch('airflow.providers.amazon.aws.operators.gcs_to_s3.GCSHook')
    def test_execute_incremental_with_replace(self, mock_hook, mock_hook2):
        mock_hook.return_value.list.return_value = MOCK_FILES
        mock_hook.return_value.download.return_value = b"testing"
        mock_hook2.return_value.list.return_value = MOCK_FILES

        operator = GCSToS3Operator(task_id=TASK_ID,
                                   bucket=GCS_BUCKET,
                                   prefix=PREFIX,
                                   delimiter=DELIMITER,
                                   dest_aws_conn_id=None,
                                   dest_s3_key=S3_BUCKET,
                                   replace=True)
        # create dest bucket with just two files (the first two files in MOCK_FILES)
        hook = S3Hook(aws_conn_id=None)
        bucket = hook.get_bucket('bucket')
        bucket.create()
        for mock_file in MOCK_FILES[:2]:
            bucket.put_object(Key=mock_file, Body=b'testing')

        # we expect all the MOCK_FILES to be uploaded and replace the existing ones
        # and all MOCK_FILES to be present at the S3 bucket
        uploaded_files = operator.execute(None)
        self.assertEqual(sorted(MOCK_FILES),
                         sorted(uploaded_files))
        self.assertEqual(sorted(MOCK_FILES),
                         sorted(hook.list_keys('bucket', delimiter='/')))
