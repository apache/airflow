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

from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from airflow.hooks.S3_hook import S3Hook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

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


class GoogleCloudStorageToS3OperatorTest(unittest.TestCase):

    @mock_s3
    @mock.patch('airflow.contrib.operators.gcs_list_operator.GoogleCloudStorageHook')
    @mock.patch('airflow.contrib.operators.gcs_to_s3.GoogleCloudStorageHook')
    def test_execute(self, mock_hook, mock_hook2):
        mock_hook.return_value.list.return_value = MOCK_FILES
        mock_hook.return_value.download.return_value = b"testing"
        mock_hook2.return_value.list.return_value = MOCK_FILES

        operator = GoogleCloudStorageToS3Operator(task_id=TASK_ID,
                                                  bucket=GCS_BUCKET,
                                                  prefix=PREFIX,
                                                  delimiter=DELIMITER,
                                                  dest_aws_conn_id=None,
                                                  dest_s3_key=S3_BUCKET)
        # create dest bucket
        hook = S3Hook(aws_conn_id=None)
        b = hook.get_bucket('bucket')
        b.create()
        b.put_object(Key=MOCK_FILES[0], Body=b'testing')

        # we expect MOCK_FILES[1:] to be uploaded
        # and all MOCK_FILES to be present at the S3 bucket
        uploaded_files = operator.execute(None)
        self.assertEqual(sorted(MOCK_FILES[1:]),
                         sorted(uploaded_files))
        self.assertEqual(sorted(MOCK_FILES),
                         sorted(hook.list_keys('bucket', delimiter='/')))
