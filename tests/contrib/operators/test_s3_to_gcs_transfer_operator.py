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
import collections

from airflow.contrib.operators.s3_to_gcs_transfer_operator import \
    S3ToGoogleCloudStorageTransferOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


TASK_ID = 'test-s3-gcs-transfer-operator'
S3_BUCKET = 'test-s3-bucket'
GCS_BUCKET = 'test-gcs-bucket'
PROJECT_ID = 'test-project'
ACCESS_KEY = 'test-access-key'
SECRET_KEY = 'test-secret-key'


Credentials = collections.namedtuple(
    'Credentials', ['access_key', 'secret_key'])


class S3ToGoogleCloudStorageTransferOperatorTest(unittest.TestCase):
    def test_constructor(self):
        """Test S3ToGoogleCloudStorageTransferOperator instance is properly initialized."""

        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            gcs_bucket=GCS_BUCKET,
            project_id=PROJECT_ID,
        )

        self.assertEqual(operator.task_id, TASK_ID)
        self.assertEqual(operator.s3_bucket, S3_BUCKET)
        self.assertEqual(operator.gcs_bucket, GCS_BUCKET)
        self.assertEqual(operator.project_id, PROJECT_ID)

    @mock.patch('airflow.contrib.operators.s3_to_gcs_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.s3_to_gcs_transfer_operator.S3Hook')
    def test_execute(self, mock_s3_hook, mock_transfer_hook):
        """Test the execute function when the run is successful."""

        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            gcs_bucket=GCS_BUCKET,
            project_id=PROJECT_ID,
        )

        mock_s3_hook.return_value.get_credentials.return_value = Credentials(
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            project_id=PROJECT_ID,
            transfer_spec={
                'awsS3DataSource': {
                    'bucketName': S3_BUCKET,
                    'awsAccessKey': {
                        'accessKeyId': ACCESS_KEY,
                        'secretAccessKey': SECRET_KEY,
                    }
                },
                'gcsDataSink': {
                    'bucketName': GCS_BUCKET,
                },
                'objectConditions': {},
                'transferOptions': {}
            }
        )
