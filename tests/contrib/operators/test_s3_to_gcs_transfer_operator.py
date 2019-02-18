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
GCP_PROJECT_ID = 'test-project'
DESCRIPTION = 'test-description'
ACCESS_KEY = 'test-access-key'
SECRET_KEY = 'test-secret-key'
SCHEDULE = {
    'scheduleStartDate': {'month': 10, 'day': 1, 'year': 2018},
    'scheduleEndDate': {'month': 10, 'day': 31, 'year': 2018},
}


Credentials = collections.namedtuple(
    'Credentials', ['access_key', 'secret_key'])


class S3ToGoogleCloudStorageTransferOperatorTest(unittest.TestCase):
    def test_constructor(self):
        """Test S3ToGoogleCloudStorageTransferOperator instance is properly initialized."""

        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            gcs_bucket=GCS_BUCKET,
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE,
        )

        self.assertEqual(operator.task_id, TASK_ID)
        self.assertEqual(operator.s3_bucket, S3_BUCKET)
        self.assertEqual(operator.gcs_bucket, GCS_BUCKET)
        self.assertEqual(operator.project_id, GCP_PROJECT_ID)
        self.assertEqual(operator.description, DESCRIPTION)
        self.assertEqual(operator.schedule, SCHEDULE)

    @mock.patch('airflow.contrib.operators.s3_to_gcs_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.s3_to_gcs_transfer_operator.S3Hook')
    def test_execute(self, mock_s3_hook, mock_transfer_hook):
        """Test the execute function with a custom schedule."""

        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            gcs_bucket=GCS_BUCKET,
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE,
        )

        mock_s3_hook.return_value.get_credentials.return_value = Credentials(
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE,
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

        mock_transfer_hook.return_value.wait_for_transfer_job.assert_called_once_with(
            mock_transfer_hook.return_value.create_transfer_job.return_value
        )

    @mock.patch('airflow.contrib.operators.s3_to_gcs_transfer_operator.GCPTransferServiceHook')
    @mock.patch('airflow.contrib.operators.s3_to_gcs_transfer_operator.S3Hook')
    def test_execute_skip_wait(self, mock_s3_hook, mock_transfer_hook):
        """Test the execute function and wait until transfer is complete."""

        operator = S3ToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            s3_bucket=S3_BUCKET,
            gcs_bucket=GCS_BUCKET,
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            wait=False,
        )

        mock_s3_hook.return_value.get_credentials.return_value = Credentials(
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            description=DESCRIPTION,
            schedule=None,
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

        assert not mock_transfer_hook.return_value.wait_for_transfer_job.called
