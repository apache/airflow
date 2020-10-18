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

from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

TASK_ID = 'test-s3-gcs-operator'
S3_BUCKET = 'test-bucket'
S3_PREFIX = 'TEST'
S3_DELIMITER = '/'
GCS_PATH_PREFIX = 'gs://gcs-bucket/data/'
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
AWS_CONN_ID = 'aws_default'
GCS_CONN_ID = 'google_cloud_default'
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestS3ToGoogleCloudStorageOperator(unittest.TestCase):
    def test_init(self):
        """Test S3ToGCSOperator instance is properly initialized."""

        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )

        self.assertEqual(operator.task_id, TASK_ID)
        self.assertEqual(operator.bucket, S3_BUCKET)
        self.assertEqual(operator.prefix, S3_PREFIX)
        self.assertEqual(operator.delimiter, S3_DELIMITER)
        self.assertEqual(operator.gcp_conn_id, GCS_CONN_ID)
        self.assertEqual(operator.dest_gcs, GCS_PATH_PREFIX)
        self.assertEqual(operator.google_impersonation_chain, IMPERSONATION_CHAIN)

    @mock.patch('airflow.providers.google.cloud.transfers.s3_to_gcs.S3Hook')
    @mock.patch('airflow.providers.amazon.aws.operators.s3_list.S3Hook')
    @mock.patch('airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook')
    def test_execute(self, gcs_mock_hook, s3_one_mock_hook, s3_two_mock_hook):
        """Test the execute function when the run is successful."""

        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            dest_gcs_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )

        s3_one_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        s3_two_mock_hook.return_value.list_keys.return_value = MOCK_FILES

        uploaded_files = operator.execute(None)
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call('gcs-bucket', 'data/TEST1.csv', mock.ANY, gzip=False),
                mock.call('gcs-bucket', 'data/TEST3.csv', mock.ANY, gzip=False),
                mock.call('gcs-bucket', 'data/TEST2.csv', mock.ANY, gzip=False),
            ],
            any_order=True,
        )

        s3_one_mock_hook.assert_called_once_with(aws_conn_id=AWS_CONN_ID, verify=None)
        s3_two_mock_hook.assert_called_once_with(aws_conn_id=AWS_CONN_ID, verify=None)
        gcs_mock_hook.assert_called_once_with(
            google_cloud_storage_conn_id=GCS_CONN_ID,
            delegate_to=None,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        # we expect MOCK_FILES to be uploaded
        self.assertEqual(sorted(MOCK_FILES), sorted(uploaded_files))

    @mock.patch('airflow.providers.google.cloud.transfers.s3_to_gcs.S3Hook')
    @mock.patch('airflow.providers.amazon.aws.operators.s3_list.S3Hook')
    @mock.patch('airflow.providers.google.cloud.transfers.s3_to_gcs.GCSHook')
    def test_execute_with_gzip(self, gcs_mock_hook, s3_one_mock_hook, s3_two_mock_hook):
        """Test the execute function when the run is successful."""

        operator = S3ToGCSOperator(
            task_id=TASK_ID,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
            delimiter=S3_DELIMITER,
            dest_gcs_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            gzip=True,
        )

        s3_one_mock_hook.return_value.list_keys.return_value = MOCK_FILES
        s3_two_mock_hook.return_value.list_keys.return_value = MOCK_FILES

        operator.execute(None)
        gcs_mock_hook.assert_called_once_with(
            google_cloud_storage_conn_id=GCS_CONN_ID,
            delegate_to=None,
            impersonation_chain=None,
        )
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call('gcs-bucket', 'data/TEST2.csv', mock.ANY, gzip=True),
                mock.call('gcs-bucket', 'data/TEST1.csv', mock.ANY, gzip=True),
                mock.call('gcs-bucket', 'data/TEST3.csv', mock.ANY, gzip=True),
            ],
            any_order=True,
        )
