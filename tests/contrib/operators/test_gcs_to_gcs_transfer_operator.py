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

from airflow.contrib.operators.gcs_to_gcs_transfer_operator import \
    GoogleCloudStorageToGoogleCloudStorageTransferOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


TASK_ID = 'test-gcs-gcs-transfer-operator'
SOURCE_BUCKET = 'test-source-bucket'
DESTINATION_BUCKET = 'test-destination-bucket'
PROJECT_ID = 'test-project'
DESCRIPTION = 'test-description'
SCHEDULE = {
    'scheduleStartDate': {'month': 10, 'day': 1, 'year': 2018},
    'scheduleEndDate': {'month': 10, 'day': 31, 'year': 2018},
}


class GoogleCloudStorageToGoogleCloudStorageTransferOperatorTest(unittest.TestCase):
    def test_constructor(self):
        """Test GoogleCloudStorageToGoogleCloudStorageTransferOperator instance is properly initialized."""

        operator = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            source_bucket=SOURCE_BUCKET,
            destination_bucket=DESTINATION_BUCKET,
            project_id=PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE,
        )

        self.assertEqual(operator.task_id, TASK_ID)
        self.assertEqual(operator.source_bucket, SOURCE_BUCKET)
        self.assertEqual(operator.destination_bucket, DESTINATION_BUCKET)
        self.assertEqual(operator.project_id, PROJECT_ID)
        self.assertEqual(operator.description, DESCRIPTION)
        self.assertEqual(operator.schedule, SCHEDULE)

    @mock.patch('airflow.contrib.operators.gcs_to_gcs_transfer_operator.GCPTransferServiceHook')
    def test_execute(self, mock_transfer_hook):
        """Test the execute function when the run is successful."""

        operator = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            source_bucket=SOURCE_BUCKET,
            destination_bucket=DESTINATION_BUCKET,
            project_id=PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            project_id=PROJECT_ID,
            description=DESCRIPTION,
            schedule=SCHEDULE,
            transfer_spec={
                'gcsDataSource': {
                    'bucketName': SOURCE_BUCKET,
                },
                'gcsDataSink': {
                    'bucketName': DESTINATION_BUCKET,
                },
                'objectConditions': {},
                'transferOptions': {}
            }
        )

        mock_transfer_hook.return_value.wait_for_transfer_job.assert_called_once_with(
            mock_transfer_hook.return_value.create_transfer_job.return_value
        )

    @mock.patch('airflow.contrib.operators.gcs_to_gcs_transfer_operator.GCPTransferServiceHook')
    def test_execute_skip_wait(self, mock_transfer_hook):
        """Test the execute function when the run is successful."""

        operator = GoogleCloudStorageToGoogleCloudStorageTransferOperator(
            task_id=TASK_ID,
            source_bucket=SOURCE_BUCKET,
            destination_bucket=DESTINATION_BUCKET,
            project_id=PROJECT_ID,
            description=DESCRIPTION,
            wait=False,
        )

        operator.execute(None)

        mock_transfer_hook.return_value.create_transfer_job.assert_called_once_with(
            project_id=PROJECT_ID,
            description=DESCRIPTION,
            schedule=None,
            transfer_spec={
                'gcsDataSource': {
                    'bucketName': SOURCE_BUCKET,
                },
                'gcsDataSink': {
                    'bucketName': DESTINATION_BUCKET,
                },
                'objectConditions': {},
                'transferOptions': {}
            }
        )

        assert not mock_transfer_hook.return_value.wait_for_transfer_job.called
