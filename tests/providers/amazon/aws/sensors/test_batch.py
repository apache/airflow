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
from unittest import mock

from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.batch import BatchSensor

TASK_ID = 'batch_job_sensor'
JOB_ID = '8222a1c2-b246-4e19-b1b8-0039bb4407c0'


class TestBatchSensor(unittest.TestCase):
    def setUp(self):
        self.batch_sensor = BatchSensor(
            task_id='batch_job_sensor',
            job_id=JOB_ID,
        )

    @mock.patch('airflow.providers.amazon.aws.sensors.batch.AwsBatchClientHook')
    def test_poke_on_success_state(self, mock_batch_client_hook):
        mock_job_description = {'status': 'SUCCEEDED'}

        hook_instance = mock_batch_client_hook.return_value
        hook_instance.get_job_description.return_value = mock_job_description

        self.assertTrue(self.batch_sensor.poke(None))
        hook_instance.get_job_description.assert_called_once_with(JOB_ID)

    @mock.patch('airflow.providers.amazon.aws.sensors.batch.AwsBatchClientHook')
    def test_poke_on_failure_state(self, mock_batch_client_hook):
        mock_job_description = {'status': 'FAILED'}

        hook_instance = mock_batch_client_hook.return_value
        hook_instance.get_job_description.return_value = mock_job_description

        with self.assertRaises(AirflowException) as e:
            self.batch_sensor.poke(None)
        self.assertEqual('Batch sensor failed. Batch Job Status: FAILED', str(e.exception))
        hook_instance.get_job_description.assert_called_once_with(JOB_ID)

    @parameterized.expand(
        [
            ('SUBMITTED',),
            ('PENDING',),
            ('RUNNABLE',),
            ('STARTING',),
            ('RUNNING',),
        ]
    )
    @mock.patch('airflow.providers.amazon.aws.sensors.batch.AwsBatchClientHook')
    def test_poke_on_intermediate_state(self, job_status, mock_batch_client_hook):
        mock_job_description = {'status': job_status}

        hook_instance = mock_batch_client_hook.return_value
        hook_instance.get_job_description.return_value = mock_job_description

        self.assertFalse(self.batch_sensor.poke(None))
        hook_instance.get_job_description.assert_called_once_with(JOB_ID)
