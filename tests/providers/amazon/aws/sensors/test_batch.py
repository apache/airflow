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
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.sensors.batch import BatchSensor

TASK_ID = 'batch_job_sensor'
JOB_ID = '8222a1c2-b246-4e19-b1b8-0039bb4407c0'


class TestBatchSensor(unittest.TestCase):
    def setUp(self):
        self.batch_sensor = BatchSensor(
            task_id='batch_job_sensor',
            job_id=JOB_ID,
        )

    @mock.patch.object(BatchClientHook, 'get_job_description')
    def test_poke_on_success_state(self, mock_get_job_description):
        mock_get_job_description.return_value = {'status': 'SUCCEEDED'}
        self.assertTrue(self.batch_sensor.poke({}))
        mock_get_job_description.assert_called_once_with(JOB_ID)

    @mock.patch.object(BatchClientHook, 'get_job_description')
    def test_poke_on_failure_state(self, mock_get_job_description):
        mock_get_job_description.return_value = {'status': 'FAILED'}
        with self.assertRaises(AirflowException) as e:
            self.batch_sensor.poke({})

        self.assertEqual('Batch sensor failed. AWS Batch job status: FAILED', str(e.exception))
        mock_get_job_description.assert_called_once_with(JOB_ID)

    @mock.patch.object(BatchClientHook, 'get_job_description')
    def test_poke_on_invalid_state(self, mock_get_job_description):
        mock_get_job_description.return_value = {'status': 'INVALID'}
        with self.assertRaises(AirflowException) as e:
            self.batch_sensor.poke({})

        self.assertEqual('Batch sensor failed. Unknown AWS Batch job status: INVALID', str(e.exception))
        mock_get_job_description.assert_called_once_with(JOB_ID)

    @parameterized.expand(
        [
            ('SUBMITTED',),
            ('PENDING',),
            ('RUNNABLE',),
            ('STARTING',),
            ('RUNNING',),
        ]
    )
    @mock.patch.object(BatchClientHook, 'get_job_description')
    def test_poke_on_intermediate_state(self, job_status, mock_get_job_description):
        mock_get_job_description.return_value = {'status': job_status}
        self.assertFalse(self.batch_sensor.poke({}))
        mock_get_job_description.assert_called_once_with(JOB_ID)
