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
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.sensors.batch import (
    BatchComputeEnvironmentSensor,
    BatchJobQueueSensor,
    BatchSensor,
)

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


class TestBatchComputeEnvironmentSensor(unittest.TestCase):
    def setUp(self):
        self.environment_name = 'environment_name'
        self.sensor = BatchComputeEnvironmentSensor(
            task_id='test_batch_compute_environment_sensor',
            compute_environment=self.environment_name,
        )

    @mock.patch.object(BatchClientHook, 'client')
    def test_poke_no_environment(self, mock_batch_client):
        mock_batch_client.describe_compute_environments.return_value = {'computeEnvironments': []}
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke({})
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[self.environment_name],
        )
        assert 'not found' in str(ctx.value)

    @mock.patch.object(BatchClientHook, 'client')
    def test_poke_valid(self, mock_batch_client):
        mock_batch_client.describe_compute_environments.return_value = {
            'computeEnvironments': [{'status': 'VALID'}]
        }
        assert self.sensor.poke({})
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[self.environment_name],
        )

    @mock.patch.object(BatchClientHook, 'client')
    def test_poke_running(self, mock_batch_client):
        mock_batch_client.describe_compute_environments.return_value = {
            'computeEnvironments': [
                {
                    'status': 'CREATING',
                }
            ]
        }
        assert not self.sensor.poke({})
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[self.environment_name],
        )

    @mock.patch.object(BatchClientHook, 'client')
    def test_poke_invalid(self, mock_batch_client):
        mock_batch_client.describe_compute_environments.return_value = {
            'computeEnvironments': [
                {
                    'status': 'INVALID',
                }
            ]
        }
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke({})
        mock_batch_client.describe_compute_environments.assert_called_once_with(
            computeEnvironments=[self.environment_name],
        )
        assert 'AWS Batch compute environment failed' in str(ctx.value)


class TestBatchJobQueueSensor(unittest.TestCase):
    def setUp(self):
        self.job_queue = 'job_queue'
        self.sensor = BatchJobQueueSensor(
            task_id='test_batch_job_queue_sensor',
            job_queue=self.job_queue,
        )

    @mock.patch.object(BatchClientHook, 'client')
    def test_poke_no_queue(self, mock_batch_client):
        mock_batch_client.describe_job_queues.return_value = {'jobQueues': []}
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke({})
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[self.job_queue],
        )
        assert 'not found' in str(ctx.value)

    @mock.patch.object(BatchClientHook, 'client')
    def test_poke_no_queue_with_treat_non_existing_as_deleted(self, mock_batch_client):
        self.sensor.treat_non_existing_as_deleted = True
        mock_batch_client.describe_job_queues.return_value = {'jobQueues': []}
        assert self.sensor.poke({})
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[self.job_queue],
        )

    @mock.patch.object(BatchClientHook, 'client')
    def test_poke_valid(self, mock_batch_client):
        mock_batch_client.describe_job_queues.return_value = {'jobQueues': [{'status': 'VALID'}]}
        assert self.sensor.poke({})
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[self.job_queue],
        )

    @mock.patch.object(BatchClientHook, 'client')
    def test_poke_running(self, mock_batch_client):
        mock_batch_client.describe_job_queues.return_value = {
            'jobQueues': [
                {
                    'status': 'CREATING',
                }
            ]
        }
        assert not self.sensor.poke({})
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[self.job_queue],
        )

    @mock.patch.object(BatchClientHook, 'client')
    def test_poke_invalid(self, mock_batch_client):
        mock_batch_client.describe_job_queues.return_value = {
            'jobQueues': [
                {
                    'status': 'INVALID',
                }
            ]
        }
        with pytest.raises(AirflowException) as ctx:
            self.sensor.poke({})
        mock_batch_client.describe_job_queues.assert_called_once_with(
            jobQueues=[self.job_queue],
        )
        assert 'AWS Batch job queue failed' in str(ctx.value)
