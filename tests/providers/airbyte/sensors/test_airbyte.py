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

import pytest

from airflow import AirflowException
from airflow.models import Connection
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.utils import db

TASK_ID = "task-id"
AIRBYTE_CONN_ID = "airbyte-conn"
JOB_ID = '1'
TIMEOUT = 120


class TestAirbyteJobSensor(unittest.TestCase):
    def get_job(self, status):
        response = mock.Mock()
        response.json.return_value = {'job': {'status': status}}
        return response

    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job')
    def test_done(self, mock_get_job):
        mock_get_job.return_value = self.get_job('succeeded')

        sensor = AirbyteJobSensor(
            task_id=TASK_ID,
            airbyte_job_id=JOB_ID,
            airbyte_conn_id=AIRBYTE_CONN_ID,
        )
        ret = sensor.poke(context={})
        mock_get_job.assert_called_once_with(job_id=JOB_ID)
        assert ret

    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job')
    def test_failed(self, mock_get_job):
        mock_get_job.return_value = self.get_job('failed')

        sensor = AirbyteJobSensor(
            task_id=TASK_ID,
            airbyte_job_id=JOB_ID,
            airbyte_conn_id=AIRBYTE_CONN_ID,
        )
        with pytest.raises(AirflowException, match="Job failed"):
            sensor.poke(context={})

        mock_get_job.assert_called_once_with(job_id=JOB_ID)

    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job')
    def test_running(self, mock_get_job):
        mock_get_job.return_value = self.get_job('running')

        sensor = AirbyteJobSensor(
            task_id=TASK_ID,
            airbyte_job_id=JOB_ID,
            airbyte_conn_id=AIRBYTE_CONN_ID,
        )
        ret = sensor.poke(context={})

        mock_get_job.assert_called_once_with(job_id=JOB_ID)

        assert not ret

    @mock.patch('airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job')
    def test_cancelled(self, mock_get_job):
        mock_get_job.return_value = self.get_job('cancelled')

        sensor = AirbyteJobSensor(
            task_id=TASK_ID,
            airbyte_job_id=JOB_ID,
            airbyte_conn_id=AIRBYTE_CONN_ID,
        )
        with pytest.raises(AirflowException, match="Job was cancelled"):
            sensor.poke(context={})

        mock_get_job.assert_called_once_with(job_id=JOB_ID)
