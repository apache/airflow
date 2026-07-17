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

from unittest import mock

import pytest
from airbyte_api.api import GetJobRequest
from airbyte_api.models import JobResponse, JobStatusEnum, JobTypeEnum

from airflow.models import Connection
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.common.compat.sdk import AirflowException


class TestAirbyteJobSensor:
    task_id = "task-id"
    airbyte_conn_id = "airbyte-conn-test"
    job_id = 1
    timeout = 120

    def get_job(self, status):
        response = mock.Mock()
        response.job_response = JobResponse(
            connection_id="connection-mock",
            job_id=self.job_id,
            start_time="today",
            job_type=JobTypeEnum.SYNC,
            status=status,
        )
        return response

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(conn_id=self.airbyte_conn_id, conn_type="airbyte", host="http://test-airbyte")
        )

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    def test_done(self, mock_get_job):
        mock_get_job.return_value = self.get_job(JobStatusEnum.SUCCEEDED)

        sensor = AirbyteJobSensor(
            task_id=self.task_id,
            airbyte_job_id=self.job_id,
            airbyte_conn_id=self.airbyte_conn_id,
        )
        ret = sensor.poke(context={})
        mock_get_job.assert_called_once_with(request=GetJobRequest(job_id=self.job_id))
        assert ret

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    def test_failed(self, mock_get_job):
        mock_get_job.return_value = self.get_job(JobStatusEnum.FAILED)

        sensor = AirbyteJobSensor(
            task_id=self.task_id,
            airbyte_job_id=self.job_id,
            airbyte_conn_id=self.airbyte_conn_id,
        )
        with pytest.raises(AirflowException, match="Job failed"):
            sensor.poke(context={})

        mock_get_job.assert_called_once_with(request=GetJobRequest(job_id=self.job_id))

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    def test_running(self, mock_get_job):
        mock_get_job.return_value = self.get_job(JobStatusEnum.RUNNING)

        sensor = AirbyteJobSensor(
            task_id=self.task_id,
            airbyte_job_id=self.job_id,
            airbyte_conn_id=self.airbyte_conn_id,
        )
        ret = sensor.poke(context={})

        mock_get_job.assert_called_once_with(request=GetJobRequest(job_id=self.job_id))

        assert not ret

    @mock.patch("airbyte_api.jobs.Jobs.get_job")
    def test_cancelled(self, mock_get_job):
        mock_get_job.return_value = self.get_job(JobStatusEnum.CANCELLED)

        sensor = AirbyteJobSensor(
            task_id=self.task_id,
            airbyte_job_id=self.job_id,
            airbyte_conn_id=self.airbyte_conn_id,
        )
        with pytest.raises(AirflowException, match="Job was cancelled"):
            sensor.poke(context={})

        mock_get_job.assert_called_once_with(request=GetJobRequest(job_id=self.job_id))

    def test_airbyte_job_sensor_init(self):
        """Test initializing AirbyteJobSensor with `poke_interval`."""
        sensor = AirbyteJobSensor(
            task_id="test_sensor",
            airbyte_job_id=1,
            deferrable=True,
            poke_interval=10,
            timeout=3600,
        )
        assert sensor.poke_interval == 10
        assert sensor.timeout == 3600
