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

import asyncio
import time
from unittest import mock

import pytest

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.airbyte.triggers.airbyte import AirbyteSyncTrigger


class TestAirbyteSyncTrigger:
    DAG_ID = "airbyte_sync_run"
    TASK_ID = "airbyte_sync_run_task_op"
    JOB_ID = 1234
    CONN_ID = "airbyte_default"
    END_TIME = time.time() + 60 * 60 * 24 * 7
    POLL_INTERVAL = 3.0

    def test_serialization(self):
        """Assert TestAirbyteSyncTrigger correctly serializes its arguments and classpath."""
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID, poll_interval=self.POLL_INTERVAL, end_time=self.END_TIME, job_id=self.JOB_ID
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger"
        assert kwargs == {
            "job_id": self.JOB_ID,
            "conn_id": self.CONN_ID,
            "end_time": self.END_TIME,
            "poll_interval": self.POLL_INTERVAL,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger.is_still_running")
    async def test_airbyte_run_sync_trigger(self, mocked_is_still_running):
        """Test AirbyteSyncTrigger is triggered with mocked details and run successfully."""
        mocked_is_still_running.return_value = True
        trigger = AirbyteSyncTrigger(
            conn_id=self.CONN_ID,
            poll_interval=self.POLL_INTERVAL,
            end_time=self.END_TIME,
            job_id=self.JOB_ID,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()


class TestAirbyteJobSensor:
    task_id = "task-id"
    airbyte_conn_id = "airbyte-conn-test"
    job_id = 1
    timeout = 120

    def get_job(self, status):
        response = mock.Mock()
        response.json.return_value = {"job": {"status": status}}
        return response

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job")
    def test_done(self, mock_get_job):
        mock_get_job.return_value = self.get_job("succeeded")

        sensor = AirbyteJobSensor(
            task_id=self.task_id,
            airbyte_job_id=self.job_id,
            airbyte_conn_id=self.airbyte_conn_id,
        )
        ret = sensor.poke(context={})
        mock_get_job.assert_called_once_with(job_id=self.job_id)
        assert ret

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job")
    def test_failed(self, mock_get_job, soft_fail: bool, expected_exception: type[AirflowException]):
        mock_get_job.return_value = self.get_job("failed")

        sensor = AirbyteJobSensor(
            task_id=self.task_id,
            airbyte_job_id=self.job_id,
            airbyte_conn_id=self.airbyte_conn_id,
            soft_fail=soft_fail,
        )
        with pytest.raises(expected_exception, match="Job failed"):
            sensor.poke(context={})

        mock_get_job.assert_called_once_with(job_id=self.job_id)

    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job")
    def test_running(self, mock_get_job):
        mock_get_job.return_value = self.get_job("running")

        sensor = AirbyteJobSensor(
            task_id=self.task_id,
            airbyte_job_id=self.job_id,
            airbyte_conn_id=self.airbyte_conn_id,
        )
        ret = sensor.poke(context={})

        mock_get_job.assert_called_once_with(job_id=self.job_id)

        assert not ret

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.airbyte.hooks.airbyte.AirbyteHook.get_job")
    def test_cancelled(self, mock_get_job, soft_fail: bool, expected_exception: type[AirflowException]):
        mock_get_job.return_value = self.get_job("cancelled")

        sensor = AirbyteJobSensor(
            task_id=self.task_id,
            airbyte_job_id=self.job_id,
            airbyte_conn_id=self.airbyte_conn_id,
            soft_fail=soft_fail,
        )
        with pytest.raises(expected_exception, match="Job was cancelled"):
            sensor.poke(context={})

        mock_get_job.assert_called_once_with(job_id=self.job_id)
