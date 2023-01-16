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
from unittest.mock import Mock

import pytest
from google.api_core.exceptions import ServerError
from google.cloud.dataproc_v1.types import Batch, JobStatus

from airflow import AirflowException
from airflow.providers.google.cloud.sensors.dataproc import DataprocBatchSensor, DataprocJobSensor
from airflow.version import version as airflow_version

AIRFLOW_VERSION = "v" + airflow_version.replace(".", "-").replace("+", "-")

DATAPROC_PATH = "airflow.providers.google.cloud.sensors.dataproc.{}"

TASK_ID = "task-id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
TIMEOUT = 120


class TestDataprocJobSensor(unittest.TestCase):
    def create_job(self, state: int):
        job = mock.Mock()
        job.status = mock.Mock()
        job.status.state = state
        return job

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_done(self, mock_hook):
        job = self.create_job(JobStatus.State.DONE)
        job_id = "job_id"
        mock_hook.return_value.get_job.return_value = job

        sensor = DataprocJobSensor(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataproc_job_id=job_id,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        ret = sensor.poke(context={})

        mock_hook.return_value.get_job.assert_called_once_with(
            job_id=job_id, region=GCP_LOCATION, project_id=GCP_PROJECT
        )
        assert ret

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_error(self, mock_hook):
        job = self.create_job(JobStatus.State.ERROR)
        job_id = "job_id"
        mock_hook.return_value.get_job.return_value = job

        sensor = DataprocJobSensor(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataproc_job_id=job_id,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )

        with pytest.raises(AirflowException, match="Job failed"):
            sensor.poke(context={})

        mock_hook.return_value.get_job.assert_called_once_with(
            job_id=job_id, region=GCP_LOCATION, project_id=GCP_PROJECT
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_wait(self, mock_hook):
        job = self.create_job(JobStatus.State.RUNNING)
        job_id = "job_id"
        mock_hook.return_value.get_job.return_value = job

        sensor = DataprocJobSensor(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataproc_job_id=job_id,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        ret = sensor.poke(context={})

        mock_hook.return_value.get_job.assert_called_once_with(
            job_id=job_id, region=GCP_LOCATION, project_id=GCP_PROJECT
        )
        assert not ret

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_cancelled(self, mock_hook):
        job = self.create_job(JobStatus.State.CANCELLED)
        job_id = "job_id"
        mock_hook.return_value.get_job.return_value = job

        sensor = DataprocJobSensor(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataproc_job_id=job_id,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        with pytest.raises(AirflowException, match="Job was cancelled"):
            sensor.poke(context={})

        mock_hook.return_value.get_job.assert_called_once_with(
            job_id=job_id, region=GCP_LOCATION, project_id=GCP_PROJECT
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_missing_region(self, mock_hook):
        with pytest.raises(AirflowException):
            sensor = DataprocJobSensor(
                task_id=TASK_ID,
                project_id=GCP_PROJECT,
                dataproc_job_id="job_id",
                gcp_conn_id=GCP_CONN_ID,
                timeout=TIMEOUT,
            )
            sensor.poke(context={})

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_wait_timeout(self, mock_hook):
        job_id = "job_id"
        mock_hook.return_value.get_job.side_effect = ServerError("Job are not ready")

        sensor = DataprocJobSensor(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataproc_job_id=job_id,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
            wait_timeout=300,
        )

        sensor._duration = Mock()
        sensor._duration.return_value = 200

        result = sensor.poke(context={})
        assert not result

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_wait_timeout_raise_exception(self, mock_hook):
        job_id = "job_id"
        mock_hook.return_value.get_job.side_effect = ServerError("Job are not ready")

        sensor = DataprocJobSensor(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataproc_job_id=job_id,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
            wait_timeout=300,
        )

        sensor._duration = Mock()
        sensor._duration.return_value = 301

        with pytest.raises(AirflowException, match="Timeout: dataproc job job_id is not ready after 300s"):
            sensor.poke(context={})


class TestDataprocBatchSensor(unittest.TestCase):
    def create_batch(self, state: int):
        batch = mock.Mock()
        batch.state = mock.Mock()
        batch.state = state
        return batch

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_succeeded(self, mock_hook):
        batch = self.create_batch(Batch.State.SUCCEEDED)
        mock_hook.return_value.get_batch.return_value = batch

        sensor = DataprocBatchSensor(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            batch_id="batch_id",
            poke_interval=10,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        ret = sensor.poke(context={})
        mock_hook.return_value.get_batch.assert_called_once_with(
            batch_id="batch_id", region=GCP_LOCATION, project_id=GCP_PROJECT
        )
        assert ret

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_cancelled(self, mock_hook):
        batch = self.create_batch(Batch.State.CANCELLED)
        mock_hook.return_value.get_batch.return_value = batch

        sensor = DataprocBatchSensor(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            batch_id="batch_id",
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        with pytest.raises(AirflowException, match="Batch was cancelled."):
            sensor.poke(context={})

        mock_hook.return_value.get_batch.assert_called_once_with(
            batch_id="batch_id", region=GCP_LOCATION, project_id=GCP_PROJECT
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_error(self, mock_hook):
        batch = self.create_batch(Batch.State.FAILED)
        mock_hook.return_value.get_batch.return_value = batch

        sensor = DataprocBatchSensor(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT,
            batch_id="batch_id",
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )

        with pytest.raises(AirflowException, match="Batch failed"):
            sensor.poke(context={})

        mock_hook.return_value.get_batch.assert_called_once_with(
            batch_id="batch_id", region=GCP_LOCATION, project_id=GCP_PROJECT
        )
