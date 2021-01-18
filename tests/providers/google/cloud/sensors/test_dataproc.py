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
from google.cloud.dataproc_v1beta2.types import JobStatus

from airflow import AirflowException
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
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
        job = self.create_job(JobStatus.DONE)
        job_id = "job_id"
        mock_hook.return_value.get_job.return_value = job

        sensor = DataprocJobSensor(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataproc_job_id=job_id,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        ret = sensor.poke(context={})

        mock_hook.return_value.get_job.assert_called_once_with(
            job_id=job_id, location=GCP_LOCATION, project_id=GCP_PROJECT
        )
        assert ret

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_error(self, mock_hook):
        job = self.create_job(JobStatus.ERROR)
        job_id = "job_id"
        mock_hook.return_value.get_job.return_value = job

        sensor = DataprocJobSensor(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataproc_job_id=job_id,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )

        with pytest.raises(AirflowException, match="Job failed"):
            sensor.poke(context={})

        mock_hook.return_value.get_job.assert_called_once_with(
            job_id=job_id, location=GCP_LOCATION, project_id=GCP_PROJECT
        )

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_wait(self, mock_hook):
        job = self.create_job(JobStatus.RUNNING)
        job_id = "job_id"
        mock_hook.return_value.get_job.return_value = job

        sensor = DataprocJobSensor(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataproc_job_id=job_id,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        ret = sensor.poke(context={})

        mock_hook.return_value.get_job.assert_called_once_with(
            job_id=job_id, location=GCP_LOCATION, project_id=GCP_PROJECT
        )
        assert not ret

    @mock.patch(DATAPROC_PATH.format("DataprocHook"))
    def test_cancelled(self, mock_hook):
        job = self.create_job(JobStatus.CANCELLED)
        job_id = "job_id"
        mock_hook.return_value.get_job.return_value = job

        sensor = DataprocJobSensor(
            task_id=TASK_ID,
            location=GCP_LOCATION,
            project_id=GCP_PROJECT,
            dataproc_job_id=job_id,
            gcp_conn_id=GCP_CONN_ID,
            timeout=TIMEOUT,
        )
        with pytest.raises(AirflowException, match="Job was cancelled"):
            sensor.poke(context={})

        mock_hook.return_value.get_job.assert_called_once_with(
            job_id=job_id, location=GCP_LOCATION, project_id=GCP_PROJECT
        )
