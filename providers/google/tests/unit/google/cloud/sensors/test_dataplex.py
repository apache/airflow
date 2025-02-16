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
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.dataplex_v1.types import DataScanJob

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataplex import AirflowDataQualityScanResultTimeoutException
from airflow.providers.google.cloud.sensors.dataplex import (
    DataplexDataProfileJobStatusSensor,
    DataplexDataQualityJobStatusSensor,
    DataplexTaskStateSensor,
    TaskState,
)

DATAPLEX_HOOK = "airflow.providers.google.cloud.sensors.dataplex.DataplexHook"

TASK_ID = "test-sensor"
PROJECT_ID = "project-id"
REGION = "region"
LAKE_ID = "lake-id"
BODY = {"body": "test"}
DATAPLEX_TASK_ID = "testTask001"

GCP_CONN_ID = "google_cloud_default"
API_VERSION = "v1"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
TEST_JOB_ID = "test_job_id"
TEST_DATA_SCAN_ID = "test-data-scan-id"
TIMEOUT = 120


class TestDataplexTaskStateSensor:
    def create_task(self, state: int):
        task = mock.Mock()
        task.state = state
        return task

    @mock.patch(DATAPLEX_HOOK)
    def test_done(self, mock_hook):
        task = self.create_task(TaskState.ACTIVE)
        mock_hook.return_value.get_task.return_value = task

        sensor = DataplexTaskStateSensor(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            dataplex_task_id=DATAPLEX_TASK_ID,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        result = sensor.poke(context={})

        mock_hook.return_value.get_task.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            dataplex_task_id=DATAPLEX_TASK_ID,
            retry=DEFAULT,
            metadata=(),
        )

        assert result

    @mock.patch(DATAPLEX_HOOK)
    def test_deleting(self, mock_hook):
        task = self.create_task(TaskState.DELETING)
        mock_hook.return_value.get_task.return_value = task

        sensor = DataplexTaskStateSensor(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            dataplex_task_id=DATAPLEX_TASK_ID,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        with pytest.raises(AirflowException, match="Task is going to be deleted"):
            sensor.poke(context={})

        mock_hook.return_value.get_task.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            dataplex_task_id=DATAPLEX_TASK_ID,
            retry=DEFAULT,
            metadata=(),
        )


class TestDataplexDataQualityJobStatusSensor:
    def run_job(self, state: int):
        job = mock.Mock()
        job.state = state
        return job

    @mock.patch(DATAPLEX_HOOK)
    def test_done(self, mock_hook):
        job = self.run_job(DataScanJob.State.SUCCEEDED)
        mock_hook.return_value.get_data_scan_job.return_value = job

        sensor = DataplexDataQualityJobStatusSensor(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            job_id=TEST_JOB_ID,
            data_scan_id=TEST_DATA_SCAN_ID,
            region=REGION,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT,
        )
        result = sensor.poke(context={})

        mock_hook.return_value.get_data_scan_job.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            job_id=TEST_JOB_ID,
            data_scan_id=TEST_DATA_SCAN_ID,
            timeout=TIMEOUT,
            retry=DEFAULT,
            metadata=(),
        )

        assert result

    def test_start_sensor_time(self):
        sensor = DataplexDataQualityJobStatusSensor(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            job_id=TEST_JOB_ID,
            data_scan_id=TEST_DATA_SCAN_ID,
            region=REGION,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT,
        )

        assert sensor.start_sensor_time is None

        duration_1 = sensor._duration()
        duration_2 = sensor._duration()

        assert sensor.start_sensor_time
        assert 0 < duration_1 < duration_2

    @mock.patch.object(DataplexDataQualityJobStatusSensor, "_duration")
    def test_start_sensor_time_timeout(self, mock_duration):
        result_timeout = 100
        mock_duration.return_value = result_timeout + 1

        sensor = DataplexDataQualityJobStatusSensor(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            job_id=TEST_JOB_ID,
            data_scan_id=TEST_DATA_SCAN_ID,
            region=REGION,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT,
            result_timeout=result_timeout,
        )

        with pytest.raises(AirflowDataQualityScanResultTimeoutException):
            sensor.poke(context={})


class TestDataplexDataProfileJobStatusSensor:
    def run_job(self, state: int):
        job = mock.Mock()
        job.state = state
        return job

    @mock.patch(DATAPLEX_HOOK)
    def test_done(self, mock_hook):
        job = self.run_job(DataScanJob.State.SUCCEEDED)
        mock_hook.return_value.get_data_scan_job.return_value = job

        sensor = DataplexDataProfileJobStatusSensor(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            job_id=TEST_JOB_ID,
            data_scan_id=TEST_DATA_SCAN_ID,
            region=REGION,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT,
        )
        result = sensor.poke(context={})

        mock_hook.return_value.get_data_scan_job.assert_called_once_with(
            project_id=PROJECT_ID,
            region=REGION,
            job_id=TEST_JOB_ID,
            data_scan_id=TEST_DATA_SCAN_ID,
            timeout=TIMEOUT,
            retry=DEFAULT,
            metadata=(),
        )

        assert result

    def test_start_sensor_time(self):
        sensor = DataplexDataProfileJobStatusSensor(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            job_id=TEST_JOB_ID,
            data_scan_id=TEST_DATA_SCAN_ID,
            region=REGION,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT,
        )

        assert sensor.start_sensor_time is None

        duration_1 = sensor._duration()
        duration_2 = sensor._duration()

        assert sensor.start_sensor_time
        assert 0 < duration_1 < duration_2

    @mock.patch.object(DataplexDataProfileJobStatusSensor, "_duration")
    def test_start_sensor_time_timeout(self, mock_duration):
        result_timeout = 100
        mock_duration.return_value = result_timeout + 1

        sensor = DataplexDataProfileJobStatusSensor(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            job_id=TEST_JOB_ID,
            data_scan_id=TEST_DATA_SCAN_ID,
            region=REGION,
            api_version=API_VERSION,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            timeout=TIMEOUT,
            result_timeout=result_timeout,
        )

        with pytest.raises(AirflowDataQualityScanResultTimeoutException):
            sensor.poke(context={})
