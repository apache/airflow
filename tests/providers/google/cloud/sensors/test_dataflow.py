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
from unittest import mock

from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor

TEST_TASK_ID = "tesk-id"
TEST_JOB_ID = "test_job_id"
TEST_PROJECT_ID = "test_project"
TEST_LOCATION = "us-central1"
TEST_DELEGATE_TO = "test_delegate_to"
TEST_GCP_CONN_ID = 'test_gcp_conn_id'
TEST_IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestDataflowJobStatusSensor(unittest.TestCase):
    @parameterized.expand(
        [
            (DataflowJobStatus.JOB_STATE_DONE, DataflowJobStatus.JOB_STATE_DONE, True),
            (DataflowJobStatus.JOB_STATE_DONE, DataflowJobStatus.JOB_STATE_RUNNING, False),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke(self, exptected_status, current_status, sensor_return, mock_hook):
        mock_get_job = mock_hook.return_value.get_job
        task = DataflowJobStatusSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            expected_statuses=exptected_status,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": current_status}
        results = task.poke(mock.MagicMock())

        self.assertEqual(sensor_return, results)

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.assert_called_once_with(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )

    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke_raise_exception(self, mock_hook):
        mock_get_job = mock_hook.return_value.get_job
        task = DataflowJobStatusSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            expected_statuses=DataflowJobStatus.JOB_STATE_RUNNING,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": DataflowJobStatus.JOB_STATE_CANCELLED}

        with self.assertRaisesRegex(
            AirflowException,
            f"Job with id '{TEST_JOB_ID}' is already in terminal state: "
            f"{DataflowJobStatus.JOB_STATE_CANCELLED}",
        ):
            task.poke(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            delegate_to=TEST_DELEGATE_TO,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.assert_called_once_with(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
