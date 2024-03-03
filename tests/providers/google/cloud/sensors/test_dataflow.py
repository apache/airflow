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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobMetricsSensor,
    DataflowJobStatusSensor,
)

TEST_TASK_ID = "task_id"
TEST_JOB_ID = "test_job_id"
TEST_PROJECT_ID = "test_project"
TEST_LOCATION = "us-central1"
TEST_GCP_CONN_ID = "test_gcp_conn_id"
TEST_IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestDataflowJobStatusSensor:
    @pytest.mark.parametrize(
        "expected_status, current_status, sensor_return",
        [
            (DataflowJobStatus.JOB_STATE_DONE, DataflowJobStatus.JOB_STATE_DONE, True),
            (DataflowJobStatus.JOB_STATE_DONE, DataflowJobStatus.JOB_STATE_RUNNING, False),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke(self, mock_hook, expected_status, current_status, sensor_return):
        mock_get_job = mock_hook.return_value.get_job
        task = DataflowJobStatusSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            expected_statuses=expected_status,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": current_status}
        results = task.poke(mock.MagicMock())

        assert sensor_return == results

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.assert_called_once_with(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke_raise_exception(self, mock_hook, soft_fail, expected_exception):
        mock_get_job = mock_hook.return_value.get_job
        task = DataflowJobStatusSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            expected_statuses=DataflowJobStatus.JOB_STATE_RUNNING,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            soft_fail=soft_fail,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": DataflowJobStatus.JOB_STATE_CANCELLED}

        with pytest.raises(
            expected_exception,
            match=f"Job with id '{TEST_JOB_ID}' is already in terminal state: "
            f"{DataflowJobStatus.JOB_STATE_CANCELLED}",
        ):
            task.poke(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.assert_called_once_with(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )


class TestDataflowJobMetricsSensor:
    @pytest.mark.parametrize(
        "job_current_state, fail_on_terminal_state",
        [
            (DataflowJobStatus.JOB_STATE_RUNNING, True),
            (DataflowJobStatus.JOB_STATE_RUNNING, False),
            (DataflowJobStatus.JOB_STATE_DONE, False),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke(self, mock_hook, job_current_state, fail_on_terminal_state):
        mock_get_job = mock_hook.return_value.get_job
        mock_fetch_job_metrics_by_id = mock_hook.return_value.fetch_job_metrics_by_id
        callback = mock.MagicMock()

        task = DataflowJobMetricsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=callback,
            fail_on_terminal_state=fail_on_terminal_state,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": job_current_state}
        results = task.poke(mock.MagicMock())

        assert callback.return_value == results

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_fetch_job_metrics_by_id.assert_called_once_with(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        mock_fetch_job_metrics_by_id.return_value.__getitem__.assert_called_once_with("metrics")
        callback.assert_called_once_with(mock_fetch_job_metrics_by_id.return_value.__getitem__.return_value)


class DataflowJobMessagesSensorTest:
    @pytest.mark.parametrize(
        "job_current_state, fail_on_terminal_state",
        [
            (DataflowJobStatus.JOB_STATE_RUNNING, True),
            (DataflowJobStatus.JOB_STATE_RUNNING, False),
            (DataflowJobStatus.JOB_STATE_DONE, False),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke(self, mock_hook, job_current_state, fail_on_terminal_state):
        mock_get_job = mock_hook.return_value.get_job
        mock_fetch_job_messages_by_id = mock_hook.return_value.fetch_job_messages_by_id
        callback = mock.MagicMock()

        task = DataflowJobMessagesSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=callback,
            fail_on_terminal_state=fail_on_terminal_state,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": job_current_state}

        results = task.poke(mock.MagicMock())

        assert callback.return_value == results

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_fetch_job_messages_by_id.assert_called_once_with(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        callback.assert_called_once_with(mock_fetch_job_messages_by_id.return_value)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke_raise_exception(self, mock_hook, soft_fail, expected_exception):
        mock_get_job = mock_hook.return_value.get_job
        mock_fetch_job_messages_by_id = mock_hook.return_value.fetch_job_messages_by_id
        callback = mock.MagicMock()

        task = DataflowJobMessagesSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=callback,
            fail_on_terminal_state=True,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            soft_fail=soft_fail,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": DataflowJobStatus.JOB_STATE_DONE}

        with pytest.raises(
            expected_exception,
            match=f"Job with id '{TEST_JOB_ID}' is already in terminal state: "
            f"{DataflowJobStatus.JOB_STATE_DONE}",
        ):
            task.poke(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_fetch_job_messages_by_id.assert_not_called()
        callback.assert_not_called()


class DataflowJobAutoScalingEventsSensorTest:
    @pytest.mark.parametrize(
        "job_current_state, fail_on_terminal_state",
        [
            (DataflowJobStatus.JOB_STATE_RUNNING, True),
            (DataflowJobStatus.JOB_STATE_RUNNING, False),
            (DataflowJobStatus.JOB_STATE_DONE, False),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke(self, mock_hook, job_current_state, fail_on_terminal_state):
        mock_get_job = mock_hook.return_value.get_job
        mock_fetch_job_autoscaling_events_by_id = mock_hook.return_value.fetch_job_autoscaling_events_by_id
        callback = mock.MagicMock()

        task = DataflowJobAutoScalingEventsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=callback,
            fail_on_terminal_state=fail_on_terminal_state,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": job_current_state}

        results = task.poke(mock.MagicMock())

        assert callback.return_value == results

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_fetch_job_autoscaling_events_by_id.assert_called_once_with(
            job_id=TEST_JOB_ID, project_id=TEST_PROJECT_ID, location=TEST_LOCATION
        )
        callback.assert_called_once_with(mock_fetch_job_autoscaling_events_by_id.return_value)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke_raise_exception_on_terminal_state(self, mock_hook, soft_fail, expected_exception):
        mock_get_job = mock_hook.return_value.get_job
        mock_fetch_job_autoscaling_events_by_id = mock_hook.return_value.fetch_job_autoscaling_events_by_id
        callback = mock.MagicMock()

        task = DataflowJobAutoScalingEventsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=callback,
            fail_on_terminal_state=True,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            soft_fail=soft_fail,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": DataflowJobStatus.JOB_STATE_DONE}

        with pytest.raises(
            expected_exception,
            match=f"Job with id '{TEST_JOB_ID}' is already in terminal state: "
            f"{DataflowJobStatus.JOB_STATE_DONE}",
        ):
            task.poke(mock.MagicMock())

        mock_hook.assert_called_once_with(
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_fetch_job_autoscaling_events_by_id.assert_not_called()
        callback.assert_not_called()
