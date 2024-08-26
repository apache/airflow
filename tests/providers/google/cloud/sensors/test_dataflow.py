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

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobAutoScalingEventsSensor,
    DataflowJobMessagesSensor,
    DataflowJobMetricsSensor,
    DataflowJobStatusSensor,
)
from airflow.providers.google.cloud.triggers.dataflow import (
    DataflowJobAutoScalingEventTrigger,
    DataflowJobMessagesTrigger,
    DataflowJobMetricsTrigger,
    DataflowJobStatusTrigger,
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
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": DataflowJobStatus.JOB_STATE_CANCELLED}

        with pytest.raises(
            AirflowException,
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

    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowJobStatusSensor.poke")
    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook")
    def test_execute_enters_deferred_state(self, mock_hook, mock_poke):
        """
        Tests that DataflowJobStatusTrigger will be fired when the DataflowJobStatusSensor
        is executed and deferrable is set to True.
        """
        task = DataflowJobStatusSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            expected_statuses=DataflowJobStatus.JOB_STATE_DONE,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        mock_hook.return_value.exists.return_value = False
        mock_poke.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            task.execute(None)
        assert isinstance(
            exc.value.trigger, DataflowJobStatusTrigger
        ), "Trigger is not a DataflowJobStatusTrigger"

    def test_execute_complete_success(self):
        """Tests that the trigger event contains expected values if no callback function is provided."""
        expected_result = True
        task = DataflowJobStatusSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            expected_statuses=DataflowJobStatus.JOB_STATE_DONE,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        actual_message = task.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": f"Job with id '{TEST_JOB_ID}' has reached an expected state: {DataflowJobStatus.JOB_STATE_DONE}",
            },
        )
        assert actual_message == expected_result

    def test_execute_complete_not_success_status_raises_exception(self):
        """Tests that AirflowException or AirflowSkipException is raised if the trigger event contains an error."""
        task = DataflowJobStatusSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            location=TEST_LOCATION,
            expected_statuses=DataflowJobStatus.JOB_STATE_DONE,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context=None, event={"status": "error", "message": "test error message"})


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

    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke_raise_exception(self, mock_hook):
        mock_get_job = mock_hook.return_value.get_job
        mock_fetch_job_messages_by_id = mock_hook.return_value.fetch_job_messages_by_id
        callback = mock.MagicMock()

        task = DataflowJobMetricsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=callback,
            fail_on_terminal_state=True,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": DataflowJobStatus.JOB_STATE_DONE}

        with pytest.raises(
            AirflowException,
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

    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook")
    def test_execute_enters_deferred_state(self, mock_hook):
        """
        Tests that DataflowJobMetricsTrigger will be fired when the DataflowJobMetricsSensor
        is executed and deferrable is set to True.
        """
        task = DataflowJobMetricsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
            callback=None,
        )
        mock_hook.return_value.exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            task.execute(None)
        assert isinstance(
            exc.value.trigger, DataflowJobMetricsTrigger
        ), "Trigger is not a DataflowJobMetricsTrigger"

    def test_execute_complete_success_without_callback_function(self):
        """Tests that the trigger event contains expected values if no callback function is provided."""
        expected_result = []
        task = DataflowJobMetricsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
            callback=None,
        )
        actual_message = task.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": f"Detected 2 metrics for job '{TEST_JOB_ID}'",
                "result": [],
            },
        )
        assert actual_message == expected_result

    def test_execute_complete_success_with_callback_function(self):
        """Tests that the trigger event contains expected values if the callback function is provided."""
        expected_result = [
            {
                "name": {"origin": "", "name": "", "context": {}},
                "scalar": 0.0,
                "update_time": "2024-03-20T12:36:05.229Z",
                "kind": "",
                "cumulative": False,
            },
            {
                "name": {"origin": "", "name": "", "context": {}},
                "scalar": 0.0,
                "update_time": "2024-03-20T12:36:05.229Z",
                "kind": "",
                "cumulative": False,
            },
        ]
        task = DataflowJobMetricsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=lambda res: res,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        actual_result = task.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": f"Detected 2 job messages for job '{TEST_JOB_ID}'",
                "result": expected_result,
            },
        )
        assert actual_result == expected_result

    def test_execute_complete_not_success_status_raises_exception(self):
        """Tests that AirflowException or AirflowSkipException is raised if the trigger event contains an error."""
        task = DataflowJobMetricsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=None,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context=None, event={"status": "error", "message": "test error message", "result": None}
            )


class TestDataflowJobMessagesSensor:
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

    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke_raise_exception(self, mock_hook):
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
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": DataflowJobStatus.JOB_STATE_DONE}

        with pytest.raises(
            AirflowException,
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

    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook")
    def test_execute_enters_deferred_state(self, mock_hook):
        """
        Tests that DataflowJobMessagesTrigger will be fired when the DataflowJobMessagesSensor
        is executed and deferrable is set to True.
        """
        task = DataflowJobMessagesSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
            callback=None,
        )
        mock_hook.return_value.exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            task.execute(None)
        assert isinstance(
            exc.value.trigger, DataflowJobMessagesTrigger
        ), "Trigger is not a DataflowJobMessagesTrigger"

    def test_execute_complete_success_without_callback_function(self):
        """Tests that the trigger event contains expected values if no callback function is provided."""
        expected_result = []
        task = DataflowJobMessagesSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
            callback=None,
        )
        actual_message = task.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": f"Detected 2 job messages for job '{TEST_JOB_ID}'",
                "result": [],
            },
        )
        assert actual_message == expected_result

    def test_execute_complete_success_with_callback_function(self):
        """Tests that the trigger event contains expected values if the callback function is provided."""
        expected_result = [
            {
                "id": "1707695235850",
                "time": "2024-02-06T23:47:15.850Z",
                "message_text": "msg.",
                "message_importance": 5,
            },
            {
                "id": "1707695635401",
                "time": "2024-02-06T23:53:55.401Z",
                "message_text": "msg.",
                "message_importance": 5,
            },
        ]
        task = DataflowJobMessagesSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=lambda res: res,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        actual_result = task.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": f"Detected 2 job messages for job '{TEST_JOB_ID}'",
                "result": expected_result,
            },
        )
        assert actual_result == expected_result

    def test_execute_complete_not_success_status_raises_exception(self):
        """Tests that AirflowException or AirflowSkipException is raised if the trigger event contains an error."""
        task = DataflowJobMessagesSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=None,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context=None, event={"status": "error", "message": "test error message", "result": None}
            )


class TestDataflowJobAutoScalingEventsSensor:
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

    @mock.patch("airflow.providers.google.cloud.sensors.dataflow.DataflowHook")
    def test_poke_raise_exception_on_terminal_state(self, mock_hook):
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
        )
        mock_get_job.return_value = {"id": TEST_JOB_ID, "currentState": DataflowJobStatus.JOB_STATE_DONE}

        with pytest.raises(
            AirflowException,
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

    @mock.patch("airflow.providers.google.cloud.hooks.dataflow.AsyncDataflowHook")
    def test_execute_enters_deferred_state(self, mock_hook):
        """
        Tests that AutoScalingEventTrigger will be fired when the DataflowJobAutoScalingEventSensor
        is executed and deferrable is set to True.
        """
        task = DataflowJobAutoScalingEventsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
            callback=None,
        )
        mock_hook.return_value.exists.return_value = False
        with pytest.raises(TaskDeferred) as exc:
            task.execute(None)
        assert isinstance(
            exc.value.trigger, DataflowJobAutoScalingEventTrigger
        ), "Trigger is not a DataflowJobAutoScalingEventTrigger"

    def test_execute_complete_success_without_callback_function(self):
        """Tests that the trigger event contains expected values if no callback function is provided."""
        expected_result = []
        task = DataflowJobAutoScalingEventsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
            callback=None,
        )
        actual_message = task.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": f"Detected 2 autoscaling events for job '{TEST_JOB_ID}'",
                "result": [],
            },
        )
        assert actual_message == expected_result

    def test_execute_complete_success_with_callback_function(self):
        """Tests that the trigger event contains expected values if the callback function is provided."""
        expected_result = [
            {
                "event_type": 2,
                "description": {},
                "time": "2024-02-05T13:43:31.066611771Z",
            },
            {
                "event_type": 1,
                "description": {},
                "time": "2024-02-05T13:43:31.066611771Z",
            },
        ]
        task = DataflowJobAutoScalingEventsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=lambda res: res,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        actual_result = task.execute_complete(
            context=None,
            event={
                "status": "success",
                "message": f"Detected 2 autoscaling events for job '{TEST_JOB_ID}'",
                "result": expected_result,
            },
        )
        assert actual_result == expected_result

    def test_execute_complete_not_success_status_raises_exception(self):
        """Tests that AirflowException or AirflowSkipException is raised if the trigger event contains an error."""
        task = DataflowJobAutoScalingEventsSensor(
            task_id=TEST_TASK_ID,
            job_id=TEST_JOB_ID,
            callback=None,
            fail_on_terminal_state=False,
            location=TEST_LOCATION,
            project_id=TEST_PROJECT_ID,
            gcp_conn_id=TEST_GCP_CONN_ID,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context=None,
                event={"status": "error", "message": "test error message", "result": None},
            )
