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
"""
This module contains various unit tests for GCP Cloud Build Operators
"""

from __future__ import annotations

from unittest import mock

import pytest
from google.api_core.exceptions import Aborted, AlreadyExists, PermissionDenied
from google.cloud.exceptions import GoogleCloudError
from google.cloud.run_v2 import Job, Service

from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunCreateJobOperator,
    CloudRunCreateServiceOperator,
    CloudRunDeleteJobOperator,
    CloudRunDeleteServiceOperator,
    CloudRunExecuteJobOperator,
    CloudRunListJobsOperator,
    CloudRunUpdateJobOperator,
)
from airflow.providers.google.cloud.triggers.cloud_run import RunJobStatus

CLOUD_RUN_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_run.CloudRunHook"
CLOUD_RUN_SERVICE_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_run.CloudRunServiceHook"
GCP_LOGGING_PATH = "airflow.providers.google.cloud.operators.cloud_run.gcp_logging"
TASK_ID = "test"
PROJECT_ID = "testproject"
REGION = "us-central1"
JOB_NAME = "jobname"
EXECUTION_NAME = "jobname-abc12"
EXECUTION_FULL_NAME = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}/executions/{EXECUTION_NAME}"
SERVICE_NAME = "servicename"
OVERRIDES = {
    "container_overrides": [{"args": ["python", "main.py"]}],
    "task_count": 1,
    "timeout": "60s",
}

JOB = Job()
JOB.name = JOB_NAME

SERVICE = Service()
SERVICE.name = SERVICE_NAME


def _assert_common_template_fields(template_fields):
    assert "project_id" in template_fields
    assert "region" in template_fields
    assert "gcp_conn_id" in template_fields
    assert "impersonation_chain" in template_fields


class TestCloudRunCreateJobOperator:
    def test_template_fields(self):
        operator = CloudRunCreateJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB
        )

        _assert_common_template_fields(operator.template_fields)
        assert "job_name" in operator.template_fields

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_create(self, hook_mock):
        hook_mock.return_value.create_job.return_value = JOB

        operator = CloudRunCreateJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.create_job.assert_called_once_with(
            job_name=JOB_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            job=JOB,
            use_regional_endpoint=False,
        )


class TestCloudRunExecuteJobOperator:
    def test_template_fields(self):
        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, overrides=OVERRIDES
        )

        _assert_common_template_fields(operator.template_fields)
        assert "job_name" in operator.template_fields
        assert "overrides" in operator.template_fields
        assert "polling_period_seconds" in operator.template_fields
        assert "timeout_seconds" in operator.template_fields
        assert "transport" in operator.template_fields

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_with_transport(self, hook_mock):
        """Test that transport parameter is passed to CloudRunHook."""
        hook_mock.return_value.get_job.return_value = JOB
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 3, 0)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            transport="rest",
        )

        operator.execute(context=mock.MagicMock())

        # Verify that CloudRunHook was instantiated with transport parameter
        hook_mock.assert_called_once()
        call_kwargs = hook_mock.call_args[1]
        assert call_kwargs["transport"] == "rest"

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_success(self, hook_mock):
        hook_mock.return_value.get_job.return_value = JOB
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 3, 0)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.get_job.assert_called_once_with(
            job_name=mock.ANY,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )

        hook_mock.return_value.execute_job.assert_called_once_with(
            job_name=JOB_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            overrides=None,
            use_regional_endpoint=False,
        )

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_fail_one_failed_task(self, hook_mock):
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 2, 1)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        with pytest.raises(AirflowException) as exception:
            operator.execute(context=mock.MagicMock())

        assert "Some tasks failed execution" in str(exception.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_fail_all_failed_tasks(self, hook_mock):
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 0, 3)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        with pytest.raises(AirflowException) as exception:
            operator.execute(context=mock.MagicMock())

        assert "Some tasks failed execution" in str(exception.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_fail_incomplete_failed_tasks(self, hook_mock):
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 2, 0)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        with pytest.raises(AirflowException) as exception:
            operator.execute(context=mock.MagicMock())

        assert "Not all tasks finished execution" in str(exception.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_fail_incomplete_succeeded_tasks(self, hook_mock):
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 0, 2)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        with pytest.raises(AirflowException) as exception:
            operator.execute(context=mock.MagicMock())

        assert "Not all tasks finished execution" in str(exception.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_deferrable(self, hook_mock):
        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, deferrable=True
        )

        with pytest.raises(TaskDeferred):
            operator.execute(mock.MagicMock())

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_deferrable_execute_complete_method_timeout(self, hook_mock):
        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, deferrable=True
        )

        event = {"status": RunJobStatus.TIMEOUT.value, "job_name": JOB_NAME}

        with pytest.raises(AirflowException) as e:
            operator.execute_complete(mock.MagicMock(), event)

        assert "Operation timed out" in str(e.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_deferrable_execute_complete_method_fail(self, hook_mock):
        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, deferrable=True
        )

        error_code = 10
        error_message = "error message"

        event = {
            "status": RunJobStatus.FAIL.value,
            "operation_error_code": error_code,
            "operation_error_message": error_message,
            "job_name": JOB_NAME,
        }

        with pytest.raises(AirflowException) as e:
            operator.execute_complete(mock.MagicMock(), event)

        assert f"Operation failed with error code [{error_code}] and error message [{error_message}]" in str(
            e.value
        )

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_deferrable_execute_complete_method_fail_on_cancellation(self, hook_mock):
        """
        Pin the contract that a FAIL event emitted by the trigger when a Cloud Run Job is
        cancelled (no ``operation.error`` but ``cancelled_count > 0``) propagates as an
        AirflowException — see #57791.
        """
        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, deferrable=True
        )

        event = {
            "status": RunJobStatus.FAIL.value,
            "operation_error_code": None,
            "operation_error_message": (
                "Cloud Run Job did not finish all tasks: task_count=3, succeeded_count=1, "
                "failed_count=0, cancelled_count=2."
            ),
            "job_name": JOB_NAME,
        }

        with pytest.raises(AirflowException) as e:
            operator.execute_complete(mock.MagicMock(), event)

        assert "cancelled_count=2" in str(e.value)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_deferrable_execute_complete_method_success(self, hook_mock):
        hook_mock.return_value.get_job.return_value = JOB

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, deferrable=True
        )

        event = {"status": RunJobStatus.SUCCESS.value, "job_name": JOB_NAME}

        result = operator.execute_complete(mock.MagicMock(), event)

        hook_mock.return_value.get_job.assert_called_once_with(
            job_name=mock.ANY,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )
        assert result["name"] == JOB_NAME

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_overrides(self, hook_mock):
        hook_mock.return_value.get_job.return_value = JOB
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 3, 0)

        overrides = {
            "container_overrides": [{"args": ["python", "main.py"]}],
            "task_count": 1,
            "timeout": "60s",
        }

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, overrides=overrides
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.get_job.assert_called_once_with(
            job_name=mock.ANY,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )

        hook_mock.return_value.execute_job.assert_called_once_with(
            job_name=JOB_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            overrides=overrides,
            use_regional_endpoint=False,
        )

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_overrides_with_invalid_task_count(self, hook_mock):
        overrides = {
            "container_overrides": [{"args": ["python", "main.py"]}],
            "task_count": -1,
            "timeout": "60s",
        }

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, overrides=overrides
        )

        with pytest.raises(AirflowException):
            operator.execute(context=mock.MagicMock())

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_overrides_with_invalid_timeout(self, hook_mock):
        overrides = {
            "container_overrides": [{"args": ["python", "main.py"]}],
            "task_count": 1,
            "timeout": "60",
        }

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, overrides=overrides
        )

        with pytest.raises(AirflowException):
            operator.execute(context=mock.MagicMock())

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_overrides_with_invalid_container_args(self, hook_mock):
        overrides = {
            "container_overrides": [{"name": "job", "args": "python main.py"}],
            "task_count": 1,
            "timeout": "60s",
        }

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, overrides=overrides
        )

        with pytest.raises(AirflowException):
            operator.execute(context=mock.MagicMock())

    @mock.patch(GCP_LOGGING_PATH)
    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_verbose_false_does_not_call_logging_client(self, hook_mock, gcp_logging_mock):
        """Default behaviour (``verbose=False``) must not touch Cloud Logging."""
        hook_mock.return_value.get_job.return_value = JOB
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 3, 0)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )
        operator.execute(context=mock.MagicMock())

        gcp_logging_mock.Client.assert_not_called()

    @mock.patch(GCP_LOGGING_PATH)
    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_verbose_true_forwards_container_logs(self, hook_mock, gcp_logging_mock):
        """``verbose=True`` fetches Cloud Logging entries for the execution and logs each one."""
        execution = self._mock_execution(3, 3, 0)
        execution.name = EXECUTION_FULL_NAME
        execution.job = JOB_NAME
        operation = mock.MagicMock()
        operation.result.return_value = execution
        hook_mock.return_value.execute_job.return_value = operation
        hook_mock.return_value.get_job.return_value = JOB

        gcp_logging_mock.Client.return_value.list_entries.return_value = [
            mock.MagicMock(payload="Starting Task #0, Attempt #0 ..."),
            mock.MagicMock(payload="Completed Task #0, Attempt #0"),
            mock.MagicMock(payload="Container called exit(0)."),
        ]

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            verbose=True,
        )
        operator._cached_logger = operator._log = mock.MagicMock()
        operator.execute(context=mock.MagicMock())

        hook_mock.assert_called_once()
        gcp_logging_mock.Client.assert_called_once_with(
            project=PROJECT_ID,
            credentials=hook_mock.return_value.get_credentials.return_value,
        )
        log_filter = gcp_logging_mock.Client.return_value.list_entries.call_args.kwargs["filter_"]
        assert f'resource.labels.job_name="{JOB_NAME}"' in log_filter
        assert f'"run.googleapis.com/execution_name"="{EXECUTION_NAME}"' in log_filter
        # Audit logs (logName starting with cloudaudit.googleapis.com/...) live under the same
        # cloud_run_job resource and otherwise pollute the Airflow task log with structured
        # AuditLog dumps. The filter must exclude them at the API level.
        assert 'NOT logName:"cloudaudit.googleapis.com"' in log_filter
        operator._cached_logger.info.assert_has_calls(
            [
                mock.call("Starting Task #0, Attempt #0 ..."),
                mock.call("Completed Task #0, Attempt #0"),
                mock.call("Container called exit(0)."),
            ]
        )

    @mock.patch(GCP_LOGGING_PATH)
    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_verbose_true_forwards_structured_log_payload(self, hook_mock, gcp_logging_mock):
        """Cloud Logging entries can carry a non-string (JSON / proto) payload. Those must be
        forwarded via ``log.info("%s", payload)`` so the structured value still reaches the
        Airflow task log instead of being passed as a non-string ``msg`` to the stdlib logger.
        """
        execution = self._mock_execution(3, 3, 0)
        execution.name = EXECUTION_FULL_NAME
        execution.job = JOB_NAME
        operation = mock.MagicMock()
        operation.result.return_value = execution
        hook_mock.return_value.execute_job.return_value = operation
        hook_mock.return_value.get_job.return_value = JOB

        structured_payload = {"severity": "ERROR", "message": "boom", "code": 42}
        gcp_logging_mock.Client.return_value.list_entries.return_value = [
            mock.MagicMock(payload="Starting Task #0, Attempt #0 ..."),
            mock.MagicMock(payload=structured_payload),
        ]

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            verbose=True,
        )
        operator._cached_logger = operator._log = mock.MagicMock()
        operator.execute(context=mock.MagicMock())

        operator._cached_logger.info.assert_has_calls(
            [
                mock.call("Starting Task #0, Attempt #0 ..."),
                mock.call("%s", structured_payload),
            ]
        )

    @mock.patch(GCP_LOGGING_PATH)
    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_verbose_true_logging_api_failure_warns_and_succeeds(self, hook_mock, gcp_logging_mock):
        """A Cloud Logging API failure during log forwarding must not fail the task."""
        execution = self._mock_execution(3, 3, 0)
        execution.name = EXECUTION_FULL_NAME
        execution.job = JOB_NAME
        operation = mock.MagicMock()
        operation.result.return_value = execution
        hook_mock.return_value.execute_job.return_value = operation
        hook_mock.return_value.get_job.return_value = JOB

        gcp_logging_mock.Client.return_value.list_entries.side_effect = PermissionDenied(
            "Missing roles/logging.viewer"
        )

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            verbose=True,
        )
        operator._cached_logger = operator._log = mock.MagicMock()
        result = operator.execute(context=mock.MagicMock())

        assert result["name"] == JOB.name
        hook_mock.assert_called_once()
        operator._cached_logger.warning.assert_called_once()

    @mock.patch(GCP_LOGGING_PATH)
    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_complete_verbose_true_forwards_container_logs(self, hook_mock, gcp_logging_mock):
        """Deferrable path: ``execute_complete`` fetches and forwards container logs too."""
        hook_mock.return_value.get_job.return_value = JOB
        gcp_logging_mock.Client.return_value.list_entries.return_value = [
            mock.MagicMock(payload="Starting Task #0, Attempt #0 ..."),
            mock.MagicMock(payload="Completed Task #0, Attempt #0"),
        ]

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            deferrable=True,
            verbose=True,
        )
        operator._cached_logger = operator._log = mock.MagicMock()

        event = {
            "status": RunJobStatus.SUCCESS.value,
            "job_name": JOB_NAME,
            "execution_name": EXECUTION_NAME,
        }
        operator.execute_complete(mock.MagicMock(), event)

        hook_mock.assert_called_once()
        log_filter = gcp_logging_mock.Client.return_value.list_entries.call_args.kwargs["filter_"]
        assert f'"run.googleapis.com/execution_name"="{EXECUTION_NAME}"' in log_filter
        assert 'NOT logName:"cloudaudit.googleapis.com"' in log_filter
        operator._cached_logger.info.assert_has_calls(
            [
                mock.call("Starting Task #0, Attempt #0 ..."),
                mock.call("Completed Task #0, Attempt #0"),
            ]
        )

    @mock.patch(GCP_LOGGING_PATH)
    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_verbose_true_with_failed_execution_still_forwards_logs(
        self, hook_mock, gcp_logging_mock
    ):
        """
        When the execution has failed tasks (``failed_count > 0``), ``verbose=True`` MUST
        still forward the container logs into the Airflow task log BEFORE the operator
        raises the failure. Debugging a failed job is the case the user needs the logs the
        most — see issue #36963.
        """
        execution = self._mock_execution(task_count=1, succeeded_count=0, failed_count=1)
        execution.name = EXECUTION_FULL_NAME
        execution.job = JOB_NAME
        operation = mock.MagicMock()
        operation.result.return_value = execution
        hook_mock.return_value.execute_job.return_value = operation

        gcp_logging_mock.Client.return_value.list_entries.return_value = [
            mock.MagicMock(payload="2026/05/18 00:00:00 Starting Task #0, Attempt #0 ..."),
            mock.MagicMock(payload="Task failed with exit code 1"),
        ]

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            verbose=True,
        )
        operator._cached_logger = operator._log = mock.MagicMock()

        with (
            mock.patch("airflow.providers.google.cloud.operators.cloud_run.time.sleep"),
            pytest.raises(AirflowException, match="Some tasks failed execution"),
        ):
            operator.execute(context=mock.MagicMock())

        hook_mock.assert_called_once()
        operator._cached_logger.info.assert_has_calls(
            [
                mock.call("2026/05/18 00:00:00 Starting Task #0, Attempt #0 ..."),
                mock.call("Task failed with exit code 1"),
            ]
        )

    @mock.patch(GCP_LOGGING_PATH)
    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_verbose_true_with_operation_exception_still_forwards_logs(
        self, hook_mock, gcp_logging_mock
    ):
        """
        Some Cloud Run job failures make ``operation.result()`` raise before returning
        an Execution. ``verbose=True`` must still use the operation metadata to forward
        container logs before raising the AirflowException.
        """
        operation = mock.MagicMock()
        operation.metadata.name = EXECUTION_FULL_NAME
        operation.metadata.log_uri = None
        operation.result.side_effect = Aborted("The container exited with an error.")
        operation.exception.return_value = Aborted("The container exited with an error.")
        hook_mock.return_value.execute_job.return_value = operation

        gcp_logging_mock.Client.return_value.list_entries.return_value = [
            mock.MagicMock(payload="Starting failing task"),
            mock.MagicMock(payload="Task failed badly"),
            mock.MagicMock(payload="Container called exit(1)."),
        ]

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            verbose=True,
        )
        operator._cached_logger = operator._log = mock.MagicMock()

        with (
            mock.patch("airflow.providers.google.cloud.operators.cloud_run.time.sleep"),
            pytest.raises(AirflowException, match="The container exited with an error"),
        ):
            operator.execute(context=mock.MagicMock())

        hook_mock.assert_called_once()
        log_filter = gcp_logging_mock.Client.return_value.list_entries.call_args.kwargs["filter_"]
        assert f'"run.googleapis.com/execution_name"="{EXECUTION_NAME}"' in log_filter
        operator._cached_logger.info.assert_has_calls(
            [
                mock.call("Starting failing task"),
                mock.call("Task failed badly"),
                mock.call("Container called exit(1)."),
            ]
        )

    @mock.patch(GCP_LOGGING_PATH)
    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_verbose_true_with_operation_exception_waits_for_late_logs(
        self, hook_mock, gcp_logging_mock
    ):
        """
        Cloud Logging can expose entries shortly after the Cloud Run operation has
        already failed. ``verbose=True`` waits briefly before one fetch so stdout/stderr
        that arrive late are less likely to be missed.
        """
        operation = mock.MagicMock()
        operation.metadata.name = EXECUTION_FULL_NAME
        operation.metadata.log_uri = None
        operation.result.side_effect = Aborted("The container exited with an error.")
        operation.exception.return_value = Aborted("The container exited with an error.")
        hook_mock.return_value.execute_job.return_value = operation

        gcp_logging_mock.Client.return_value.list_entries.return_value = [
            mock.MagicMock(payload="Starting failing task"),
            mock.MagicMock(payload="Task failed badly"),
            mock.MagicMock(payload="Container called exit(1)."),
        ]

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            verbose=True,
        )
        operator._cached_logger = operator._log = mock.MagicMock()

        with (
            mock.patch("airflow.providers.google.cloud.operators.cloud_run.time.sleep") as sleep_mock,
            pytest.raises(AirflowException, match="The container exited with an error"),
        ):
            operator.execute(context=mock.MagicMock())

        sleep_mock.assert_called_once_with(1)
        hook_mock.assert_called_once()
        gcp_logging_mock.Client.return_value.list_entries.assert_called_once()
        operator._cached_logger.info.assert_has_calls(
            [
                mock.call("Starting failing task"),
                mock.call("Task failed badly"),
                mock.call("Container called exit(1)."),
            ]
        )
        assert operator._cached_logger.info.call_count == 3

    @mock.patch(GCP_LOGGING_PATH)
    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_complete_verbose_true_fail_event_still_forwards_logs(self, hook_mock, gcp_logging_mock):
        """
        Deferrable path: when the trigger reports a FAIL event with an ``execution_name``,
        ``verbose=True`` MUST still forward the container logs before re-raising as
        AirflowException. See issue #36963.
        """
        gcp_logging_mock.Client.return_value.list_entries.return_value = [
            mock.MagicMock(payload="2026/05/18 00:00:00 Starting Task #0, Attempt #0 ..."),
            mock.MagicMock(payload="Task failed with exit code 1"),
        ]

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            deferrable=True,
            verbose=True,
        )
        operator._cached_logger = operator._log = mock.MagicMock()

        event = {
            "status": RunJobStatus.FAIL.value,
            "operation_error_code": 13,
            "operation_error_message": "operation error",
            "job_name": JOB_NAME,
            "execution_name": EXECUTION_NAME,
        }

        with (
            mock.patch("airflow.providers.google.cloud.operators.cloud_run.time.sleep"),
            pytest.raises(AirflowException, match="Operation failed"),
        ):
            operator.execute_complete(mock.MagicMock(), event)

        hook_mock.assert_called_once()
        operator._cached_logger.info.assert_has_calls(
            [
                mock.call("2026/05/18 00:00:00 Starting Task #0, Attempt #0 ..."),
                mock.call("Task failed with exit code 1"),
            ]
        )

    def _mock_operation(self, task_count, succeeded_count, failed_count):
        operation = mock.MagicMock()
        operation.result.return_value = self._mock_execution(task_count, succeeded_count, failed_count)
        return operation

    def _mock_execution(self, task_count, succeeded_count, failed_count):
        execution = mock.MagicMock()
        execution.task_count = task_count
        execution.succeeded_count = succeeded_count
        execution.failed_count = failed_count
        return execution


class TestCloudRunDeleteJobOperator:
    def test_template_fields(self):
        operator = CloudRunDeleteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        _assert_common_template_fields(operator.template_fields)
        assert "job_name" in operator.template_fields

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute(self, hook_mock):
        hook_mock.return_value.delete_job.return_value = JOB

        operator = CloudRunDeleteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        deleted_job = operator.execute(context=mock.MagicMock())

        assert deleted_job["name"] == JOB.name

        hook_mock.return_value.delete_job.assert_called_once_with(
            job_name=JOB_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )


class TestCloudRunUpdateJobOperator:
    def test_template_fields(self):
        operator = CloudRunUpdateJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB
        )

        _assert_common_template_fields(operator.template_fields)
        assert "job_name" in operator.template_fields

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute(self, hook_mock):
        hook_mock.return_value.update_job.return_value = JOB

        operator = CloudRunUpdateJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB
        )

        updated_job = operator.execute(context=mock.MagicMock())

        assert updated_job["name"] == JOB.name

        hook_mock.return_value.update_job.assert_called_once_with(
            job_name=JOB_NAME,
            job=JOB,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )


class TestCloudRunListJobsOperator:
    def test_template_fields(self):
        operator = CloudRunListJobsOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, limit=2, show_deleted=False
        )

        _assert_common_template_fields(operator.template_fields)

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute(self, hook_mock):
        limit = 2
        show_deleted = True
        operator = CloudRunListJobsOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, limit=limit, show_deleted=show_deleted
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.list_jobs.assert_called_once_with(
            region=REGION,
            project_id=PROJECT_ID,
            limit=limit,
            show_deleted=show_deleted,
            use_regional_endpoint=False,
        )

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_with_invalid_limit(self, hook_mock):
        limit = -1
        with pytest.raises(expected_exception=AirflowException):
            CloudRunListJobsOperator(task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, limit=limit)


class TestCloudRunCreateServiceOperator:
    def test_template_fields(self):
        operator = CloudRunCreateServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service=SERVICE,
            service_name=SERVICE_NAME,
        )

        _assert_common_template_fields(operator.template_fields)
        assert "service_name" in operator.template_fields

    @mock.patch(CLOUD_RUN_SERVICE_HOOK_PATH)
    def test_execute(self, hook_mock):
        hook_mock.return_value.create_service.return_value = SERVICE

        operator = CloudRunCreateServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service=SERVICE,
            service_name=SERVICE_NAME,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.create_service.assert_called_once_with(
            service=SERVICE,
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )

    @mock.patch(CLOUD_RUN_SERVICE_HOOK_PATH)
    def test_execute_already_exists(self, hook_mock):
        hook_mock.return_value.create_service.side_effect = AlreadyExists("Service already exists")
        hook_mock.return_value.get_service.return_value = SERVICE

        operator = CloudRunCreateServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service=SERVICE,
            service_name=SERVICE_NAME,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.create_service.assert_called_once_with(
            service=SERVICE,
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )
        hook_mock.return_value.get_service.assert_called_once_with(
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )

    @mock.patch(CLOUD_RUN_SERVICE_HOOK_PATH)
    def test_execute_when_other_error(self, hook_mock):
        error_message = "An error occurred. Exiting."
        hook_mock.return_value.create_service.side_effect = GoogleCloudError(error_message, errors=None)

        operator = CloudRunCreateServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service=SERVICE,
            service_name=SERVICE_NAME,
        )

        with pytest.raises(expected_exception=GoogleCloudError) as context:
            operator.execute(context=mock.MagicMock())

        assert error_message == context.value.message

        hook_mock.return_value.create_service.assert_called_once_with(
            service=SERVICE,
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )


class TestCloudRunDeleteServiceOperator:
    def test_template_fields(self):
        operator = CloudRunDeleteServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service_name=SERVICE_NAME,
        )

        _assert_common_template_fields(operator.template_fields)
        assert "service_name" in operator.template_fields

    @mock.patch(CLOUD_RUN_SERVICE_HOOK_PATH)
    def test_execute(self, hook_mock):
        hook_mock.return_value.delete_service.return_value = SERVICE

        operator = CloudRunDeleteServiceOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            service_name=SERVICE_NAME,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.delete_service.assert_called_once_with(
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
            use_regional_endpoint=False,
        )
