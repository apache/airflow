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
from google.api_core.exceptions import AlreadyExists
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
TASK_ID = "test"
PROJECT_ID = "testproject"
REGION = "us-central1"
JOB_NAME = "jobname"
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
            job_name=JOB_NAME, region=REGION, project_id=PROJECT_ID, job=JOB
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

    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_success(self, hook_mock):
        hook_mock.return_value.get_job.return_value = JOB
        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 3, 0)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.get_job.assert_called_once_with(
            job_name=mock.ANY, region=REGION, project_id=PROJECT_ID
        )

        hook_mock.return_value.execute_job.assert_called_once_with(
            job_name=JOB_NAME, region=REGION, project_id=PROJECT_ID, overrides=None
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
    def test_execute_deferrable_execute_complete_method_success(self, hook_mock):
        hook_mock.return_value.get_job.return_value = JOB

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, deferrable=True
        )

        event = {"status": RunJobStatus.SUCCESS.value, "job_name": JOB_NAME}

        result = operator.execute_complete(mock.MagicMock(), event)

        hook_mock.return_value.get_job.assert_called_once_with(
            job_name=mock.ANY, region=REGION, project_id=PROJECT_ID
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
            job_name=mock.ANY, region=REGION, project_id=PROJECT_ID
        )

        hook_mock.return_value.execute_job.assert_called_once_with(
            job_name=JOB_NAME, region=REGION, project_id=PROJECT_ID, overrides=overrides
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
            job_name=JOB_NAME, region=REGION, project_id=PROJECT_ID
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
            job_name=JOB_NAME, job=JOB, region=REGION, project_id=PROJECT_ID
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
            region=REGION, project_id=PROJECT_ID, limit=limit, show_deleted=show_deleted
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
        )
        hook_mock.return_value.get_service.assert_called_once_with(
            service_name=SERVICE_NAME,
            region=REGION,
            project_id=PROJECT_ID,
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
        )
