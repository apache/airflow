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
from google.cloud.run_v2 import Job

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunDeleteJobOperator,
    CloudRunExecuteJobOperator,
    CloudRunListJobsOperator,
    CloudRunUpdateJobOperator,
)

CLOUD_RUN_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_run.CloudRunHook"
TASK_ID = "test"
PROJECT_ID = "testproject"
REGION = "us-central1"
JOB_NAME = "jobname"
JOB = Job()
JOB.name = JOB_NAME


class TestCloudRunExecuteJobOperator:
    @mock.patch(CLOUD_RUN_HOOK_PATH)
    def test_execute_success(self, hook_mock):

        hook_mock.return_value.execute_job.return_value = self._mock_operation(3, 3, 0)

        operator = CloudRunExecuteJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.execute_job.assert_called_once_with(
            job_name=JOB_NAME, region=REGION, project_id=PROJECT_ID
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
