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
from google.cloud import batch_v1

from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchDeleteJobOperator,
    CloudBatchListJobsOperator,
    CloudBatchListTasksOperator,
    CloudBatchSubmitJobOperator,
)

CLOUD_BATCH_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_batch.CloudBatchHook"
TASK_ID = "test"
PROJECT_ID = "testproject"
REGION = "us-central1"
JOB_NAME = "test"
JOB = batch_v1.Job()
JOB.name = JOB_NAME


class TestCloudBatchSubmitJobOperator:
    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute(self, mock):
        mock.return_value.wait_for_job.return_value = JOB
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB
        )

        completed_job = operator.execute(context=mock.MagicMock())

        assert completed_job["name"] == JOB_NAME

        mock.return_value.submit_batch_job.assert_called_with(
            job_name=JOB_NAME, job=JOB, region=REGION, project_id=PROJECT_ID
        )
        mock.return_value.wait_for_job.assert_called()

    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute_deferrable(self, mock):
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB, deferrable=True
        )

        with pytest.raises(expected_exception=TaskDeferred):
            operator.execute(context=mock.MagicMock())

    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute_complete(self, mock):
        mock.return_value.get_job.return_value = JOB
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB, deferrable=True
        )

        event = {"status": "success", "job_name": JOB_NAME, "message": "test error"}
        completed_job = operator.execute_complete(context=mock.MagicMock(), event=event)

        assert completed_job["name"] == JOB_NAME

        mock.return_value.get_job.assert_called_once_with(job_name=JOB_NAME)

    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute_complete_exception(self, mock):
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB, deferrable=True
        )

        event = {"status": "error", "job_name": JOB_NAME, "message": "test error"}
        with pytest.raises(
            expected_exception=AirflowException, match="Unexpected error in the operation: test error"
        ):
            operator.execute_complete(context=mock.MagicMock(), event=event)


class TestCloudBatchDeleteJobOperator:
    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute(self, hook_mock):
        delete_operation_mock = self._delete_operation_mock()
        hook_mock.return_value.delete_job.return_value = delete_operation_mock

        operator = CloudBatchDeleteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.delete_job.assert_called_once_with(
            job_name=JOB_NAME, region=REGION, project_id=PROJECT_ID
        )
        delete_operation_mock.result.assert_called_once()

    def _delete_operation_mock(self):
        operation = mock.MagicMock()
        operation.result.return_value = mock.MagicMock()
        return operation


class TestCloudBatchListJobsOperator:
    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute(self, hook_mock):
        filter = "filter_description"
        limit = 2
        operator = CloudBatchListJobsOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, filter=filter, limit=limit
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.list_jobs.assert_called_once_with(
            region=REGION, project_id=PROJECT_ID, filter=filter, limit=limit
        )

    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute_with_invalid_limit(self, hook_mock):
        filter = "filter_description"
        limit = -1

        with pytest.raises(expected_exception=AirflowException):
            CloudBatchListJobsOperator(
                task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, filter=filter, limit=limit
            )


class TestCloudBatchListTasksOperator:
    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute(self, hook_mock):
        filter = "filter_description"
        limit = 2
        job_name = "test_job"

        operator = CloudBatchListTasksOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=job_name,
            filter=filter,
            limit=limit,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.list_tasks.assert_called_once_with(
            region=REGION,
            project_id=PROJECT_ID,
            filter=filter,
            job_name=job_name,
            limit=limit,
            group_name="group0",
        )

    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute_with_invalid_limit(self, hook_mock):
        filter = "filter_description"
        limit = -1
        job_name = "test_job"

        with pytest.raises(expected_exception=AirflowException):
            CloudBatchListTasksOperator(
                task_id=TASK_ID,
                project_id=PROJECT_ID,
                region=REGION,
                job_name=job_name,
                filter=filter,
                limit=limit,
            )
