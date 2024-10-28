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
from google.cloud.batch_v1 import Job, JobStatus

from airflow.providers.google.cloud.triggers.cloud_batch import (
    CloudBatchJobFinishedTrigger,
)
from airflow.triggers.base import TriggerEvent

JOB_NAME = "jobName"
PROJECT_ID = "projectId"
LOCATION = "us-central1"
GCP_CONNECTION_ID = "gcp_connection_id"
POLL_SLEEP = 0.01
TIMEOUT = 0.02
IMPERSONATION_CHAIN = "impersonation_chain"


@pytest.fixture
def trigger():
    return CloudBatchJobFinishedTrigger(
        job_name=JOB_NAME,
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONNECTION_ID,
        polling_period_seconds=POLL_SLEEP,
        timeout=TIMEOUT,
        impersonation_chain=IMPERSONATION_CHAIN,
    )


class TestCloudBatchJobFinishedTrigger:
    def test_serialization(self, trigger):
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == "airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchJobFinishedTrigger"
        )
        assert kwargs == {
            "project_id": PROJECT_ID,
            "job_name": JOB_NAME,
            "location": LOCATION,
            "gcp_conn_id": GCP_CONNECTION_ID,
            "polling_period_seconds": POLL_SLEEP,
            "timeout": TIMEOUT,
            "impersonation_chain": IMPERSONATION_CHAIN,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchAsyncHook")
    async def test_trigger_on_success_yield_successfully(
        self, mock_hook, trigger: CloudBatchJobFinishedTrigger
    ):
        """
        Tests the CloudBatchJobFinishedTrigger fires once the job execution reaches a successful state.
        """
        state = JobStatus.State(JobStatus.State.SUCCEEDED)
        mock_hook.return_value.get_batch_job.return_value = self._mock_job_with_state(
            state
        )
        generator = trigger.run()
        actual = await generator.asend(None)  # type:ignore[attr-defined]
        assert (
            TriggerEvent(
                {
                    "job_name": JOB_NAME,
                    "status": "success",
                    "message": "Job completed",
                }
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchAsyncHook")
    async def test_trigger_on_deleted_yield_successfully(
        self, mock_hook, trigger: CloudBatchJobFinishedTrigger
    ):
        """
        Tests the CloudBatchJobFinishedTrigger fires once the job execution reaches a successful state.
        """
        state = JobStatus.State(JobStatus.State.DELETION_IN_PROGRESS)
        mock_hook.return_value.get_batch_job.return_value = self._mock_job_with_state(
            state
        )
        generator = trigger.run()
        actual = await generator.asend(None)  # type:ignore[attr-defined]
        assert (
            TriggerEvent(
                {
                    "job_name": JOB_NAME,
                    "status": "deleted",
                    "message": f"Batch job with name {JOB_NAME} is being deleted",
                }
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchAsyncHook")
    async def test_trigger_on_deleted_yield_exception(
        self, mock_hook, trigger: CloudBatchJobFinishedTrigger
    ):
        """
        Tests the CloudBatchJobFinishedTrigger fires once the job execution
        reaches an state with an error message.
        """
        mock_hook.return_value.get_batch_job.side_effect = Exception("Test Exception")
        generator = trigger.run()
        actual = await generator.asend(None)  # type:ignore[attr-defined]
        assert (
            TriggerEvent(
                {
                    "status": "error",
                    "message": "Test Exception",
                }
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchAsyncHook")
    async def test_trigger_timeout(
        self, mock_hook, trigger: CloudBatchJobFinishedTrigger
    ):
        """
        Tests the CloudBatchJobFinishedTrigger fires once the job execution times out with an error message.
        """

        async def _mock_job(job_name):
            job = mock.MagicMock()
            job.status.state = JobStatus.State.RUNNING
            return job

        mock_hook.return_value.get_batch_job = _mock_job

        generator = trigger.run()
        actual = await generator.asend(None)  # type:ignore[attr-defined]
        assert (
            TriggerEvent(
                {
                    "job_name": JOB_NAME,
                    "status": "timed out",
                    "message": f"Batch job with name {JOB_NAME} timed out",
                }
            )
            == actual
        )

    async def _mock_job_with_state(self, state: JobStatus.State):
        job: Job = mock.MagicMock()
        job.status.state = state
        return job
