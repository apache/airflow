from __future__ import annotations

from unittest import mock

import pytest
from airflow.providers.google.cloud.triggers.cloud_batch import CloudBatchJobFinishedTrigger
from tests.providers.google.cloud.utils.compat import async_mock
from google.cloud import batch_v1
from airflow.triggers.base import TriggerEvent

JOB_NAME = 'jobName'
PROJECT_ID = 'projectId'
LOCATION = 'us-central1'
GCP_CONNECTION_ID = 'gcp_connection_id'
POLL_SLEEP = 0.01
TIMEOUT = 0.02
IMPERSONATION_CHAIN = 'impersonation_chain'

@pytest.fixture
def trigger():
    return CloudBatchJobFinishedTrigger(
        job_name=JOB_NAME,
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONNECTION_ID,
        polling_period_seconds=POLL_SLEEP,
        timeout=TIMEOUT,
        impersonation_chain=IMPERSONATION_CHAIN
    )


class TestCloudBatchJobFinishedTrigger:
    def test_serialization(self, trigger):
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchJobFinishedTrigger"
        assert kwargs == {
            "project_id": PROJECT_ID,
            "job_name": JOB_NAME,
            "location": LOCATION,
            "gcp_conn_id": GCP_CONNECTION_ID,
            "polling_period_seconds": POLL_SLEEP,
            "timeout" : TIMEOUT,
            "impersonation_chain": IMPERSONATION_CHAIN,
        }

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchAsyncHook")
    async def test_trigger_on_success_yield_successfully(self, mock_hook, trigger: CloudBatchJobFinishedTrigger):
        """
        Tests the CloudBuildCreateBuildTrigger fires once the job execution reaches a successful state.
        """
        state = batch_v1.JobStatus.State.SUCCEEDED
        mock_hook.return_value.get_build_job.return_value = self._mock_job_with_state(state)
        generator = trigger.run()
        actual = await generator.asend(None)
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
    @async_mock.patch("airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchAsyncHook")
    async def test_trigger_on_deleted_yield_successfully(self, mock_hook, trigger: CloudBatchJobFinishedTrigger):
        """
        Tests the CloudBuildCreateBuildTrigger fires once the job execution reaches a successful state.
        """
        state = batch_v1.JobStatus.State.DELETION_IN_PROGRESS
        mock_hook.return_value.get_build_job.return_value = self._mock_job_with_state(state)
        generator = trigger.run()
        actual = await generator.asend(None)
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
    @async_mock.patch("airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchAsyncHook")
    async def test_trigger_on_deleted_yield_exception(self, mock_hook, trigger: CloudBatchJobFinishedTrigger):
        """
        Tests the CloudBuildCreateBuildTrigger fires once the job execution reaches an state with an error message.
        """
        mock_hook.return_value.get_build_job.side_effect = Exception("Test Exception")
        generator = trigger.run()
        actual = await generator.asend(None)
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
    @async_mock.patch("airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchAsyncHook")
    async def test_trigger_timeout(self, mock_hook, trigger: CloudBatchJobFinishedTrigger):
        """
        Tests the CloudBuildCreateBuildTrigger fires once the job execution times out with an error message.
        """
        async def _mock_job(job_name):
            job = mock.MagicMock()
            job.status.state = batch_v1.JobStatus.State.RUNNING
            return job
        
        mock_hook.return_value.get_build_job = _mock_job

        generator = trigger.run()
        actual = await generator.asend(None)
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


    async def _mock_job_with_state(self, state: batch_v1.JobStatus.State):
        job: batch_v1.Job = mock.MagicMock()
        job.status.state = state
        return job
        