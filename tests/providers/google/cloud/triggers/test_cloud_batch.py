from __future__ import annotations

import asyncio
import logging
from asyncio import Future
from unittest import mock

import pytest
from airflow.providers.google.cloud.triggers.cloud_batch import CloudBatchJobFinishedTrigger
from tests.providers.google.cloud.utils.compat import async_mock
from google.cloud import batch_v1
from airflow.triggers.base import BaseTrigger, TriggerEvent

JOB_NAME = 'jobName'
PROJECT_ID = 'projectId'
LOCATION = 'us-central1'
GCP_CONNECTION_ID = 'gcp_connection_id'
POLL_SLEEP = 11
IMPERSONATION_CHAIN = 'impersonation_chain'

@pytest.fixture
def trigger():
    return CloudBatchJobFinishedTrigger(
        job_name=JOB_NAME,
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONNECTION_ID,
        poll_sleep=POLL_SLEEP,
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
            "poll_sleep": POLL_SLEEP,
            "impersonation_chain": IMPERSONATION_CHAIN,
        }

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchAsyncHook")
    async def test_trigger_on_success_yield_successfully(self, mock_hook, trigger: CloudBatchJobFinishedTrigger):
        """
        Tests the CloudBuildCreateBuildTrigger only fires once the job execution reaches a successful state.
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
        Tests the CloudBuildCreateBuildTrigger only fires once the job execution reaches a successful state.
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
        Tests the CloudBuildCreateBuildTrigger only fires once the job execution reaches a successful state.
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
    async def test_trigger_running(self, mock_hook, trigger: CloudBatchJobFinishedTrigger):
        state = batch_v1.JobStatus.State.RUNNING
        mock_hook.return_value.get_build_job.return_value = self._mock_job_with_state(state)

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False

        # assert "Build is still running..." in caplog.text
        # assert f"Sleeping for {TEST_POLL_INTERVAL} seconds." in caplog.text

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()


    async def _mock_job_with_state(self, state: batch_v1.JobStatus.State):
        job: batch_v1.Job = mock.MagicMock()
        job.status.state = state
        return job
        