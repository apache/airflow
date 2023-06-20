from airflow.providers.google.cloud.operators.cloud_batch import CloudBatchSubmitJobOperator, CloudBatchDeleteJobOperator
from airflow.exceptions import TaskDeferred, AirflowException
from google.cloud import batch_v1
from unittest import mock
import pytest

CLOUD_BUILD_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_batch.CloudBatchHook"
TASK_ID = 'test'
PROJECT_ID = 'testproject'
REGION = 'us-central1'
JOB_NAME = 'test'
JOB = batch_v1.Job()
JOB.name = JOB_NAME


class TestCloudBatchSubmitJobOperator:

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_execute(self, mock):
        mock.return_value.wait_for_job.return_value = JOB
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            job=JOB)
        
        completed_job = operator.execute(context=mock.MagicMock())

        assert isinstance(completed_job, batch_v1.Job)
        assert completed_job.name == JOB_NAME

        mock.return_value.submit_build_job.assert_called_with(
            JOB_NAME, JOB, REGION, PROJECT_ID)
        mock.return_value.wait_for_job.assert_called()

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_execute_deferrable(self, mock):
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            job=JOB,
            deferrable=True)

        with pytest.raises(expected_exception=TaskDeferred):
            operator.execute(context=mock.MagicMock())

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_execute_complete(self, mock):
        mock.return_value.get_job.return_value = JOB
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            job=JOB,
            deferrable=True)

        event = {"status": "success",
                 "job_name": JOB_NAME, "message": "test error"}
        completed_job = operator.execute_complete(context=mock.MagicMock(), event=event)

        assert isinstance(completed_job, batch_v1.Job)
        assert completed_job.name == JOB_NAME

        mock.return_value.get_job.assert_called_once_with(job_name=JOB_NAME)

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_execute_complete_exception(self, mock):
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            job=JOB,
            deferrable=True)

        event = {"status": "error", "job_name": JOB_NAME,
                 "message": "test error"}
        with pytest.raises(expected_exception=AirflowException, match="Unexpected error in the operation: test error"):
            operator.execute_complete(context=mock.MagicMock(), event=event)


class TestCloudBatchDeleteJobOperator:

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
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
            JOB_NAME, REGION, PROJECT_ID)
        delete_operation_mock.result.assert_called_once()

    def _delete_operation_mock(self):
        operation = mock.MagicMock()
        operation.result.return_value = mock.MagicMock()
        return operation
