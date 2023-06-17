from airflow.providers.google.cloud.operators.cloud_batch import CloudBatchSubmitJobOperator
from google.cloud import batch_v1
from unittest import mock

CLOUD_BUILD_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_batch.CloudBatchHook"


class TestCloudBatchSubmitJobOperator:

    @mock.patch(CLOUD_BUILD_HOOK_PATH)
    def test_execute(self, mock):

        task_id = 'test'
        project_id = 'testproject'
        region = 'us-central1'
        job_name = 'test'
        job = batch_v1.Job()
        deferrable = False

        operator = CloudBatchSubmitJobOperator(
            task_id=task_id,
            project_id=project_id,
            region=region,
            job_name=job_name,
            job=job,
            deferrable=deferrable)
        operator.execute(context=mock.MagicMock())

        mock.return_value.submit_build_job.assert_called_with(job_name, job, region, project_id)
        mock.return_value.wait_for_job.assert_called()
