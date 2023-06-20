from __future__ import annotations

from typing import Sequence, Union

from time import sleep

from google.cloud.batch_v1 import BatchServiceClient, BatchServiceAsyncClient, JobStatus, Job, CreateJobRequest, DeleteJobRequest
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook
from airflow.exceptions import AirflowException
from google.api_core import operation  # type: ignore


class CloudBatchHook(GoogleBaseHook):
    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)
        self.client: BatchServiceClient = BatchServiceClient()

    def submit_build_job(
            self,
            job_name: str,
            job: Job,
            region: str,
            project_id: str = PROVIDE_PROJECT_ID

    ) -> Job:

        create_request = CreateJobRequest()
        create_request.job = job
        create_request.job_id = job_name
        create_request.parent = f"projects/{project_id}/locations/{region}"

        return self.client.create_job(create_request)
    

    def delete_job(
            self,
            job_name: str,
            region: str,
            project_id: str = PROVIDE_PROJECT_ID

    ) -> operation.Operation:
        return self.client.delete_job(name=f"projects/{project_id}/locations/{region}/jobs/{job_name}")
       

    def wait_for_job(
            self,
            job_name: str,
            polling_period_seconds: float = 10,
            timeout: Union[float, None] = None
    ) -> Job:
        
        while timeout == None or timeout > 0:
            try:
                job = self.client.get_job(name=f"{job_name}")
                status: JobStatus.State = job.status.state
                if status == JobStatus.State.SUCCEEDED \
                or status == JobStatus.State.FAILED \
                or status == JobStatus.State.DELETION_IN_PROGRESS:
                    return job
                else:
                    sleep(polling_period_seconds)
            except Exception as e:
                self.log.exception(
                    "Exception occurred while checking for job completion.")
                raise e
            
            if timeout != None:
                timeout -= polling_period_seconds
        
        raise AirflowException(f"Job with name [{job_name}] timed out")

    def get_job(self, job_name) -> Job:
        return self.client.get_job(name=f"{job_name}")


class CloudBatchAsyncHook(GoogleBaseHook):

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )

        self._client: BatchServiceAsyncClient = BatchServiceAsyncClient()
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)

    async def get_build_job(
        self,
        job_name: str,
    ) -> Job:
        return await self._client.get_job(name=f"{job_name}")

