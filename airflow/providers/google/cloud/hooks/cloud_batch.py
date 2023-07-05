from __future__ import annotations

from typing import Sequence, Union, Iterable, Optional

from time import sleep

from google.cloud.batch_v1.services.batch_service import pagers
from google.cloud.batch_v1 import BatchServiceClient, BatchServiceAsyncClient, JobStatus, Job, CreateJobRequest, Task
from google.cloud.batch import ListJobsRequest, ListTasksRequest
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
        self._client: BatchServiceClient | None = None

    def get_conn(self) -> BatchServiceClient:
        """
        Retrieves connection to GCE Batch.
        :return: BatchServiceClient
        """
        if self._client == None:
            self._client = BatchServiceClient()
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
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

        return self.get_conn().create_job(create_request)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_job(
            self,
            job_name: str,
            region: str,
            project_id: str = PROVIDE_PROJECT_ID

    ) -> operation.Operation:
        return self.get_conn().delete_job(name=f"projects/{project_id}/locations/{region}/jobs/{job_name}")

    @GoogleBaseHook.fallback_to_default_project_id
    def list_jobs(
            self,
            region: str,
            project_id: str = PROVIDE_PROJECT_ID,
            filter: Optional[str] = None,
            limit: Optional[int] = None) -> Iterable[Job]:

        if limit is not None and limit < 0:
            raise AirflowException(
                "The limit for the list jobs request should be greater or equal to zero")

        list_jobs_request: ListJobsRequest = ListJobsRequest(
            parent=f"projects/{project_id}/locations/{region}",
            filter=filter
        )

        jobs: pagers.ListJobsPager = self.get_conn().list_jobs(
            request=list_jobs_request)

        return self._limit_list(jobs, limit)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_tasks(
            self,
            region: str,
            job_name: str,
            project_id: str = PROVIDE_PROJECT_ID,
            group_name: str = 'group0',
            filter: Optional[str] = None,
            limit: Optional[int] = None) -> Iterable[Task]:

        if limit is not None and limit < 0:
            raise AirflowException(
                "The limit for the list tasks request should be greater or equal to zero")

        list_tasks_request: ListTasksRequest = ListTasksRequest(
            parent=f"projects/{project_id}/locations/{region}/jobs/{job_name}/taskGroups/{group_name}",
            filter=filter
        )

        tasks: pagers.ListTasksPager = self.get_conn().list_tasks(
            request=list_tasks_request)

        return self._limit_list(tasks, limit)

    def wait_for_job(
            self,
            job_name: str,
            polling_period_seconds: float = 10,
            timeout: Union[float, None] = None
    ) -> Job:
        client = self.get_conn()
        while timeout == None or timeout > 0:
            try:
                job = client.get_job(name=f"{job_name}")
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
        return self.get_conn().get_job(name=f"{job_name}")

    def _limit_list(self, jobs, limit):
        result = []
        for item in jobs:
            if limit is not None and limit == 0:
                break
            result.append(item)
            if limit is not None:
                limit -= 1
        return result


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

    def get_conn(self):
        if self._client == None:
            self._client = BatchServiceAsyncClient()

        return self._client

    async def get_build_job(
        self,
        job_name: str,
    ) -> Job:
        client = self.get_conn()
        return await client.get_job(name=f"{job_name}")
