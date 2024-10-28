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

import itertools
import json
import time
from typing import TYPE_CHECKING, Iterable, Sequence

from google.cloud.batch import ListJobsRequest, ListTasksRequest
from google.cloud.batch_v1 import (
    BatchServiceAsyncClient,
    BatchServiceClient,
    CreateJobRequest,
    Job,
    JobStatus,
    Task,
)

from airflow.exceptions import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseHook,
)

if TYPE_CHECKING:
    from google.api_core import operation
    from google.cloud.batch_v1.services.batch_service import pagers


class CloudBatchHook(GoogleBaseHook):
    """
    Hook for the Google Cloud Batch service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)
        self._client: BatchServiceClient | None = None

    def get_conn(self):
        """
        Retrieve connection to GCE Batch.

        :return: Google Batch Service client object.
        """
        if self._client is None:
            self._client = BatchServiceClient(
                credentials=self.get_credentials(), client_info=CLIENT_INFO
            )
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def submit_batch_job(
        self, job_name: str, job: Job, region: str, project_id: str = PROVIDE_PROJECT_ID
    ) -> Job:
        if isinstance(job, dict):
            job = Job.from_json(json.dumps(job))

        create_request = CreateJobRequest()
        create_request.job = job
        create_request.job_id = job_name
        create_request.parent = f"projects/{project_id}/locations/{region}"

        return self.get_conn().create_job(create_request)

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_job(
        self, job_name: str, region: str, project_id: str = PROVIDE_PROJECT_ID
    ) -> operation.Operation:
        return self.get_conn().delete_job(
            name=f"projects/{project_id}/locations/{region}/jobs/{job_name}"
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def list_jobs(
        self,
        region: str,
        project_id: str = PROVIDE_PROJECT_ID,
        filter: str | None = None,
        limit: int | None = None,
    ) -> Iterable[Job]:
        if limit is not None and limit < 0:
            raise AirflowException(
                "The limit for the list jobs request should be greater or equal to zero"
            )

        list_jobs_request: ListJobsRequest = ListJobsRequest(
            parent=f"projects/{project_id}/locations/{region}", filter=filter
        )

        jobs: pagers.ListJobsPager = self.get_conn().list_jobs(request=list_jobs_request)

        return list(itertools.islice(jobs, limit))

    @GoogleBaseHook.fallback_to_default_project_id
    def list_tasks(
        self,
        region: str,
        job_name: str,
        project_id: str = PROVIDE_PROJECT_ID,
        group_name: str = "group0",
        filter: str | None = None,
        limit: int | None = None,
    ) -> Iterable[Task]:
        if limit is not None and limit < 0:
            raise AirflowException(
                "The limit for the list tasks request should be greater or equal to zero"
            )

        list_tasks_request: ListTasksRequest = ListTasksRequest(
            parent=f"projects/{project_id}/locations/{region}/jobs/{job_name}/taskGroups/{group_name}",
            filter=filter,
        )

        tasks: pagers.ListTasksPager = self.get_conn().list_tasks(
            request=list_tasks_request
        )

        return list(itertools.islice(tasks, limit))

    def wait_for_job(
        self,
        job_name: str,
        polling_period_seconds: float = 10,
        timeout: float | None = None,
    ) -> Job:
        client = self.get_conn()
        while timeout is None or timeout > 0:
            try:
                job = client.get_job(name=f"{job_name}")
                status: JobStatus.State = job.status.state
                if (
                    status == JobStatus.State.SUCCEEDED
                    or status == JobStatus.State.FAILED
                    or status == JobStatus.State.DELETION_IN_PROGRESS
                ):
                    return job
                else:
                    time.sleep(polling_period_seconds)
            except Exception as e:
                self.log.exception(
                    "Exception occurred while checking for job completion."
                )
                raise e

            if timeout is not None:
                timeout -= polling_period_seconds

        raise AirflowException(f"Job with name [{job_name}] timed out")

    def get_job(self, job_name) -> Job:
        return self.get_conn().get_job(name=job_name)


class CloudBatchAsyncHook(GoogleBaseHook):
    """
    Async hook for the Google Cloud Batch service.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        self._client: BatchServiceAsyncClient | None = None
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain)

    def get_conn(self):
        if self._client is None:
            self._client = BatchServiceAsyncClient(
                credentials=self.get_credentials(), client_info=CLIENT_INFO
            )

        return self._client

    async def get_batch_job(
        self,
        job_name: str,
    ) -> Job:
        client = self.get_conn()
        return await client.get_job(name=f"{job_name}")
