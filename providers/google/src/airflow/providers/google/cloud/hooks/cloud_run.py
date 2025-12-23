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
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

from google.cloud.run_v2 import (
    CreateJobRequest,
    CreateServiceRequest,
    DeleteJobRequest,
    DeleteServiceRequest,
    GetJobRequest,
    GetServiceRequest,
    Job,
    JobsAsyncClient,
    JobsClient,
    ListJobsRequest,
    RunJobRequest,
    Service,
    ServicesAsyncClient,
    ServicesClient,
    UpdateJobRequest,
)
from google.longrunning import operations_pb2

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.consts import CLIENT_INFO
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID,
    GoogleBaseAsyncHook,
    GoogleBaseHook,
)

if TYPE_CHECKING:
    from google.api_core import operation
    from google.api_core.operation_async import AsyncOperation
    from google.cloud.run_v2.services.jobs import pagers


class CloudRunHook(GoogleBaseHook):
    """
    Hook for the Google Cloud Run service.

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
        **kwargs,
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)
        self._client: JobsClient | None = None

    def get_conn(self):
        """
        Retrieve connection to Cloud Run.

        :return: Cloud Run Jobs client object.
        """
        if self._client is None:
            self._client = JobsClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_job(self, job_name: str, region: str, project_id: str = PROVIDE_PROJECT_ID) -> Job:
        delete_request = DeleteJobRequest()
        delete_request.name = f"projects/{project_id}/locations/{region}/jobs/{job_name}"

        operation = self.get_conn().delete_job(delete_request)
        return operation.result()

    @GoogleBaseHook.fallback_to_default_project_id
    def create_job(
        self, job_name: str, job: Job | dict, region: str, project_id: str = PROVIDE_PROJECT_ID
    ) -> Job:
        if isinstance(job, dict):
            job = Job(job)

        create_request = CreateJobRequest()
        create_request.job = job
        create_request.job_id = job_name
        create_request.parent = f"projects/{project_id}/locations/{region}"

        operation = self.get_conn().create_job(create_request)
        return operation.result()

    @GoogleBaseHook.fallback_to_default_project_id
    def update_job(
        self, job_name: str, job: Job | dict, region: str, project_id: str = PROVIDE_PROJECT_ID
    ) -> Job:
        if isinstance(job, dict):
            job = Job(job)

        update_request = UpdateJobRequest()
        job.name = f"projects/{project_id}/locations/{region}/jobs/{job_name}"
        update_request.job = job
        operation = self.get_conn().update_job(update_request)
        return operation.result()

    @GoogleBaseHook.fallback_to_default_project_id
    def execute_job(
        self,
        job_name: str,
        region: str,
        project_id: str = PROVIDE_PROJECT_ID,
        overrides: dict[str, Any] | None = None,
    ) -> operation.Operation:
        run_job_request = RunJobRequest(
            name=f"projects/{project_id}/locations/{region}/jobs/{job_name}", overrides=overrides
        )
        operation = self.get_conn().run_job(request=run_job_request)
        return operation

    @GoogleBaseHook.fallback_to_default_project_id
    def get_job(self, job_name: str, region: str, project_id: str = PROVIDE_PROJECT_ID):
        get_job_request = GetJobRequest(name=f"projects/{project_id}/locations/{region}/jobs/{job_name}")
        return self.get_conn().get_job(get_job_request)

    @GoogleBaseHook.fallback_to_default_project_id
    def list_jobs(
        self,
        region: str,
        project_id: str = PROVIDE_PROJECT_ID,
        show_deleted: bool = False,
        limit: int | None = None,
    ) -> Iterable[Job]:
        if limit is not None and limit < 0:
            raise AirflowException("The limit for the list jobs request should be greater or equal to zero")

        list_jobs_request: ListJobsRequest = ListJobsRequest(
            parent=f"projects/{project_id}/locations/{region}", show_deleted=show_deleted
        )

        jobs: pagers.ListJobsPager = self.get_conn().list_jobs(request=list_jobs_request)

        return list(itertools.islice(jobs, limit))


class CloudRunAsyncHook(GoogleBaseAsyncHook):
    """
    Async hook for the Google Cloud Run service.

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

    sync_hook_class = CloudRunHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        self._client: JobsAsyncClient | None = None
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)

    async def get_conn(self):
        if self._client is None:
            sync_hook = await self.get_sync_hook()
            self._client = JobsAsyncClient(credentials=sync_hook.get_credentials(), client_info=CLIENT_INFO)

        return self._client

    async def get_operation(self, operation_name: str) -> operations_pb2.Operation:
        conn = await self.get_conn()
        return await conn.get_operation(operations_pb2.GetOperationRequest(name=operation_name), timeout=120)


class CloudRunServiceHook(GoogleBaseHook):
    """
    Hook for the Google Cloud Run services.

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
        **kwargs,
    ):
        self._client: ServicesClient | None = None
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)

    def get_conn(self):
        if self._client is None:
            self._client = ServicesClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)

        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def get_service(self, service_name: str, region: str, project_id: str = PROVIDE_PROJECT_ID):
        get_service_request = GetServiceRequest(
            name=f"projects/{project_id}/locations/{region}/services/{service_name}"
        )
        return self.get_conn().get_service(get_service_request)

    @GoogleBaseHook.fallback_to_default_project_id
    def create_service(
        self, service_name: str, service: Service | dict, region: str, project_id: str = PROVIDE_PROJECT_ID
    ) -> Service:
        if isinstance(service, dict):
            service = Service(service)

        create_request = CreateServiceRequest(
            parent=f"projects/{project_id}/locations/{region}",
            service=service,
            service_id=service_name,
        )

        operation = self.get_conn().create_service(create_request)
        return operation.result()

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_service(self, service_name: str, region: str, project_id: str = PROVIDE_PROJECT_ID) -> Service:
        delete_request = DeleteServiceRequest(
            name=f"projects/{project_id}/locations/{region}/services/{service_name}"
        )

        operation = self.get_conn().delete_service(delete_request)
        return operation.result()


class CloudRunServiceAsyncHook(GoogleBaseAsyncHook):
    """
    Async hook for the Google Cloud Run services.

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

    sync_hook_class = CloudRunServiceHook

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        self._client: ServicesClient | None = None
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)

    def get_conn(self):
        if self._client is None:
            self._client = ServicesAsyncClient(credentials=self.get_credentials(), client_info=CLIENT_INFO)

        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    async def create_service(
        self, service_name: str, service: Service | dict, region: str, project_id: str = PROVIDE_PROJECT_ID
    ) -> AsyncOperation:
        if isinstance(service, dict):
            service = Service(service)

        create_request = CreateServiceRequest(
            parent=f"projects/{project_id}/locations/{region}",
            service=service,
            service_id=service_name,
        )

        return await self.get_conn().create_service(create_request)

    @GoogleBaseHook.fallback_to_default_project_id
    async def delete_service(
        self, service_name: str, region: str, project_id: str = PROVIDE_PROJECT_ID
    ) -> AsyncOperation:
        delete_request = DeleteServiceRequest(
            name=f"projects/{project_id}/locations/{region}/services/{service_name}"
        )

        return await self.get_conn().delete_service(delete_request)
