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

from time import sleep
from typing import Iterable, Optional, Sequence, Union


from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import (
    PROVIDE_PROJECT_ID, GoogleBaseHook)

from google.cloud.run_v2 import Job, JobsClient, CreateJobRequest, ListJobsRequest

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
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(gcp_conn_id=gcp_conn_id,
                         impersonation_chain=impersonation_chain)
        self._client: JobsClient | None = None

    def get_conn(self) -> JobsClient:
        """
        Retrieves connection to Cloud Run.
        :return: JobsClient
        """
        if self._client is None:
            self._client = JobsClient()
        return self._client

    @GoogleBaseHook.fallback_to_default_project_id
    def create_job(
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

        operation = self.get_conn().create_job(create_request)
        operation.result()

    @GoogleBaseHook.fallback_to_default_project_id
    def list_jobs(
            self,
            region: str,
            project_id: str = PROVIDE_PROJECT_ID,
            show_deleted: bool = False,
            limit: Optional[int] = None) -> Iterable[Job]:

        if limit is not None and limit < 0:
            raise AirflowException(
                "The limit for the list jobs request should be greater or equal to zero")

        list_jobs_request: ListJobsRequest = ListJobsRequest(
            parent=f"projects/{project_id}/locations/{region}",
            show_deleted=show_deleted
        )

        jobs: pagers.ListJobsPager = self.get_conn().list_jobs(
            request=list_jobs_request)

        return self._limit_list(jobs, limit)

    def _limit_list(self, jobs, limit):
        result = []
        for item in jobs:
            if limit is not None and limit == 0:
                break
            result.append(item)
            if limit is not None:
                limit -= 1
        return result
