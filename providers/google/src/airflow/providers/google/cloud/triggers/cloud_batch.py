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

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any

from google.cloud.batch_v1 import Job, JobStatus

from airflow.providers.google.cloud.hooks.cloud_batch import CloudBatchAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

DEFAULT_BATCH_LOCATION = "us-central1"


class CloudBatchJobFinishedTrigger(BaseTrigger):
    """
    Cloud Batch trigger to check if templated job has been finished.

    :param job_name: Required. Name of the job.
    :param project_id: Required. the Google Cloud project ID in which the job was started.
    :param location: Optional. the location where job is executed. If set to None then
        the value of DEFAULT_BATCH_LOCATION will be used
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param poll_sleep: Polling period in seconds to check for the status

    """

    def __init__(
        self,
        job_name: str,
        project_id: str | None,
        location: str = DEFAULT_BATCH_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        polling_period_seconds: float = 10,
        timeout: float | None = None,
    ):
        super().__init__()
        self.project_id = project_id
        self.job_name = job_name
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.timeout = timeout
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.cloud_batch.CloudBatchJobFinishedTrigger",
            {
                "project_id": self.project_id,
                "job_name": self.job_name,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "polling_period_seconds": self.polling_period_seconds,
                "timeout": self.timeout,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Fetch job status or yield certain Events.

        Main loop of the class in where it is fetching the job status and yields certain Event.

        If the job has status success then it yields TriggerEvent with success status, if job has
        status failed - with error status and if the job is being deleted - with deleted status.
        In any other case Trigger will wait for specified amount of time
        stored in self.polling_period_seconds variable.
        """
        timeout = self.timeout
        hook = self._get_async_hook()
        try:
            while timeout is None or timeout > 0:
                job: Job = await hook.get_batch_job(job_name=self.job_name)

                status: JobStatus.State = job.status.state
                if status == JobStatus.State.SUCCEEDED:
                    yield TriggerEvent(
                        {
                            "job_name": self.job_name,
                            "status": "success",
                            "message": "Job completed",
                        }
                    )
                    return
                elif status == JobStatus.State.FAILED:
                    yield TriggerEvent(
                        {
                            "job_name": self.job_name,
                            "status": "error",
                            "message": f"Batch job with name {self.job_name} has failed its execution",
                        }
                    )
                    return
                elif status == JobStatus.State.DELETION_IN_PROGRESS:
                    yield TriggerEvent(
                        {
                            "job_name": self.job_name,
                            "status": "deleted",
                            "message": f"Batch job with name {self.job_name} is being deleted",
                        }
                    )
                    return
                else:
                    self.log.info("Current job status is: %s", status)
                    self.log.info("Sleeping for %s seconds.", self.polling_period_seconds)
                    if timeout is not None:
                        timeout -= self.polling_period_seconds

                    if timeout is None or timeout > 0:
                        await asyncio.sleep(self.polling_period_seconds)

        except Exception as e:
            self.log.exception("Exception occurred while checking for job completion.")
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

        self.log.exception("Job with name [%s] timed out", self.job_name)
        yield TriggerEvent(
            {
                "job_name": self.job_name,
                "status": "timed out",
                "message": f"Batch job with name {self.job_name} timed out",
            }
        )

    def _get_async_hook(self) -> CloudBatchAsyncHook:
        return CloudBatchAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
