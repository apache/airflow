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

import asyncio
from typing import Any, AsyncIterator, Iterable

from google.api_core.exceptions import GoogleAPIError
from google.cloud.storage_transfer_v1.types import TransferOperation

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    CloudDataTransferServiceAsyncHook,
)
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.triggers.base import BaseTrigger, TriggerEvent


class CloudStorageTransferServiceCreateJobsTrigger(BaseTrigger):
    """
    StorageTransferJobTrigger run on the trigger worker to perform Cloud Storage Transfer job.

    :param job_names: List of transfer jobs names.
    :param project_id: GCP project id.
    :param poll_interval: Interval in seconds between polls.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    """

    def __init__(
        self,
        job_names: list[str],
        project_id: str = PROVIDE_PROJECT_ID,
        poll_interval: int = 10,
        gcp_conn_id: str = "google_cloud_default",
    ) -> None:
        super().__init__()
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.job_names = job_names
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize StorageTransferJobsTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__ }.{self.__class__.__qualname__}",
            {
                "project_id": self.project_id,
                "job_names": self.job_names,
                "poll_interval": self.poll_interval,
                "gcp_conn_id": self.gcp_conn_id,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Get current data storage transfer jobs and yields a TriggerEvent."""
        async_hook: CloudDataTransferServiceAsyncHook = self.get_async_hook()

        while True:
            self.log.info("Attempting to request jobs statuses")
            jobs_completed_successfully = 0
            try:
                jobs_pager = await async_hook.get_jobs(job_names=self.job_names)
                jobs, awaitable_operations = [], []
                async for job in jobs_pager:
                    awaitable_operation = async_hook.get_latest_operation(job)
                    jobs.append(job)
                    awaitable_operations.append(awaitable_operation)

                operations: Iterable[TransferOperation | None] = await asyncio.gather(
                    *awaitable_operations
                )

                for job, operation in zip(jobs, operations):
                    if operation is None:
                        yield TriggerEvent(
                            {
                                "status": "error",
                                "message": f"Transfer job {job.name} has no latest operation.",
                            }
                        )
                        return
                    elif operation.status == TransferOperation.Status.SUCCESS:
                        jobs_completed_successfully += 1
                    elif operation.status in (
                        TransferOperation.Status.FAILED,
                        TransferOperation.Status.ABORTED,
                    ):
                        yield TriggerEvent(
                            {
                                "status": "error",
                                "message": f"Transfer operation {operation.name} failed with status "
                                f"{TransferOperation.Status(operation.status).name}",
                            }
                        )
                        return
            except (GoogleAPIError, AirflowException) as ex:
                yield TriggerEvent({"status": "error", "message": str(ex)})
                return

            jobs_total = len(self.job_names)
            self.log.info(
                "Transfer jobs completed: %s of %s",
                jobs_completed_successfully,
                jobs_total,
            )
            if jobs_completed_successfully == jobs_total:
                s = "s" if jobs_total > 1 else ""
                job_names = ", ".join(j for j in self.job_names)
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": f"Transfer job{s} {job_names} completed successfully",
                    }
                )
                return

            self.log.info("Sleeping for %s seconds", self.poll_interval)
            await asyncio.sleep(self.poll_interval)

    def get_async_hook(self) -> CloudDataTransferServiceAsyncHook:
        return CloudDataTransferServiceAsyncHook(
            project_id=self.project_id,
            gcp_conn_id=self.gcp_conn_id,
        )
