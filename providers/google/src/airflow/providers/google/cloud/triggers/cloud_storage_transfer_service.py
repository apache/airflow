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
from collections.abc import AsyncIterator, Iterable, Sequence
from typing import Any

from google.api_core.exceptions import GoogleAPIError
from google.cloud.storage_transfer_v1.types import TransferOperation
from google.protobuf.json_format import MessageToDict

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import (
    CloudDataTransferServiceAsyncHook,
    GcpTransferOperationStatus,
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
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            {
                "project_id": self.project_id,
                "job_names": self.job_names,
                "poll_interval": self.poll_interval,
                "gcp_conn_id": self.gcp_conn_id,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
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

                operations: Iterable[TransferOperation | None] = await asyncio.gather(*awaitable_operations)

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
            self.log.info("Transfer jobs completed: %s of %s", jobs_completed_successfully, jobs_total)
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


class CloudStorageTransferServiceCheckJobStatusTrigger(BaseTrigger):
    """
    CloudStorageTransferServiceCheckJobStatusTrigger run on the trigger worker to check Cloud Storage Transfer job.

    :param job_name: The name of the transfer job
    :param expected_statuses: The expected state of the operation.
        See:
        https://cloud.google.com/storage-transfer/docs/reference/rest/v1/transferOperations#Status
    :param project_id: The ID of the project that owns the Transfer Job.
    :param poke_interval: Polling period in seconds to check for the status
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        job_name: str,
        expected_statuses: set[str] | str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        poke_interval: float = 10.0,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__()
        self.job_name = job_name
        self.expected_statuses = expected_statuses
        self.project_id = project_id
        self.poke_interval = poke_interval
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize CloudStorageTransferServiceCheckJobStatusTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            {
                "job_name": self.job_name,
                "expected_statuses": self.expected_statuses,
                "project_id": self.project_id,
                "poke_interval": self.poke_interval,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    def _get_async_hook(self) -> CloudDataTransferServiceAsyncHook:
        return CloudDataTransferServiceAsyncHook(
            project_id=self.project_id,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Check the status of the transfer job and yield a TriggerEvent."""
        hook = self._get_async_hook()
        expected_statuses = (
            {GcpTransferOperationStatus.SUCCESS} if not self.expected_statuses else self.expected_statuses
        )

        try:
            while True:
                operations = await hook.list_transfer_operations(
                    request_filter={
                        "project_id": self.project_id or hook.project_id,
                        "job_names": [self.job_name],
                    }
                )
                check = await CloudDataTransferServiceAsyncHook.operations_contain_expected_statuses(
                    operations=operations,
                    expected_statuses=expected_statuses,
                )
                if check:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Transfer operation completed successfully",
                            "operations": operations,
                        }
                    )
                    return

                self.log.info(
                    "Sleeping for %s seconds.",
                    self.poke_interval,
                )
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            self.log.exception("Exception occurred while checking for query completion")
            yield TriggerEvent({"status": "error", "message": str(e)})


class CloudDataTransferServiceRunJobTrigger(BaseTrigger):
    """
    CloudDataTransferServiceRunJobTrigger run on the trigger worker to run Cloud Storage Transfer job.

    :param job_name: The name of the transfer job
    :param project_id: The ID of the project that owns the Transfer Job.
    :param poke_interval: Polling period in seconds to check for the status
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        job_name: str,
        project_id: str = PROVIDE_PROJECT_ID,
        poke_interval: float = 10.0,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__()
        self.job_name = job_name
        self.project_id = project_id
        self.poke_interval = poke_interval
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize CloudDataTransferServiceRunJobTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__qualname__}",
            {
                "job_name": self.job_name,
                "project_id": self.project_id,
                "poke_interval": self.poke_interval,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    def _get_async_hook(self) -> CloudDataTransferServiceAsyncHook:
        return CloudDataTransferServiceAsyncHook(
            project_id=self.project_id,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Run the transfer job and yield a TriggerEvent."""
        hook = self._get_async_hook()

        try:
            job_operation = await hook.run_transfer_job(self.job_name)
            while True:
                job_completed = await job_operation.done()
                if job_completed:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Transfer operation run completed successfully",
                            "job_result": {
                                "name": job_operation.operation.name,
                                "metadata": MessageToDict(
                                    job_operation.operation.metadata, preserving_proto_field_name=True
                                ),
                                "response": MessageToDict(
                                    job_operation.operation.response, preserving_proto_field_name=True
                                ),
                            },
                        }
                    )
                    return

                self.log.info(
                    "Sleeping for %s seconds.",
                    self.poke_interval,
                )
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            self.log.exception("Exception occurred while running transfer job")
            yield TriggerEvent({"status": "error", "message": str(e)})
