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
from enum import Enum
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.cloud_run import CloudRunAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from google.longrunning import operations_pb2

DEFAULT_BATCH_LOCATION = "us-central1"


class RunJobStatus(Enum):
    """Enum to represent the status of a job run."""

    SUCCESS = "Success"
    FAIL = "Fail"
    TIMEOUT = "Timeout"


class CloudRunJobFinishedTrigger(BaseTrigger):
    """
    Cloud Run trigger to check if templated job has been finished.

    :param operation_name: Required. Name of the operation.
    :param job_name: Required. Name of the job.
    :param project_id: Required. the Google Cloud project ID in which the job was started.
    :param location: Optional. the location where job is executed.
        If set to None then the value of DEFAULT_BATCH_LOCATION will be used.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token of the last account
        in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param poll_sleep: Polling period in seconds to check for the status.
    :timeout: The time to wait before failing the operation.
    """

    def __init__(
        self,
        operation_name: str,
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
        self.operation_name = operation_name
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.timeout = timeout
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.cloud_run.CloudRunJobFinishedTrigger",
            {
                "project_id": self.project_id,
                "operation_name": self.operation_name,
                "job_name": self.job_name,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "polling_period_seconds": self.polling_period_seconds,
                "timeout": self.timeout,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        timeout = self.timeout
        hook = self._get_async_hook()
        while timeout is None or timeout > 0:
            operation: operations_pb2.Operation = await hook.get_operation(self.operation_name)
            if operation.done:
                # An operation can only have one of those two combinations: if it is failed, then
                # the error field will be populated, else, then the response field will be.
                if operation.error.SerializeToString():
                    yield TriggerEvent(
                        {
                            "status": RunJobStatus.FAIL.value,
                            "operation_error_code": operation.error.code,
                            "operation_error_message": operation.error.message,
                            "job_name": self.job_name,
                        }
                    )
                else:
                    yield TriggerEvent(
                        {
                            "status": RunJobStatus.SUCCESS.value,
                            "job_name": self.job_name,
                        }
                    )
                return
            elif operation.error.message:
                raise AirflowException(f"Cloud Run Job error: {operation.error.message}")

            if timeout is not None:
                timeout -= self.polling_period_seconds

            if timeout is None or timeout > 0:
                await asyncio.sleep(self.polling_period_seconds)

        yield TriggerEvent(
            {
                "status": RunJobStatus.TIMEOUT.value,
                "job_name": self.job_name,
            }
        )

    def _get_async_hook(self) -> CloudRunAsyncHook:
        return CloudRunAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
