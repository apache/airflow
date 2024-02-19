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
from typing import Any, AsyncIterator, Sequence

from google.cloud.bigquery_datatransfer_v1 import TransferRun, TransferState
from google.oauth2.service_account import _DEFAULT_TOKEN_LIFETIME_SECS

from airflow.providers.google.cloud.hooks.bigquery_dts import AsyncBiqQueryDataTransferServiceHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class BigQueryDataTransferRunTrigger(BaseTrigger):
    """
    Triggers class to watch the Transfer Run state to define when the job is done.

    :param project_id: The BigQuery project id where the transfer configuration should be
    :param config_id: ID of the config of the Transfer Run which should be watched.
    :param run_id: ID of the Transfer Run which should be watched.
    :param poll_interval: Optional. Interval which defines how often triggers check status of the job.
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param location: BigQuery Transfer Service location for regional transfers.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param token_refresh_interval_seconds: GCP STS Token refresh interval in seconds.
    """

    def __init__(
        self,
        project_id: str | None,
        config_id: str,
        run_id: str,
        poll_interval: int = 10,
        gcp_conn_id: str = "google_cloud_default",
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        token_refresh_interval_seconds: int = _DEFAULT_TOKEN_LIFETIME_SECS // 2,
    ):
        super().__init__()
        self.project_id = project_id
        self.config_id = config_id
        self.run_id = run_id
        self.poll_interval = poll_interval
        self.gcp_conn_id = gcp_conn_id
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.token_refresh_interval_seconds = token_refresh_interval_seconds

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.bigquery_dts.BigQueryDataTransferRunTrigger",
            {
                "project_id": self.project_id,
                "config_id": self.config_id,
                "run_id": self.run_id,
                "poll_interval": self.poll_interval,
                "gcp_conn_id": self.gcp_conn_id,
                "location": self.location,
                "impersonation_chain": self.impersonation_chain,
                "token_refresh_interval_seconds": self.token_refresh_interval_seconds,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """If the Transfer Run is in a terminal state, then yield TriggerEvent object."""
        hook = self._get_async_hook()
        idx = 0
        try:
            while True:
                current_tick_div, current_tick_mod = divmod(
                    idx * self.poll_interval, self.token_refresh_interval_seconds
                )
                next_tick_div, next_tick_mod = divmod(
                    (idx + 1) * self.poll_interval, self.token_refresh_interval_seconds
                )
                if (current_tick_div < next_tick_div and 0 < next_tick_mod) or (
                    current_tick_mod == 0 and 1 <= current_tick_div
                ):
                    _ = await hook.refresh_credentials()
                    self.log.info(
                        f"Credentials were refreshed on tick: idx={idx}, idx*interval={idx * self.poll_interval} sec"
                    )

                transfer_run: TransferRun = await hook.get_transfer_run(
                    project_id=self.project_id,
                    config_id=self.config_id,
                    run_id=self.run_id,
                    location=self.location,
                )
                state = transfer_run.state
                self.log.info("Current state is %s", state)

                if state == TransferState.SUCCEEDED:
                    self.log.info("Job has completed its work.")
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "run_id": self.run_id,
                            "message": "Job completed",
                            "config_id": self.config_id,
                        }
                    )
                    return
                elif state == TransferState.FAILED:
                    self.log.info("Job has failed")
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "run_id": self.run_id,
                            "message": "Job has failed",
                        }
                    )
                    return
                if state == TransferState.CANCELLED:
                    self.log.info("Job has been cancelled.")
                    yield TriggerEvent(
                        {
                            "status": "cancelled",
                            "run_id": self.run_id,
                            "message": "Job was cancelled",
                        }
                    )
                    return
                else:
                    self.log.info("Job is still working...")
                    self.log.info("Waiting for %s seconds", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)

                idx += 1
        except Exception as e:
            yield TriggerEvent(
                {
                    "status": "failed",
                    "message": f"Trigger failed with exception: {e}",
                }
            )

    def _get_async_hook(self) -> AsyncBiqQueryDataTransferServiceHook:
        return AsyncBiqQueryDataTransferServiceHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
