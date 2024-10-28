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
"""This module contains Google Dataplex triggers."""

from __future__ import annotations

import asyncio
from typing import AsyncIterator, Sequence

from google.cloud.dataplex_v1.types import DataScanJob

from airflow.providers.google.cloud.hooks.dataplex import DataplexAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class DataplexDataQualityJobTrigger(BaseTrigger):
    """
    DataplexDataQualityJobTrigger runs on the trigger worker and waits for the job to be `SUCCEEDED` state.

    :param job_id: Optional. The ID of a Dataplex job.
    :param data_scan_id: Required. DataScan identifier.
    :param project_id: Google Cloud Project where the job is running.
    :param region: The ID of the Google Cloud region that the job belongs to.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param polling_interval_seconds: polling period in seconds to check for the status.
    """

    def __init__(
        self,
        job_id: str | None,
        data_scan_id: str,
        project_id: str | None,
        region: str,
        gcp_conn_id: str = "google_cloud_default",
        polling_interval_seconds: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.data_scan_id = data_scan_id
        self.project_id = project_id
        self.region = region
        self.gcp_conn_id = gcp_conn_id
        self.polling_interval_seconds = polling_interval_seconds
        self.impersonation_chain = impersonation_chain

    def serialize(self):
        return (
            "airflow.providers.google.cloud.triggers.dataplex.DataplexDataQualityJobTrigger",
            {
                "job_id": self.job_id,
                "data_scan_id": self.data_scan_id,
                "project_id": self.project_id,
                "region": self.region,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval_seconds": self.polling_interval_seconds,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = DataplexAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        while True:
            job = await hook.get_data_scan_job(
                project_id=self.project_id,
                region=self.region,
                job_id=self.job_id,
                data_scan_id=self.data_scan_id,
            )
            state = job.state
            if state in (
                DataScanJob.State.FAILED,
                DataScanJob.State.SUCCEEDED,
                DataScanJob.State.CANCELLED,
            ):
                break
            self.log.info(
                "Current state is: %s, sleeping for %s seconds.",
                DataScanJob.State(state).name,
                self.polling_interval_seconds,
            )
            await asyncio.sleep(self.polling_interval_seconds)
        yield TriggerEvent(
            {"job_id": self.job_id, "job_state": state, "job": self._convert_to_dict(job)}
        )

    def _convert_to_dict(self, job: DataScanJob) -> dict:
        """Return a representation of a DataScanJob instance as a dict."""
        return DataScanJob.to_dict(job)


class DataplexDataProfileJobTrigger(BaseTrigger):
    """
    DataplexDataProfileJobTrigger runs on the trigger worker and waits for the job to be `SUCCEEDED` state.

    :param job_id: Optional. The ID of a Dataplex job.
    :param data_scan_id: Required. DataScan identifier.
    :param project_id: Google Cloud Project where the job is running.
    :param region: The ID of the Google Cloud region that the job belongs to.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param polling_interval_seconds: polling period in seconds to check for the status.
    """

    def __init__(
        self,
        job_id: str | None,
        data_scan_id: str,
        project_id: str | None,
        region: str,
        gcp_conn_id: str = "google_cloud_default",
        polling_interval_seconds: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.data_scan_id = data_scan_id
        self.project_id = project_id
        self.region = region
        self.gcp_conn_id = gcp_conn_id
        self.polling_interval_seconds = polling_interval_seconds
        self.impersonation_chain = impersonation_chain

    def serialize(self):
        return (
            "airflow.providers.google.cloud.triggers.dataplex.DataplexDataProfileJobTrigger",
            {
                "job_id": self.job_id,
                "data_scan_id": self.data_scan_id,
                "project_id": self.project_id,
                "region": self.region,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval_seconds": self.polling_interval_seconds,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = DataplexAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        while True:
            job = await hook.get_data_scan_job(
                project_id=self.project_id,
                region=self.region,
                job_id=self.job_id,
                data_scan_id=self.data_scan_id,
            )
            state = job.state
            if state in (
                DataScanJob.State.FAILED,
                DataScanJob.State.SUCCEEDED,
                DataScanJob.State.CANCELLED,
            ):
                break
            self.log.info(
                "Current state is: %s, sleeping for %s seconds.",
                DataScanJob.State(state).name,
                self.polling_interval_seconds,
            )
            await asyncio.sleep(self.polling_interval_seconds)
        yield TriggerEvent(
            {"job_id": self.job_id, "job_state": state, "job": self._convert_to_dict(job)}
        )

    def _convert_to_dict(self, job: DataScanJob) -> dict:
        """Return a representation of a DataScanJob instance as a dict."""
        return DataScanJob.to_dict(job)
