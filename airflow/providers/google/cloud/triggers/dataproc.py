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
"""This module contains Google Dataproc triggers."""
from __future__ import annotations

import asyncio
import time
import warnings
from typing import Any, AsyncIterator, Sequence

from google.api_core.exceptions import NotFound
from google.cloud.dataproc_v1 import Batch, ClusterStatus, JobStatus

from airflow import AirflowException
from airflow.providers.google.cloud.hooks.dataproc import DataprocAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class DataprocSubmitTrigger(BaseTrigger):
    """
    Trigger that periodically polls information from Dataproc API to verify job status.
    Implementation leverages asynchronous transport.
    """

    def __init__(
        self,
        job_id: str,
        region: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        delegate_to: str | None = None,
        polling_interval_seconds: int = 30,
    ):
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.job_id = job_id
        self.project_id = project_id
        self.region = region
        self.polling_interval_seconds = polling_interval_seconds
        if delegate_to:
            warnings.warn(
                "'delegate_to' parameter is deprecated, please use 'impersonation_chain'", DeprecationWarning
            )
        self.delegate_to = delegate_to
        self.hook = DataprocAsyncHook(
            delegate_to=self.delegate_to,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def serialize(self):
        return (
            "airflow.providers.google.cloud.triggers.dataproc.DataprocSubmitTrigger",
            {
                "job_id": self.job_id,
                "project_id": self.project_id,
                "region": self.region,
                "gcp_conn_id": self.gcp_conn_id,
                "delegate_to": self.delegate_to,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval_seconds": self.polling_interval_seconds,
            },
        )

    async def run(self):
        while True:
            job = await self.hook.get_job(project_id=self.project_id, region=self.region, job_id=self.job_id)
            state = job.status.state
            self.log.info("Dataproc job: %s is in state: %s", self.job_id, state)
            if state in (JobStatus.State.ERROR, JobStatus.State.DONE, JobStatus.State.CANCELLED):
                if state in (JobStatus.State.DONE, JobStatus.State.CANCELLED):
                    break
                elif state == JobStatus.State.ERROR:
                    raise AirflowException(f"Dataproc job execution failed {self.job_id}")
            await asyncio.sleep(self.polling_interval_seconds)
        yield TriggerEvent({"job_id": self.job_id, "job_state": state})


class DataprocClusterTrigger(BaseTrigger):
    """
    Trigger that periodically polls information from Dataproc API to verify status.
    Implementation leverages asynchronous transport.
    """

    def __init__(
        self,
        cluster_name: str,
        region: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        polling_interval_seconds: int = 10,
    ):
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.cluster_name = cluster_name
        self.project_id = project_id
        self.region = region
        self.polling_interval_seconds = polling_interval_seconds

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.google.cloud.triggers.dataproc.DataprocClusterTrigger",
            {
                "cluster_name": self.cluster_name,
                "project_id": self.project_id,
                "region": self.region,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval_seconds": self.polling_interval_seconds,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        hook = self._get_hook()
        while True:
            cluster = await hook.get_cluster(
                project_id=self.project_id, region=self.region, cluster_name=self.cluster_name
            )
            state = cluster.status.state
            self.log.info("Dataproc cluster: %s is in state: %s", self.cluster_name, state)
            if state in (
                ClusterStatus.State.ERROR,
                ClusterStatus.State.RUNNING,
            ):
                break
            self.log.info("Sleeping for %s seconds.", self.polling_interval_seconds)
            await asyncio.sleep(self.polling_interval_seconds)
        yield TriggerEvent({"cluster_name": self.cluster_name, "cluster_state": state, "cluster": cluster})

    def _get_hook(self) -> DataprocAsyncHook:
        return DataprocAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class DataprocBatchTrigger(BaseTrigger):
    """
    DataprocCreateBatchTrigger run on the trigger worker to perform create Build operation

    :param batch_id: The ID of the build.
    :param project_id: Google Cloud Project where the job is running
    :param region: The Cloud Dataproc region in which to handle the request.
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param polling_interval_seconds: polling period in seconds to check for the status
    """

    def __init__(
        self,
        batch_id: str,
        region: str,
        project_id: str | None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        polling_interval_seconds: float = 5.0,
    ):
        super().__init__()
        self.batch_id = batch_id
        self.project_id = project_id
        self.region = region
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.polling_interval_seconds = polling_interval_seconds

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes DataprocBatchTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataproc.DataprocBatchTrigger",
            {
                "batch_id": self.batch_id,
                "project_id": self.project_id,
                "region": self.region,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval_seconds": self.polling_interval_seconds,
            },
        )

    async def run(self):
        hook = DataprocAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        while True:
            batch = await hook.get_batch(
                project_id=self.project_id, region=self.region, batch_id=self.batch_id
            )
            state = batch.state

            if state in (Batch.State.FAILED, Batch.State.SUCCEEDED, Batch.State.CANCELLED):
                break
            self.log.info("Current state is %s", state)
            self.log.info("Sleeping for %s seconds.", self.polling_interval_seconds)
            await asyncio.sleep(self.polling_interval_seconds)
        yield TriggerEvent({"batch_id": self.batch_id, "batch_state": state})


class DataprocDeleteClusterTrigger(BaseTrigger):
    """
    Asynchronously checks the status of a cluster.

    :param cluster_name: The name of the cluster
    :param end_time: Time in second left to check the cluster status
    :param project_id: The ID of the Google Cloud project the cluster belongs to
    :param region: The Cloud Dataproc region in which to handle the request
    :param metadata: Additional metadata that is provided to the method
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :param polling_interval: Time in seconds to sleep between checks of cluster status
    """

    def __init__(
        self,
        cluster_name: str,
        end_time: float,
        project_id: str | None = None,
        region: str | None = None,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.end_time = end_time
        self.project_id = project_id
        self.region = region
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.polling_interval = polling_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes DataprocDeleteClusterTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataproc.DataprocDeleteClusterTrigger",
            {
                "cluster_name": self.cluster_name,
                "end_time": self.end_time,
                "project_id": self.project_id,
                "region": self.region,
                "metadata": self.metadata,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval": self.polling_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """Wait until cluster is deleted completely"""
        hook = self._get_hook()
        while self.end_time > time.time():
            try:
                cluster = await hook.get_cluster(
                    region=self.region,  # type: ignore[arg-type]
                    cluster_name=self.cluster_name,
                    project_id=self.project_id,  # type: ignore[arg-type]
                    metadata=self.metadata,
                )
                self.log.info(
                    "Cluster status is %s. Sleeping for %s seconds.",
                    cluster.status.state,
                    self.polling_interval,
                )
                await asyncio.sleep(self.polling_interval)
            except NotFound:
                yield TriggerEvent({"status": "success", "message": ""})
            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})
        yield TriggerEvent({"status": "error", "message": "Timeout"})

    def _get_hook(self) -> DataprocAsyncHook:
        return DataprocAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
