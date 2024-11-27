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
import re
import time
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any

from google.api_core.exceptions import NotFound
from google.cloud.dataproc_v1 import Batch, Cluster, ClusterStatus, JobStatus

from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.hooks.dataproc import DataprocAsyncHook, DataprocHook
from airflow.providers.google.cloud.utils.dataproc import DataprocOperationType
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.session import provide_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


class DataprocBaseTrigger(BaseTrigger):
    """Base class for Dataproc triggers."""

    def __init__(
        self,
        region: str,
        project_id: str = PROVIDE_PROJECT_ID,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        polling_interval_seconds: int = 30,
        cancel_on_kill: bool = True,
        delete_on_error: bool = True,
    ):
        super().__init__()
        self.region = region
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.polling_interval_seconds = polling_interval_seconds
        self.cancel_on_kill = cancel_on_kill
        self.delete_on_error = delete_on_error

    def get_async_hook(self):
        return DataprocAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def get_sync_hook(self):
        # The synchronous hook is utilized to delete the cluster when a task is cancelled.
        # This is because the asynchronous hook deletion is not awaited when the trigger task
        # is cancelled. The call for deleting the cluster or job through the sync hook is not a blocking
        # call, which means it does not wait until the cluster or job is deleted.
        return DataprocHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class DataprocSubmitTrigger(DataprocBaseTrigger):
    """
    DataprocSubmitTrigger run on the trigger worker to perform create Build operation.

    :param job_id: The ID of a Dataproc job.
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

    def __init__(self, job_id: str, **kwargs):
        self.job_id = job_id
        super().__init__(**kwargs)

    def serialize(self):
        return (
            "airflow.providers.google.cloud.triggers.dataproc.DataprocSubmitTrigger",
            {
                "job_id": self.job_id,
                "project_id": self.project_id,
                "region": self.region,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval_seconds": self.polling_interval_seconds,
                "cancel_on_kill": self.cancel_on_kill,
            },
        )

    @provide_session
    def get_task_instance(self, session: Session) -> TaskInstance:
        """
        Get the task instance for the current task.

        :param session: Sqlalchemy session
        """
        query = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.task_instance.dag_id,
            TaskInstance.task_id == self.task_instance.task_id,
            TaskInstance.run_id == self.task_instance.run_id,
            TaskInstance.map_index == self.task_instance.map_index,
        )
        task_instance = query.one_or_none()
        if task_instance is None:
            raise AirflowException(
                "TaskInstance with dag_id: %s,task_id: %s, run_id: %s and map_index: %s is not found",
                self.task_instance.dag_id,
                self.task_instance.task_id,
                self.task_instance.run_id,
                self.task_instance.map_index,
            )
        return task_instance

    def safe_to_cancel(self) -> bool:
        """
        Whether it is safe to cancel the external job which is being executed by this trigger.

        This is to avoid the case that `asyncio.CancelledError` is called because the trigger itself is stopped.
        Because in those cases, we should NOT cancel the external job.
        """
        # Database query is needed to get the latest state of the task instance.
        task_instance = self.get_task_instance()  # type: ignore[call-arg]
        return task_instance.state != TaskInstanceState.DEFERRED

    async def run(self):
        try:
            while True:
                job = await self.get_async_hook().get_job(
                    project_id=self.project_id, region=self.region, job_id=self.job_id
                )
                state = job.status.state
                self.log.info("Dataproc job: %s is in state: %s", self.job_id, state)
                if state in (JobStatus.State.DONE, JobStatus.State.CANCELLED, JobStatus.State.ERROR):
                    break
                await asyncio.sleep(self.polling_interval_seconds)
            yield TriggerEvent({"job_id": self.job_id, "job_state": state, "job": job})
        except asyncio.CancelledError:
            self.log.info("Task got cancelled.")
            try:
                if self.job_id and self.cancel_on_kill and self.safe_to_cancel():
                    self.log.info(
                        "Cancelling the job as it is safe to do so. Note that the airflow TaskInstance is not"
                        " in deferred state."
                    )
                    self.log.info("Cancelling the job: %s", self.job_id)
                    # The synchronous hook is utilized to delete the cluster when a task is cancelled. This
                    # is because the asynchronous hook deletion is not awaited when the trigger task is
                    # cancelled. The call for deleting the cluster or job through the sync hook is not a
                    # blocking call, which means it does not wait until the cluster or job is deleted.
                    self.get_sync_hook().cancel_job(
                        job_id=self.job_id, project_id=self.project_id, region=self.region
                    )
                    self.log.info("Job: %s is cancelled", self.job_id)
                    yield TriggerEvent({"job_id": self.job_id, "job_state": ClusterStatus.State.DELETING})
            except Exception as e:
                self.log.error("Failed to cancel the job: %s with error : %s", self.job_id, str(e))
                raise e


class DataprocClusterTrigger(DataprocBaseTrigger):
    """
    DataprocClusterTrigger run on the trigger worker to perform create Build operation.

    :param cluster_name: The name of the cluster.
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

    def __init__(self, cluster_name: str, **kwargs):
        super().__init__(**kwargs)
        self.cluster_name = cluster_name

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
                "delete_on_error": self.delete_on_error,
            },
        )

    @provide_session
    def get_task_instance(self, session: Session) -> TaskInstance:
        query = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.task_instance.dag_id,
            TaskInstance.task_id == self.task_instance.task_id,
            TaskInstance.run_id == self.task_instance.run_id,
            TaskInstance.map_index == self.task_instance.map_index,
        )
        task_instance = query.one_or_none()
        if task_instance is None:
            raise AirflowException(
                "TaskInstance with dag_id: %s,task_id: %s, run_id: %s and map_index: %s is not found.",
                self.task_instance.dag_id,
                self.task_instance.task_id,
                self.task_instance.run_id,
                self.task_instance.map_index,
            )
        return task_instance

    def safe_to_cancel(self) -> bool:
        """
        Whether it is safe to cancel the external job which is being executed by this trigger.

        This is to avoid the case that `asyncio.CancelledError` is called because the trigger itself is stopped.
        Because in those cases, we should NOT cancel the external job.
        """
        # Database query is needed to get the latest state of the task instance.
        task_instance = self.get_task_instance()  # type: ignore[call-arg]
        return task_instance.state != TaskInstanceState.DEFERRED

    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            while True:
                cluster = await self.fetch_cluster()
                state = cluster.status.state
                if state == ClusterStatus.State.ERROR:
                    await self.delete_when_error_occurred(cluster)
                    yield TriggerEvent(
                        {
                            "cluster_name": self.cluster_name,
                            "cluster_state": ClusterStatus.State.DELETING,
                            "cluster": cluster,
                        }
                    )
                    return
                elif state == ClusterStatus.State.RUNNING:
                    yield TriggerEvent(
                        {
                            "cluster_name": self.cluster_name,
                            "cluster_state": state,
                            "cluster": cluster,
                        }
                    )
                    return
                self.log.info("Current state is %s", state)
                self.log.info("Sleeping for %s seconds.", self.polling_interval_seconds)
                await asyncio.sleep(self.polling_interval_seconds)
        except asyncio.CancelledError:
            try:
                if self.delete_on_error and self.safe_to_cancel():
                    self.log.info(
                        "Deleting the cluster as it is safe to delete as the airflow TaskInstance is not in "
                        "deferred state."
                    )
                    self.log.info("Deleting cluster %s.", self.cluster_name)
                    # The synchronous hook is utilized to delete the cluster when a task is cancelled.
                    # This is because the asynchronous hook deletion is not awaited when the trigger task
                    # is cancelled. The call for deleting the cluster through the sync hook is not a blocking
                    # call, which means it does not wait until the cluster is deleted.
                    self.get_sync_hook().delete_cluster(
                        region=self.region, cluster_name=self.cluster_name, project_id=self.project_id
                    )
                    self.log.info("Deleted cluster %s during cancellation.", self.cluster_name)
            except Exception as e:
                self.log.error("Error during cancellation handling: %s", e)
                raise AirflowException("Error during cancellation handling: %s", e)

    async def fetch_cluster(self) -> Cluster:
        """Fetch the cluster status."""
        return await self.get_async_hook().get_cluster(
            project_id=self.project_id, region=self.region, cluster_name=self.cluster_name
        )

    async def delete_when_error_occurred(self, cluster: Cluster) -> None:
        """
        Delete the cluster on error.

        :param cluster: The cluster to delete.
        """
        if self.delete_on_error:
            self.log.info("Deleting cluster %s.", self.cluster_name)
            await self.get_async_hook().delete_cluster(
                region=self.region, cluster_name=self.cluster_name, project_id=self.project_id
            )
            self.log.info("Cluster %s has been deleted.", self.cluster_name)
        else:
            self.log.info("Cluster %s is not deleted as delete_on_error is set to False.", self.cluster_name)


class DataprocBatchTrigger(DataprocBaseTrigger):
    """
    DataprocCreateBatchTrigger run on the trigger worker to perform create Build operation.

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

    def __init__(self, batch_id: str, **kwargs):
        super().__init__(**kwargs)
        self.batch_id = batch_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize DataprocBatchTrigger arguments and classpath."""
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
        while True:
            batch = await self.get_async_hook().get_batch(
                project_id=self.project_id, region=self.region, batch_id=self.batch_id
            )
            state = batch.state

            if state in (Batch.State.FAILED, Batch.State.SUCCEEDED, Batch.State.CANCELLED):
                break
            self.log.info("Current state is %s", state)
            self.log.info("Sleeping for %s seconds.", self.polling_interval_seconds)
            await asyncio.sleep(self.polling_interval_seconds)

        yield TriggerEvent(
            {"batch_id": self.batch_id, "batch_state": state, "batch_state_message": batch.state_message}
        )


class DataprocDeleteClusterTrigger(DataprocBaseTrigger):
    """
    DataprocDeleteClusterTrigger run on the trigger worker to perform delete cluster operation.

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
    :param polling_interval_seconds: Time in seconds to sleep between checks of cluster status
    """

    def __init__(
        self,
        cluster_name: str,
        end_time: float,
        metadata: Sequence[tuple[str, str]] = (),
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.end_time = end_time
        self.metadata = metadata

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize DataprocDeleteClusterTrigger arguments and classpath."""
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
                "polling_interval_seconds": self.polling_interval_seconds,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Wait until cluster is deleted completely."""
        try:
            while self.end_time > time.time():
                cluster = await self.get_async_hook().get_cluster(
                    region=self.region,  # type: ignore[arg-type]
                    cluster_name=self.cluster_name,
                    project_id=self.project_id,  # type: ignore[arg-type]
                    metadata=self.metadata,
                )
                self.log.info(
                    "Cluster status is %s. Sleeping for %s seconds.",
                    cluster.status.state,
                    self.polling_interval_seconds,
                )
                await asyncio.sleep(self.polling_interval_seconds)
        except NotFound:
            yield TriggerEvent({"status": "success", "message": ""})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
        else:
            yield TriggerEvent({"status": "error", "message": "Timeout"})


class DataprocOperationTrigger(DataprocBaseTrigger):
    """
    Trigger that periodically polls information on a long running operation from Dataproc API to verify status.

    Implementation leverages asynchronous transport.
    """

    def __init__(self, name: str, operation_type: str | None = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.name = name
        self.operation_type = operation_type

    def serialize(self):
        return (
            "airflow.providers.google.cloud.triggers.dataproc.DataprocOperationTrigger",
            {
                "name": self.name,
                "operation_type": self.operation_type,
                "project_id": self.project_id,
                "region": self.region,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "polling_interval_seconds": self.polling_interval_seconds,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = self.get_async_hook()
        try:
            while True:
                operation = await hook.get_operation(region=self.region, operation_name=self.name)
                if operation.done:
                    if operation.error.message:
                        status = "error"
                        message = operation.error.message
                    else:
                        status = "success"
                        message = "Operation is successfully ended."
                    if self.operation_type == DataprocOperationType.DIAGNOSE.value:
                        gcs_regex = rb"gs:\/\/[a-z0-9][a-z0-9_-]{1,61}[a-z0-9_\-\/]*"
                        gcs_uri_value = operation.response.value
                        match = re.search(gcs_regex, gcs_uri_value)
                        if match:
                            output_uri = match.group(0).decode("utf-8", "ignore")
                        else:
                            output_uri = gcs_uri_value
                        yield TriggerEvent(
                            {
                                "status": status,
                                "message": message,
                                "output_uri": output_uri,
                            }
                        )
                    else:
                        yield TriggerEvent(
                            {
                                "operation_name": operation.name,
                                "operation_done": operation.done,
                                "status": status,
                                "message": message,
                            }
                        )
                    return
                else:
                    self.log.info("Sleeping for %s seconds.", self.polling_interval_seconds)
                    await asyncio.sleep(self.polling_interval_seconds)
        except Exception as e:
            self.log.exception("Exception occurred while checking operation status.")
            yield TriggerEvent(
                {
                    "status": "failed",
                    "message": str(e),
                }
            )
