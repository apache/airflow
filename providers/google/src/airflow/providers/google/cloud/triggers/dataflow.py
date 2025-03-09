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
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from google.cloud.dataflow_v1beta3 import JobState
from google.cloud.dataflow_v1beta3.types import (
    AutoscalingEvent,
    Job,
    JobMessage,
    JobMetrics,
    JobType,
    MetricUpdate,
)

from airflow.providers.google.cloud.hooks.dataflow import AsyncDataflowHook, DataflowJobStatus
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from google.cloud.dataflow_v1beta3.services.messages_v1_beta3.pagers import ListJobMessagesAsyncPager


DEFAULT_DATAFLOW_LOCATION = "us-central1"


class TemplateJobStartTrigger(BaseTrigger):
    """
    Dataflow trigger to check if templated job has been finished.

    :param project_id: Required. the Google Cloud project ID in which the job was started.
    :param job_id: Required. ID of the job.
    :param location: Optional. the location where job is executed. If set to None then
        the value of DEFAULT_DATAFLOW_LOCATION will be used
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_timeout: Optional. How long (in seconds) operator should wait for the pipeline to be
        successfully cancelled when task is being killed.
    """

    def __init__(
        self,
        job_id: str,
        project_id: str | None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        cancel_timeout: int | None = 5 * 60,
    ):
        super().__init__()
        self.project_id = project_id
        self.job_id = job_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain
        self.cancel_timeout = cancel_timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataflow.TemplateJobStartTrigger",
            {
                "project_id": self.project_id,
                "job_id": self.job_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_sleep": self.poll_sleep,
                "impersonation_chain": self.impersonation_chain,
                "cancel_timeout": self.cancel_timeout,
            },
        )

    async def run(self):
        """
        Fetch job status or yield certain Events.

        Main loop of the class in where it is fetching the job status and yields certain Event.

        If the job has status success then it yields TriggerEvent with success status, if job has
        status failed - with error status. In any other case Trigger will wait for specified
        amount of time stored in self.poll_sleep variable.
        """
        hook = self._get_async_hook()
        try:
            while True:
                status = await hook.get_job_status(
                    project_id=self.project_id,
                    job_id=self.job_id,
                    location=self.location,
                )
                if status == JobState.JOB_STATE_DONE:
                    yield TriggerEvent(
                        {
                            "job_id": self.job_id,
                            "status": "success",
                            "message": "Job completed",
                        }
                    )
                    return
                elif status == JobState.JOB_STATE_FAILED:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Dataflow job with id {self.job_id} has failed its execution",
                        }
                    )
                    return
                elif status == JobState.JOB_STATE_STOPPED:
                    yield TriggerEvent(
                        {
                            "status": "stopped",
                            "message": f"Dataflow job with id {self.job_id} was stopped",
                        }
                    )
                    return
                else:
                    self.log.info("Job is still running...")
                    self.log.info("Current job status is: %s", status.name)
                    self.log.info("Sleeping for %s seconds.", self.poll_sleep)
                    await asyncio.sleep(self.poll_sleep)
        except Exception as e:
            self.log.exception("Exception occurred while checking for job completion.")
            yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> AsyncDataflowHook:
        return AsyncDataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
            cancel_timeout=self.cancel_timeout,
        )


class DataflowJobStatusTrigger(BaseTrigger):
    """
    Trigger that monitors if a Dataflow job has reached any of the expected statuses.

    :param job_id: Required. ID of the job.
    :param expected_statuses: The expected state(s) of the operation.
        See: https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState
    :param project_id: Required. The Google Cloud project ID in which the job was started.
    :param location: Optional. The location where the job is executed. If set to None then
        the value of DEFAULT_DATAFLOW_LOCATION will be used.
    :param gcp_conn_id: The connection ID to use for connecting to Google Cloud.
    :param poll_sleep: Time (seconds) to wait between two consecutive calls to check the job.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
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
        job_id: str,
        expected_statuses: set[str],
        project_id: str | None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__()
        self.job_id = job_id
        self.expected_statuses = expected_statuses
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobStatusTrigger",
            {
                "job_id": self.job_id,
                "expected_statuses": self.expected_statuses,
                "project_id": self.project_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_sleep": self.poll_sleep,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    async def run(self):
        """
        Loop until the job reaches an expected or terminal state.

        Yields a TriggerEvent with success status, if the client returns an expected job status.

        Yields a TriggerEvent with error status, if the client returns an unexpected terminal
        job status or any exception is raised while looping.

        In any other case the Trigger will wait for a specified amount of time
        stored in self.poll_sleep variable.
        """
        try:
            while True:
                job_status = await self.async_hook.get_job_status(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location,
                )
                if job_status.name in self.expected_statuses:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"Job with id '{self.job_id}' has reached an expected state: {job_status.name}",
                        }
                    )
                    return
                elif job_status.name in DataflowJobStatus.TERMINAL_STATES:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Job with id '{self.job_id}' is already in terminal state: {job_status.name}",
                        }
                    )
                    return
                self.log.info("Sleeping for %s seconds.", self.poll_sleep)
                await asyncio.sleep(self.poll_sleep)
        except Exception as e:
            self.log.error("Exception occurred while checking for job status!")
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(e),
                }
            )

    @cached_property
    def async_hook(self) -> AsyncDataflowHook:
        return AsyncDataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
        )


class DataflowStartYamlJobTrigger(BaseTrigger):
    """
    Dataflow trigger that checks the state of a Dataflow YAML job.

    :param job_id: Required. ID of the job.
    :param project_id: Required. The Google Cloud project ID in which the job was started.
    :param location: The location where job is executed. If set to None then
        the value of DEFAULT_DATAFLOW_LOCATION will be used.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param poll_sleep: Optional. The time in seconds to sleep between polling Google Cloud Platform
        for the Dataflow job.
    :param cancel_timeout: Optional. How long (in seconds) operator should wait for the pipeline to be
        successfully cancelled when task is being killed.
    :param expected_terminal_state: Optional. The expected terminal state of the Dataflow job at which the
        operator task is set to succeed. Defaults to 'JOB_STATE_DONE' for the batch jobs and
        'JOB_STATE_RUNNING' for the streaming jobs.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
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
        job_id: str,
        project_id: str | None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        cancel_timeout: int | None = 5 * 60,
        expected_terminal_state: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__()
        self.project_id = project_id
        self.job_id = job_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.cancel_timeout = cancel_timeout
        self.expected_terminal_state = expected_terminal_state
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowStartYamlJobTrigger",
            {
                "project_id": self.project_id,
                "job_id": self.job_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_sleep": self.poll_sleep,
                "expected_terminal_state": self.expected_terminal_state,
                "impersonation_chain": self.impersonation_chain,
                "cancel_timeout": self.cancel_timeout,
            },
        )

    async def run(self):
        """
        Fetch job and yield events depending on the job's type and state.

        Yield TriggerEvent if the job reaches a terminal state.
        Otherwise awaits for a specified amount of time stored in self.poll_sleep variable.
        """
        hook: AsyncDataflowHook = self._get_async_hook()
        try:
            while True:
                job: Job = await hook.get_job(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location,
                )
                job_state = job.current_state
                job_type = job.type_
                if job_state.name == self.expected_terminal_state:
                    yield TriggerEvent(
                        {
                            "job": Job.to_dict(job),
                            "status": "success",
                            "message": f"Job reached the expected terminal state: {self.expected_terminal_state}.",
                        }
                    )
                    return
                elif job_type == JobType.JOB_TYPE_STREAMING and job_state == JobState.JOB_STATE_RUNNING:
                    yield TriggerEvent(
                        {
                            "job": Job.to_dict(job),
                            "status": "success",
                            "message": "Streaming job reached the RUNNING state.",
                        }
                    )
                    return
                elif job_type == JobType.JOB_TYPE_BATCH and job_state == JobState.JOB_STATE_DONE:
                    yield TriggerEvent(
                        {
                            "job": Job.to_dict(job),
                            "status": "success",
                            "message": "Batch job completed.",
                        }
                    )
                    return
                elif job_state == JobState.JOB_STATE_FAILED:
                    yield TriggerEvent(
                        {
                            "job": Job.to_dict(job),
                            "status": "error",
                            "message": "Job failed.",
                        }
                    )
                    return
                elif job_state == JobState.JOB_STATE_STOPPED:
                    yield TriggerEvent(
                        {
                            "job": Job.to_dict(job),
                            "status": "stopped",
                            "message": "Job was stopped.",
                        }
                    )
                    return
                else:
                    self.log.info("Current job status is: %s", job_state.name)
                    self.log.info("Sleeping for %s seconds.", self.poll_sleep)
                    await asyncio.sleep(self.poll_sleep)
        except Exception as e:
            self.log.exception("Exception occurred while checking for job completion.")
            yield TriggerEvent({"job": None, "status": "error", "message": str(e)})

    def _get_async_hook(self) -> AsyncDataflowHook:
        return AsyncDataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
            cancel_timeout=self.cancel_timeout,
        )


class DataflowJobMetricsTrigger(BaseTrigger):
    """
    Trigger that checks for metrics associated with a Dataflow job.

    :param job_id: Required. ID of the job.
    :param project_id: Required. The Google Cloud project ID in which the job was started.
    :param location: Optional. The location where the job is executed. If set to None then
        the value of DEFAULT_DATAFLOW_LOCATION will be used.
    :param gcp_conn_id: The connection ID to use for connecting to Google Cloud.
    :param poll_sleep: Time (seconds) to wait between two consecutive calls to check the job.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param fail_on_terminal_state: If set to True the trigger will yield a TriggerEvent with
        error status if the job reaches a terminal state.
    """

    def __init__(
        self,
        job_id: str,
        project_id: str | None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        fail_on_terminal_state: bool = True,
    ):
        super().__init__()
        self.project_id = project_id
        self.job_id = job_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain
        self.fail_on_terminal_state = fail_on_terminal_state

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobMetricsTrigger",
            {
                "project_id": self.project_id,
                "job_id": self.job_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_sleep": self.poll_sleep,
                "impersonation_chain": self.impersonation_chain,
                "fail_on_terminal_state": self.fail_on_terminal_state,
            },
        )

    async def run(self):
        """
        Loop until a terminal job status or any job metrics are returned.

        Yields a TriggerEvent with success status, if the client returns any job metrics
        and fail_on_terminal_state attribute is False.

        Yields a TriggerEvent with error status, if the client returns a job status with
        a terminal state value and fail_on_terminal_state attribute is True.

        Yields a TriggerEvent with error status, if any exception is raised while looping.

        In any other case the Trigger will wait for a specified amount of time
        stored in self.poll_sleep variable.
        """
        try:
            while True:
                job_status = await self.async_hook.get_job_status(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location,
                )
                job_metrics = await self.get_job_metrics()
                if self.fail_on_terminal_state and job_status.name in DataflowJobStatus.TERMINAL_STATES:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Job with id '{self.job_id}' is already in terminal state: {job_status.name}",
                            "result": None,
                        }
                    )
                    return
                if job_metrics:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"Detected {len(job_metrics)} metrics for job '{self.job_id}'",
                            "result": job_metrics,
                        }
                    )
                    return
                self.log.info("Sleeping for %s seconds.", self.poll_sleep)
                await asyncio.sleep(self.poll_sleep)
        except Exception as e:
            self.log.error("Exception occurred while checking for job's metrics!")
            yield TriggerEvent({"status": "error", "message": str(e), "result": None})

    async def get_job_metrics(self) -> list[dict[str, Any]]:
        """Wait for the Dataflow client response and then return it in a serialized list."""
        job_response: JobMetrics = await self.async_hook.get_job_metrics(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
        )
        return self._get_metrics_from_job_response(job_response)

    def _get_metrics_from_job_response(self, job_response: JobMetrics) -> list[dict[str, Any]]:
        """Return a list of serialized MetricUpdate objects."""
        return [MetricUpdate.to_dict(metric) for metric in job_response.metrics]

    @cached_property
    def async_hook(self) -> AsyncDataflowHook:
        return AsyncDataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
        )


class DataflowJobAutoScalingEventTrigger(BaseTrigger):
    """
    Trigger that checks for autoscaling events associated with a Dataflow job.

    :param job_id: Required. ID of the job.
    :param project_id: Required. The Google Cloud project ID in which the job was started.
    :param location: Optional. The location where the job is executed. If set to None then
        the value of DEFAULT_DATAFLOW_LOCATION will be used.
    :param gcp_conn_id: The connection ID to use for connecting to Google Cloud.
    :param poll_sleep: Time (seconds) to wait between two consecutive calls to check the job.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param fail_on_terminal_state: If set to True the trigger will yield a TriggerEvent with
        error status if the job reaches a terminal state.
    """

    def __init__(
        self,
        job_id: str,
        project_id: str | None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        fail_on_terminal_state: bool = True,
    ):
        super().__init__()
        self.project_id = project_id
        self.job_id = job_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain
        self.fail_on_terminal_state = fail_on_terminal_state

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobAutoScalingEventTrigger",
            {
                "project_id": self.project_id,
                "job_id": self.job_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_sleep": self.poll_sleep,
                "impersonation_chain": self.impersonation_chain,
                "fail_on_terminal_state": self.fail_on_terminal_state,
            },
        )

    async def run(self):
        """
        Loop until a terminal job status or any autoscaling events are returned.

        Yields a TriggerEvent with success status, if the client returns any autoscaling events
        and fail_on_terminal_state attribute is False.

        Yields a TriggerEvent with error status, if the client returns a job status with
        a terminal state value and fail_on_terminal_state attribute is True.

        Yields a TriggerEvent with error status, if any exception is raised while looping.

        In any other case the Trigger will wait for a specified amount of time
        stored in self.poll_sleep variable.
        """
        try:
            while True:
                job_status = await self.async_hook.get_job_status(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location,
                )
                autoscaling_events = await self.list_job_autoscaling_events()
                if self.fail_on_terminal_state and job_status.name in DataflowJobStatus.TERMINAL_STATES:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Job with id '{self.job_id}' is already in terminal state: {job_status.name}",
                            "result": None,
                        }
                    )
                    return
                if autoscaling_events:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"Detected {len(autoscaling_events)} autoscaling events for job '{self.job_id}'",
                            "result": autoscaling_events,
                        }
                    )
                    return
                self.log.info("Sleeping for %s seconds.", self.poll_sleep)
                await asyncio.sleep(self.poll_sleep)
        except Exception as e:
            self.log.error("Exception occurred while checking for job's autoscaling events!")
            yield TriggerEvent({"status": "error", "message": str(e), "result": None})

    async def list_job_autoscaling_events(self) -> list[dict[str, str | dict]]:
        """Wait for the Dataflow client response and then return it in a serialized list."""
        job_response: ListJobMessagesAsyncPager = await self.async_hook.list_job_messages(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
        )
        return self._get_autoscaling_events_from_job_response(job_response)

    def _get_autoscaling_events_from_job_response(
        self, job_response: ListJobMessagesAsyncPager
    ) -> list[dict[str, str | dict]]:
        """Return a list of serialized AutoscalingEvent objects."""
        return [AutoscalingEvent.to_dict(event) for event in job_response.autoscaling_events]

    @cached_property
    def async_hook(self) -> AsyncDataflowHook:
        return AsyncDataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
        )


class DataflowJobMessagesTrigger(BaseTrigger):
    """
    Trigger that checks for job messages associated with a Dataflow job.

    :param job_id: Required. ID of the job.
    :param project_id: Required. The Google Cloud project ID in which the job was started.
    :param location: Optional. The location where the job is executed. If set to None then
        the value of DEFAULT_DATAFLOW_LOCATION will be used.
    :param gcp_conn_id: The connection ID to use for connecting to Google Cloud.
    :param poll_sleep: Time (seconds) to wait between two consecutive calls to check the job.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param fail_on_terminal_state: If set to True the trigger will yield a TriggerEvent with
        error status if the job reaches a terminal state.
    """

    def __init__(
        self,
        job_id: str,
        project_id: str | None,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        poll_sleep: int = 10,
        impersonation_chain: str | Sequence[str] | None = None,
        fail_on_terminal_state: bool = True,
    ):
        super().__init__()
        self.project_id = project_id
        self.job_id = job_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.poll_sleep = poll_sleep
        self.impersonation_chain = impersonation_chain
        self.fail_on_terminal_state = fail_on_terminal_state

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.dataflow.DataflowJobMessagesTrigger",
            {
                "project_id": self.project_id,
                "job_id": self.job_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_sleep": self.poll_sleep,
                "impersonation_chain": self.impersonation_chain,
                "fail_on_terminal_state": self.fail_on_terminal_state,
            },
        )

    async def run(self):
        """
        Loop until a terminal job status or any job messages are returned.

        Yields a TriggerEvent with success status, if the client returns any job messages
        and fail_on_terminal_state attribute is False.

        Yields a TriggerEvent with error status, if the client returns a job status with
        a terminal state value and fail_on_terminal_state attribute is True.

        Yields a TriggerEvent with error status, if any exception is raised while looping.

        In any other case the Trigger will wait for a specified amount of time
        stored in self.poll_sleep variable.
        """
        try:
            while True:
                job_status = await self.async_hook.get_job_status(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location,
                )
                job_messages = await self.list_job_messages()
                if self.fail_on_terminal_state and job_status.name in DataflowJobStatus.TERMINAL_STATES:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Job with id '{self.job_id}' is already in terminal state: {job_status.name}",
                            "result": None,
                        }
                    )
                    return
                if job_messages:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"Detected {len(job_messages)} job messages for job '{self.job_id}'",
                            "result": job_messages,
                        }
                    )
                    return
                self.log.info("Sleeping for %s seconds.", self.poll_sleep)
                await asyncio.sleep(self.poll_sleep)
        except Exception as e:
            self.log.error("Exception occurred while checking for job's messages!")
            yield TriggerEvent({"status": "error", "message": str(e), "result": None})

    async def list_job_messages(self) -> list[dict[str, str | dict]]:
        """Wait for the Dataflow client response and then return it in a serialized list."""
        job_response: ListJobMessagesAsyncPager = await self.async_hook.list_job_messages(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
        )
        return self._get_job_messages_from_job_response(job_response)

    def _get_job_messages_from_job_response(
        self, job_response: ListJobMessagesAsyncPager
    ) -> list[dict[str, str | dict]]:
        """Return a list of serialized JobMessage objects."""
        return [JobMessage.to_dict(message) for message in job_response.job_messages]

    @cached_property
    def async_hook(self) -> AsyncDataflowHook:
        return AsyncDataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            poll_sleep=self.poll_sleep,
            impersonation_chain=self.impersonation_chain,
        )
