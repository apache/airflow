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
"""This module contains a Google Cloud Dataflow sensor."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataflow import (
    DEFAULT_DATAFLOW_LOCATION,
    DataflowHook,
    DataflowJobStatus,
)
from airflow.providers.google.cloud.triggers.dataflow import (
    DataflowJobAutoScalingEventTrigger,
    DataflowJobMessagesTrigger,
    DataflowJobMetricsTrigger,
    DataflowJobStatusTrigger,
)
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DataflowJobStatusSensor(BaseSensorOperator):
    """
    Checks for the status of a job in Google Cloud Dataflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowJobStatusSensor`

    :param job_id: ID of the job to be checked.
    :param expected_statuses: The expected state(s) of the operation.
        See:
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs#Job.JobState
    :param project_id: Optional, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param location: The location of the Dataflow job (for example europe-west1). See:
        https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: If True, run the sensor in the deferrable mode.
    :param poll_interval: Time (seconds) to wait between two consecutive calls to check the job.
    """

    template_fields: Sequence[str] = ("job_id",)

    def __init__(
        self,
        *,
        job_id: str,
        expected_statuses: set[str] | str,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.expected_statuses = (
            {expected_statuses} if isinstance(expected_statuses, str) else expected_statuses
        )
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def poke(self, context: Context) -> bool:
        self.log.info(
            "Waiting for job %s to be in one of the states: %s.",
            self.job_id,
            ", ".join(self.expected_statuses),
        )

        job = self.hook.get_job(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
        )

        job_status = job["currentState"]
        self.log.debug("Current job status for job %s: %s.", self.job_id, job_status)

        if job_status in self.expected_statuses:
            return True
        elif job_status in DataflowJobStatus.TERMINAL_STATES:
            message = f"Job with id '{self.job_id}' is already in terminal state: {job_status}"
            raise AirflowException(message)

        return False

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        if not self.deferrable:
            super().execute(context)
        elif not self.poke(context=context):
            self.defer(
                timeout=self.execution_timeout,
                trigger=DataflowJobStatusTrigger(
                    job_id=self.job_id,
                    expected_statuses=self.expected_statuses,
                    project_id=self.project_id,
                    location=self.location,
                    gcp_conn_id=self.gcp_conn_id,
                    poll_sleep=self.poll_interval,
                    impersonation_chain=self.impersonation_chain,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, str | list]) -> bool:
        """
        Execute this method when the task resumes its execution on the worker after deferral.

        Returns True if the trigger returns an event with the success status, otherwise raises
        an exception.
        """
        if event["status"] == "success":
            self.log.info(event["message"])
            return True
        raise AirflowException(f"Sensor failed with the following message: {event['message']}")

    @cached_property
    def hook(self) -> DataflowHook:
        return DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class DataflowJobMetricsSensor(BaseSensorOperator):
    """
    Checks for metrics associated with a single job in Google Cloud Dataflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowJobMetricsSensor`

    :param job_id: ID of the job to be checked.
    :param callback: callback which is called with list of read job metrics
        See:
        https://cloud.google.com/dataflow/docs/reference/rest/v1b3/MetricUpdate
    :param fail_on_terminal_state: If set to true sensor will raise Exception when
        job is in terminal state
    :param project_id: Optional, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param location: The location of the Dataflow job (for example europe-west1). See:
        https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: If True, run the sensor in the deferrable mode.
    :param poll_interval: Time (seconds) to wait between two consecutive calls to check the job.

    """

    template_fields: Sequence[str] = ("job_id",)

    def __init__(
        self,
        *,
        job_id: str,
        callback: Callable | None = None,
        fail_on_terminal_state: bool = True,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.project_id = project_id
        self.callback = callback
        self.fail_on_terminal_state = fail_on_terminal_state
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def poke(self, context: Context) -> bool:
        if self.fail_on_terminal_state:
            job = self.hook.get_job(
                job_id=self.job_id,
                project_id=self.project_id,
                location=self.location,
            )
            job_status = job["currentState"]
            if job_status in DataflowJobStatus.TERMINAL_STATES:
                message = f"Job with id '{self.job_id}' is already in terminal state: {job_status}"
                raise AirflowException(message)

        result = self.hook.fetch_job_metrics_by_id(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
        )
        return result["metrics"] if self.callback is None else self.callback(result["metrics"])

    def execute(self, context: Context) -> Any:
        """Airflow runs this method on the worker and defers using the trigger."""
        if not self.deferrable:
            super().execute(context)
        else:
            self.defer(
                timeout=self.execution_timeout,
                trigger=DataflowJobMetricsTrigger(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location,
                    gcp_conn_id=self.gcp_conn_id,
                    poll_sleep=self.poll_interval,
                    impersonation_chain=self.impersonation_chain,
                    fail_on_terminal_state=self.fail_on_terminal_state,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, str | list]) -> Any:
        """
        Execute this method when the task resumes its execution on the worker after deferral.

        If the trigger returns an event with success status - passes the event result to the callback function.
        Returns the event result if no callback function is provided.

        If the trigger returns an event with error status - raises an exception.
        """
        if event["status"] == "success":
            self.log.info(event["message"])
            return event["result"] if self.callback is None else self.callback(event["result"])
        raise AirflowException(f"Sensor failed with the following message: {event['message']}")

    @cached_property
    def hook(self) -> DataflowHook:
        return DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class DataflowJobMessagesSensor(BaseSensorOperator):
    """
    Checks for job messages associated with a single job in Google Cloud Dataflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowJobMessagesSensor`

    :param job_id: ID of the Dataflow job to be checked.
    :param callback: a function that can accept a list of serialized job messages.
        It can do whatever you want it to do. If the callback function is not provided,
        then on successful completion the task will exit with True value.
        For more info about the job message content see:
        https://cloud.google.com/python/docs/reference/dataflow/latest/google.cloud.dataflow_v1beta3.types.JobMessage
    :param fail_on_terminal_state: If set to True the sensor will raise an exception when the job reaches a terminal state.
        No job messages will be returned.
    :param project_id: Optional, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param location: The location of the Dataflow job (for example europe-west1).
        If set to None then the value of DEFAULT_DATAFLOW_LOCATION will be used.
        See: https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: If True, run the sensor in the deferrable mode.
    :param poll_interval: Time (seconds) to wait between two consecutive calls to check the job.
    """

    template_fields: Sequence[str] = ("job_id",)

    def __init__(
        self,
        *,
        job_id: str,
        callback: Callable | None = None,
        fail_on_terminal_state: bool = True,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.project_id = project_id
        self.callback = callback
        self.fail_on_terminal_state = fail_on_terminal_state
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def poke(self, context: Context) -> bool:
        if self.fail_on_terminal_state:
            job = self.hook.get_job(
                job_id=self.job_id,
                project_id=self.project_id,
                location=self.location,
            )
            job_status = job["currentState"]
            if job_status in DataflowJobStatus.TERMINAL_STATES:
                message = f"Job with id '{self.job_id}' is already in terminal state: {job_status}"
                raise AirflowException(message)

        result = self.hook.fetch_job_messages_by_id(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
        )

        return result if self.callback is None else self.callback(result)

    def execute(self, context: Context) -> Any:
        """Airflow runs this method on the worker and defers using the trigger."""
        if not self.deferrable:
            super().execute(context)
        else:
            self.defer(
                timeout=self.execution_timeout,
                trigger=DataflowJobMessagesTrigger(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location,
                    gcp_conn_id=self.gcp_conn_id,
                    poll_sleep=self.poll_interval,
                    impersonation_chain=self.impersonation_chain,
                    fail_on_terminal_state=self.fail_on_terminal_state,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, str | list]) -> Any:
        """
        Execute this method when the task resumes its execution on the worker after deferral.

        If the trigger returns an event with success status - passes the event result to the callback function.
        Returns the event result if no callback function is provided.

        If the trigger returns an event with error status - raises an exception.
        """
        if event["status"] == "success":
            self.log.info(event["message"])
            return event["result"] if self.callback is None else self.callback(event["result"])
        raise AirflowException(f"Sensor failed with the following message: {event['message']}")

    @cached_property
    def hook(self) -> DataflowHook:
        return DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )


class DataflowJobAutoScalingEventsSensor(BaseSensorOperator):
    """
    Checks for autoscaling events associated with a single job in Google Cloud Dataflow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataflowJobAutoScalingEventsSensor`

    :param job_id: ID of the Dataflow job to be checked.
    :param callback: a function that can accept a list of serialized autoscaling events.
        It can do whatever you want it to do. If the callback function is not provided,
        then on successful completion the task will exit with True value.
        For more info about the autoscaling event content see:
        https://cloud.google.com/python/docs/reference/dataflow/latest/google.cloud.dataflow_v1beta3.types.AutoscalingEvent
    :param fail_on_terminal_state: If set to True the sensor will raise an exception when the job reaches a terminal state.
        No autoscaling events will be returned.
    :param project_id: Optional, the Google Cloud project ID in which to start a job.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
    :param location: The location of the Dataflow job (for example europe-west1).
        If set to None then the value of DEFAULT_DATAFLOW_LOCATION will be used.
        See: https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: If True, run the sensor in the deferrable mode.
    :param poll_interval: Time (seconds) to wait between two consecutive calls to check the job.
    """

    template_fields: Sequence[str] = ("job_id",)

    def __init__(
        self,
        *,
        job_id: str,
        callback: Callable | None = None,
        fail_on_terminal_state: bool = True,
        project_id: str = PROVIDE_PROJECT_ID,
        location: str = DEFAULT_DATAFLOW_LOCATION,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job_id = job_id
        self.project_id = project_id
        self.callback = callback
        self.fail_on_terminal_state = fail_on_terminal_state
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def poke(self, context: Context) -> bool:
        if self.fail_on_terminal_state:
            job = self.hook.get_job(
                job_id=self.job_id,
                project_id=self.project_id,
                location=self.location,
            )
            job_status = job["currentState"]
            if job_status in DataflowJobStatus.TERMINAL_STATES:
                message = f"Job with id '{self.job_id}' is already in terminal state: {job_status}"
                raise AirflowException(message)

        result = self.hook.fetch_job_autoscaling_events_by_id(
            job_id=self.job_id,
            project_id=self.project_id,
            location=self.location,
        )

        return result if self.callback is None else self.callback(result)

    def execute(self, context: Context) -> Any:
        """Airflow runs this method on the worker and defers using the trigger."""
        if not self.deferrable:
            super().execute(context)
        else:
            self.defer(
                trigger=DataflowJobAutoScalingEventTrigger(
                    job_id=self.job_id,
                    project_id=self.project_id,
                    location=self.location,
                    gcp_conn_id=self.gcp_conn_id,
                    poll_sleep=self.poll_interval,
                    impersonation_chain=self.impersonation_chain,
                    fail_on_terminal_state=self.fail_on_terminal_state,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, str | list]) -> Any:
        """
        Execute this method when the task resumes its execution on the worker after deferral.

        If the trigger returns an event with success status - passes the event result to the callback function.
        Returns the event result if no callback function is provided.

        If the trigger returns an event with error status - raises an exception.
        """
        if event["status"] == "success":
            self.log.info(event["message"])
            return event["result"] if self.callback is None else self.callback(event["result"])
        raise AirflowException(f"Sensor failed with the following message: {event['message']}")

    @cached_property
    def hook(self) -> DataflowHook:
        return DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
