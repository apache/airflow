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
"""This module contains Google Dataplex sensors."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.api_core.retry import Retry

    from airflow.utils.context import Context

from google.api_core.exceptions import GoogleAPICallError
from google.api_core.gapic_v1.method import DEFAULT, _MethodDefault
from google.cloud.dataplex_v1.types import DataScanJob

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataplex import (
    AirflowDataQualityScanException,
    AirflowDataQualityScanResultTimeoutException,
    DataplexHook,
)
from airflow.sensors.base import BaseSensorOperator


class TaskState:
    """Dataplex Task states."""

    STATE_UNSPECIFIED = 0
    ACTIVE = 1
    CREATING = 2
    DELETING = 3
    ACTION_REQUIRED = 4


class DataplexTaskStateSensor(BaseSensorOperator):
    """
    Check the status of the Dataplex task.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param lake_id: Required. The ID of the Google Cloud lake that the task belongs to.
    :param dataplex_task_id: Required. Task identifier.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ["dataplex_task_id"]

    def __init__(
        self,
        project_id: str,
        region: str,
        lake_id: str,
        dataplex_task_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.lake_id = lake_id
        self.dataplex_task_id = dataplex_task_id
        self.api_version = api_version
        self.retry = retry
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def poke(self, context: Context) -> bool:
        self.log.info("Waiting for task %s to be %s", self.dataplex_task_id, TaskState.ACTIVE)
        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        task = hook.get_task(
            project_id=self.project_id,
            region=self.region,
            lake_id=self.lake_id,
            dataplex_task_id=self.dataplex_task_id,
            retry=self.retry,
            metadata=self.metadata,
        )
        task_status = task.state

        if task_status == TaskState.DELETING:
            message = f"Task is going to be deleted {self.dataplex_task_id}"
            raise AirflowException(message)

        self.log.info("Current status of the Dataplex task %s => %s", self.dataplex_task_id, task_status)

        return task_status == TaskState.ACTIVE


class DataplexDataQualityJobStatusSensor(BaseSensorOperator):
    """
    Check the status of the Dataplex DataQuality job.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param data_scan_id: Required. Data Quality scan identifier.
    :param job_id: Required. Job ID.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param result_timeout: Value in seconds for which operator will wait for the Data Quality scan result.
        Throws exception if there is no result found after specified amount of seconds.
    :param fail_on_dq_failure: If set to true and not all Data Quality scan rules have been passed,
        an exception is thrown. If set to false and not all Data Quality scan rules have been passed,
        execution will finish with success.

    :return: Boolean indicating if the job run has reached the ``DataScanJob.State.SUCCEEDED``.
    """

    template_fields = ["job_id"]

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        job_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        fail_on_dq_failure: bool = False,
        result_timeout: float = 60.0 * 10,
        start_sensor_time: float | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.job_id = job_id
        self.api_version = api_version
        self.retry = retry
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.fail_on_dq_failure = fail_on_dq_failure
        self.result_timeout = result_timeout
        self.start_sensor_time = start_sensor_time

    def _duration(self):
        if not self.start_sensor_time:
            self.start_sensor_time = time.monotonic()
        return time.monotonic() - self.start_sensor_time

    def poke(self, context: Context) -> bool:
        self.log.info("Waiting for job %s to be %s", self.job_id, DataScanJob.State.SUCCEEDED)
        if self.result_timeout:
            duration = self._duration()
            if duration > self.result_timeout:
                message = (
                    f"Timeout: Data Quality scan {self.job_id} is not ready after {self.result_timeout}s"
                )
                raise AirflowDataQualityScanResultTimeoutException(message)

        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            job = hook.get_data_scan_job(
                project_id=self.project_id,
                region=self.region,
                data_scan_id=self.data_scan_id,
                job_id=self.job_id,
                timeout=self.timeout,
                retry=self.retry,
                metadata=self.metadata,
            )
        except GoogleAPICallError as e:
            message = f"Error occurred when trying to retrieve Data Quality scan job: {self.data_scan_id}"
            raise AirflowException(message, e)

        job_status = job.state
        self.log.info(
            "Current status of the Dataplex Data Quality scan job %s => %s", self.job_id, job_status
        )
        if job_status == DataScanJob.State.FAILED:
            message = f"Data Quality scan job failed: {self.job_id}"
            raise AirflowException(message)
        if job_status == DataScanJob.State.CANCELLED:
            message = f"Data Quality scan job cancelled: {self.job_id}"
            raise AirflowException(message)
        if self.fail_on_dq_failure:
            if job_status == DataScanJob.State.SUCCEEDED and not job.data_quality_result.passed:
                message = (
                    f"Data Quality job {self.job_id} execution failed due to failure of its scanning "
                    f"rules: {self.data_scan_id}"
                )
                raise AirflowDataQualityScanException(message)
        return job_status == DataScanJob.State.SUCCEEDED


class DataplexDataProfileJobStatusSensor(BaseSensorOperator):
    """
    Check the status of the Dataplex DataProfile job.

    :param project_id: Required. The ID of the Google Cloud project that the task belongs to.
    :param region: Required. The ID of the Google Cloud region that the task belongs to.
    :param data_scan_id: Required. Data Quality scan identifier.
    :param job_id: Required. Job ID.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param retry: A retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :param metadata: Additional metadata that is provided to the method.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param result_timeout: Value in seconds for which operator will wait for the Data Quality scan result.
        Throws exception if there is no result found after specified amount of seconds.

    :return: Boolean indicating if the job run has reached the ``DataScanJob.State.SUCCEEDED``.
    """

    template_fields = ["job_id"]

    def __init__(
        self,
        project_id: str,
        region: str,
        data_scan_id: str,
        job_id: str,
        api_version: str = "v1",
        retry: Retry | _MethodDefault = DEFAULT,
        metadata: Sequence[tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        result_timeout: float = 60.0 * 10,
        start_sensor_time: float | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.region = region
        self.data_scan_id = data_scan_id
        self.job_id = job_id
        self.api_version = api_version
        self.retry = retry
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.result_timeout = result_timeout
        self.start_sensor_time = start_sensor_time

    def _duration(self):
        if not self.start_sensor_time:
            self.start_sensor_time = time.monotonic()
        return time.monotonic() - self.start_sensor_time

    def poke(self, context: Context) -> bool:
        self.log.info("Waiting for job %s to be %s", self.job_id, DataScanJob.State.SUCCEEDED)
        if self.result_timeout:
            duration = self._duration()
            if duration > self.result_timeout:
                message = (
                    f"Timeout: Data Profile scan {self.job_id} is not ready after {self.result_timeout}s"
                )
                raise AirflowDataQualityScanResultTimeoutException(message)

        hook = DataplexHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        try:
            job = hook.get_data_scan_job(
                project_id=self.project_id,
                region=self.region,
                data_scan_id=self.data_scan_id,
                job_id=self.job_id,
                timeout=self.timeout,
                retry=self.retry,
                metadata=self.metadata,
            )
        except GoogleAPICallError as e:
            message = f"Error occurred when trying to retrieve Data Profile scan job: {self.data_scan_id}"
            raise AirflowException(message, e)

        job_status = job.state
        self.log.info(
            "Current status of the Dataplex Data Profile scan job %s => %s", self.job_id, job_status
        )
        if job_status == DataScanJob.State.FAILED:
            message = f"Data Profile scan job failed: {self.job_id}"
            raise AirflowException(message)
        if job_status == DataScanJob.State.CANCELLED:
            message = f"Data Profile scan job cancelled: {self.job_id}"
            raise AirflowException(message)
        return job_status == DataScanJob.State.SUCCEEDED
