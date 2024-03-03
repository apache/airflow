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
"""This module contains a Dataproc Job sensor."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Sequence

from google.api_core.exceptions import ServerError
from google.cloud.dataproc_v1.types import Batch, JobStatus

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DataprocJobSensor(BaseSensorOperator):
    """
    Check for the state of a previously submitted Dataproc job.

    :param dataproc_job_id: The Dataproc job ID to poll. (templated)
    :param region: Required. The Cloud Dataproc region in which to handle the request. (templated)
    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :param wait_timeout: How many seconds wait for job to be ready.
    """

    template_fields: Sequence[str] = ("project_id", "region", "dataproc_job_id")
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        dataproc_job_id: str,
        region: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        wait_timeout: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.dataproc_job_id = dataproc_job_id
        self.region = region
        self.wait_timeout = wait_timeout
        self.start_sensor_time: float | None = None

    def execute(self, context: Context) -> None:
        self.start_sensor_time = time.monotonic()
        super().execute(context)

    def _duration(self):
        return time.monotonic() - self.start_sensor_time

    def poke(self, context: Context) -> bool:
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        if self.wait_timeout:
            try:
                job = hook.get_job(
                    job_id=self.dataproc_job_id, region=self.region, project_id=self.project_id
                )
            except ServerError as err:
                duration = self._duration()
                self.log.info("DURATION RUN: %f", duration)
                if duration > self.wait_timeout:
                    # TODO: remove this if check when min_airflow_version is set to higher than 2.7.1
                    message = (
                        f"Timeout: dataproc job {self.dataproc_job_id} "
                        f"is not ready after {self.wait_timeout}s"
                    )
                    if self.soft_fail:
                        raise AirflowSkipException(message)
                    raise AirflowException(message)
                self.log.info("Retrying. Dataproc API returned server error when waiting for job: %s", err)
                return False
        else:
            job = hook.get_job(job_id=self.dataproc_job_id, region=self.region, project_id=self.project_id)

        state = job.status.state
        if state == JobStatus.State.ERROR:
            # TODO: remove this if check when min_airflow_version is set to higher than 2.7.1
            message = f"Job failed:\n{job}"
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)
        elif state in {
            JobStatus.State.CANCELLED,
            JobStatus.State.CANCEL_PENDING,
            JobStatus.State.CANCEL_STARTED,
        }:
            # TODO: remove this if check when min_airflow_version is set to higher than 2.7.1
            message = f"Job was cancelled:\n{job}"
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)
        elif JobStatus.State.DONE == state:
            self.log.debug("Job %s completed successfully.", self.dataproc_job_id)
            return True
        elif JobStatus.State.ATTEMPT_FAILURE == state:
            self.log.debug("Job %s attempt has failed.", self.dataproc_job_id)

        self.log.info("Waiting for job %s to complete.", self.dataproc_job_id)
        return False


class DataprocBatchSensor(BaseSensorOperator):
    """
    Check for the state of batch.

    :param batch_id: The Dataproc batch ID to poll. (templated)
    :param region: Required. The Cloud Dataproc region in which to handle the request. (templated)
    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :param wait_timeout: How many seconds wait for job to be ready.
    """

    template_fields: Sequence[str] = ("project_id", "region", "batch_id")
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        batch_id: str,
        region: str,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        wait_timeout: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.batch_id = batch_id
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.region = region
        self.wait_timeout = wait_timeout
        self.start_sensor_time: float | None = None

    def execute(self, context: Context) -> None:
        self.start_sensor_time = time.monotonic()
        super().execute(context)

    def _duration(self):
        return time.monotonic() - self.start_sensor_time

    def poke(self, context: Context) -> bool:
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        if self.wait_timeout:
            try:
                batch = hook.get_batch(batch_id=self.batch_id, region=self.region, project_id=self.project_id)
            except ServerError as err:
                duration = self._duration()
                self.log.info("DURATION RUN: %f", duration)

                if duration > self.wait_timeout:
                    raise AirflowException(
                        f"Timeout: dataproc batch {self.batch_id} is not ready after {self.wait_timeout}s"
                    )
                self.log.info("Retrying. Dataproc API returned server error when waiting for batch: %s", err)
                return False
        else:
            batch = hook.get_batch(batch_id=self.batch_id, region=self.region, project_id=self.project_id)

        state = batch.state
        if state == Batch.State.FAILED:
            # TODO: remove this if check when min_airflow_version is set to higher than 2.7.1
            message = "Batch failed"
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)
        elif state in {
            Batch.State.CANCELLED,
            Batch.State.CANCELLING,
        }:
            # TODO: remove this if check when min_airflow_version is set to higher than 2.7.1
            message = "Batch was cancelled."
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)
        elif state == Batch.State.SUCCEEDED:
            self.log.debug("Batch %s completed successfully.", self.batch_id)
            return True

        self.log.info("Waiting for the batch %s to complete.", self.batch_id)
        return False
