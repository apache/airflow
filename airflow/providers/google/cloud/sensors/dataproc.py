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
# pylint: disable=C0302
import warnings
from typing import Optional

from google.cloud.dataproc_v1beta2.types import JobStatus

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.sensors.base import BaseSensorOperator


class DataprocJobSensor(BaseSensorOperator):
    """
    Check for the state of a previously submitted Dataproc job.

    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :type project_id: str
    :param dataproc_job_id: The Dataproc job ID to poll. (templated)
    :type dataproc_job_id: str
    :param region: Required. The Cloud Dataproc region in which to handle the request. (templated)
    :type region: str
    :param location: (To be deprecated). The Cloud Dataproc region in which to handle the request. (templated)
    :type location: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = ('project_id', 'region', 'dataproc_job_id')
    ui_color = '#f0eee4'

    def __init__(
        self,
        *,
        project_id: str,
        dataproc_job_id: str,
        region: str = None,
        location: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        **kwargs,
    ) -> None:
        if region is None:
            if location is not None:
                warnings.warn(
                    "Parameter `location` will be deprecated. "
                    "Please provide value through `region` parameter instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                region = location
            else:
                raise TypeError("missing 1 required keyword argument: 'region'")
        super().__init__(**kwargs)
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.dataproc_job_id = dataproc_job_id
        self.region = region

    def poke(self, context: dict) -> bool:
        hook = DataprocHook(gcp_conn_id=self.gcp_conn_id)
        job = hook.get_job(job_id=self.dataproc_job_id, region=self.region, project_id=self.project_id)
        state = job.status.state

        if state == JobStatus.State.ERROR:
            raise AirflowException(f'Job failed:\n{job}')
        elif state in {
            JobStatus.State.CANCELLED,
            JobStatus.State.CANCEL_PENDING,
            JobStatus.State.CANCEL_STARTED,
        }:
            raise AirflowException(f'Job was cancelled:\n{job}')
        elif JobStatus.State.DONE == state:
            self.log.debug("Job %s completed successfully.", self.dataproc_job_id)
            return True
        elif JobStatus.State.ATTEMPT_FAILURE == state:
            self.log.debug("Job %s attempt has failed.", self.dataproc_job_id)

        self.log.info("Waiting for job %s to complete.", self.dataproc_job_id)
        return False
