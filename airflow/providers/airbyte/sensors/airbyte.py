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
"""This module contains a Airbyte Job sensor."""

from airflow.exceptions import AirflowException
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class AirbyteJobSensor(BaseSensorOperator):
    """
    Check for the state of a previously submitted Dataproc job.

    :param project_id: The ID of the google cloud project in which
        to create the cluster. (templated)
    :type project_id: str
    :param dataproc_job_id: The Dataproc job ID to poll. (templated)
    :type dataproc_job_id: str
    :param location: Required. The Cloud Dataproc region in which to handle the request. (templated)
    :type location: str
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud Platform.
    :type gcp_conn_id: str
    """

    template_fields = 'airbyte_job_id'
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
        self,
        *,
        airbyte_job_id: str,
        airbyte_conn_id: str = 'airbyte_conn_id',
        api_version: str = "v1",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.airbyte_job_id = airbyte_job_id
        self.api_version = api_version

    def poke(self, context: dict) -> bool:
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)
        job = hook.get_job(job_id=self.airbyte_job_id)
        status = job.json().get('job').get('status')

        if status == hook.FAILED:
            raise AirflowException(f'Job failed: \n{job}')
        elif status == hook.CANCELLED:
            raise AirflowException(f'Job was cancelled: \n{job}')
        elif status == hook.SUCCEEDED:
            self.log.debug("Job %s completed successfully.", self.airbyte_job_id)
            return True
        elif status == hook.ERROR:
            self.log.debug("Job %s attempt has failed.", self.airbyte_job_id)

        self.log.info("Waiting for job %s to complete.", self.airbyte_job_id)
        return False
