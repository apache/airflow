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
from typing import TYPE_CHECKING, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AirbyteTriggerSyncOperator(BaseOperator):
    """
    This operator allows you to submit a job to an Airbyte server to run a integration
    process between your source and destination.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AirbyteTriggerSyncOperator`

    :param airbyte_conn_id: Required. The name of the Airflow connection to get connection
        information for Airbyte.
    :param connection_id: Required. The Airbyte ConnectionId UUID between a source and destination.
    :param asynchronous: Optional. Flag to get job_id after submitting the job to the Airbyte API.
        This is useful for submitting long running jobs and
        waiting on them asynchronously using the AirbyteJobSensor.
    :param api_version: Optional. Airbyte API version.
    :param wait_seconds: Optional. Number of seconds between checks. Only used when ``asynchronous`` is False.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Only used when ``asynchronous`` is False.
    """

    template_fields: Sequence[str] = ('connection_id',)

    def __init__(
        self,
        connection_id: str,
        airbyte_conn_id: str = "airbyte_default",
        asynchronous: Optional[bool] = False,
        api_version: str = "v1",
        wait_seconds: float = 3,
        timeout: Optional[float] = 3600,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.connection_id = connection_id
        self.timeout = timeout
        self.api_version = api_version
        self.wait_seconds = wait_seconds
        self.asynchronous = asynchronous

    def execute(self, context: 'Context') -> None:
        """Create Airbyte Job and wait to finish"""
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)
        job_object = hook.submit_sync_connection(connection_id=self.connection_id)
        job_id = job_object.json()['job']['id']

        self.log.info("Job %s was submitted to Airbyte Server", job_id)
        if not self.asynchronous:
            self.log.info('Waiting for job %s to complete', job_id)
            hook.wait_for_job(job_id=job_id, wait_seconds=self.wait_seconds, timeout=self.timeout)
            self.log.info('Job %s completed successfully', job_id)

        return job_id
