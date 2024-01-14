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
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AirbyteTriggerSyncOperator(BaseOperator):
    """
    Submits a job to an Airbyte server to run a integration process between your source and destination.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AirbyteTriggerSyncOperator`

    :param airbyte_conn_id: Optional. The name of the Airflow connection to get connection
        information for Airbyte. Defaults to "airbyte_default".
    :param connection_id: Required. The Airbyte ConnectionId UUID between a source and destination.
    :param asynchronous: Optional. Flag to get job_id after submitting the job to the Airbyte API.
        This is useful for submitting long running jobs and
        waiting on them asynchronously using the AirbyteJobSensor. Defaults to False.
    :param api_version: Optional. Airbyte API version. Defaults to "v1".
    :param wait_seconds: Optional. Number of seconds between checks. Only used when ``asynchronous`` is False.
        Defaults to 3 seconds.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Only used when ``asynchronous`` is False. Defaults to 3600 seconds (or 1 hour).
    """

    template_fields: Sequence[str] = ("connection_id",)

    def __init__(
        self,
        connection_id: str,
        airbyte_conn_id: str = "airbyte_default",
        asynchronous: bool = False,
        api_version: str = "v1",
        wait_seconds: float = 3,
        timeout: float = 3600,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.connection_id = connection_id
        self.timeout = timeout
        self.api_version = api_version
        self.wait_seconds = wait_seconds
        self.asynchronous = asynchronous

    def execute(self, context: Context) -> None:
        """Create Airbyte Job and wait to finish."""
        self.hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)
        job_object = self.hook.submit_sync_connection(connection_id=self.connection_id)
        self.job_id = job_object.json()["job"]["id"]

        self.log.info("Job %s was submitted to Airbyte Server", self.job_id)
        if not self.asynchronous:
            self.log.info("Waiting for job %s to complete", self.job_id)
            self.hook.wait_for_job(job_id=self.job_id, wait_seconds=self.wait_seconds, timeout=self.timeout)
            self.log.info("Job %s completed successfully", self.job_id)

        return self.job_id

    def on_kill(self):
        """Cancel the job if task is cancelled."""
        if self.job_id:
            self.log.info("on_kill: cancel the airbyte Job %s", self.job_id)
            self.hook.cancel_job(self.job_id)
