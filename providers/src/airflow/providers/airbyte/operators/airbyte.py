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

import time
from typing import TYPE_CHECKING, Any, Sequence

from airbyte_api.models import JobStatusEnum

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.providers.airbyte.triggers.airbyte import AirbyteSyncTrigger

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
    :param deferrable: Run operator in the deferrable mode.
    :param api_version: Optional. Airbyte API version. Defaults to "v1".
    :param wait_seconds: Optional. Number of seconds between checks. Only used when ``asynchronous`` is False.
        Defaults to 3 seconds.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Only used when ``asynchronous`` is False. Defaults to 3600 seconds (or 1 hour).
    """

    template_fields: Sequence[str] = ("connection_id",)
    ui_color = "#6C51FD"

    def __init__(
        self,
        connection_id: str,
        airbyte_conn_id: str = "airbyte_default",
        asynchronous: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
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
        self.deferrable = deferrable

    def execute(self, context: Context) -> None:
        """Create Airbyte Job and wait to finish."""
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)
        job_object = hook.submit_sync_connection(connection_id=self.connection_id)
        self.job_id = job_object.job_id
        state = job_object.status
        end_time = time.time() + self.timeout

        self.log.info("Job %s was submitted to Airbyte Server", self.job_id)

        if self.asynchronous:
            self.log.info("Async Task returning job_id %s", self.job_id)
            return self.job_id

        if not self.deferrable:
            hook.wait_for_job(job_id=self.job_id, wait_seconds=self.wait_seconds, timeout=self.timeout)
        else:
            if state in (JobStatusEnum.RUNNING, JobStatusEnum.PENDING, JobStatusEnum.INCOMPLETE):
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=AirbyteSyncTrigger(
                        conn_id=self.airbyte_conn_id,
                        job_id=self.job_id,
                        end_time=end_time,
                        poll_interval=60,
                    ),
                    method_name="execute_complete",
                )
            elif state == JobStatusEnum.SUCCEEDED:
                self.log.info("Job %s completed successfully", self.job_id)
                return
            elif state == JobStatusEnum.FAILED:
                raise AirflowException(f"Job failed:\n{self.job_id}")
            elif state == JobStatusEnum.CANCELLED:
                raise AirflowException(f"Job was cancelled:\n{self.job_id}")
            else:
                raise AirflowException(f"Encountered unexpected state `{state}` for job_id `{self.job_id}")

        return self.job_id

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """
        Invoke this callback when the trigger fires; return immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info("%s completed successfully.", self.task_id)
        return None

    def on_kill(self):
        """Cancel the job if task is cancelled."""
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_type=self.api_type)
        if self.job_id:
            self.log.info("on_kill: cancel the airbyte Job %s", self.job_id)
            hook.cancel_job(self.job_id)
