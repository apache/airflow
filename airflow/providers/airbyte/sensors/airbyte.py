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
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Sequence

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.providers.airbyte.triggers.airbyte import AirbyteSyncTrigger
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AirbyteJobSensor(BaseSensorOperator):
    """
    Check for the state of a previously submitted Airbyte job.

    :param airbyte_job_id: Required. Id of the Airbyte job
    :param airbyte_conn_id: Optional. The name of the Airflow connection to get
        connection information for Airbyte. Defaults to "airbyte_default".
    :param api_version: Optional. Airbyte API version. Defaults to "v1".
    """

    template_fields: Sequence[str] = ("airbyte_job_id",)
    ui_color = "#6C51FD"

    def __init__(
        self,
        *,
        airbyte_job_id: int,
        airbyte_conn_id: str = "airbyte_default",
        api_version: str = "v1",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.airbyte_job_id = airbyte_job_id
        self.api_version = api_version

    def poke(self, context: Context) -> bool:
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)
        job = hook.get_job(job_id=self.airbyte_job_id)
        status = job.json()["job"]["status"]

        if status == hook.FAILED:
            # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
            message = f"Job failed: \n{job}"
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)
        elif status == hook.CANCELLED:
            # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
            message = f"Job was cancelled: \n{job}"
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)
        elif status == hook.SUCCEEDED:
            self.log.info("Job %s completed successfully.", self.airbyte_job_id)
            return True
        elif status == hook.ERROR:
            self.log.info("Job %s attempt has failed.", self.airbyte_job_id)

        self.log.info("Waiting for job %s to complete.", self.airbyte_job_id)
        return False


class AirbyteJobSensorAsync(AirbyteJobSensor):
    """
    A deferrable drop-in replacement for AirbyteJobSensor.

    Will defers itself to avoid taking up a worker slot while it is waiting.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:AirbyteJobSensorAsync`

    """

    def execute(self, context: Context) -> Any:
        """Submits a job which generates a run_id and gets deferred."""
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id)
        job = hook.get_job(job_id=(int(self.airbyte_job_id)))
        state = job.json()["job"]["status"]
        end_time = time.time() + self.timeout

        self.log.info("Airbyte Job Id: Job %s" % self.airbyte_job_id)

        if state in (hook.RUNNING, hook.PENDING, hook.INCOMPLETE):
            self.defer(
                timeout=self.execution_timeout,
                trigger=AirbyteSyncTrigger(
                    conn_id=self.airbyte_conn_id,
                    job_id=self.airbyte_job_id,
                    end_time=end_time,
                    poll_interval=60,
                ),
                method_name="execute_complete",
            )
        elif state == hook.SUCCEEDED:
            self.log.info("%s completed successfully.", self.task_id)
            return
        elif state == hook.ERROR:
            raise AirflowException(f"Job failed:\n{job}")
        elif state == hook.CANCELLED:
            raise AirflowException(f"Job was cancelled:\n{job}")
        else:
            raise Exception(f"Encountered unexpected state `{state}` for job_id `{self.airbyte_job_id}")

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info("%s completed successfully.", self.task_id)
        return None
