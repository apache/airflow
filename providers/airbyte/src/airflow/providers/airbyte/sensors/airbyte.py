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
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airbyte_api.models import JobStatusEnum

from airflow.configuration import conf
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.providers.airbyte.triggers.airbyte import AirbyteSyncTrigger
from airflow.providers.common.compat.sdk import AirflowException, BaseSensorOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class AirbyteJobSensor(BaseSensorOperator):
    """
    Check for the state of a previously submitted Airbyte job.

    :param airbyte_job_id: Required. Id of the Airbyte job.
    :param airbyte_conn_id: Optional. The name of the Airflow connection to get
        connection information for Airbyte. Defaults to "airbyte_default".
    :param deferrable: Optional. Runs the sensor in deferrable mode. Defaults
        to the config value "default_deferrable" or False, if not defined.
    :param api_version: Optional. Airbyte API version. Defaults to "v1".
    """

    template_fields: Sequence[str] = ("airbyte_job_id",)
    ui_color = "#6C51FD"

    def __init__(
        self,
        *,
        airbyte_job_id: int,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        airbyte_conn_id: str = "airbyte_default",
        api_version: str = "v1",
        **kwargs,
    ) -> None:
        if deferrable:
            if "poke_interval" not in kwargs:
                kwargs["poke_interval"] = 5

            if "timeout" not in kwargs:
                kwargs["timeout"] = 60 * 60 * 24 * 7

        super().__init__(**kwargs)
        self.deferrable = deferrable
        self.airbyte_conn_id = airbyte_conn_id
        self.airbyte_job_id = airbyte_job_id
        self.api_version = api_version

    def poke(self, context: Context) -> bool:
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)
        job = hook.get_job_details(job_id=self.airbyte_job_id)
        status = job.status

        if status == JobStatusEnum.FAILED:
            message = f"Job failed: \n{job}"
            self.log.debug("Failed with context: %s", context)
            raise AirflowException(message)
        if status == JobStatusEnum.CANCELLED:
            message = f"Job was cancelled: \n{job}"
            raise AirflowException(message)
        if status == JobStatusEnum.SUCCEEDED:
            self.log.info("Job %s completed successfully.", self.airbyte_job_id)
            return True

        self.log.info("Waiting for job %s to complete.", self.airbyte_job_id)
        return False

    def execute(self, context: Context) -> Any:
        """Submit a job which generates a run_id and gets deferred."""
        if not self.deferrable:
            super().execute(context)
        else:
            hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)
            job = hook.get_job_details(job_id=(int(self.airbyte_job_id)))
            state = job.status
            end_time = time.time() + self.timeout

            self.log.info("Airbyte Job Id: Job %s", self.airbyte_job_id)

            if state in (JobStatusEnum.RUNNING, JobStatusEnum.PENDING, JobStatusEnum.INCOMPLETE):
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
            elif state == JobStatusEnum.SUCCEEDED:
                self.log.info("%s completed successfully.", self.task_id)
                return
            elif state == JobStatusEnum.FAILED:
                self.log.debug("Failed with context: %s", context)
                raise AirflowException(f"Job failed:\n{job}")
            elif state == JobStatusEnum.CANCELLED:
                raise AirflowException(f"Job was cancelled:\n{job}")
            else:
                raise AirflowException(
                    f"Encountered unexpected state `{state}` for job_id `{self.airbyte_job_id}"
                )

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """
        Invoke this callback when the trigger fires; return immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            self.log.debug("An error occurred with context: %s", context)
            raise AirflowException(event["message"])

        self.log.info("%s completed successfully.", self.task_id)
        return None
