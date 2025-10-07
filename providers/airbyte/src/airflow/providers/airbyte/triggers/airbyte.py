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

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

from airbyte_api.models import JobStatusEnum

from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AirbyteSyncTrigger(BaseTrigger):
    """
    Triggers Airbyte Sync, makes an asynchronous HTTP call to get the status via a job ID.

    This trigger is designed to initiate and monitor the status of Airbyte Sync jobs. It
    makes use of asynchronous communication to check the progress of a job run over time.

    :param conn_id: The connection identifier for connecting to Airbyte.
    :param job_id: The ID of an Airbyte Sync job.
    :param end_time: Time in seconds to wait for a job run to reach a terminal status. Defaults to 7 days.
    :param poll_interval:  polling period in seconds to check for the status.
    """

    def __init__(
        self,
        job_id: int,
        conn_id: str,
        end_time: float,
        poll_interval: float,
    ):
        super().__init__()
        self.job_id = job_id
        self.conn_id = conn_id
        self.end_time = end_time
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize AirbyteSyncTrigger arguments and classpath."""
        return (
            "airflow.providers.airbyte.triggers.airbyte.AirbyteSyncTrigger",
            {
                "job_id": self.job_id,
                "conn_id": self.conn_id,
                "end_time": self.end_time,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make async connection to Airbyte, polls for the pipeline run status."""
        hook = AirbyteHook(airbyte_conn_id=self.conn_id)
        try:
            while await self.is_still_running(hook):
                if self.end_time < time.time():
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Job run {self.job_id} has not reached a terminal status after "
                            f"{self.end_time} seconds.",
                            "job_id": self.job_id,
                        }
                    )
                    return
                await asyncio.sleep(self.poll_interval)
            job_run_status = hook.get_job_status(self.job_id)
            if job_run_status == JobStatusEnum.SUCCEEDED:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": f"Job run {self.job_id} has completed successfully.",
                        "job_id": self.job_id,
                    }
                )
            elif job_run_status == JobStatusEnum.CANCELLED:
                yield TriggerEvent(
                    {
                        "status": "cancelled",
                        "message": f"Job run {self.job_id} has been cancelled.",
                        "job_id": self.job_id,
                    }
                )
            else:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": f"Job run {self.job_id} has failed.",
                        "job_id": self.job_id,
                    }
                )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e), "job_id": self.job_id})

    async def is_still_running(self, hook: AirbyteHook) -> bool:
        """
        Async function to check whether the job is submitted via async API.

        If job is in running state returns True if it is still running else return False
        """
        job_run_status = hook.get_job_status(self.job_id)
        if job_run_status in (JobStatusEnum.RUNNING, JobStatusEnum.PENDING, JobStatusEnum.INCOMPLETE):
            self.log.debug(
                "Job run status is: %s with context: %s", job_run_status, hook.get_job_details(self.job_id)
            )
            return True
        return False
