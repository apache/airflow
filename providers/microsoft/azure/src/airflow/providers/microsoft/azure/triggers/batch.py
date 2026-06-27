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

from azure.batch import models as batch_models

from airflow.providers.microsoft.azure.hooks.batch import AzureBatchHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AzureBatchTrigger(BaseTrigger):
    """
    Trigger when Azure Batch job tasks reach a terminal state.

    :param job_id: Azure Batch job identifier.
    :param azure_batch_conn_id: Azure Batch connection id.
    :param end_time: Absolute timeout deadline as determined using ``time.time()``.
    :param poll_interval: Poll interval in seconds.
    """

    def __init__(
        self,
        job_id: str,
        azure_batch_conn_id: str,
        end_time: float,
        poll_interval: int = 30,
    ):
        super().__init__()

        self.job_id = job_id
        self.azure_batch_conn_id = azure_batch_conn_id
        self.end_time = end_time
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "job_id": self.job_id,
                "azure_batch_conn_id": self.azure_batch_conn_id,
                "end_time": self.end_time,
                "poll_interval": self.poll_interval,
            },
        )

    def _get_incomplete_tasks(
        self,
        tasks: list[batch_models.CloudTask],
    ) -> list[batch_models.CloudTask]:
        """Return tasks that have not yet completed."""
        return [task for task in tasks if task.state != batch_models.TaskState.completed]

    def _build_trigger_event(
        self,
        tasks: list[batch_models.CloudTask],
    ) -> TriggerEvent | None:
        """
        Convert Batch task states to TriggerEvent.

        Returns None if tasks are still running.
        """
        if not tasks:
            return TriggerEvent(
                {
                    "status": "error",
                    "message": f"Azure Batch job {self.job_id} contains no tasks.",
                    "job_id": self.job_id,
                }
            )

        if self._get_incomplete_tasks(tasks):
            return None

        failed_tasks = [
            task.id
            for task in tasks
            if task.execution_info and task.execution_info.result == batch_models.TaskExecutionResult.failure
        ]

        if failed_tasks:
            return TriggerEvent(
                {
                    "status": "error",
                    "message": f"Azure Batch job {self.job_id} failed.",
                    "job_id": self.job_id,
                    "failed_tasks": failed_tasks,
                }
            )

        return TriggerEvent(
            {
                "status": "success",
                "message": f"Azure Batch job {self.job_id} completed successfully.",
                "job_id": self.job_id,
            }
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll Azure Batch job tasks until completion or timeout."""
        hook = AzureBatchHook(
            azure_batch_conn_id=self.azure_batch_conn_id,
        )

        try:
            while time.time() <= self.end_time:
                tasks = await asyncio.to_thread(lambda: list(hook.connection.task.list(self.job_id)))

                event = self._build_trigger_event(tasks)

                if event:
                    yield event
                    return

                incomplete_tasks = self._get_incomplete_tasks(tasks)

                self.log.info(
                    "Azure Batch job %s still running. Incomplete tasks: %s. Sleeping %s seconds.",
                    self.job_id,
                    incomplete_tasks,
                    self.poll_interval,
                )

                await asyncio.sleep(self.poll_interval)

            # Final check before timeout event in case job completed
            # during the last sleep interval.
            tasks = await asyncio.to_thread(lambda: list(hook.connection.task.list(self.job_id)))

            event = self._build_trigger_event(tasks)

            if event:
                yield event
                return

            yield TriggerEvent(
                {
                    "status": "timeout",
                    "message": f"Timeout waiting for Azure Batch job {self.job_id}.",
                    "job_id": self.job_id,
                }
            )

        except Exception as e:
            self.log.exception(
                "Azure Batch trigger failed for job %s.",
                self.job_id,
            )

            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(e),
                    "job_id": self.job_id,
                }
            )
