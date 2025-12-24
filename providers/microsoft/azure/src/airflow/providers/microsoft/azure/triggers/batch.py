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
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

from azure.batch import models as batch_models

from airflow.providers.microsoft.azure.hooks.batch import AzureBatchHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone


class AzureBatchJobTrigger(BaseTrigger):
    """
    Poll Azure Batch for task completion for a given job.

    :param job_id: The Azure Batch job identifier to poll.
    :param azure_batch_conn_id: Connection id for Azure Batch.
    :param timeout: Maximum wait time in minutes.
    :param poll_interval: Seconds to sleep between polls.
    """

    def __init__(
        self,
        job_id: str,
        azure_batch_conn_id: str = "azure_batch_default",
        timeout: int = 25,
        poll_interval: int = 15,
    ) -> None:
        super().__init__()
        self.job_id = job_id
        self.azure_batch_conn_id = azure_batch_conn_id
        self.timeout = timeout
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger configuration."""
        return (
            "airflow.providers.microsoft.azure.triggers.batch.AzureBatchJobTrigger",
            {
                "job_id": self.job_id,
                "azure_batch_conn_id": self.azure_batch_conn_id,
                "timeout": self.timeout,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = AzureBatchHook(self.azure_batch_conn_id)
        timeout_time = timezone.utcnow() + timedelta(minutes=self.timeout)

        try:
            while timezone.utcnow() < timeout_time:
                tasks = await asyncio.to_thread(lambda: list(hook.connection.task.list(self.job_id)))
                incomplete_tasks = [task for task in tasks if task.state != batch_models.TaskState.completed]

                if not incomplete_tasks:
                    fail_task_ids = [
                        task.id
                        for task in tasks
                        if task.execution_info
                        and task.execution_info.result == batch_models.TaskExecutionResult.failure
                    ]
                    status = "success" if not fail_task_ids else "failure"
                    yield TriggerEvent({"status": status, "fail_task_ids": fail_task_ids})
                    return

                await asyncio.sleep(self.poll_interval)

            yield TriggerEvent(
                {
                    "status": "timeout",
                    "message": "Timed out waiting for tasks to complete",
                }
            )
        except Exception as exc:
            yield TriggerEvent({"status": "error", "message": str(exc)})
