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
from typing import Any, AsyncIterator

from airflow.providers.openai.hooks.openai import BatchStatus, OpenAIHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class OpenAIBatchTrigger(BaseTrigger):
    """Triggers OpenAI Batch API."""

    def __init__(
        self,
        conn_id: str,
        batch_id: str,
        poll_interval: float,
        end_time: float,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.poll_interval = poll_interval
        self.batch_id = batch_id
        self.end_time = end_time

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize OpenAIBatchTrigger arguments and class path."""
        return (
            "airflow.providers.openai.triggers.openai.OpenAIBatchTrigger",
            {
                "conn_id": self.conn_id,
                "batch_id": self.batch_id,
                "poll_interval": self.poll_interval,
                "end_time": self.end_time,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make connection to OpenAI Client, and poll the status of batch."""
        hook = OpenAIHook(conn_id=self.conn_id)
        try:
            while (batch := hook.get_batch(self.batch_id)) and BatchStatus.is_in_progress(batch.status):
                if self.end_time < time.time():
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Batch {self.batch_id} has not reached a terminal status after "
                            f"{time.time() - self.end_time} seconds.",
                            "batch_id": self.batch_id,
                        }
                    )
                    return
                await asyncio.sleep(self.poll_interval)
            if batch.status == BatchStatus.COMPLETED:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": f"Batch {self.batch_id} has completed successfully.",
                        "batch_id": self.batch_id,
                    }
                )
            elif batch.status in {BatchStatus.CANCELLED, BatchStatus.CANCELLING}:
                yield TriggerEvent(
                    {
                        "status": "cancelled",
                        "message": f"Batch {self.batch_id} has been cancelled.",
                        "batch_id": self.batch_id,
                    }
                )
            elif batch.status == BatchStatus.FAILED:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": f"Batch failed:\n{self.batch_id}",
                        "batch_id": self.batch_id,
                    }
                )
            elif batch.status == BatchStatus.EXPIRED:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": f"Batch couldn't be completed within the hour time window :\n{self.batch_id}",
                        "batch_id": self.batch_id,
                    }
                )

            yield TriggerEvent(
                {
                    "status": "error",
                    "message": f"Batch {self.batch_id} has failed.",
                    "batch_id": self.batch_id,
                }
            )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e), "batch_id": self.batch_id})
