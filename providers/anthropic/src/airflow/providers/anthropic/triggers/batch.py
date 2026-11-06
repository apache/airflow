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

from airflow.providers.anthropic.hooks.anthropic import (
    MAX_CONSECUTIVE_POLL_FAILURES,
    AnthropicHook,
    BatchStatus,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AnthropicBatchTrigger(BaseTrigger):
    """
    Poll an Anthropic Message Batch until it reaches the terminal ``ended`` status.

    :param conn_id: The Anthropic connection ID.
    :param batch_id: The batch to poll.
    :param poll_interval: Seconds to sleep between polls.
    :param end_time: Wall-clock deadline (``time.time()`` epoch seconds) after which a
        ``timeout`` event is emitted. Wall-clock is used deliberately: the trigger is
        serialized to the metadata DB and may resume in a different triggerer process,
        so a per-process ``time.monotonic()`` value would not survive serialization.
    """

    def __init__(self, conn_id: str, batch_id: str, poll_interval: float, end_time: float) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.batch_id = batch_id
        self.poll_interval = poll_interval
        self.end_time = end_time

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize AnthropicBatchTrigger arguments and class path."""
        return (
            "airflow.providers.anthropic.triggers.batch.AnthropicBatchTrigger",
            {
                "conn_id": self.conn_id,
                "batch_id": self.batch_id,
                "poll_interval": self.poll_interval,
                "end_time": self.end_time,
            },
        )

    async def on_kill(self) -> None:
        """
        Cancel the batch when a user kills the deferred task.

        Runs in the triggerer event loop on Airflow 3.3+ (a no-op override on older
        versions, which never call it). Closes the gap the operator's ``on_kill`` leaves
        for deferred tasks, which have released their worker slot.
        """
        hook = AnthropicHook(conn_id=self.conn_id)
        try:
            await asyncio.to_thread(hook.cancel_batch, self.batch_id)
            self.log.info("on_kill: cancelled Anthropic batch %s", self.batch_id)
        except Exception as e:
            self.log.warning("on_kill: failed to cancel batch %s: %s", self.batch_id, e)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the batch status and yield exactly one terminal event."""
        hook = AnthropicHook(conn_id=self.conn_id)
        consecutive_failures = 0
        while True:
            try:
                # get_batch is a blocking SDK HTTP call; run it off the event loop so a
                # single poll does not stall every other trigger on this triggerer.
                batch = await asyncio.to_thread(hook.get_batch, self.batch_id)
            except Exception as e:
                # Tolerate transient polling errors rather than failing a (up to 24h) wait.
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES or time.time() > self.end_time:
                    yield TriggerEvent({"status": "error", "batch_id": self.batch_id, "message": str(e)})
                    return
                self.log.warning("Polling batch %s failed (%s); retrying.", self.batch_id, e)
                await asyncio.sleep(self.poll_interval)
                continue

            consecutive_failures = 0
            self.log.debug("Batch %s status=%s", self.batch_id, batch.processing_status)
            if not BatchStatus.is_in_progress(batch.processing_status):
                counts = batch.request_counts
                yield TriggerEvent(
                    {
                        "status": "success",
                        "batch_id": self.batch_id,
                        "message": f"Batch {self.batch_id} has ended.",
                        "request_counts": {
                            "succeeded": counts.succeeded,
                            "errored": counts.errored,
                            "canceled": counts.canceled,
                            "expired": counts.expired,
                            "processing": counts.processing,
                        },
                    }
                )
                return
            if time.time() > self.end_time:
                yield TriggerEvent(
                    {
                        "status": "timeout",
                        "batch_id": self.batch_id,
                        "message": (
                            f"Batch {self.batch_id} did not reach a terminal status "
                            "before the configured timeout."
                        ),
                    }
                )
                return
            await asyncio.sleep(self.poll_interval)
