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

from airflow.providers.openai.hooks.openai import BatchStatus, OpenAIHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class OpenAIBatchTrigger(BaseTrigger):
    """
    Triggers OpenAI Batch API.

    :param conn_id: The OpenAI connection ID to use.
    :param batch_id: The ID of the OpenAI batch to wait on.
    :param poll_interval: Seconds between batch status polls.
    :param timeout: Total seconds to wait for the batch to reach a terminal
        state before giving up. This is the preferred way to bound the wait
        because the trigger measures elapsed time with :func:`time.monotonic`,
        which is not affected by wall-clock jumps (NTP corrections, DST,
        container clock skew, VM pause/resume). Either ``timeout`` or
        ``end_time`` must be provided.
    :param end_time: Deprecated. Absolute wall-clock deadline (``time.time()``
        based) after which the trigger reports a timeout. Kept for backward
        compatibility with triggers that were serialized by the previous
        version of the operator and are still in flight during an upgrade.
        Prefer ``timeout`` for new code.
    """

    def __init__(
        self,
        conn_id: str,
        batch_id: str,
        poll_interval: float,
        end_time: float | None = None,
        timeout: float | None = None,
    ) -> None:
        if timeout is None and end_time is None:
            raise ValueError("OpenAIBatchTrigger requires either 'timeout' or 'end_time'.")
        if timeout is not None and end_time is not None:
            raise ValueError("OpenAIBatchTrigger accepts either 'timeout' or 'end_time', not both.")
        super().__init__()
        self.conn_id = conn_id
        self.poll_interval = poll_interval
        self.batch_id = batch_id
        self.end_time = end_time
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize OpenAIBatchTrigger arguments and class path.

        The trigger stores exactly the argument it was constructed with
        (``timeout`` or ``end_time``) so that a rolling upgrade never rewrites
        the schema of an in-flight deferred trigger.
        """
        kwargs: dict[str, Any] = {
            "conn_id": self.conn_id,
            "batch_id": self.batch_id,
            "poll_interval": self.poll_interval,
        }
        if self.timeout is not None:
            kwargs["timeout"] = self.timeout
        else:
            kwargs["end_time"] = self.end_time
        return ("airflow.providers.openai.triggers.openai.OpenAIBatchTrigger", kwargs)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Make connection to OpenAI Client, and poll the status of batch."""
        # Measure elapsed time with time.monotonic() so the timeout is robust
        # against wall-clock adjustments (NTP, DST, VM pause/resume, etc.).
        # For legacy ``end_time`` callers we derive a best-effort remaining
        # duration from the wall clock exactly once, then track the rest with
        # the monotonic clock.
        if self.timeout is not None:
            timeout = self.timeout
        else:
            timeout = max(0.0, self.end_time - time.time())  # type: ignore[operator]
        start_monotonic = time.monotonic()
        hook = OpenAIHook(conn_id=self.conn_id)
        try:
            while (batch := hook.get_batch(self.batch_id)) and BatchStatus.is_in_progress(batch.status):
                elapsed = time.monotonic() - start_monotonic
                if elapsed >= timeout:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": (
                                f"Batch {self.batch_id} has not reached a terminal status after "
                                f"{elapsed:.0f} seconds."
                            ),
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
