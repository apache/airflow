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
"""Trigger that polls a ``@task.llm_batch`` batch until it reaches a terminal state."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

from airflow.providers.common.ai.batch.dispatch import build_adapter
from airflow.providers.common.ai.batch.polling import MAX_CONSECUTIVE_POLL_FAILURES, BatchPoller
from airflow.triggers.base import BaseTrigger, TriggerEvent

__all__ = ["MAX_CONSECUTIVE_POLL_FAILURES", "LLMBatchTrigger"]


class LLMBatchTrigger(BaseTrigger):
    """
    Poll a batch adapter until the batch reaches a terminal state.

    Deliberately thin: this trigger only polls and, on kill or timeout,
    cancels. It never downloads or validates results; that is
    :meth:`~airflow.providers.common.ai.operators.llm_batch.LLMBatchOperator.execute_complete`'s
    job, back on the worker, so a 100k-row download never runs inside the
    triggerer (a shared process serving many tasks).

    :param llm_conn_id: Airflow connection ID, re-resolved into a live
        adapter via :func:`~airflow.providers.common.ai.batch.dispatch.build_adapter`
        (adapters are not serializable, so only the connection id travels).
    :param adapter: The already-resolved adapter name (``"openai"`` /
        ``"anthropic"``), not the adapter class or instance.
    :param batch_id: The provider batch id to poll.
    :param poll_interval: Seconds to sleep between polls.
    :param end_time: Wall-clock deadline (``time.time()`` epoch seconds).
        Wall-clock, not ``time.monotonic()``, because this trigger is
        serialized to the metadata DB and may resume in a different
        triggerer process after a restart.
    :param timeout: The configured budget in seconds, used only to build the
        timeout message.
    :param cancel_on_kill: Cancel the batch from ``on_kill`` when the
        deferred task is killed. Only takes effect on Airflow 3.3+, which is
        the first version whose triggerer calls a trigger's ``on_kill``.
    :param cancel_on_timeout: Cancel the batch when ``end_time`` passes
        without the batch reaching a terminal state. When ``False``, the
        task still fails with a ``timeout`` event, but the batch is left
        running (and billing) and a later retry re-attaches to it.
    """

    def __init__(
        self,
        *,
        llm_conn_id: str,
        adapter: str,
        batch_id: str,
        poll_interval: int,
        end_time: float,
        cancel_on_kill: bool,
        cancel_on_timeout: bool,
        timeout: int = 0,
    ) -> None:
        super().__init__()
        self.llm_conn_id = llm_conn_id
        self.adapter = adapter
        self.batch_id = batch_id
        self.poll_interval = poll_interval
        self.end_time = end_time
        self.timeout = timeout
        self.cancel_on_kill = cancel_on_kill
        self.cancel_on_timeout = cancel_on_timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger arguments and class path."""
        return (
            "airflow.providers.common.ai.triggers.llm_batch.LLMBatchTrigger",
            {
                "llm_conn_id": self.llm_conn_id,
                "adapter": self.adapter,
                "batch_id": self.batch_id,
                "poll_interval": self.poll_interval,
                "end_time": self.end_time,
                "timeout": self.timeout,
                "cancel_on_kill": self.cancel_on_kill,
                "cancel_on_timeout": self.cancel_on_timeout,
            },
        )

    async def on_kill(self) -> None:
        """
        Cancel the batch when a user kills the deferred task.

        Runs in the triggerer event loop on Airflow 3.3+ only; older versions
        never call a trigger's ``on_kill``, so a killed deferred task's batch
        is not cancelled automatically there. Both the connection lookup and
        the cancel call are blocking I/O, so both run off the event loop.
        """
        if not self.cancel_on_kill:
            return
        adapter = None
        try:
            adapter = await asyncio.to_thread(build_adapter, self.adapter, llm_conn_id=self.llm_conn_id)
            await asyncio.to_thread(adapter.cancel_batch, self.batch_id)
            self.log.info("on_kill: cancelled batch %s", self.batch_id)
        except Exception as e:
            self.log.warning("on_kill: failed to cancel batch %s: %s", self.batch_id, e)
        finally:
            if adapter is not None:
                await asyncio.to_thread(adapter.close)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the batch status and yield exactly one terminal event."""
        # build_adapter resolves the connection (a blocking call) and constructs the SDK
        # client; neither belongs on the shared triggerer event loop.
        adapter = await asyncio.to_thread(build_adapter, self.adapter, llm_conn_id=self.llm_conn_id)
        poller = BatchPoller(
            batch_id=self.batch_id,
            end_time=self.end_time,
            timeout=self.timeout,
            cancel_on_timeout=self.cancel_on_timeout,
        )
        try:
            while True:
                try:
                    # get_batch is a blocking SDK HTTP call; run it off the event loop so one
                    # poll never stalls every other trigger on this triggerer.
                    state = await asyncio.to_thread(adapter.get_batch, self.batch_id)
                except Exception as e:
                    outcome = poller.on_error(e, now=time.time())
                    if outcome.event is None:
                        self.log.warning(
                            "Polling batch %s failed (attempt %d/%d): %s; retrying.",
                            self.batch_id,
                            poller.consecutive_failures,
                            MAX_CONSECUTIVE_POLL_FAILURES,
                            e,
                        )
                else:
                    outcome = poller.on_state(state, now=time.time())
                    self.log.debug("Batch %s status=%s", self.batch_id, state.status)

                if outcome.event is None:
                    await asyncio.sleep(self.poll_interval)
                    continue
                if outcome.event["status"] == "timeout":
                    yield TriggerEvent(await self._finish_timeout(adapter, poller, cancel=outcome.cancel))
                    return
                yield TriggerEvent(outcome.event)
                return
        finally:
            await asyncio.to_thread(adapter.close)

    async def _finish_timeout(self, adapter: Any, poller: BatchPoller, *, cancel: bool) -> dict[str, Any]:
        cancelled = False
        cancel_error: str | None = None
        if cancel:
            try:
                await asyncio.to_thread(adapter.cancel_batch, self.batch_id)
                cancelled = True
                self.log.info(
                    "Cancelled batch %s: timeout=%ss exceeded with cancel_on_timeout=True",
                    self.batch_id,
                    self.timeout,
                )
            except Exception as e:
                cancel_error = str(e)
                self.log.warning("Failed to cancel batch %s on timeout: %s", self.batch_id, e)
        return poller.finish_timeout(cancelled=cancelled, cancel_error=cancel_error)
