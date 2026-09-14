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

from airflow.providers.common.ai.batch.base import IN_PROGRESS_STATUSES, TERMINAL_STATUS_MAP
from airflow.providers.common.ai.batch.dispatch import build_adapter
from airflow.triggers.base import BaseTrigger, TriggerEvent

#: Mirrors AnthropicHook.wait_for_batch's tolerance for a run of transient poll failures
#: before giving up -- a single blip must not fail an otherwise-healthy, still-running,
#: already-paid-for batch.
MAX_CONSECUTIVE_POLL_FAILURES = 5


class LLMBatchTrigger(BaseTrigger):
    """
    Poll a batch adapter until the batch reaches a terminal state.

    Deliberately thin: this trigger only polls and, on kill or timeout,
    cancels. It never downloads or validates results -- that is
    :meth:`~airflow.providers.common.ai.operators.llm_batch.LLMBatchOperator.execute_complete`'s
    job, back on the worker. Keeping a 100k-row download/validation pass out
    of the triggerer (a shared process serving many tasks) is the entire
    reason the split exists (§4).

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
        triggerer process after a restart (D4) -- a per-process monotonic
        value would not survive that and would let the deferral run forever
        across restarts, with no cost ceiling.
    :param cancel_on_kill: Cancel the batch from ``on_kill`` when the
        deferred task is killed. Only takes effect on Airflow 3.3+, which is
        the first version whose triggerer calls a trigger's ``on_kill``; see
        :meth:`on_kill`.
    :param cancel_on_timeout: Cancel the batch when ``end_time`` passes
        without the batch reaching a terminal state. When ``False``, the
        task still fails with a ``timeout`` event, but the batch is left
        running (and billing) -- §8's row for this combination is exactly
        the "stop waiting, but let the paid-for batch finish" case; a later
        retry re-attaches to it via the state layer (§5) instead of paying
        twice.
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
    ) -> None:
        super().__init__()
        self.llm_conn_id = llm_conn_id
        self.adapter = adapter
        self.batch_id = batch_id
        self.poll_interval = poll_interval
        self.end_time = end_time
        self.cancel_on_kill = cancel_on_kill
        self.cancel_on_timeout = cancel_on_timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize trigger arguments and class path.

        Deliberately excludes ``result_path``, ``requests``, and
        ``output_type`` (§4): this trigger never downloads or validates
        results, so it has no use for them, and ``output_type`` is a Python
        class that has no business in the metadata DB.
        """
        return (
            "airflow.providers.common.ai.triggers.llm_batch.LLMBatchTrigger",
            {
                "llm_conn_id": self.llm_conn_id,
                "adapter": self.adapter,
                "batch_id": self.batch_id,
                "poll_interval": self.poll_interval,
                "end_time": self.end_time,
                "cancel_on_kill": self.cancel_on_kill,
                "cancel_on_timeout": self.cancel_on_timeout,
            },
        )

    async def on_kill(self) -> None:
        """
        Cancel the batch when a user kills the deferred task.

        Runs in the triggerer event loop on Airflow 3.3+ only -- older
        versions never call a trigger's ``on_kill`` at all, so a killed
        deferred task's batch is not cancelled automatically on those
        versions (the operator's own ``on_kill`` only covers the
        non-deferred, worker-alive path).
        """
        if not self.cancel_on_kill:
            return
        try:
            # M12/M13: build_adapter does blocking I/O (connection lookup, SDK client
            # construction) -- both the construction and the cancel call must be off the
            # event loop, and both must be inside the same try so a construction failure is
            # caught the same way a cancel-call failure is, instead of crashing on_kill.
            adapter = await asyncio.to_thread(build_adapter, self.adapter, llm_conn_id=self.llm_conn_id)
            await asyncio.to_thread(adapter.cancel_batch, self.batch_id)
            self.log.info("on_kill: cancelled batch %s", self.batch_id)
        except Exception as e:
            self.log.warning("on_kill: failed to cancel batch %s: %s", self.batch_id, e)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the batch status and yield exactly one terminal event."""
        # M13: off the event loop -- build_adapter resolves the connection (a blocking DB/
        # secrets-backend call) and constructs the SDK client, neither of which belongs on
        # the shared triggerer event loop any more than the polling calls below do.
        adapter = await asyncio.to_thread(build_adapter, self.adapter, llm_conn_id=self.llm_conn_id)
        consecutive_failures = 0
        while True:
            try:
                # get_batch is a blocking SDK HTTP call (both providers use a synchronous
                # client); run it off the event loop so one poll never stalls every other
                # trigger on this triggerer. Do NOT call it directly here -- that is
                # exactly the pattern providers/openai/.../triggers/openai.py:100 uses and
                # it blocks the whole triggerer process on every poll.
                state = await asyncio.to_thread(adapter.get_batch, self.batch_id)
            except Exception as e:
                consecutive_failures += 1
                timed_out = time.time() > self.end_time
                if consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES or timed_out:
                    # M11: giving up because the wall-clock budget is actually exhausted is a
                    # real timeout (§8), not a generic polling error -- it must honor
                    # cancel_on_timeout and be labeled "timeout" the same as the healthy-poll
                    # timeout path below. Giving up early due to persistent poll failures
                    # alone (without having exceeded end_time) is left as "error": the batch's
                    # own health is unknown, so cancelling it would be presumptuous.
                    if timed_out and self.cancel_on_timeout:
                        try:
                            await asyncio.to_thread(adapter.cancel_batch, self.batch_id)
                        except Exception as cancel_error:
                            self.log.warning(
                                "Failed to cancel batch %s on timeout: %s", self.batch_id, cancel_error
                            )
                    yield TriggerEvent(
                        {
                            "status": "timeout" if timed_out else "error",
                            "batch_id": self.batch_id,
                            "counts": None,
                            "message": str(e),
                        }
                    )
                    return
                self.log.warning("Polling batch %s failed (%s); retrying.", self.batch_id, e)
                await asyncio.sleep(self.poll_interval)
                continue

            consecutive_failures = 0
            self.log.debug("Batch %s status=%s", self.batch_id, state.status)

            if state.status not in IN_PROGRESS_STATUSES:
                event_status = TERMINAL_STATUS_MAP.get(state.status, "error")
                message = state.error_message or f"Batch {self.batch_id} reached status {state.status!r}."
                yield TriggerEvent(
                    {
                        "status": event_status,
                        "batch_id": self.batch_id,
                        "counts": dict(state.counts) if state.counts is not None else None,
                        "message": message,
                    }
                )
                return

            if time.time() > self.end_time:
                if self.cancel_on_timeout:
                    try:
                        await asyncio.to_thread(adapter.cancel_batch, self.batch_id)
                    except Exception as e:
                        self.log.warning("Failed to cancel batch %s on timeout: %s", self.batch_id, e)
                yield TriggerEvent(
                    {
                        "status": "timeout",
                        "batch_id": self.batch_id,
                        "counts": None,
                        "message": (
                            f"Batch {self.batch_id} did not reach a terminal status before the "
                            "configured timeout."
                        ),
                    }
                )
                return

            await asyncio.sleep(self.poll_interval)
