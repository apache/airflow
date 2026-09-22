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
"""
The poll-until-terminal decision logic shared by the trigger and the operator's sync path.

Both loops do the same thing with different I/O primitives (``await
asyncio.to_thread(...)`` vs a direct call, ``asyncio.sleep`` vs ``time.sleep``).
The decision of *what to do* with each poll result lives here, once, so the
two cannot drift: the caller feeds in either a :class:`~airflow.providers.common.ai.batch.base.BatchState`
or the exception a poll raised, and gets back "keep polling" or a finished
event dict, plus whether the batch should be cancelled first.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from airflow.providers.common.ai.batch.base import IN_PROGRESS_STATUSES, TERMINAL_STATUS_MAP, BatchState

#: A single blip must not fail an otherwise-healthy, still-running, already-paid-for batch.
MAX_CONSECUTIVE_POLL_FAILURES = 5


@dataclass(frozen=True)
class PollOutcome:
    """
    What the poll loop should do after one status check.

    ``event`` is ``None`` while the batch is still running. Otherwise it is the
    dict the trigger yields as its ``TriggerEvent`` payload (and the operator's
    sync loop returns), with ``status`` in ``success``/``failed``/``expired``/
    ``cancelled``/``timeout``/``error``. ``cancel`` says the caller should
    cancel the batch before emitting the event (our own deadline passed with
    ``cancel_on_timeout=True``); the caller then reports the result of that
    cancel through :meth:`BatchPoller.finish_timeout`.
    """

    event: dict[str, Any] | None
    cancel: bool = False


class BatchPoller:
    """
    Tracks consecutive failures and the wall-clock deadline across polls.

    :param batch_id: The provider batch id, echoed into every event.
    :param end_time: Wall-clock deadline in epoch seconds.
    :param timeout: The configured budget in seconds, for the timeout message.
    :param cancel_on_timeout: Whether the deadline should cancel the batch.
    """

    def __init__(self, *, batch_id: str, end_time: float, timeout: int, cancel_on_timeout: bool) -> None:
        self.batch_id = batch_id
        self.end_time = end_time
        self.timeout = timeout
        self.cancel_on_timeout = cancel_on_timeout
        self.consecutive_failures = 0
        self._last_error: str | None = None

    @property
    def deadline_iso(self) -> str:
        return datetime.fromtimestamp(self.end_time, tz=timezone.utc).isoformat(timespec="seconds")

    def on_state(self, state: BatchState, *, now: float) -> PollOutcome:
        """Decide after a successful status check."""
        self.consecutive_failures = 0
        if state.status not in IN_PROGRESS_STATUSES:
            return PollOutcome(event=terminal_event(self.batch_id, state))
        if now > self.end_time:
            return self._timeout_outcome()
        return PollOutcome(event=None)

    def on_error(self, exc: Exception, *, now: float) -> PollOutcome:
        """
        Decide after a status check raised.

        Persistent failures past the deadline are a real timeout and honor
        ``cancel_on_timeout``. Persistent failures inside the deadline give up
        with ``"error"`` and leave the batch alone: its health is unknown, so
        cancelling it would be presumptuous.
        """
        self.consecutive_failures += 1
        self._last_error = str(exc)
        timed_out = now > self.end_time
        if timed_out:
            return self._timeout_outcome()
        if self.consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES:
            return PollOutcome(
                event={
                    "status": "error",
                    "batch_id": self.batch_id,
                    "counts": None,
                    "message": (
                        f"Gave up polling batch {self.batch_id} after {self.consecutive_failures} consecutive "
                        f"failures (last error: {self._last_error}). The batch was left in place; a retry "
                        "re-attaches to it."
                    ),
                }
            )
        return PollOutcome(event=None)

    def _timeout_outcome(self) -> PollOutcome:
        return PollOutcome(
            event={"status": "timeout", "batch_id": self.batch_id, "counts": None, "message": ""},
            cancel=self.cancel_on_timeout,
        )

    def finish_timeout(self, *, cancelled: bool, cancel_error: str | None = None) -> dict[str, Any]:
        """
        Build the final ``timeout`` event once the caller has (not) cancelled the batch.

        The message names the budget, the deadline, and what happened to the
        batch, so an on-call reader can tell "cancelled, a retry resubmits"
        from "still running and billing, a retry re-attaches".
        """
        if self.cancel_on_timeout and cancelled:
            fate = "The batch was cancelled (cancel_on_timeout=True); a retry submits a new one."
        elif self.cancel_on_timeout:
            fate = (
                f"Cancelling the batch failed ({cancel_error}); it may still be running and billing. "
                "A retry re-attaches to it."
            )
        else:
            fate = (
                "The batch was left running (cancel_on_timeout=False); a retry re-attaches to it and "
                "waits again."
            )
        prefix = (
            f"Batch {self.batch_id} did not reach a terminal status within timeout={self.timeout}s "
            f"(deadline {self.deadline_iso})."
        )
        if self._last_error is not None and self.consecutive_failures:
            prefix += f" The last {self.consecutive_failures} status check(s) failed: {self._last_error}."
        return {"status": "timeout", "batch_id": self.batch_id, "counts": None, "message": f"{prefix} {fate}"}


def terminal_event(batch_id: str, state: BatchState) -> dict[str, Any]:
    """Reshape a terminal :class:`~airflow.providers.common.ai.batch.base.BatchState` into the shared event dict."""
    return {
        "status": TERMINAL_STATUS_MAP.get(state.status, "error"),
        "batch_id": batch_id,
        "counts": dict(state.counts) if state.counts is not None else None,
        "message": state.error_message or f"Batch {batch_id} reached status {state.status!r}.",
    }
