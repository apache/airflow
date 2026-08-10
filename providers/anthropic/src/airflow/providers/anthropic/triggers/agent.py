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

from airflow.providers.anthropic.hooks.anthropic import MAX_CONSECUTIVE_POLL_FAILURES, AnthropicHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AnthropicAgentSessionTrigger(BaseTrigger):
    """
    Poll a Managed Agents session until it reaches a terminal status.

    :param conn_id: The Anthropic connection ID.
    :param session_id: The session to poll.
    :param poll_interval: Seconds to sleep between polls.
    :param end_time: Wall-clock deadline (``time.time()`` epoch seconds). Wall-clock is
        used deliberately: the trigger is serialized and may resume in a different
        triggerer process, where a per-process ``time.monotonic()`` value is meaningless.
    :param expect_outcome: Whether the session is running a ``user.define_outcome`` loop
        (completion is then judged from ``outcome_evaluations``, not from ``idle`` alone).
    :param kickoff_event_id: ID of the kickoff event, used to correlate the terminal idle
        event on a ``message`` run (defeats the start race).
    """

    def __init__(
        self,
        conn_id: str,
        session_id: str,
        poll_interval: float,
        end_time: float,
        expect_outcome: bool = False,
        kickoff_event_id: str | None = None,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.session_id = session_id
        self.poll_interval = poll_interval
        self.end_time = end_time
        self.expect_outcome = expect_outcome
        self.kickoff_event_id = kickoff_event_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize AnthropicAgentSessionTrigger arguments and class path."""
        return (
            "airflow.providers.anthropic.triggers.agent.AnthropicAgentSessionTrigger",
            {
                "conn_id": self.conn_id,
                "session_id": self.session_id,
                "poll_interval": self.poll_interval,
                "end_time": self.end_time,
                "expect_outcome": self.expect_outcome,
                "kickoff_event_id": self.kickoff_event_id,
            },
        )

    async def on_kill(self) -> None:
        """
        Archive the session when a user kills the deferred task.

        Runs in the triggerer event loop on Airflow 3.3+ (a no-op override on older
        versions, which never call it). Closes the gap the operator's ``on_kill`` leaves
        for deferred tasks, which have released their worker slot.
        """
        hook = AnthropicHook(conn_id=self.conn_id)
        try:
            await asyncio.to_thread(hook.archive_session, self.session_id)
            self.log.info("on_kill: archived Anthropic session %s", self.session_id)
        except Exception as e:
            self.log.warning("on_kill: failed to archive session %s: %s", self.session_id, e)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the session and yield exactly one terminal event."""
        hook = AnthropicHook(conn_id=self.conn_id)
        consecutive_failures = 0
        while True:
            try:
                # poll_session_completion does blocking SDK HTTP calls; run off the event loop.
                done, error_message = await asyncio.to_thread(
                    hook.poll_session_completion,
                    self.session_id,
                    expect_outcome=self.expect_outcome,
                    kickoff_event_id=self.kickoff_event_id,
                )
            except Exception as e:
                # Tolerate transient polling errors rather than failing the whole wait.
                consecutive_failures += 1
                if consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES or time.time() > self.end_time:
                    yield TriggerEvent({"status": "error", "session_id": self.session_id, "message": str(e)})
                    return
                self.log.warning("Polling session %s failed (%s); retrying.", self.session_id, e)
                await asyncio.sleep(self.poll_interval)
                continue

            consecutive_failures = 0
            if done:
                if error_message:
                    yield TriggerEvent(
                        {"status": "error", "session_id": self.session_id, "message": error_message}
                    )
                else:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "session_id": self.session_id,
                            "message": f"Session {self.session_id} completed.",
                        }
                    )
                return
            if time.time() > self.end_time:
                yield TriggerEvent(
                    {
                        "status": "timeout",
                        "session_id": self.session_id,
                        "message": (
                            f"Session {self.session_id} did not reach a terminal status "
                            "before the configured timeout."
                        ),
                    }
                )
                return
            await asyncio.sleep(self.poll_interval)
