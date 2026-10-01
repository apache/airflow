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

from airflow.providers.openai.hooks.openai import OpenAIHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class OpenAIAgentSessionTrigger(BaseTrigger):
    """
    Wait for the first turn of an exclusively owned Managed Agents session.

    :param conn_id: OpenAI connection ID.
    :param session_id: Fresh session whose first turn is being awaited.
    :param poll_interval: Seconds between polls.
    :param end_time: Epoch deadline, preserved across triggerer restarts.
    """

    def __init__(self, conn_id: str, session_id: str, poll_interval: float, end_time: float) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.session_id = session_id
        self.poll_interval = poll_interval
        self.end_time = end_time

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize identifiers and the absolute deadline, without credentials."""
        return (
            "airflow.providers.openai.triggers.agent.OpenAIAgentSessionTrigger",
            {
                "conn_id": self.conn_id,
                "session_id": self.session_id,
                "poll_interval": self.poll_interval,
                "end_time": self.end_time,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll off the event loop and emit one terminal result."""
        hook = OpenAIHook(conn_id=self.conn_id)
        consecutive_failures = 0
        while time.time() < self.end_time:
            try:
                result = await asyncio.to_thread(hook.poll_agent_session, self.session_id)
            except Exception as exc:
                # Tolerate transient polling errors rather than cancelling a live session.
                consecutive_failures += 1
                if consecutive_failures >= OpenAIHook.MAX_CONSECUTIVE_POLL_FAILURES:
                    yield TriggerEvent(
                        {"status": "error", "session_id": self.session_id, "message": str(exc)}
                    )
                    return
                self.log.warning("Polling agent session %s failed (%s); retrying.", self.session_id, exc)
            else:
                consecutive_failures = 0
                if result is not None:
                    yield TriggerEvent(result)
                    return
            await asyncio.sleep(min(self.poll_interval, max(0, self.end_time - time.time())))
        yield TriggerEvent(
            {"status": "timeout", "session_id": self.session_id, "message": "Agent session timed out"}
        )

    async def on_kill(self) -> None:
        """Cancel a killed deferred task's turn on Airflow versions supporting trigger cleanup."""
        hook = OpenAIHook(conn_id=self.conn_id)
        try:
            await asyncio.to_thread(hook.cancel_agent_session, self.session_id)
        except Exception:
            self.log.exception("Could not cancel agent session %s", self.session_id)
