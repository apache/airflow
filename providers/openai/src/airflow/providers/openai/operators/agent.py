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

import math
import time
from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.openai.exceptions import OpenAIAgentSessionError, OpenAITriggerEventError
from airflow.providers.openai.hooks.openai import OpenAIHook
from airflow.providers.openai.triggers.agent import OpenAIAgentSessionTrigger

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class OpenAIAgentSessionOperator(BaseOperator):
    """
    Run one turn in a fresh OpenAI Managed Agents session and return its session ID.

    The session is retained for downstream retrieval of items and artifacts. A retry
    creates a new session and can repeat external side effects.

    :param input: Initial user message. (templated)
    :param environment: SDK environment configuration or template reference. (templated)
    :param agent_id: Saved agent ID. Alternatively supply agent.model in session_kwargs. (templated)
    :param session_kwargs: Additional SDK session creation arguments, such as agent,
        vault_ids and metadata. Must not contain input, environment, agent_id or stream. (templated)
    :param conn_id: OpenAI connection ID. (templated)
    :param deferrable: Release the worker while waiting for completion.
    :param poll_interval: Seconds between polls.
    :param timeout: Maximum seconds to wait for the initial turn. A shorter
        ``execution_timeout`` still applies and preempts the cancel-on-timeout path.
    """

    template_fields: Sequence[str] = ("input", "environment", "agent_id", "session_kwargs", "conn_id")
    template_fields_renderers = {"environment": "json", "session_kwargs": "json"}

    def __init__(
        self,
        *,
        input: str,
        environment: dict[str, Any],
        agent_id: str | None = None,
        session_kwargs: dict[str, Any] | None = None,
        conn_id: str = OpenAIHook.default_conn_name,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 10,
        timeout: float = 3600,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        for name, value in (("poll_interval", poll_interval), ("timeout", timeout)):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        self.input = input
        self.environment = environment
        self.agent_id = agent_id
        self.session_kwargs = session_kwargs or {}
        self.conn_id = conn_id
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.session_id: str | None = None

    @cached_property
    def hook(self) -> OpenAIHook:
        """Return the connection's OpenAI hook."""
        return OpenAIHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> str:
        reserved = {"input", "environment", "agent_id", "stream"} & self.session_kwargs.keys()
        if reserved:
            raise ValueError(f"Reserved session_kwargs: {sorted(reserved)}")
        agent = self.session_kwargs.get("agent")
        if agent is not None and not isinstance(agent, dict):
            raise ValueError("session_kwargs['agent'] must be a dict of SDK agent fields")
        if not self.agent_id and not (agent or {}).get("model"):
            raise ValueError("Supply agent_id or session_kwargs['agent']['model']")
        if not self.input:
            raise ValueError("input must not be empty")
        create_kwargs = dict(self.session_kwargs)
        if self.agent_id:
            create_kwargs["agent_id"] = self.agent_id
        session = self.hook.create_agent_session(
            input=self.input, environment=self.environment, **create_kwargs
        )
        self.session_id = session.id
        try:
            if self.do_xcom_push:
                context["ti"].xcom_push(key="session_id", value=session.id)
            if self.deferrable:
                self.defer(
                    trigger=OpenAIAgentSessionTrigger(
                        conn_id=self.conn_id,
                        session_id=session.id,
                        poll_interval=self.poll_interval,
                        end_time=time.time() + self.timeout,
                    ),
                    method_name="execute_complete",
                    kwargs={"session_id": session.id},
                    timeout=self.execution_timeout
                    or timedelta(seconds=self.timeout + self.poll_interval + 60),
                )
            deadline = time.monotonic() + self.timeout
            consecutive_failures = 0
            while time.monotonic() < deadline:
                try:
                    result = self.hook.poll_agent_session(session.id)
                except Exception as exc:
                    consecutive_failures += 1
                    if consecutive_failures >= OpenAIHook.MAX_CONSECUTIVE_POLL_FAILURES:
                        raise
                    self.log.warning("Polling agent session %s failed (%s); retrying.", session.id, exc)
                else:
                    consecutive_failures = 0
                    if result is not None:
                        break
                time.sleep(min(self.poll_interval, max(0, deadline - time.monotonic())))
            else:
                raise OpenAIAgentSessionError(f"Agent session {session.id} timed out")
        except Exception:
            self.on_kill()
            raise
        return self.execute_complete(context, result, session_id=session.id)

    def execute_complete(self, context: Context, event: Any = None, session_id: str | None = None) -> str:
        """Validate completion, record usage, and return the owned session ID."""
        self.session_id = session_id or self.session_id
        if (
            not isinstance(event, dict)
            or event.get("status") not in ("success", "error", "timeout")
            or not self.session_id
            or event.get("session_id") != self.session_id
        ):
            self.on_kill()
            raise OpenAITriggerEventError("Invalid Managed Agents trigger event")
        if event["status"] != "success":
            self.on_kill()
        if self.do_xcom_push:
            try:
                if event.get("turn_id"):
                    context["ti"].xcom_push(key="turn_id", value=event["turn_id"])
                usage = event.get("usage")
                if usage is not None:
                    context["ti"].xcom_push(
                        key="usage", value={**usage, "try_number": context["ti"].try_number}
                    )
            except Exception:
                self.log.exception("Could not record agent turn usage for session %s", self.session_id)
        if event["status"] != "success":
            raise OpenAIAgentSessionError(event.get("message", "Agent session failed"))
        return self.session_id

    def on_kill(self) -> None:
        """Request cancellation without deleting session history or artifacts."""
        if self.session_id:
            try:
                self.hook.cancel_agent_session(self.session_id)
            except Exception:
                self.log.exception("Could not cancel agent session %s", self.session_id)
