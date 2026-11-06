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
"""Mixin that cancels an operator's in-flight pydantic-ai run when the task is killed."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from pydantic_ai import CancellationToken

if TYPE_CHECKING:
    from pydantic_ai import Agent, AgentRunResult


class CancellableAgentRunMixin:
    """
    Run a pydantic-ai agent synchronously with kill-time cancellation wired in.

    The wrapper holds the in-flight run's ``CancellationToken`` so :meth:`on_kill` can
    cancel it. Cancelling makes ``run_sync`` raise ``RunCancelled`` and unwind, giving the
    agent's toolsets a chance to exit (tearing down a provisioned sandbox, for one) before
    SIGKILL rather than leaving the run to die mid-flight.

    This needs the Task SDK to call ``on_kill`` on SIGTERM, which lands in Airflow 3.0.4 and
    3.1.0. On 3.0.0 to 3.0.3 the handler is absent, so a kill runs the pre-existing path (the
    run continues until SIGKILL).
    """

    # Set only while a run is in flight. Read by on_kill from the signal handler.
    _cancellation_token: CancellationToken | None = None

    # Provided by BaseOperator at runtime. Declared here for the type checker.
    log: Any

    def run_agent_sync(
        self, agent: Agent[Any, Any], user_prompt: Any, **run_kwargs: Any
    ) -> AgentRunResult[Any]:
        """Call ``agent.run_sync`` under a fresh cancellation token held for :meth:`on_kill`."""
        self._cancellation_token = CancellationToken()
        try:
            return agent.run_sync(user_prompt, cancellation_token=self._cancellation_token, **run_kwargs)
        finally:
            self._cancellation_token = None

    def on_kill(self) -> None:
        token = self._cancellation_token
        if token is None:
            return
        self.log.info("Task killed, cancelling in-flight agent run")
        # Cancel from a separate thread, not inline. on_kill runs in the Task SDK's
        # SIGTERM handler on the same thread that drives run_sync, and cancel() only
        # interrupts a blocked run when issued from a different thread. Called inline it
        # defers until the in-flight await returns, so the worker is SIGKILLed at the
        # grace deadline before the run unwinds.
        threading.Thread(target=token.cancel, name="agent-run-cancel", daemon=True).start()
