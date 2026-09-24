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

import threading
import time
from unittest.mock import DEFAULT, MagicMock

import pytest
from pydantic_ai import CancellationToken

from airflow.providers.common.ai.mixins.cancellable_run import CancellableAgentRunMixin
from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.operators.llm import LLMOperator


class TestRunAgentSync:
    def test_forwards_held_cancellation_token_and_clears_after_success(self):
        mixin = CancellableAgentRunMixin()
        agent = MagicMock(spec=["run_sync"])
        held: dict[str, object] = {}

        def capture(*args, **kwargs):
            # Capture the token the mixin holds mid-run: that is the one on_kill would cancel.
            held["token"] = mixin._cancellation_token
            return DEFAULT

        agent.run_sync.side_effect = capture

        result = mixin.run_agent_sync(agent, "prompt", usage_limits=None)

        assert result is agent.run_sync.return_value
        passed = agent.run_sync.call_args.kwargs["cancellation_token"]
        assert isinstance(passed, CancellationToken)
        # run_sync must receive the exact token on_kill cancels, not just some CancellationToken.
        assert passed is held["token"]
        agent.run_sync.assert_called_once_with("prompt", cancellation_token=passed, usage_limits=None)
        # The token is dropped once the run returns so a later on_kill is a no-op.
        assert mixin._cancellation_token is None

    def test_clears_token_when_run_raises(self):
        mixin = CancellableAgentRunMixin()
        agent = MagicMock(spec=["run_sync"])
        agent.run_sync.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError):
            mixin.run_agent_sync(agent, "prompt")

        assert mixin._cancellation_token is None


class TestOnKill:
    def test_noop_when_no_run_active(self):
        mixin = CancellableAgentRunMixin()
        mixin.log = MagicMock()
        # No run in flight: on_kill must not raise and must not cancel anything.
        mixin.on_kill()

    def test_cancels_active_token_off_the_calling_thread(self):
        """cancel() only interrupts a blocked run_sync when issued from a thread other than
        the one running the run. on_kill runs in the SIGTERM handler on that same thread, so
        it must hand the cancel to a separate thread. An inline cancel would run on the
        calling thread and fail this test."""
        mixin = CancellableAgentRunMixin()
        mixin.log = MagicMock()
        cancel_thread: dict[str, int] = {}
        token = MagicMock(spec=CancellationToken)
        token.cancel.side_effect = lambda: cancel_thread.setdefault("id", threading.get_ident())
        mixin._cancellation_token = token

        mixin.on_kill()

        deadline = time.monotonic() + 2
        while "id" not in cancel_thread and time.monotonic() < deadline:
            time.sleep(0.01)
        token.cancel.assert_called_once_with()
        assert cancel_thread["id"] != threading.get_ident()


class TestOnKillMroBinding:
    @pytest.mark.parametrize("operator_cls", [AgentOperator, LLMOperator])
    def test_operator_on_kill_resolves_to_mixin(self, operator_cls):
        """The mixin must precede BaseOperator in the bases so its on_kill wins. Reordering it
        after BaseOperator (to match the sibling mixins) would silently restore the no-op and
        disable kill-time cancellation, which the direct on_kill tests above would not catch."""
        assert operator_cls.on_kill is CancellableAgentRunMixin.on_kill
