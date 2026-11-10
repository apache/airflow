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
from pydantic_ai import Agent, CancellationToken
from pydantic_ai.exceptions import FallbackExceptionGroup, ModelHTTPError
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.fallback import FallbackModel
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.toolsets import FunctionToolset

from airflow.providers.common.ai.durable import AirflowDurability
from airflow.providers.common.ai.durable.journal import DurableJournal, journal_scope
from airflow.providers.common.ai.exceptions import FallbackChainExhaustedError
from airflow.providers.common.ai.mixins.cancellable_run import CancellableAgentRunMixin
from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.operators.llm import LLMOperator

from unit.common.ai.durable.memory_storage import MemoryStorage


def _failing_member(status_code: int):
    def fail(messages, info: AgentInfo) -> ModelResponse:
        raise ModelHTTPError(status_code=status_code, model_name="member")

    return fail


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


class TestFallbackChainExhausted:
    def test_group_is_raised_again_as_chain_error_with_members_in_chain_order(self):
        # Without the ``except FallbackExceptionGroup`` in run_agent_sync the run raises the bare
        # group, and ``pytest.raises(FallbackChainExhaustedError)`` fails.
        chain = FallbackModel(FunctionModel(_failing_member(429)), FunctionModel(_failing_member(400)))

        with pytest.raises(FallbackChainExhaustedError) as caught:
            CancellableAgentRunMixin().run_agent_sync(Agent(chain), "go")

        error = caught.value
        assert isinstance(error, FallbackExceptionGroup)
        assert isinstance(error.__cause__, FallbackExceptionGroup)
        assert not isinstance(error.__cause__, FallbackChainExhaustedError)
        assert error.member_errors == error.__cause__.exceptions
        assert [member.status_code for member in error.member_errors] == [429, 400]
        assert error.attempts == error.__cause__.attempts
        # Not durable: no journal progress to report.
        assert error.replayed_steps is None
        assert error.recorded_steps is None

    def test_durable_retry_reports_what_the_attempt_replayed_and_recorded(self):
        # Drops ``replayed_steps``/``recorded_steps`` (hard-coded None) -> both asserts below fail.
        storage = MemoryStorage()
        mixin = CancellableAgentRunMixin()
        failing_after = {"responses": 1}

        def member(messages, info: AgentInfo) -> ModelResponse:
            answered = sum(isinstance(message, ModelResponse) for message in messages)
            if answered >= failing_after["responses"]:
                raise ModelHTTPError(status_code=429, model_name="member")
            return ModelResponse(parts=[ToolCallPart("query", {"sql": "select 1"})])

        def build() -> Agent[None, str]:
            toolset = FunctionToolset(id="db")

            @toolset.tool_plain
            def query(sql: str) -> str:
                return "3 rows"

            return Agent(
                FallbackModel(FunctionModel(member), FunctionModel(member)),
                name="analyst",
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        def run_attempt() -> FallbackChainExhaustedError:
            with journal_scope(DurableJournal(storage)):
                with pytest.raises(FallbackChainExhaustedError) as caught:
                    mixin.run_agent_sync(build(), "go")
            return caught.value

        first = run_attempt()
        # The second request of the first attempt fails; one model step and one tool step were recorded.
        assert (first.replayed_steps, first.recorded_steps) == (0, 2)

        failing_after["responses"] = 2
        second = run_attempt()
        assert (second.replayed_steps, second.recorded_steps) == (2, 2)

    def test_error_that_is_already_a_chain_error_is_not_wrapped_again(self):
        agent = MagicMock(spec=["run_sync"])
        original = FallbackChainExhaustedError("all failed", [RuntimeError("a")])
        agent.run_sync.side_effect = original

        with pytest.raises(FallbackChainExhaustedError) as caught:
            CancellableAgentRunMixin().run_agent_sync(agent, "go")

        assert caught.value is original


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
