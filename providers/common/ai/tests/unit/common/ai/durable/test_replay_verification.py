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
End-to-end replay verification through a real pydantic-ai agent loop.

Simulates the retry scenario durable execution exists for: attempt 1 fails
partway with steps cached, attempt 2 starts a fresh counter against the same
cache file. Replay must happen when the agent is unchanged and must NOT happen
when the agent changed between attempts (the positional-keying staleness bug).
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.toolsets import FunctionToolset

from airflow.providers.common.ai.durable.caching_model import CachingModel
from airflow.providers.common.ai.durable.caching_toolset import CachingToolset
from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.sdk import ObjectStoragePath


@pytest.fixture
def storage(tmp_path):
    with patch("airflow.providers.common.ai.durable.storage._get_base_path") as mock_base:
        mock_base.return_value = ObjectStoragePath(f"file://{tmp_path.as_posix()}")
        yield DurableStorage(dag_id="d", task_id="t", run_id="r")


class AgentHarness:
    """A scripted two-step agent: one tool call, then a final answer."""

    def __init__(self, storage: DurableStorage, *, system_prompt: str, rate: float, fail: bool):
        self.live_model_calls = 0
        self.live_tool_calls = 0
        self.counter = DurableStepCounter()

        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            self.live_model_calls += 1
            if not any(isinstance(m, ModelResponse) for m in messages):
                return ModelResponse(parts=[ToolCallPart(tool_name="get_fx_rate", args={})])
            if fail:
                raise RuntimeError("simulated transient failure")
            returned = next(p.content for m in messages for p in m.parts if p.part_kind == "tool-return")
            return ModelResponse(parts=[TextPart(content=f"rate={returned}")])

        def get_fx_rate() -> float:
            """Return the USD to EUR exchange rate."""
            self.live_tool_calls += 1
            return rate

        # Constructor form: the @toolset.tool decorator is typed for
        # RunContext-taking functions only.
        toolset = FunctionToolset(tools=[get_fx_rate])

        self.agent = Agent(
            model=CachingModel(FunctionModel(model_fn), storage=storage, counter=self.counter),
            system_prompt=system_prompt,
            toolsets=[CachingToolset(wrapped=toolset, storage=storage, counter=self.counter)],
        )

    async def run(self) -> str:
        result = await self.agent.run("Convert 100 USD to EUR")
        return result.output


async def failed_first_attempt(storage: DurableStorage, system_prompt: str = "currency bot") -> None:
    """Attempt 1: tool result 0.42 gets cached, then the final model call fails."""
    harness = AgentHarness(storage, system_prompt=system_prompt, rate=0.42, fail=True)
    with pytest.raises(RuntimeError, match="simulated transient failure"):
        await harness.run()
    storage._cache = None  # retry runs in a new process: in-memory cache is cold


class TestUnchangedRetryReplays:
    @pytest.mark.asyncio
    async def test_completed_steps_replay_only_failed_step_reruns(self, storage):
        await failed_first_attempt(storage)

        retry = AgentHarness(storage, system_prompt="currency bot", rate=0.42, fail=False)
        output = await retry.run()

        assert output == "rate=0.42"
        assert retry.counter.replayed_model == 1
        assert retry.counter.replayed_tool == 1
        assert retry.live_tool_calls == 0
        assert retry.live_model_calls == 1  # only the step that failed on attempt 1


class TestChangedAgentDoesNotReplayStaleSteps:
    @pytest.mark.asyncio
    async def test_changed_system_prompt_invalidates_replay(self, storage):
        """Regression test for positional-keying staleness: a prompt tweak between
        attempts must re-run the conversation, not replay the old one."""
        await failed_first_attempt(storage)

        retry = AgentHarness(storage, system_prompt="careful currency bot", rate=0.99, fail=False)
        output = await retry.run()

        # The fixed tool ran in the new conversation; nothing stale was replayed.
        assert output == "rate=0.99"
        assert retry.counter.replayed_model == 0
        assert retry.counter.replayed_tool == 0
        assert retry.live_tool_calls == 1

    @pytest.mark.asyncio
    async def test_divergence_chains_to_downstream_tool_steps(self, storage):
        """A live model call mints fresh tool_call_ids, so cached tool results
        recorded under the old conversation cannot be cross-wired into the new one."""
        await failed_first_attempt(storage)

        retry = AgentHarness(storage, system_prompt="careful currency bot", rate=0.42, fail=False)
        await retry.run()

        # Same tool name and args as attempt 1, but the conversation diverged
        # at step 0 -- the tool must run live, not replay.
        assert retry.live_tool_calls == 1
        assert retry.counter.replayed_tool == 0
