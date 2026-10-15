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
AirflowDurability with pydantic-ai-harness's ``CodeMode``, through a real agent loop.

``run_code`` is not a step of its own: a retry runs each snippet again, and the tool
calls the snippet makes replay from the journal one by one.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from pydantic_ai import Agent, RunContext
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.toolsets import FunctionToolset
from pydantic_ai_harness.code_mode import CodeMode

from airflow.providers.common.ai.durable import AirflowDurability
from airflow.providers.common.ai.durable.journal import DurableJournal, journal_scope

from unit.common.ai.durable.test_capability import Calls, attempt, responses_so_far, tool_returns

GATHER = "import asyncio\na, b = await asyncio.gather(fetch(name='a'), fetch(name='b'))\n[a, b]"


def fetch_toolset(calls: Calls, started: dict[str, asyncio.Event] | None = None) -> FunctionToolset[Any]:
    toolset = FunctionToolset(id="db")

    @toolset.tool_plain
    async def fetch(name: str) -> str:
        """Fetch a record."""
        calls.bump(f"fetch:{name}")
        if started is not None and name in started:
            started[name].set()
        return f"{name}-data"

    return toolset


def run_code_then_answer(calls: Calls, snippets: list[str], *, fail_final: list[bool]):
    """A model that sends each snippet to ``run_code`` in turn, then answers with the last result."""

    def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.bump("model")
        if (sent := responses_so_far(messages)) < len(snippets):
            return ModelResponse(parts=[ToolCallPart("run_code", {"code": snippets[sent]})])
        if fail_final[0]:
            raise RuntimeError("worker died")
        return ModelResponse(parts=[TextPart(f"answer: {tool_returns(messages)[-1].content}")])

    return model_fn


def streamed(model_fn):
    """The streaming form of ``model_fn``, for runs a capability streams (``eager``, ``speculate``)."""

    async def stream_fn(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str | DeltaToolCalls]:
        for part in model_fn(messages, info).parts:
            if isinstance(part, ToolCallPart):
                yield {0: DeltaToolCall(name=part.tool_name, json_args=part.args_as_json_str())}
            elif isinstance(part, TextPart):
                yield part.content

    return stream_fn


class _HoldBack(AbstractCapability[Any]):
    """Holds one tool call back until another has started, so ``asyncio.gather`` starts them in another order."""

    def __init__(self, held_back: str, until_started: asyncio.Event) -> None:
        self.held_back = held_back
        self.until_started = until_started

    async def before_tool_execute(self, ctx: RunContext[Any], *, call, tool_def, args):
        if args.get("name") == self.held_back:
            await asyncio.wait_for(self.until_started.wait(), timeout=5)
        return args


class TestReplay:
    @pytest.mark.asyncio
    async def test_a_retry_replays_the_steps_after_a_run_code_call(self, memory_storage):
        """
        The snippet runs again and its nested calls replay, so the ``run_code`` return the
        next model request sees differs only in the timestamps of those nested calls.
        """
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(
                    run_code_then_answer(calls, [GATHER, "await fetch(name='c')"], fail_final=fail_final)
                ),
                name="analyst",
                toolsets=[fetch_toolset(calls)],
                capabilities=[CodeMode(), AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        fail_final[0] = False
        with journal_scope(journal := DurableJournal(memory_storage)):
            result = await build().run("go")

        assert result.output == "answer: c-data"
        assert dict(journal.stats.replayed) == {"model": 2, "tool": 3}
        assert dict(journal.stats.recorded) == {"model": 1}
        assert (calls["fetch:a"], calls["fetch:b"], calls["fetch:c"]) == (1, 1, 1)

    @pytest.mark.asyncio
    async def test_nested_calls_started_in_another_order_run_live(self, memory_storage):
        """
        A nested call's fingerprint holds its arguments and its call id, which code mode
        numbers in the order the snippet makes the calls. A call that reaches the journal
        in another position never replays the result recorded for its neighbour.
        """
        calls = Calls()
        fail_final = [True]

        def build(*capabilities: AbstractCapability[Any], started=None) -> Agent[None, str]:
            return Agent(
                FunctionModel(run_code_then_answer(calls, [GATHER], fail_final=fail_final)),
                name="analyst",
                toolsets=[fetch_toolset(calls, started)],
                capabilities=[CodeMode(), *capabilities, AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        fail_final[0] = False
        b_started = asyncio.Event()
        with journal_scope(journal := DurableJournal(memory_storage)):
            result = await build(_HoldBack("a", until_started=b_started), started={"b": b_started}).run("go")

        assert result.output == "answer: ['a-data', 'b-data']"
        assert dict(journal.stats.replayed) == {"model": 1}
        assert (calls["fetch:a"], calls["fetch:b"]) == (2, 2)


class TestEarlyExecution:
    """CodeMode's ``eager`` and ``speculate`` run tool calls ahead of the snippet; not while durable."""

    @pytest.mark.parametrize("durable", [False, True])
    @pytest.mark.asyncio
    async def test_speculation_is_off_while_durable(self, memory_storage, durable):
        calls = Calls()
        speculating = CodeMode(speculate=["fetch"])
        model_fn = run_code_then_answer(calls, ["await fetch(name='a')"], fail_final=[False])
        agent = Agent(
            FunctionModel(model_fn, stream_function=streamed(model_fn)),
            name="analyst",
            toolsets=[fetch_toolset(calls)],
            capabilities=[speculating, AirflowDurability()],
        )

        if durable:
            await attempt(memory_storage, agent)
        else:
            await agent.run("go")

        assert (speculating.speculation_stats.launched > 0) is not durable
        assert calls["fetch:a"] == 1

    @pytest.mark.parametrize("durable", [False, True])
    @pytest.mark.asyncio
    async def test_eager_execution_is_off_while_durable(self, memory_storage, durable):
        calls = Calls()
        seen_mid_stream: list[int] = []
        a_started = asyncio.Event()
        code = "a = await fetch(name='a')\nb = await fetch(name='b')\n[a, b]"
        args = json.dumps({"code": code})
        # A statement counts as complete once the next one has streamed in full.
        first_statement = args.index("\\n", args.index("\\n") + 2) + 2

        async def stream_fn(
            messages: list[ModelMessage], info: AgentInfo
        ) -> AsyncIterator[str | DeltaToolCalls]:
            if responses_so_far(messages):
                yield "done"
                return
            yield {0: DeltaToolCall(name="run_code", json_args=args[:first_statement], tool_call_id="rc")}
            # Give an eager pump time to run the first statement before the call completes.
            # Durable runs never start it, so they wait only long enough to show that.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(a_started.wait(), timeout=0.5 if durable else 5)
            seen_mid_stream.append(calls["fetch:a"])
            yield {0: DeltaToolCall(json_args=args[first_statement:])}

        async def handler(ctx, events) -> None:
            async for _ in events:
                pass

        agent = Agent(
            FunctionModel(stream_function=stream_fn),
            name="analyst",
            toolsets=[fetch_toolset(calls, started={"a": a_started})],
            capabilities=[CodeMode(eager=True), AirflowDurability()],
        )

        if durable:
            await attempt(memory_storage, agent, event_stream_handler=handler)
        else:
            await agent.run("go", event_stream_handler=handler)

        assert seen_mid_stream == [0 if durable else 1]
        assert (calls["fetch:a"], calls["fetch:b"]) == (1, 1)
