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
AirflowDurability through a real pydantic-ai agent loop.

Each test runs the scenario durable execution exists for: an attempt fails partway, and
the retry runs the agent again from the top with a fresh journal over the same storage.
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import TYPE_CHECKING, Any

import pytest
from pydantic_ai import Agent, CancellationToken, RunContext
from pydantic_ai.capabilities import AbstractCapability, durable_operation
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.toolsets import FunctionToolset
from pydantic_ai.usage import RequestUsage, RunUsage, UsageLimits

from airflow.providers.common.ai.durable import AirflowDurability
from airflow.providers.common.ai.durable.journal import DurableJournal, journal_scope
from airflow.providers.common.ai.exceptions import DurableJournalError
from airflow.providers.common.ai.utils.toolset_base import AirflowToolset, ensure_masked

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import ToolsetTool


class Calls:
    """Counts the live calls one test's model and tools make, across attempts."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def bump(self, name: str) -> None:
        self.counts[name] = self.counts.get(name, 0) + 1

    def __getitem__(self, name: str) -> int:
        return self.counts.get(name, 0)


def responses_so_far(messages: list[ModelMessage]) -> int:
    return sum(isinstance(message, ModelResponse) for message in messages)


def tool_returns(messages: list[ModelMessage]) -> list[ToolReturnPart]:
    return [part for message in messages for part in message.parts if isinstance(part, ToolReturnPart)]


async def attempt(storage, agent: Agent[Any, Any], prompt: str = "go", **run_kwargs: Any) -> Any:
    """Run one task attempt: a fresh journal over the storage the attempts share."""
    with journal_scope(DurableJournal(storage)):
        return await agent.run(prompt, **run_kwargs)


class _WarehouseToolset(AirflowToolset):
    """An Airflow toolset with one ``query`` tool, standing in for SQLToolset and friends."""

    def __init__(self, calls: Calls, result: Any = "3 rows", *, replayable: bool = True) -> None:
        self._calls = calls
        self._result = result
        self.replayable = replayable

        def query(sql: str) -> str:
            """Run a query."""
            raise AssertionError("served by execute_tool")

        self._inner = FunctionToolset(tools=[query])

    @property
    def id(self) -> str:
        return "warehouse"

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools = await self._inner.get_tools(ctx)
        return {name: dataclasses.replace(tool, toolset=self) for name, tool in tools.items()}

    async def execute_tool(self, name, tool_args, *, ctx, tool) -> Any:
        self._calls.bump("query")
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result


def tool_then_answer(calls: Calls, tool_name: str, *, fail_final: list[bool]):
    """A model that calls ``tool_name`` once, then answers with what the tool returned."""

    def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.bump("model")
        if responses_so_far(messages) == 0:
            return ModelResponse(parts=[ToolCallPart(tool_name, {"sql": "select 1"})])
        if fail_final[0]:
            raise RuntimeError("worker died")
        return ModelResponse(parts=[TextPart(f"answer: {tool_returns(messages)[-1].content}")])

    return model_fn


class TestReplay:
    @pytest.mark.asyncio
    async def test_retry_replays_completed_steps_and_runs_only_the_failed_one(self, memory_storage):
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            toolset = FunctionToolset(id="db")

            @toolset.tool_plain
            def query(sql: str) -> str:
                calls.bump("query")
                return "3 rows"

            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail_final)),
                name="analyst",
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        fail_final[0] = False
        result = await attempt(memory_storage, build())

        assert result.output == "answer: 3 rows"
        assert calls["query"] == 1
        # The first model step replayed; only the one that failed runs again.
        assert calls["model"] == 3

    @pytest.mark.asyncio
    async def test_changed_prompt_runs_everything_again(self, memory_storage):
        calls = Calls()
        fail_final = [True]

        def build(instructions: str) -> Agent[None, str]:
            toolset = FunctionToolset(id="db")

            @toolset.tool_plain
            def query(sql: str) -> str:
                calls.bump("query")
                return "3 rows"

            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail_final)),
                name="analyst",
                instructions=instructions,
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError):
            await attempt(memory_storage, build("be terse"))
        fail_final[0] = False
        await attempt(memory_storage, build("be thorough"))

        assert calls["query"] == 2
        assert calls["model"] == 4

    @pytest.mark.asyncio
    async def test_parallel_tool_calls_replay_by_the_order_they_started(self, memory_storage):
        calls = Calls()
        delays: dict[int, float] = {}
        fail_final = [True]

        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            if responses_so_far(messages) == 0:
                return ModelResponse(parts=[ToolCallPart("charge", {"amount": n}) for n in (1, 2, 3)])
            if fail_final[0]:
                raise RuntimeError("worker died")
            return ModelResponse(parts=[TextPart(",".join(str(p.content) for p in tool_returns(messages)))])

        def build() -> Agent[None, str]:
            toolset = FunctionToolset(id="billing")

            @toolset.tool_plain
            async def charge(amount: int) -> str:
                await asyncio.sleep(delays.get(amount, 0))
                calls.bump(f"charge_{amount}")
                return f"charged {amount}"

            return Agent(
                FunctionModel(model_fn), name="biller", toolsets=[toolset], capabilities=[AirflowDurability()]
            )

        # The first attempt finishes the calls in the reverse of the order it started them.
        delays.update({1: 0.03, 2: 0.02, 3: 0.0})
        with pytest.raises(RuntimeError):
            await attempt(memory_storage, build())
        fail_final[0] = False
        delays.clear()
        result = await attempt(memory_storage, build())

        assert result.output == "charged 1,charged 2,charged 3"
        assert calls.counts == {"charge_1": 1, "charge_2": 1, "charge_3": 1}

    @pytest.mark.asyncio
    async def test_outside_a_journal_the_agent_runs_normally(self, memory_storage):
        calls = Calls()
        toolset = FunctionToolset(id="db")

        @toolset.tool_plain
        def query(sql: str) -> str:
            calls.bump("query")
            return "3 rows"

        agent = Agent(
            FunctionModel(tool_then_answer(calls, "query", fail_final=[False])),
            name="analyst",
            toolsets=[toolset],
            capabilities=[AirflowDurability()],
        )

        result = await agent.run("go")

        assert result.output == "answer: 3 rows"
        assert memory_storage.entries == {}


class TestResultsThatCannotBeRecorded:
    @pytest.mark.asyncio
    async def test_a_function_tool_result_that_is_not_json_fails_without_retrying(self, memory_storage):
        toolset = FunctionToolset(id="db")

        @toolset.tool_plain
        def query(sql: str) -> object:
            return object()

        agent = Agent(
            FunctionModel(tool_then_answer(Calls(), "query", fail_final=[False])),
            name="analyst",
            toolsets=[toolset],
            capabilities=[AirflowDurability()],
        )

        with pytest.raises(DurableJournalError, match="not JSON-serializable"):
            await attempt(memory_storage, agent)

    @pytest.mark.asyncio
    async def test_an_airflow_toolset_result_that_is_not_json_fails_without_retrying(self, memory_storage):
        agent = Agent(
            FunctionModel(tool_then_answer(Calls(), "query", fail_final=[False])),
            name="analyst",
            toolsets=[_WarehouseToolset(Calls(), object())],
            capabilities=[AirflowDurability()],
        )

        with pytest.raises(DurableJournalError, match="not JSON-serializable"):
            await attempt(memory_storage, agent)


class TestStreaming:
    @pytest.mark.asyncio
    async def test_a_streamed_model_request_replays(self, memory_storage):
        calls = Calls()

        async def stream_fn(messages: list[ModelMessage], info: AgentInfo):
            calls.bump("model")
            yield "streamed answer"

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(stream_function=stream_fn), name="streamer", capabilities=[AirflowDurability()]
            )

        outputs = []
        for _ in range(2):
            with journal_scope(DurableJournal(memory_storage)):
                async with build().run_stream("go") as result:
                    outputs.append(await result.get_output())

        assert outputs == ["streamed answer", "streamed answer"]
        assert calls["model"] == 1


class TestCancellation:
    @pytest.mark.asyncio
    async def test_a_run_inside_the_task_accepts_a_cancellation_token(self, memory_storage):
        """AgentOperator.on_kill cancels the run in the task process, which is the durable container."""
        agent = Agent(
            FunctionModel(tool_then_answer(Calls(), "query", fail_final=[False])),
            name="analyst",
            toolsets=[_WarehouseToolset(Calls())],
            capabilities=[AirflowDurability()],
        )

        result = await attempt(memory_storage, agent, cancellation_token=CancellationToken())

        assert result.output == "answer: 3 rows"


class TestAirflowToolsets:
    """Airflow's own toolsets are not durable units of pydantic-ai's backend; the capability journals them."""

    @pytest.mark.asyncio
    async def test_airflow_toolset_calls_replay(self, memory_storage):
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail_final)),
                name="analyst",
                toolsets=[_WarehouseToolset(calls)],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError):
            await attempt(memory_storage, build())
        fail_final[0] = False
        result = await attempt(memory_storage, build())

        assert result.output == "answer: 3 rows"
        assert calls["query"] == 1

    @pytest.mark.asyncio
    async def test_a_model_retry_replays_without_running_the_tool(self, memory_storage):
        calls = Calls()
        fail_final = [True]

        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            if responses_so_far(messages) == 0:
                return ModelResponse(parts=[ToolCallPart("query", {"sql": "select nope"})])
            if fail_final[0]:
                raise RuntimeError("worker died")
            return ModelResponse(parts=[TextPart(f"retry said: {messages[-1].parts[0].content}")])

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(model_fn),
                name="analyst",
                toolsets=[_WarehouseToolset(calls, ModelRetry("no column nope"))],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError):
            await attempt(memory_storage, build())
        fail_final[0] = False
        result = await attempt(memory_storage, build())

        assert result.output == "retry said: no column nope"
        assert calls["query"] == 1

    @pytest.mark.asyncio
    async def test_a_toolset_that_is_not_replayable_runs_again(self, memory_storage):
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail_final)),
                name="analyst",
                toolsets=[_WarehouseToolset(calls, replayable=False)],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError):
            await attempt(memory_storage, build())
        fail_final[0] = False
        await attempt(memory_storage, build())

        assert calls["query"] == 2
        # The model step before it still replayed.
        assert calls["model"] == 3


@pytest.mark.enable_redact
class TestMasking:
    @pytest.mark.asyncio
    async def test_journal_holds_function_tool_results_masked(self, memory_storage, registered_secret):
        toolset = FunctionToolset(id="db")

        @toolset.tool_plain
        def query(sql: str) -> str:
            return f"password={registered_secret}"

        agent = Agent(
            FunctionModel(tool_then_answer(Calls(), "query", fail_final=[False])),
            name="analyst",
            # AgentOperator puts the masking wrapper outside the durable unit.
            toolsets=[ensure_masked(toolset)],
            capabilities=[AirflowDurability()],
        )

        result = await attempt(memory_storage, agent)

        assert result.output == "answer: password=***"
        assert registered_secret not in str(memory_storage.entries)

    @pytest.mark.asyncio
    async def test_journal_holds_airflow_toolset_results_masked(self, memory_storage, registered_secret):
        agent = Agent(
            FunctionModel(tool_then_answer(Calls(), "query", fail_final=[False])),
            name="analyst",
            toolsets=[_WarehouseToolset(Calls(), f"password={registered_secret}")],
            capabilities=[AirflowDurability()],
        )

        await attempt(memory_storage, agent)

        assert registered_secret not in str(memory_storage.entries)


class _Ledger(AbstractCapability[Any]):
    """Stands in for pydantic-ai-harness SpendLimits: it accrues through a durable operation."""

    def __init__(self, calls: Calls, id: str | None = "ledger") -> None:
        self._calls = calls
        self._id = id

    @property
    def id(self) -> str | None:
        return self._id

    async def after_model_request(
        self, ctx: RunContext[Any], *, request_context: Any, response: ModelResponse
    ):
        await self.accrue(ctx, 1)
        return response

    @durable_operation(name="accrue")
    async def accrue(self, ctx: RunContext[Any], amount: int) -> int:
        self._calls.bump("accrue")
        return amount


class TestSharedCapability:
    @pytest.mark.asyncio
    async def test_one_instance_on_two_agents_fingerprints_each_agents_own_model(self, memory_storage):
        """Changing the second agent's model between attempts must not replay its old response."""

        # FunctionModel is named after its function, so each is a different model.
        def answer_a(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            return ModelResponse(parts=[TextPart("a")])

        def answer_b1(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            return ModelResponse(parts=[TextPart("b1")])

        def answer_b2(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            return ModelResponse(parts=[TextPart("b2")])

        async def task_attempt(second_model) -> str:
            durability = AirflowDurability()
            first = Agent(FunctionModel(answer_a), name="first", capabilities=[durability])
            second = Agent(FunctionModel(second_model), name="second", capabilities=[durability])
            with journal_scope(DurableJournal(memory_storage)):
                await first.run("go")
                return (await second.run("go")).output

        await task_attempt(answer_b1)
        output = await task_attempt(answer_b2)

        assert output == "b2"


class TestCapabilityOperations:
    @pytest.mark.asyncio
    async def test_durable_operations_of_other_capabilities_replay(self, memory_storage):
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            toolset = FunctionToolset(id="db")

            @toolset.tool_plain
            def query(sql: str) -> str:
                return "3 rows"

            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail_final)),
                name="analyst",
                toolsets=[toolset],
                capabilities=[AirflowDurability(), _Ledger(calls)],
            )

        with pytest.raises(RuntimeError):
            await attempt(memory_storage, build())
        fail_final[0] = False
        await attempt(memory_storage, build())

        # One accrual per completed model request, none repeated by the retry.
        assert calls["accrue"] == 2

    @pytest.mark.asyncio
    async def test_durable_operations_run_directly_outside_a_task(self):
        calls = Calls()
        agent = Agent(
            FunctionModel(lambda messages, info: ModelResponse(parts=[TextPart("done")])),
            name="analyst",
            capabilities=[AirflowDurability(), _Ledger(calls)],
        )

        await agent.run("go")

        assert calls["accrue"] == 1


class TestRuns:
    @pytest.mark.asyncio
    async def test_each_run_of_an_attempt_replays_on_its_own(self, memory_storage):
        """A task that runs two agents replays both when the second one failed."""
        calls = Calls()
        fail_final = [True]

        def build(name: str, fail: list[bool]) -> Agent[None, str]:
            toolset = FunctionToolset(id="db")

            @toolset.tool_plain
            def query(sql: str) -> str:
                calls.bump(f"query_{name}")
                return "3 rows"

            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail)),
                name=name,
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        async def task_attempt() -> None:
            with journal_scope(DurableJournal(memory_storage)):
                await build("first", [False]).run("go")
                await build("second", fail_final).run("go")

        with pytest.raises(RuntimeError):
            await task_attempt()
        fail_final[0] = False
        await task_attempt()

        assert calls["query_first"] == 1
        assert calls["query_second"] == 1

    @pytest.mark.asyncio
    async def test_agents_called_from_tools_replay_their_own_steps(self, memory_storage):
        """
        The outer agent's first tool runs an agent that succeeds, its second one an agent that fails.

        On retry the first tool replays, so its agent never starts; the second tool's agent must
        still find its own recorded tool call rather than the first agent's.
        """
        calls = Calls()
        fail_second = [True]

        def sub_agent(label: str, fail: list[bool]) -> Agent[None, str]:
            toolset = FunctionToolset(id=f"{label}_tools")

            @toolset.tool_plain
            def fetch(sql: str) -> str:
                calls.bump(f"fetch_{label}")
                return f"{label} rows"

            return Agent(
                FunctionModel(tool_then_answer(Calls(), "fetch", fail_final=fail)),
                name=f"sub_{label}",
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        def outer_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            done = [part.tool_name for part in tool_returns(messages)]
            if not done:
                return ModelResponse(parts=[ToolCallPart("first", {})])
            if done == ["first"]:
                return ModelResponse(parts=[ToolCallPart("second", {})])
            return ModelResponse(parts=[TextPart("all done")])

        def build() -> Agent[None, str]:
            toolset = FunctionToolset(id="delegates")

            @toolset.tool_plain
            async def first() -> str:
                return (await sub_agent("first", [False]).run("go")).output

            @toolset.tool_plain
            async def second() -> str:
                return (await sub_agent("second", fail_second).run("go")).output

            return Agent(
                FunctionModel(outer_model),
                name="outer",
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError):
            await attempt(memory_storage, build())
        fail_second[0] = False
        result = await attempt(memory_storage, build())

        assert result.output == "all done"
        assert calls.counts == {"fetch_first": 1, "fetch_second": 1}

    @pytest.mark.asyncio
    async def test_a_journal_that_cleans_up_after_each_run_deletes_a_run_that_succeeded(self, memory_storage):
        toolset = FunctionToolset(id="db")

        @toolset.tool_plain
        def query(sql: str) -> str:
            return "3 rows"

        agent = Agent(
            FunctionModel(tool_then_answer(Calls(), "query", fail_final=[False])),
            name="analyst",
            toolsets=[toolset],
            capabilities=[AirflowDurability()],
        )

        with journal_scope(DurableJournal(memory_storage, clean_up_after_run=True)):
            await agent.run("go")

        assert memory_storage.entries == {}


class TestReplayUsage:
    """A replayed step adds nothing to the usage the run counts and limits."""

    @staticmethod
    def priced_model(calls: Calls, fail_final: list[bool]):
        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            calls.bump("model")
            if responses_so_far(messages) == 0:
                return ModelResponse(
                    parts=[ToolCallPart("query", {"sql": "select 1"})],
                    usage=RequestUsage(input_tokens=100, output_tokens=10),
                )
            if fail_final[0]:
                raise RuntimeError("worker died")
            return ModelResponse(
                parts=[TextPart("done")], usage=RequestUsage(input_tokens=200, output_tokens=20)
            )

        return model_fn

    def build(self, calls: Calls, fail_final: list[bool]) -> Agent[None, str]:
        toolset = FunctionToolset(id="db")

        @toolset.tool_plain
        def query(sql: str) -> str:
            return "3 rows"

        return Agent(
            FunctionModel(self.priced_model(calls, fail_final)),
            name="analyst",
            toolsets=[toolset],
            capabilities=[AirflowDurability()],
        )

    async def ledgered_attempt(self, storage, agent, usage: RunUsage, limits: UsageLimits | None = None):
        """An attempt that, like AgentOperator, carries one usage object across attempts."""
        return await attempt(storage, agent, usage=usage, usage_limits=limits)

    @pytest.mark.asyncio
    async def test_the_retry_counts_only_its_live_work(self, memory_storage):
        calls = Calls()
        fail_final = [True]
        with pytest.raises(RuntimeError):
            await self.ledgered_attempt(memory_storage, self.build(calls, fail_final), RunUsage())
        fail_final[0] = False
        usage = RunUsage()

        await self.ledgered_attempt(memory_storage, self.build(calls, fail_final), usage)

        assert (usage.requests, usage.tool_calls, usage.input_tokens, usage.output_tokens) == (1, 0, 200, 20)

    @pytest.mark.asyncio
    async def test_a_retry_whose_budget_is_spent_can_still_replay(self, memory_storage):
        """The first request replays for free, so a request limit already reached must not stop the run."""
        calls = Calls()
        fail_final = [True]
        with pytest.raises(RuntimeError):
            await self.ledgered_attempt(memory_storage, self.build(calls, fail_final), RunUsage())
        fail_final[0] = False
        # The cross-attempt budget already holds the first attempt's two requests.
        usage = RunUsage(requests=2)

        result = await self.ledgered_attempt(
            memory_storage, self.build(calls, fail_final), usage, UsageLimits(request_limit=3)
        )

        assert result.output == "done"
        assert usage.requests == 3
