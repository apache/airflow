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
import base64
import dataclasses
import functools
from collections.abc import Awaitable, Callable, Iterable
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest
from pydantic import BaseModel
from pydantic_ai import Agent, CancellationToken, RunContext
from pydantic_ai.capabilities import AbstractCapability, SetToolMetadata, durable_operation
from pydantic_ai.exceptions import ModelRetry, UnexpectedModelBehavior
from pydantic_ai.mcp import MCPToolset
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    RetryPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturn,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.toolsets import DynamicToolset, FunctionToolset
from pydantic_ai.toolsets.wrapper import WrapperToolset
from pydantic_ai.usage import RequestUsage, RunUsage, UsageLimits

from airflow.providers.common.ai.durable import AirflowDurability
from airflow.providers.common.ai.durable.base import RUNS_KEY
from airflow.providers.common.ai.durable.capability import (
    _get_callable_toolset_id,
    _give_durable_ids,
    _GivenIds,
    _JournalCodec,
)
from airflow.providers.common.ai.durable.journal import DurableJournal, Unrecordable, journal_scope
from airflow.providers.common.ai.exceptions import DurableJournalError, SQLToolsetConnectionError
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.ai.utils.toolset_base import AirflowToolset, ensure_masked
from airflow.providers.common.compat.sdk import AirflowFailException

from unit.common.ai.durable.memory_storage import MemoryStorage
from unit.common.ai.sqlite_connection import create_orders_table, run_sql, sqlite_connection
from unit.common.ai.tool_parts import is_tool_result, is_tool_retry

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool

PNG = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"


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
    return [part for message in messages for part in message.parts if is_tool_result(part)]


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
    async def test_a_first_attempt_reads_back_nothing_but_the_run_list(self, memory_storage):
        calls = Calls()
        toolset = FunctionToolset(id="db")

        @toolset.tool_plain
        def query(sql: str) -> str:
            return "3 rows"

        agent = Agent(
            FunctionModel(tool_then_answer(calls, "query", fail_final=[False])),
            name="analyst",
            toolsets=[toolset],
            capabilities=[AirflowDurability()],
        )

        await attempt(memory_storage, agent)

        assert memory_storage.loads() == [RUNS_KEY]
        assert [step["kind"] for step in memory_storage.steps()] == ["model", "tool", "model"]

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


class TestLazilyValidatedIterableArgument:
    """
    A lazily validated ``Iterable`` argument must reach the tool intact.

    pydantic validates an ``Iterable[int]`` parameter into a ``ValidatorIterator``, and
    the call is fingerprinted before the tool runs. Reading it to build the fingerprint
    would leave the tool an exhausted iterator, and the wrong answer would be recorded.
    """

    @staticmethod
    def build(calls: Calls, seen: list[int], fail_final: list[bool]) -> Agent[None, str]:
        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            calls.bump("model")
            if responses_so_far(messages) == 0:
                return ModelResponse(parts=[ToolCallPart("total", {"values": [1, 2, 3]})])
            if fail_final[0]:
                raise RuntimeError("worker died")
            return ModelResponse(parts=[TextPart(f"total={tool_returns(messages)[-1].content}")])

        toolset = FunctionToolset(id="math")

        @toolset.tool_plain
        def total(values: Iterable[int]) -> int:
            """Add up the given values."""
            consumed = list(values)
            seen.extend(consumed)
            calls.bump("total")
            return sum(consumed)

        return Agent(
            FunctionModel(model_fn), name="adder", toolsets=[toolset], capabilities=[AirflowDurability()]
        )

    @pytest.mark.asyncio
    async def test_tool_still_receives_every_value(self, memory_storage):
        seen: list[int] = []

        result = await attempt(memory_storage, self.build(Calls(), seen, [False]))

        assert seen == [1, 2, 3]
        assert result.output == "total=6"

    @pytest.mark.asyncio
    async def test_retry_replays_the_result_of_the_full_input(self, memory_storage):
        calls = Calls()
        fail_final = [True]
        with pytest.raises(RuntimeError):
            await attempt(memory_storage, self.build(calls, [], fail_final))
        fail_final[0] = False

        result = await attempt(memory_storage, self.build(calls, [], fail_final))

        assert result.output == "total=6"
        # The call has no fingerprint, so it replays only because the model step before it did.
        assert calls["total"] == 1


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

    @pytest.mark.parametrize(
        ("toolset", "codec_refuses"),
        [
            # From pydantic-ai 2.66 on, pydantic-ai records a function toolset's bytes in a
            # dataclass itself, as base64, so the codec refuses them to take the same path on
            # every version. An Airflow toolset's result goes through this codec as it is.
            pytest.param(lambda calls: _function_toolset(calls, _Image(PNG)), True, id="function-toolset"),
            pytest.param(lambda calls: _WarehouseToolset(calls, _Image(PNG)), False, id="airflow-toolset"),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_journal_that_does_not_fail_leaves_the_result_unrecorded(
        self, memory_storage, monkeypatch, toolset, codec_refuses
    ):
        if codec_refuses:
            dump = _JournalCodec.dump

            def refusing_dump(codec: _JournalCodec, tp: Any, value: Any) -> Any:
                if isinstance(getattr(value, "result", None), _Image):
                    return Unrecordable(value, TypeError("no JSON form"))
                return dump(codec, tp, value)

            monkeypatch.setattr(_JournalCodec, "dump", refusing_dump)
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail_final)),
                name="analyst",
                toolsets=[toolset(calls)],
                capabilities=[AirflowDurability()],
            )

        journals = [DurableJournal(memory_storage, fail_on_unrecordable=False) for _ in range(2)]
        with journal_scope(journals[0]), pytest.raises(RuntimeError, match="worker died"):
            await build().run("go")
        fail_final[0] = False
        with journal_scope(journals[1]):
            result = await build().run("go")

        # The model reads the bytes inside the dataclass, so the run goes on; the journal cannot.
        assert result.output == f"answer: {_Image(PNG)}"
        assert calls["query"] == 2
        assert [kind for kind, _ in journals[0].stats.not_recorded] == ["tool"]

    @pytest.mark.asyncio
    async def test_a_result_that_contains_itself_fails_without_retrying(self, memory_storage):
        looped: dict[str, Any] = {}
        looped["self"] = looped
        agent = Agent(
            FunctionModel(tool_then_answer(Calls(), "query", fail_final=[False])),
            name="analyst",
            toolsets=[_function_toolset(Calls(), looped)],
            capabilities=[AirflowDurability()],
        )

        with pytest.raises(DurableJournalError, match="not JSON-serializable"):
            await attempt(memory_storage, agent)

    @pytest.mark.parametrize("fail_on_unrecordable", [True, False])
    @pytest.mark.asyncio
    async def test_a_model_response_that_is_not_json(self, memory_storage, fail_on_unrecordable):
        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            return ModelResponse(parts=[TextPart("done")], provider_details={"handle": object()})

        agent = Agent(FunctionModel(model_fn), name="analyst", capabilities=[AirflowDurability()])
        journal = DurableJournal(memory_storage, fail_on_unrecordable=fail_on_unrecordable)

        with journal_scope(journal):
            if fail_on_unrecordable:
                with pytest.raises(DurableJournalError, match="not JSON-serializable"):
                    await agent.run("go")
            else:
                assert (await agent.run("go")).output == "done"

        assert [kind for kind, _ in journal.stats.not_recorded] == ([] if fail_on_unrecordable else ["model"])

    @pytest.mark.asyncio
    async def test_a_result_of_a_toolset_that_is_not_replayable_is_not_checked(self, memory_storage):
        calls = Calls()
        agent = Agent(
            FunctionModel(tool_then_answer(calls, "query", fail_final=[False])),
            name="analyst",
            toolsets=[_WarehouseToolset(calls, _Image(PNG), replayable=False)],
            capabilities=[AirflowDurability()],
        )
        journal = DurableJournal(memory_storage)

        with journal_scope(journal):
            result = await agent.run("go")

        assert result.output == f"answer: {_Image(PNG)}"
        assert journal.stats.not_recorded == []


@dataclasses.dataclass
class _Image:
    """A tool result whose bytes the model can read but the journal cannot record."""

    data: bytes


def _function_toolset(calls: Calls, result: Any) -> FunctionToolset[None]:
    """A function toolset with one ``query`` tool that returns ``result``."""
    toolset: FunctionToolset[None] = FunctionToolset(id="db")

    @toolset.tool_plain
    def query(sql: str) -> Any:
        calls.bump("query")
        return result

    return toolset


def _build_dynamic_toolset(calls: Calls, result: Any) -> DynamicToolset[None]:
    """A dynamic toolset that builds :func:`_function_toolset` per run step."""

    async def build(ctx: RunContext[None]) -> FunctionToolset[None]:
        return _function_toolset(calls, result)

    return DynamicToolset(build, id="dynamic_db")


@dataclasses.dataclass
class _Event:
    """A tool result of the user's own whose ``kind`` matches pydantic-ai's recorded tool return."""

    name: str
    kind: str = "tool_return"

    @property
    def result(self) -> str:
        """Not a field, so it is not the ``result`` field of pydantic-ai's recorded tool return."""
        return self.name


class TestBytesResults:
    @pytest.mark.parametrize(
        ("result", "content", "metadata"),
        [
            pytest.param(PNG, PNG, None, id="png"),
            pytest.param(b"plain text", b"plain text", None, id="utf-8"),
            pytest.param(
                {"blob": b"\xff", "rows": [b"\xfe", 1]},
                {"blob": b"\xff", "rows": [b"\xfe", 1]},
                None,
                id="nested",
            ),
            # A dict a tool built that only looks like a tag is kept as it is.
            pytest.param(
                {"__airflow_bytes__": "not base64!"},
                {"__airflow_bytes__": "not base64!"},
                None,
                id="tag-shaped",
            ),
            pytest.param(
                {"__airflow_bytes__": "café"}, {"__airflow_bytes__": "café"}, None, id="tag-shaped-non-ascii"
            ),
            # Recorded as a list, which is what the model sees either way.
            pytest.param((PNG, 1), [PNG, 1], None, id="tuple"),
            # pydantic-ai records a dict whose kind collides with its own tool return differently.
            pytest.param(
                {"kind": "tool-return", "blob": PNG}, {"kind": "tool-return", "blob": PNG}, None, id="kind"
            ),
            pytest.param(
                ToolReturn(return_value={"png": PNG}, metadata={"thumbnail": PNG}),
                {"png": PNG},
                {"thumbnail": PNG},
                id="tool-return",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_function_tool_result_with_bytes_replays_the_same_bytes(
        self, memory_storage, result, content, metadata
    ):
        calls = Calls()
        fail_final = [True]
        seen: list[tuple[Any, Any]] = []

        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            calls.bump("model")
            match responses_so_far(messages):
                case 0:
                    return ModelResponse(parts=[ToolCallPart("query", {"sql": "select 1"})])
                case 1:
                    return ModelResponse(parts=[ToolCallPart("notify", {})])
            if fail_final[0]:
                raise RuntimeError("worker died")
            part = tool_returns(messages)[0]
            seen.append((part.content, part.metadata))
            return ModelResponse(parts=[TextPart("done")])

        def build() -> Agent[None, str]:
            toolset = _function_toolset(calls, result)

            @toolset.tool_plain
            def notify() -> str:
                calls.bump("notify")
                return "sent"

            return Agent(
                FunctionModel(model_fn),
                name="analyst",
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        fail_final[0] = False
        await attempt(memory_storage, build())

        assert seen == [(content, metadata)]
        assert calls["query"] == 1
        # The model request after the replayed bytes matches the recorded one, so it replays too.
        assert calls["notify"] == 1
        assert calls["model"] == 4

    @pytest.mark.parametrize(
        "result",
        [
            pytest.param({"blob": PNG}, id="dict"),
            pytest.param(ToolReturn(return_value={"blob": PNG}), id="tool-return"),
        ],
    )
    @pytest.mark.asyncio
    async def test_an_airflow_toolset_result_with_bytes_replays_the_same_bytes(self, memory_storage, result):
        calls = Calls()
        fail_final = [True]
        expected = {"blob": PNG}

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail_final)),
                name="analyst",
                toolsets=[_WarehouseToolset(calls, result)],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        fail_final[0] = False
        run = await attempt(memory_storage, build())

        assert run.output == f"answer: {expected}"
        assert calls["query"] == 1

    @pytest.mark.asyncio
    async def test_a_result_with_its_own_kind_field_replays(self, memory_storage):
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail_final)),
                name="analyst",
                toolsets=[_function_toolset(calls, _Event("deploy"))],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        fail_final[0] = False
        run = await attempt(memory_storage, build())

        assert run.output == "answer: {'name': 'deploy', 'kind': 'tool_return'}"
        assert calls["query"] == 1

    @pytest.mark.asyncio
    async def test_a_replayed_model_response_keeps_arguments_that_look_like_bytes(self, memory_storage):
        """Only tool results are tagged, so what the model wrote replays as it was written."""
        fail_final = [True]
        seen: list[Any] = []
        args = {"payload": {"__airflow_bytes__": "aGk="}}

        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            if responses_so_far(messages) == 0:
                return ModelResponse(parts=[ToolCallPart("store", args)])
            if fail_final[0]:
                raise RuntimeError("worker died")
            seen.append(next(part.args for part in messages[1].parts if isinstance(part, ToolCallPart)))
            return ModelResponse(parts=[TextPart("done")])

        def build() -> Agent[None, str]:
            toolset = FunctionToolset(id="db")

            @toolset.tool_plain
            def store(payload: dict[str, Any]) -> str:
                return "stored"

            return Agent(
                FunctionModel(model_fn),
                name="analyst",
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        fail_final[0] = False
        await attempt(memory_storage, build())

        assert seen == [args]


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


class TestUnloadableRecords:
    """A recorded result written by another version runs live instead of failing every retry."""

    @staticmethod
    def build(calls: Calls, toolset) -> Agent[None, str]:
        return Agent(
            FunctionModel(tool_then_answer(calls, "query", fail_final=[False])),
            name="analyst",
            toolsets=[toolset],
            capabilities=[AirflowDurability()],
        )

    @staticmethod
    def function_toolset(calls: Calls) -> FunctionToolset:
        toolset = FunctionToolset(id="db")

        @toolset.tool_plain
        def query(sql: str) -> str:
            calls.bump("query")
            return "3 rows"

        return toolset

    @pytest.mark.parametrize(
        ("kind", "expected_calls"),
        [
            # Every step from the first model request on runs again.
            pytest.param("model", {"model": 4, "query": 2}, id="model response"),
            # The first model request replays; the tool call and the final request run again.
            pytest.param("tool", {"model": 3, "query": 2}, id="tool result"),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_recorded_step_that_no_longer_loads_runs_live(self, memory_storage, kind, expected_calls):
        calls = Calls()
        await attempt(memory_storage, self.build(calls, self.function_toolset(calls)))
        for entry in memory_storage.steps():
            if entry["kind"] == kind:
                entry["payload"] = "written by another version"

        journal = DurableJournal(memory_storage)
        with journal_scope(journal):
            result = await self.build(calls, self.function_toolset(calls)).run("go")

        assert result.output == "answer: 3 rows"
        assert calls.counts == expected_calls
        # Counted as run live in the attempt's summary, not as replayed.
        assert journal.stats.replayed[kind] == 0

    @pytest.mark.parametrize(
        "payload",
        [
            pytest.param({"kind": "tool_return"}, id="no result"),
            pytest.param({"kind": "renamed_in_a_later_version"}, id="unknown kind"),
        ],
    )
    @pytest.mark.asyncio
    async def test_a_function_tool_record_that_no_longer_loads_runs_live(self, memory_storage, payload):
        calls = Calls()
        await attempt(memory_storage, self.build(calls, self.function_toolset(calls)))
        for entry in memory_storage.steps():
            if entry["kind"] == "tool":
                entry["payload"] = payload

        result = await attempt(memory_storage, self.build(calls, self.function_toolset(calls)))

        assert result.output == "answer: 3 rows"
        assert calls["query"] == 2

    @pytest.mark.asyncio
    async def test_a_capability_operation_record_that_no_longer_loads_runs_live(self, memory_storage):
        calls = Calls()

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(lambda messages, info: ModelResponse(parts=[TextPart("done")])),
                name="analyst",
                capabilities=[AirflowDurability(), _Ledger(calls)],
            )

        await attempt(memory_storage, build())
        for entry in memory_storage.steps():
            if "accrue" in entry["name"]:
                entry["payload"] = "not an amount"

        await attempt(memory_storage, build())

        assert calls["accrue"] == 2

    @pytest.mark.parametrize(
        "payload",
        [
            pytest.param({"kind": "renamed_in_a_later_version"}, id="unknown-kind"),
            pytest.param({"kind": "tool_return", "tool_return": "not a tool return"}, id="tool-return"),
        ],
    )
    @pytest.mark.asyncio
    async def test_an_airflow_toolset_record_that_no_longer_loads_runs_live(self, memory_storage, payload):
        calls = Calls()
        await attempt(memory_storage, self.build(calls, _WarehouseToolset(calls)))
        for entry in memory_storage.steps():
            if entry["kind"] == "tool":
                entry["payload"] = payload

        result = await attempt(memory_storage, self.build(calls, _WarehouseToolset(calls)))

        assert result.output == "answer: 3 rows"
        assert calls["query"] == 2


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
            retry = messages[-1].parts[0]
            assert is_tool_retry(retry)
            return ModelResponse(parts=[TextPart(f"retry said: {retry.content}")])

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


def _build_ops_toolset(
    calls: Calls, *, clock_metadata: dict[str, Any] | None = None, clock_changes: bool = False
) -> FunctionToolset[None]:
    toolset = FunctionToolset(id="ops")

    @toolset.tool_plain(metadata=clock_metadata)
    def clock() -> str:
        """Read the time."""
        calls.bump("clock")
        return f"tick {calls['clock']}" if clock_changes else "tick"

    @toolset.tool_plain
    def lookup(key: str) -> str:
        """Look a key up."""
        calls.bump("lookup")
        return "value"

    return toolset


def clock_then_lookup(calls: Calls, *, fail_final: list[bool]):
    """A model that calls ``clock``, then ``lookup``, then answers."""

    def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.bump("model")
        match responses_so_far(messages):
            case 0:
                return ModelResponse(parts=[ToolCallPart("clock", {})])
            case 1:
                return ModelResponse(parts=[ToolCallPart("lookup", {"key": "a"})])
        if fail_final[0]:
            raise RuntimeError("worker died")
        return ModelResponse(parts=[TextPart("done")])

    return model_fn


def _query_then_answer(sql: str, *, fail_final: list[bool]):
    """A model that runs ``sql`` once, then answers with whatever the tool sent back, error or rows."""

    def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if responses_so_far(messages) == 0:
            return ModelResponse(parts=[ToolCallPart("query", {"sql": sql})])
        if fail_final[0]:
            raise RuntimeError("worker died")
        answered = messages[-1].parts[0]
        assert isinstance(answered, (RetryPromptPart, ToolReturnPart))
        return ModelResponse(parts=[TextPart(f"answer: {answered.content}")])

    return model_fn


class TestToolOptOut:
    """A tool whose metadata says ``{"airflow": False}`` runs live on every attempt."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("clock_metadata", "extra_capabilities"),
        [
            pytest.param({"airflow": False}, [], id="tool-metadata"),
            pytest.param(None, [SetToolMetadata(tools=["clock"], airflow=False)], id="SetToolMetadata"),
        ],
    )
    @pytest.mark.parametrize(
        ("clock_changes", "expected_model_calls", "expected_lookups"),
        [
            pytest.param(False, 4, 1, id="same-result"),
            # The request after the clock sees a different result, so it and the steps after it run again.
            pytest.param(True, 5, 2, id="new-result"),
        ],
    )
    async def test_an_opted_out_function_tool_runs_again_and_the_other_steps_replay(
        self,
        memory_storage,
        clock_metadata,
        extra_capabilities,
        clock_changes,
        expected_model_calls,
        expected_lookups,
    ):
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(clock_then_lookup(calls, fail_final=fail_final)),
                name="ops",
                toolsets=[
                    _build_ops_toolset(calls, clock_metadata=clock_metadata, clock_changes=clock_changes)
                ],
                capabilities=[AirflowDurability(), *extra_capabilities],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        fail_final[0] = False
        result = await attempt(memory_storage, build())

        assert result.output == "done"
        assert calls["clock"] == 2
        assert (calls["model"], calls["lookup"]) == (expected_model_calls, expected_lookups)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("opt_out_on_attempt", [1, 2], ids=["opted-out-first", "opted-out-on-retry"])
    @pytest.mark.parametrize("dynamic", [False, True], ids=["function", "dynamic"])
    async def test_turning_the_opt_out_on_or_off_between_attempts_keeps_the_rest_replaying(
        self, memory_storage, opt_out_on_attempt, dynamic
    ):
        calls = Calls()
        fail_final = [True]

        def build(attempt_number: int) -> Agent[None, str]:
            opt_out = [SetToolMetadata(tools=["clock"], airflow=False)]

            async def ops(ctx: RunContext[None]) -> FunctionToolset[None]:
                return _build_ops_toolset(calls)

            return Agent(
                FunctionModel(clock_then_lookup(calls, fail_final=fail_final)),
                name="ops",
                toolsets=[DynamicToolset(ops, id="dynamic_ops") if dynamic else _build_ops_toolset(calls)],
                capabilities=[
                    AirflowDurability(),
                    *(opt_out if attempt_number == opt_out_on_attempt else []),
                ],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build(1))
        fail_final[0] = False
        await attempt(memory_storage, build(2))

        # The clock is live on the attempt that opts it out, and has nothing to replay after one
        # that did; the model requests and the lookup replay either way.
        assert (calls["clock"], calls["lookup"], calls["model"]) == (2, 1, 4)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("airflow", "expected_queries"), [(False, 2), (True, 1)], ids=["opted-out", "journaled"]
    )
    @pytest.mark.parametrize(
        "build_toolset",
        [_function_toolset, _WarehouseToolset, _build_dynamic_toolset],
        ids=["function", "airflow", "dynamic"],
    )
    async def test_set_tool_metadata_decides_whether_a_tool_replays(
        self, memory_storage, build_toolset, airflow, expected_queries
    ):
        calls = Calls()
        fail_final = [True]

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail_final)),
                name="analyst",
                toolsets=[build_toolset(calls, "3 rows")],
                capabilities=[AirflowDurability(), SetToolMetadata(tools=["query"], airflow=airflow)],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        fail_final[0] = False
        result = await attempt(memory_storage, build())

        assert result.output == "answer: 3 rows"
        assert calls["query"] == expected_queries
        # The model request that chose the query replayed either way.
        assert calls["model"] == 3

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", ["false", 0])
    @pytest.mark.parametrize("build_toolset", [_function_toolset, _WarehouseToolset])
    async def test_an_airflow_metadata_value_that_is_not_a_bool_fails_the_task_before_the_model_is_asked(
        self, memory_storage, value, build_toolset
    ):
        """Every attempt would fail the same way, so the task fails once, without retrying."""
        calls = Calls()
        agent = Agent(
            FunctionModel(tool_then_answer(calls, "query", fail_final=[False])),
            name="analyst",
            toolsets=[build_toolset(calls, "3 rows")],
            capabilities=[AirflowDurability(), SetToolMetadata(tools=["query"], airflow=value)],
        )

        with pytest.raises(AirflowFailException, match="Tool 'query' has invalid 'airflow' metadata"):
            await attempt(memory_storage, agent)
        assert (calls["model"], calls["query"]) == (0, 0)

    @pytest.mark.asyncio
    async def test_an_airflow_metadata_value_that_is_not_a_bool_is_ignored_outside_a_task(self):
        calls = Calls()
        agent = Agent(
            FunctionModel(tool_then_answer(calls, "query", fail_final=[False])),
            name="analyst",
            toolsets=[_function_toolset(calls, "3 rows")],
            capabilities=[AirflowDurability(), SetToolMetadata(tools=["query"], airflow="false")],
        )

        result = await agent.run("go")

        assert result.output == "answer: 3 rows"
        assert calls["query"] == 1


def _query_tools(calls: Calls, *extra: Any, instructions: str | None = None) -> FunctionToolset[None]:
    """A ``FunctionToolset`` without an id, with a ``query`` tool and any ``extra`` tools."""

    def query(sql: str) -> str:
        calls.bump("query")
        return "3 rows"

    return FunctionToolset(tools=[query, *extra], instructions=instructions)


def _build_tools(ctx: RunContext[None]) -> FunctionToolset[None]:
    """A function that builds a toolset for each run, as a Dag file defines one."""
    return FunctionToolset()


class _ToolsBuilder:
    def __call__(self, ctx: RunContext[None]) -> FunctionToolset[None]:
        return FunctionToolset()


class TestToolsetIds:
    """Toolsets without an ``id`` replay under the ids ``_give_durable_ids`` gives them, as in AgentOperator."""

    @staticmethod
    def tool_step_names(storage) -> list[str]:
        return [step["name"] for step in storage.steps() if step["kind"] == "tool"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("built_per_run", "instructions"),
        [
            pytest.param(False, None, id="function-toolset"),
            # pydantic-ai builds instruction ids from the toolset's id: it rejects a colon in it, and the
            # request fingerprint hashes them, so the id has to be the same on every attempt.
            pytest.param(False, "Count rows with SQL.", id="function-toolset-with-instructions"),
            pytest.param(True, None, id="function-building-one"),
        ],
    )
    async def test_a_toolset_without_an_id_replays_on_retry(
        self, memory_storage, built_per_run, instructions
    ):
        calls = Calls()
        fail_final = [True]

        def build_tools(ctx: RunContext[None]) -> FunctionToolset[None]:
            return _query_tools(calls)

        def build() -> Agent[None, str]:
            toolset = build_tools if built_per_run else _query_tools(calls, instructions=instructions)
            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=fail_final)),
                name="analyst",
                toolsets=[_give_durable_ids(toolset, {})],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        names = self.tool_step_names(memory_storage)
        fail_final[0] = False
        result = await attempt(memory_storage, build())

        assert result.output == "answer: 3 rows"
        assert names == [
            f"analyst__dynamic_toolset__{__name__}.{build_tools.__qualname__}.call_tool:query"
            if built_per_run
            else "analyst__function_toolset.call_tool:query"
        ]
        assert (calls["query"], calls["model"]) == (1, 3)

    @pytest.mark.asyncio
    async def test_adding_a_tool_between_attempts_keeps_the_first_tools_step_name(self, memory_storage):
        """The retry runs again from the first request, which now offers a new tool, but names its calls alike."""
        calls = Calls()

        def forecast(city: str) -> str:
            return "sunny"

        names = []
        for tools in ((), (forecast,)):
            agent = Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=[True])),
                name="analyst",
                toolsets=[_give_durable_ids(_query_tools(calls, *tools), {})],
                capabilities=[AirflowDurability()],
            )
            with pytest.raises(RuntimeError, match="worker died"):
                await attempt(memory_storage, agent)
            names.append(self.tool_step_names(memory_storage))

        assert names == [["analyst__function_toolset.call_tool:query"]] * 2
        assert calls["query"] == 2

    @pytest.mark.asyncio
    async def test_an_id_the_author_chose_keeps_its_step_names(self, memory_storage):
        """The step names journals were recorded under before Airflow gave toolsets ids."""
        calls = Calls()
        toolset = FunctionToolset(tools=list(_query_tools(calls).tools.values()), id="db")
        agent = Agent(
            FunctionModel(tool_then_answer(calls, "query", fail_final=[True])),
            name="analyst",
            toolsets=[_give_durable_ids(toolset, {})],
            capabilities=[AirflowDurability()],
        )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, agent)

        assert self.tool_step_names(memory_storage) == ["analyst__function_toolset__db.call_tool:query"]

    def test_gives_ids_to_copies_and_one_copy_to_a_toolset_listed_twice(self):
        """
        ``_give_durable_ids`` writes ``FunctionToolset``'s private ``_id``, which only its constructor sets.

        This test and the replay tests above fail if pydantic-ai stops reading the id from there.
        """
        shared = FunctionToolset()
        given: _GivenIds = {}

        prefixed = _give_durable_ids(shared.prefixed("a"), given)
        plain = _give_durable_ids(shared, given)

        assert isinstance(prefixed, WrapperToolset)
        assert prefixed.wrapped is plain
        assert plain.id == "__airflow_function_toolset_0"
        assert shared.id is None

    def test_numbers_the_function_toolsets_it_gives_ids_in_order(self):
        given: _GivenIds = {}

        ids = [_give_durable_ids(FunctionToolset(), given).id for _ in range(2)]

        assert ids == ["__airflow_function_toolset_0", "__airflow_function_toolset_1"]

    @pytest.mark.parametrize(
        "toolset",
        [
            pytest.param(FunctionToolset(id="db").prefixed("a"), id="function-toolset-with-id"),
            pytest.param(DynamicToolset(_build_tools, id="tools"), id="dynamic-toolset-with-id"),
            pytest.param(MCPToolset("http://mcp.example.com/mcp"), id="mcp-toolset"),
        ],
    )
    def test_leaves_a_toolset_it_cannot_or_need_not_give_an_id_as_it_is(self, toolset):
        assert _give_durable_ids(toolset, {}) is toolset

    def test_a_dynamic_toolset_without_an_id_gets_its_functions_name(self):
        given = _give_durable_ids(DynamicToolset(_build_tools, per_run_step=False), {})

        assert isinstance(given, DynamicToolset)
        assert (given.id, given.per_run_step) == (f"{__name__}._build_tools", False)


class TestGetCallableToolsetId:
    def test_leaves_out_the_path_hash_airflow_puts_in_a_dag_files_module(self):
        def build_tools(ctx: RunContext[None]) -> FunctionToolset[None]:
            return FunctionToolset()

        build_tools.__module__ = f"unusual_prefix_{'0123456789abcdef' * 2}01234567_orders_dag"

        assert _get_callable_toolset_id(build_tools) == f"orders_dag.{build_tools.__qualname__}"

    @pytest.mark.parametrize(
        "function",
        [
            pytest.param(functools.partial(_build_tools), id="partial"),
            pytest.param(_ToolsBuilder(), id="callable-instance"),
        ],
    )
    def test_none_for_a_callable_without_a_qualified_name(self, function):
        assert _get_callable_toolset_id(function) is None


class TestSQLToolsetErrors:
    """A database that could not be reached is queried again on retry; a rejected statement replays."""

    @staticmethod
    def _agent(model_fn) -> Agent[None, str]:
        return Agent(
            FunctionModel(model_fn),
            name="analyst",
            toolsets=[SQLToolset("warehouse")],
            capabilities=[AirflowDurability()],
        )

    @pytest.mark.asyncio
    async def test_a_query_that_could_not_connect_runs_again_on_retry(self, memory_storage, tmp_path):
        path = tmp_path / "warehouse" / "warehouse.db"  # its directory does not exist yet
        model_fn = _query_then_answer("SELECT count(*) FROM orders", fail_final=[False])

        with sqlite_connection("warehouse", path):
            # A model handed the failure would answer with it; the attempt fails instead.
            with pytest.raises(
                SQLToolsetConnectionError, match="connection 'warehouse': .*unable to open database file"
            ):
                await attempt(memory_storage, self._agent(model_fn))
            create_orders_table(path)
            result = await attempt(memory_storage, self._agent(model_fn))

        assert '"rows":[[2]]' in result.output

    @pytest.mark.asyncio
    async def test_a_query_naming_a_missing_column_replays_on_retry(self, memory_storage, tmp_path):
        path = tmp_path / "warehouse.db"
        create_orders_table(path)
        fail_final = [True]
        model_fn = _query_then_answer("SELECT amount FROM orders", fail_final=fail_final)

        with sqlite_connection("warehouse", path):
            with pytest.raises(RuntimeError):
                await attempt(memory_storage, self._agent(model_fn))
            # Run live, the query would now succeed; replayed, it fails as it did.
            run_sql(path, "ALTER TABLE orders ADD COLUMN amount REAL")
            fail_final[0] = False
            result = await attempt(memory_storage, self._agent(model_fn))

        assert "no such column: amount" in result.output


class Answer(BaseModel):
    rows: int


def build_analyst(
    calls: Calls, queries: list[str], metadata: dict[str, Any] | None = None
) -> Agent[None, str]:
    """An analyst whose first answer queries a column that does not exist; later answers query one that does."""

    def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        calls.bump("model")
        if returns := tool_returns(messages):
            return ModelResponse(parts=[TextPart(f"answer: {returns[-1].content}")])
        sql = "select nope" if calls["model"] == 1 else "select 1"
        return ModelResponse(parts=[ToolCallPart("query", {"sql": sql})])

    toolset = FunctionToolset(id="db")

    @toolset.tool_plain(metadata=metadata)
    def query(sql: str) -> str:
        queries.append(sql)
        if "nope" in sql:
            raise ValueError("column nope does not exist")
        return "3 rows"

    return Agent(
        FunctionModel(model_fn), name="analyst", toolsets=[toolset], capabilities=[AirflowDurability()]
    )


class TestModelAtFault:
    """A run pydantic-ai fails because of the model's answers is retried by asking the model again."""

    @pytest.mark.asyncio
    async def test_a_retry_asks_the_model_again_after_its_tool_retries_ran_out(self, memory_storage):
        calls = Calls()
        sql = ["select nope"]

        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            calls.bump("model")
            if returns := tool_returns(messages):
                return ModelResponse(parts=[TextPart(f"answer: {returns[-1].content}")])
            return ModelResponse(parts=[ToolCallPart("query", {"sql": sql[0]})])

        def build() -> Agent[None, str]:
            toolset = FunctionToolset(id="db", max_retries=1)

            @toolset.tool_plain
            def query(sql: str) -> str:
                calls.bump("query")
                if "nope" in sql:
                    raise ModelRetry("no column nope")
                return "3 rows"

            return Agent(
                FunctionModel(model_fn),
                name="analyst",
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(UnexpectedModelBehavior, match="exceeded max retries"):
            await attempt(memory_storage, build())
        sql[0] = "select 1"
        with journal_scope(journal := DurableJournal(memory_storage)):
            result = await build().run("go")

        assert result.output == "answer: 3 rows"
        assert dict(journal.stats.replayed) == {"model": 1, "tool": 1}
        # Attempt 1: two bad queries. Attempt 2 replays the first request and its rejected
        # query, asks the model again (good query), then for the answer.
        assert (calls["model"], calls["query"]) == (4, 3)

    @pytest.mark.asyncio
    async def test_a_retry_asks_the_model_again_after_its_output_retries_ran_out(self, memory_storage):
        calls = Calls()
        rows: list[Any] = ["many"]

        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            calls.bump("model")
            return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, {"rows": rows[0]})])

        def build() -> Agent[None, Answer]:
            return Agent(
                FunctionModel(model_fn),
                name="analyst",
                output_type=Answer,
                retries=1,
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(UnexpectedModelBehavior, match="output retries"):
            await attempt(memory_storage, build())
        rows[0] = 3
        result = await attempt(memory_storage, build())

        assert result.output == Answer(rows=3)
        # The first invalid answer replays; only the model's last request runs again.
        assert calls["model"] == 3

    @pytest.mark.asyncio
    async def test_a_tool_that_raises_keeps_the_models_request_replaying(self, memory_storage):
        """The tool failed, not the model: only the tool runs again."""
        calls = Calls()

        def build() -> Agent[None, str]:
            toolset = FunctionToolset(id="db")

            @toolset.tool_plain
            def query(sql: str) -> str:
                calls.bump("query")
                if calls["query"] == 1:
                    raise RuntimeError("connection reset")
                return "3 rows"

            return Agent(
                FunctionModel(tool_then_answer(calls, "query", fail_final=[False])),
                name="analyst",
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="connection reset"):
            await attempt(memory_storage, build())
        await attempt(memory_storage, build())

        assert (calls["model"], calls["query"]) == (2, 2)

    @pytest.mark.asyncio
    # An opted-out call still takes its step, so its failures count the same way.
    @pytest.mark.parametrize("metadata", [None, {"airflow": False}], ids=["journaled", "opted-out"])
    async def test_a_tool_that_keeps_failing_on_the_models_arguments_asks_the_model_again(
        self, memory_storage, metadata
    ):
        """The second attempt retries the same call in case the failure was transient; the third asks the model."""
        calls = Calls()
        queries: list[str] = []

        for _ in range(2):
            with pytest.raises(ValueError, match="column nope"):
                await attempt(memory_storage, build_analyst(calls, queries, metadata))
        result = await attempt(memory_storage, build_analyst(calls, queries, metadata))

        assert result.output == "answer: 3 rows"
        assert queries == ["select nope", "select nope", "select 1"]
        # Attempt 2 replays the model's first answer; attempt 3 asks again, then for the answer.
        assert calls["model"] == 3

    @pytest.mark.asyncio
    async def test_an_attempt_after_the_answer_asked_again_succeeded_replays_it(self, memory_storage):
        """A task that keeps the journal after the run (AgentOperator, until its post-run work succeeds)."""
        calls = Calls()
        queries: list[str] = []
        for _ in range(2):
            with pytest.raises(ValueError, match="column nope"):
                await attempt(memory_storage, build_analyst(calls, queries))
        await attempt(memory_storage, build_analyst(calls, queries))

        with mock.patch.object(
            MemoryStorage, "save_step", autospec=True, side_effect=MemoryStorage.save_step
        ) as save_step:
            result = await attempt(memory_storage, build_analyst(calls, queries))

        assert result.output == "answer: 3 rows"
        assert queries == ["select nope", "select nope", "select 1"]
        assert calls["model"] == 3
        # Attempt 3 took the note down when it succeeded, so attempt 4 has nothing to write.
        save_step.assert_not_called()

    @pytest.mark.asyncio
    async def test_each_answer_with_the_same_failing_arguments_gets_its_own_retry(self, memory_storage):
        """Even an answer identical to the last, call id included, is retried once before the model is asked again."""
        calls = Calls()

        def build() -> Agent[None, str]:
            toolset = FunctionToolset(id="db")

            @toolset.tool_plain
            def query(sql: str) -> str:
                calls.bump("query")
                raise ValueError("column nope does not exist")

            def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
                calls.bump("model")
                return ModelResponse(
                    parts=[ToolCallPart("query", {"sql": "select nope"}, tool_call_id="call_1")]
                )

            return Agent(
                FunctionModel(model_fn),
                name="analyst",
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        for _ in range(4):
            with pytest.raises(ValueError, match="column nope"):
                await attempt(memory_storage, build())

        # Asked on attempts 1 and 3; attempts 2 and 4 replay those answers and retry the call.
        assert (calls["model"], calls["query"]) == (2, 4)


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
    async def test_a_masked_tool_result_does_not_stop_the_retry_replaying(
        self, memory_storage, registered_secret
    ):
        """The model saw the secret live but sees ``***`` on replay; the steps after it still replay."""
        calls = Calls()
        fail_final = [True]

        def model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            calls.bump("model")
            match responses_so_far(messages):
                case 0:
                    return ModelResponse(parts=[ToolCallPart("query", {"sql": "select 1"})])
                case 1:
                    return ModelResponse(parts=[ToolCallPart("notify", {})])
            if fail_final[0]:
                raise RuntimeError("worker died")
            return ModelResponse(parts=[TextPart("done")])

        def build() -> Agent[None, str]:
            # No masking wrapper around it, as in a plain @task.
            toolset = FunctionToolset(id="db")

            @toolset.tool_plain
            def query(sql: str) -> str:
                return f"password={registered_secret}"

            @toolset.tool_plain
            def notify() -> str:
                calls.bump("notify")
                return "sent"

            return Agent(
                FunctionModel(model_fn),
                name="analyst",
                toolsets=[toolset],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError):
            await attempt(memory_storage, build())
        fail_final[0] = False
        await attempt(memory_storage, build())

        assert calls["notify"] == 1
        # Three model steps on the first attempt, and only the one that failed on the retry.
        assert calls["model"] == 4

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

    @pytest.mark.parametrize(
        "toolset",
        [
            pytest.param(lambda calls, result: _function_toolset(calls, result), id="function-toolset"),
            pytest.param(
                lambda calls, result: _UnmaskedWarehouseToolset(calls, result), id="airflow-toolset"
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_journal_holds_secrets_inside_bytes_masked(
        self, memory_storage, registered_secret, toolset
    ):
        result = f"password={registered_secret}".encode()
        agent = Agent(
            FunctionModel(tool_then_answer(Calls(), "query", fail_final=[False])),
            name="analyst",
            # AgentOperator puts the masking wrapper outside the durable unit.
            toolsets=[ensure_masked(toolset(Calls(), result))],
            capabilities=[AirflowDurability()],
        )

        await attempt(memory_storage, agent)

        journal = str(memory_storage.entries)
        assert registered_secret not in journal
        assert base64.b64encode(result).decode() not in journal
        assert base64.b64encode(b"password=***").decode() in journal


class _UnmaskedWarehouseToolset(_WarehouseToolset):
    """An Airflow toolset whose ``call_tool`` override skips the masking the base class does."""

    async def call_tool(self, name, tool_args, ctx, tool) -> Any:
        return await self.execute_tool(name, tool_args, ctx=ctx, tool=tool)


class _Ledger(AbstractCapability[Any]):
    """Stands in for pydantic-ai-harness SpendLimits: it accrues through a durable operation."""

    def __init__(self, calls: Calls, id: str | None = "ledger") -> None:
        self._calls = calls
        self.id = id

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


class _NestingToolset(_WarehouseToolset):
    """A warehouse toolset whose ``query`` tool runs ``work`` inside its own step."""

    def __init__(
        self, calls: Calls, work: Callable[[RunContext[Any]], Awaitable[None]], *, replayable: bool = True
    ) -> None:
        super().__init__(calls, replayable=replayable)
        self._work = work

    async def execute_tool(self, name, tool_args, *, ctx, tool) -> Any:
        self._calls.bump("query")
        await self._work(ctx)
        return "3 rows"


class TestNestedOperationsInToolSteps:
    """
    A durable operation or an agent run started inside a tool's step belongs to that step.

    On retry the step replays and the work inside it does not run, so it must not have taken
    positions of its own in the outer run: the steps after the tool would move and run again.
    """

    @pytest.mark.parametrize("toolset_kind", ["airflow", "function"])
    @pytest.mark.parametrize("nested", ["durable_operation", "agent"])
    @pytest.mark.asyncio
    async def test_steps_after_the_tool_replay(self, memory_storage, toolset_kind, nested):
        calls = await self._fail_then_retry(memory_storage, toolset_kind, nested)

        assert calls["query"] == 1
        assert calls["notify"] == 1
        # One accrual per model response, the last of them live on the retry, plus the tool's
        # own; the replayed ones do not run again.
        assert calls["accrue"] == (4 if nested == "durable_operation" else 3)
        assert calls["sub_model"] == (1 if nested == "agent" else 0)

    @pytest.mark.asyncio
    async def test_an_operation_inside_a_tool_that_runs_on_every_attempt_runs_with_it(self, memory_storage):
        """The tool runs again on retry, and so does the operation it calls; the steps after it replay."""
        calls = await self._fail_then_retry(memory_storage, "airflow", "durable_operation", replayable=False)

        assert calls["query"] == 2
        assert calls["notify"] == 1
        # Three accruals on the first attempt; on the retry the tool's own and the last model response's.
        assert calls["accrue"] == 5

    @pytest.mark.asyncio
    async def test_an_agent_a_tool_runs_records_its_own_operations(self, memory_storage):
        """The inner agent's operations are steps of the inner run, so a tool that runs again replays them."""
        calls = Calls()
        fail_after_sub = [True]

        def sub_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            calls.bump("sub_model")
            return ModelResponse(parts=[TextPart("sub done")])

        async def work(ctx: RunContext[Any]) -> None:
            sub = Agent(
                FunctionModel(sub_model), name="sub", capabilities=[AirflowDurability(), _Ledger(calls)]
            )
            await sub.run("go")
            if fail_after_sub[0]:
                fail_after_sub[0] = False
                raise RuntimeError("worker died after the inner agent finished")

        def build() -> Agent[None, str]:
            return Agent(
                FunctionModel(tool_then_answer(Calls(), "query", fail_final=[False])),
                name="outer",
                toolsets=[_NestingToolset(calls, work)],
                capabilities=[AirflowDurability()],
            )

        with pytest.raises(RuntimeError, match="worker died"):
            await attempt(memory_storage, build())
        await attempt(memory_storage, build())

        # The tool ran twice; the inner agent's model request and its accrual replayed the second time.
        assert (calls["query"], calls["sub_model"], calls["accrue"]) == (2, 1, 1)

    @staticmethod
    async def _fail_then_retry(
        memory_storage, toolset_kind: str, nested: str, *, replayable: bool = True
    ) -> Calls:
        calls = Calls()
        fail_final = [True]
        ledger = _Ledger(calls)

        def sub_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            calls.bump("sub_model")
            return ModelResponse(parts=[TextPart("sub done")])

        async def work(ctx: RunContext[Any]) -> None:
            if nested == "durable_operation":
                await ledger.accrue(ctx, 5)
            else:
                sub = Agent(FunctionModel(sub_model), name="sub", capabilities=[AirflowDurability()])
                await sub.run("go")

        def outer_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            match responses_so_far(messages):
                case 0:
                    return ModelResponse(parts=[ToolCallPart("query", {"sql": "select 1"})])
                case 1:
                    return ModelResponse(parts=[ToolCallPart("notify", {})])
            if fail_final[0]:
                raise RuntimeError("worker died")
            return ModelResponse(parts=[TextPart("done")])

        def build() -> Agent[None, str]:
            alerts = FunctionToolset(id="alerts")

            @alerts.tool_plain
            def notify() -> str:
                calls.bump("notify")
                return "sent"

            warehouse: AbstractToolset[Any]
            if toolset_kind == "airflow":
                warehouse = _NestingToolset(calls, work, replayable=replayable)
            else:
                function_toolset = FunctionToolset(id="warehouse")

                @function_toolset.tool
                async def query(ctx: RunContext[Any], sql: str) -> str:
                    calls.bump("query")
                    await work(ctx)
                    return "3 rows"

                warehouse = function_toolset

            return Agent(
                FunctionModel(outer_model),
                name="analyst",
                toolsets=[warehouse, alerts],
                capabilities=[AirflowDurability(), ledger],
            )

        with pytest.raises(RuntimeError):
            await attempt(memory_storage, build())
        fail_final[0] = False
        assert (await attempt(memory_storage, build())).output == "done"
        return calls


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
