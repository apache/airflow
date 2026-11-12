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
import collections
import contextlib
import dataclasses
import datetime
import json
import logging
from collections.abc import Container
from typing import Any
from unittest import mock

import pytest
import structlog

pytest.importorskip("agents")

from agents import (
    Agent,
    AgentHooks,
    Model,
    ModelProvider,
    ModelResponse,
    ModelSettings,
    ModelTracing,
    RunConfig,
    RunHooks,
    SQLiteSession,
    ToolGuardrailFunctionOutput,
    ToolOutputText,
    Usage,
    function_tool,
    handoff,
    tool_input_guardrail,
)
from agents.exceptions import MaxTurnsExceeded, ModelBehaviorError
from agents.retry import ModelRetryBackoffSettings, ModelRetrySettings
from agents.tool_context import ToolContext
from openai.types.responses import ResponseFunctionToolCall, ResponseOutputMessage, ResponseOutputText
from pydantic import BaseModel
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.toolsets import FunctionToolset

from airflow.providers.common.ai.durable.base import build_step_key
from airflow.providers.common.ai.durable.journal import DurableJournal, journal_scope
from airflow.providers.common.ai.durable.openai_agents import (
    _build_tool_fingerprint,
    _DurableModel,
    _get_durable_tool,
    run_durably_sync,
)
from airflow.providers.common.ai.exceptions import DurableJournalError, UnsupportedToolDeferralError
from airflow.providers.common.ai.tools import AirflowTool, ToolCallError, ToolResult
from airflow.providers.common.ai.tools._from_toolset import airflow_tools_from_toolset
from airflow.providers.common.ai.tools.openai_agents import AirflowTools
from airflow.providers.standard.operators.hitl import request_input
from airflow.sdk import DAG, BaseOperator, task
from airflow.sdk.execution_time.comms import AwaitInputTask, CreateHITLDetailPayload, SetXCom
from airflow.sdk.execution_time.task_runner import run
from airflow.sdk.serde import deserialize, serialize
from airflow.utils.state import TaskInstanceState

from unit.common.ai.durable.memory_storage import MemoryStorage

# Tracing off, so no test exports spans to OpenAI.
NO_TRACING = RunConfig(tracing_disabled=True)


class _ScriptedModel(Model):
    """
    Calls ``lookup`` until it has ``lookups`` results back, then answers with the last one.

    ``calls`` counts the requests that reached the model. ``fail_on_answer`` makes the
    request that would answer raise instead, as a crashed worker would leave it.
    """

    def __init__(
        self,
        *,
        lookups: int = 1,
        fail_on_answer: bool = False,
        usage: int = 7,
        argument: str = "key",
        fail_first_request: bool = False,
        call_id_prefix: str = "call",
        fresh_call_ids: bool = False,
        value_prefix: str = "k",
    ) -> None:
        self.lookups = lookups
        self.value_prefix = value_prefix
        self.call_id_prefix = call_id_prefix
        self.fresh_call_ids = fresh_call_ids
        self.argument = argument
        self.fail_first_request = fail_first_request
        self.fail_on_answer = fail_on_answer
        self.usage = usage
        self.calls = 0
        # How many tool results each request that reached the model carried.
        self.tool_results_seen: list[int] = []

    async def get_response(self, system_instructions, input, *args: Any, **kwargs: Any) -> ModelResponse:
        self.calls += 1
        if self.fail_first_request and self.calls == 1:
            raise ConnectionError("connection reset")
        outputs = [
            item["output"]
            for item in (input if isinstance(input, list) else [])
            if isinstance(item, dict) and item.get("type") == "function_call_output"
        ]
        self.tool_results_seen.append(len(outputs))
        output: list[ResponseFunctionToolCall | ResponseOutputMessage]
        if len(outputs) < self.lookups:
            output = [
                ResponseFunctionToolCall(
                    id=f"fc-{len(outputs)}",
                    # A real model gives every call it makes a new id.
                    call_id=f"{self.call_id_prefix}-{len(outputs)}"
                    + (f"-r{self.calls}" if self.fresh_call_ids else ""),
                    name="lookup",
                    arguments=json.dumps({self.argument: f"{self.value_prefix}{len(outputs)}"}),
                    type="function_call",
                )
            ]
        else:
            if self.fail_on_answer:
                raise RuntimeError("worker lost")
            text = ResponseOutputText(text=f"answer: {outputs[-1]}", type="output_text", annotations=[])
            output = [
                ResponseOutputMessage(
                    id="msg", role="assistant", status="completed", type="message", content=[text]
                )
            ]
        usage = Usage(
            requests=1, input_tokens=self.usage, output_tokens=self.usage, total_tokens=2 * self.usage
        )
        return ModelResponse(output=list(output), usage=usage, response_id=f"resp-{self.calls}")

    def stream_response(self, *args: Any, **kwargs: Any):
        raise NotImplementedError


class _CountingTool:
    """A ``lookup`` tool that counts the calls that reached it."""

    def __init__(self, result: str = "42") -> None:
        self.result = result
        self.calls = 0

    def tool(self) -> AirflowTool:
        async def function(arguments: dict[str, Any]) -> ToolResult:
            self.calls += 1
            return ToolResult(f"{arguments['key']}={self.result}")

        return AirflowTool(
            name="lookup",
            description="Look a key up.",
            parameters={"type": "object", "properties": {"key": {"type": "string"}}},
            function=function,
        )


class _ToolsetLookup:
    """
    A ``lookup`` tool from a pydantic-ai toolset, which counts the calls that reached it.

    It raises ``ModelRetry``, a failure the model can correct, for the keys in ``retry``, and
    ``RuntimeError``, which fails the try, for those in ``broken``, after sleeping
    ``delays[key]`` seconds; a key without a delay answers without awaiting.
    """

    def __init__(
        self,
        *,
        max_retries: int,
        retry: Container[str],
        broken: Container[str] = (),
        delays: dict[str, float] | None = None,
        sequential: bool = False,
    ) -> None:
        self.max_retries = max_retries
        self.retry = retry
        self.broken = broken
        self.delays = delays or {}
        self.sequential = sequential
        self.calls = 0

    def tool(self) -> AirflowTool:
        async def lookup(key: str) -> str:
            self.calls += 1
            if key in self.delays:
                await asyncio.sleep(self.delays[key])
            if key in self.broken:
                raise RuntimeError(f"the warehouse dropped the connection looking up {key}")
            if key in self.retry:
                raise ModelRetry(f"There is no {key}.")
            return f"{key}=42"

        # A fresh tool per attempt, as a retried task builds its agent again.
        toolset = FunctionToolset([lookup], max_retries=self.max_retries, sequential=self.sequential)
        (tool,) = airflow_tools_from_toolset(toolset)
        return tool


class _TurnsModel(Model):
    """
    Asks for one ``lookup`` per key of each turn, all of a turn's in one response, then answers.

    ``fail_on_answer`` makes the request that would answer raise, as a crashed worker would leave it.
    """

    def __init__(self, turns: list[list[str]], *, fail_on_answer: bool = False) -> None:
        self.turns = turns
        self.fail_on_answer = fail_on_answer
        self.calls = 0

    async def get_response(self, system_instructions, input, *args: Any, **kwargs: Any) -> ModelResponse:
        self.calls += 1
        answered = sum(
            1
            for item in (input if isinstance(input, list) else [])
            if isinstance(item, dict) and item.get("type") == "function_call_output"
        )
        done = 0
        output: list[ResponseFunctionToolCall | ResponseOutputMessage] = []
        for keys in self.turns:
            if answered == done:
                output = [
                    ResponseFunctionToolCall(
                        id=f"fc-{key}",
                        call_id=f"call-{key}",
                        name="lookup",
                        arguments=json.dumps({"key": key}),
                        type="function_call",
                    )
                    for key in keys
                ]
                break
            done += len(keys)
        else:
            if self.fail_on_answer:
                raise RuntimeError("worker lost")
            text = ResponseOutputText(text="answer", type="output_text", annotations=[])
            output = [
                ResponseOutputMessage(
                    id="msg", role="assistant", status="completed", type="message", content=[text]
                )
            ]
        return ModelResponse(output=list(output), usage=Usage(), response_id=f"resp-{self.calls}")

    def stream_response(self, *args: Any, **kwargs: Any):
        raise NotImplementedError


class _ParallelModel(Model):
    """
    Asks for one ``lookup`` per key in a single response, then answers with their results.

    The calls get the ids ``call-<key>``. ``fail_on_answer`` makes the request that would
    answer raise, as a crashed worker would leave it.
    """

    def __init__(self, keys: tuple[str, ...] = ("a", "b"), *, fail_on_answer: bool = False) -> None:
        self.keys = keys
        self.fail_on_answer = fail_on_answer
        self.calls = 0

    async def get_response(self, system_instructions, input, *args: Any, **kwargs: Any) -> ModelResponse:
        self.calls += 1
        outputs = [
            item["output"]
            for item in (input if isinstance(input, list) else [])
            if isinstance(item, dict) and item.get("type") == "function_call_output"
        ]
        output: list[ResponseFunctionToolCall | ResponseOutputMessage]
        if not outputs:
            output = [
                ResponseFunctionToolCall(
                    id=f"fc-{key}",
                    call_id=f"call-{key}",
                    name="lookup",
                    arguments=json.dumps({"key": key}),
                    type="function_call",
                )
                for key in self.keys
            ]
        else:
            if self.fail_on_answer:
                raise RuntimeError("worker lost")
            text = ResponseOutputText(text=f"answer: {sorted(outputs)}", type="output_text", annotations=[])
            output = [
                ResponseOutputMessage(
                    id="msg", role="assistant", status="completed", type="message", content=[text]
                )
            ]
        return ModelResponse(output=list(output), usage=Usage(), response_id=f"resp-{self.calls}")

    def stream_response(self, *args: Any, **kwargs: Any):
        raise NotImplementedError


class _SleepingTools:
    """
    A ``lookup`` tool with controllable timing and failures.

    It sleeps ``body_delays[key]`` seconds, fails for the keys in ``failing``, and counts the calls per key.
    """

    def __init__(self, body_delays: dict[str, float] | None = None, failing: set[str] | None = None) -> None:
        self.body_delays = body_delays or {}
        self.failing = failing or set()
        self.calls: collections.Counter[str] = collections.Counter()

    def tools(self) -> list[Any]:
        async def function(arguments: dict[str, Any]) -> ToolResult:
            key = arguments["key"]
            self.calls[key] += 1
            await asyncio.sleep(self.body_delays.get(key, 0))
            if key in self.failing:
                raise RuntimeError(f"lookup {key} failed")
            return ToolResult(f"{key}=42")

        return AirflowTools(
            AirflowTool(
                name="lookup",
                description="Look a key up.",
                parameters={"type": "object", "properties": {"key": {"type": "string"}}},
                function=function,
            )
        )


class _TransferModel(Model):
    """Hands the run over to the ``specialist`` agent on its first request."""

    async def get_response(self, system_instructions, input, *args: Any, **kwargs: Any) -> ModelResponse:
        call = ResponseFunctionToolCall(
            id="fc-transfer",
            call_id="call-transfer",
            name="transfer_to_specialist",
            arguments="{}",
            type="function_call",
        )
        return ModelResponse(output=[call], usage=Usage(), response_id="resp-transfer")

    def stream_response(self, *args: Any, **kwargs: Any):
        raise NotImplementedError


class _Provider(ModelProvider):
    """Resolves every model name to one model."""

    def __init__(self, model: Model) -> None:
        self.model = model

    def get_model(self, model_name: str | None) -> Model:
        return self.model


class _Report(BaseModel):
    rows: int


def _attempt(
    storage: MemoryStorage,
    model: Model,
    tool: _CountingTool | _ToolsetLookup,
    prompt: str = "go",
    **kwargs: Any,
):
    """Run one task attempt: a fresh journal over the storage the attempts share."""
    agent = Agent(name="analyst", model=model, tools=AirflowTools(tool.tool()))
    journal = DurableJournal(storage, clean_up_after_run=True)
    with journal_scope(journal):
        return run_durably_sync(agent, prompt, run_config=NO_TRACING, **kwargs), journal


class TestRunDurably:
    def test_a_retry_replays_the_calls_the_failed_attempt_completed(self):
        storage, tool = MemoryStorage(), _CountingTool()
        with pytest.raises(RuntimeError, match="worker lost"):
            _attempt(storage, _ScriptedModel(fail_on_answer=True), tool)
        assert tool.calls == 1

        retry_model = _ScriptedModel()
        result, journal = _attempt(storage, retry_model, tool)

        assert result.final_output == "answer: k0=42"
        # The first request and the tool call replay; only the request that answers is new.
        assert retry_model.calls == 1
        assert tool.calls == 1
        assert journal.stats.replayed == {"model": 1, "tool": 1}

    def test_every_completed_call_replays_not_only_the_first(self):
        """A replayed answer must turn into the same next request as the live one did."""
        storage, tool = MemoryStorage(), _CountingTool()
        with pytest.raises(RuntimeError, match="worker lost"):
            _attempt(storage, _ScriptedModel(lookups=3, fail_on_answer=True), tool)

        retry_model = _ScriptedModel(lookups=3)
        _, journal = _attempt(storage, retry_model, tool)

        assert retry_model.calls == 1
        assert tool.calls == 3
        assert journal.stats.replayed == {"model": 3, "tool": 3}

    def test_the_sdks_generated_prompt_cache_key_does_not_stop_a_replay(self):
        """The runner adds a key of its own to every request of a model that accepts one."""

        class _CachingModel(_ScriptedModel):
            def _supports_default_prompt_cache_key(self) -> bool:
                return True

        storage, tool = MemoryStorage(), _CountingTool()
        with pytest.raises(RuntimeError):
            _attempt(storage, _CachingModel(fail_on_answer=True), tool)

        retry_model = _CachingModel()
        _attempt(storage, retry_model, tool)

        assert retry_model.calls == 1

    def test_a_replayed_model_call_reports_no_usage(self):
        storage, tool = MemoryStorage(), _CountingTool()
        with pytest.raises(RuntimeError):
            _attempt(storage, _ScriptedModel(fail_on_answer=True, usage=7), tool)

        result, _ = _attempt(storage, _ScriptedModel(usage=7), tool)

        assert result.context_wrapper.usage.input_tokens == 7

    def test_a_successful_run_deletes_what_it_recorded(self):
        storage, tool = MemoryStorage(), _CountingTool()

        _attempt(storage, _ScriptedModel(), tool)

        assert storage.entries == {}

    def test_a_different_prompt_runs_every_call_again(self):
        storage, tool = MemoryStorage(), _CountingTool()
        with pytest.raises(RuntimeError):
            _attempt(storage, _ScriptedModel(fail_on_answer=True), tool, prompt="first question")

        retry_model = _ScriptedModel()
        _attempt(storage, retry_model, tool, prompt="second question")

        assert retry_model.calls == 2
        assert tool.calls == 2

    def test_a_run_the_model_failed_asks_the_model_again(self):
        """Running out of turns comes from the model's answers, which would replay into the same failure."""
        storage, tool = MemoryStorage(), _CountingTool()
        with pytest.raises(MaxTurnsExceeded):
            _attempt(storage, _ScriptedModel(lookups=3), tool, max_turns=2)
        assert tool.calls == 2

        retry_model = _ScriptedModel(lookups=1)
        result, _ = _attempt(storage, retry_model, tool, max_turns=5)

        # The calls before the model's last request replay; that request is asked again, so
        # the retry answers after the first lookup instead of replaying the second.
        assert result.final_output == "answer: k0=42"
        assert retry_model.calls == 1
        assert tool.calls == 2

    def test_a_model_resolved_by_name_is_recorded(self):
        storage, tool = MemoryStorage(), _CountingTool()

        def attempt(model: _ScriptedModel) -> None:
            agent = Agent(name="analyst", model="scripted", tools=AirflowTools(tool.tool()))
            config = RunConfig(tracing_disabled=True, model_provider=_Provider(model))
            with journal_scope(DurableJournal(storage, clean_up_after_run=True)):
                run_durably_sync(agent, "go", run_config=config)

        with pytest.raises(RuntimeError):
            attempt(_ScriptedModel(fail_on_answer=True))
        retry_model = _ScriptedModel()
        attempt(retry_model)

        assert retry_model.calls == 1

    def test_an_agent_used_as_a_tool_replays_as_one_call(self):
        """Its own model calls run inside the outer tool call and must not take the outer run's positions."""
        storage, tool = MemoryStorage(), _CountingTool()

        def attempt(outer: _ScriptedModel, inner: _ScriptedModel) -> None:
            specialist = Agent(name="specialist", model="inner", tools=AirflowTools(tool.tool()))
            lead = Agent(
                name="lead",
                model=outer,
                tools=[specialist.as_tool("lookup", "Ask the specialist.")],
            )
            config = RunConfig(tracing_disabled=True, model_provider=_Provider(inner))
            with journal_scope(DurableJournal(storage, clean_up_after_run=True)):
                run_durably_sync(lead, "go", run_config=config)

        # ``as_tool`` takes the question for the specialist as ``input``.
        with pytest.raises(RuntimeError):
            attempt(_ScriptedModel(lookups=2, fail_on_answer=True, argument="input"), _ScriptedModel())
        assert tool.calls == 2
        outer, inner = _ScriptedModel(lookups=2, argument="input"), _ScriptedModel()
        attempt(outer, inner)

        # Both outer requests for the specialist and both of its answers replay.
        assert outer.calls == 1
        assert inner.calls == 0

    @pytest.mark.enable_redact
    def test_a_secret_in_a_tool_result_is_not_recorded(self, registered_secret):
        """Any function tool, not only the Airflow tools, whose results are masked already."""

        @function_tool
        def lookup(key: str) -> str:
            """Look a key up."""
            return f"{key}={registered_secret}"

        agent = Agent(name="analyst", model=_ScriptedModel(fail_on_answer=True), tools=[lookup])
        storage = MemoryStorage()

        with journal_scope(DurableJournal(storage, clean_up_after_run=True)), pytest.raises(RuntimeError):
            run_durably_sync(agent, "go", run_config=NO_TRACING)

        (recorded,) = [step for step in storage.steps() if step["name"] == "tool:lookup"]
        assert recorded["payload"] == "k0=***"
        assert registered_secret not in json.dumps(storage.entries)

    @pytest.mark.parametrize(
        "result",
        [
            pytest.param(_Report(rows=3), id="pydantic-model"),
            pytest.param(("orders", 3), id="tuple"),
            pytest.param(datetime.datetime(2026, 10, 14, 9, 30), id="datetime"),
            pytest.param({"rows": 3}, id="dict"),
            pytest.param(ToolOutputText(text="3 rows"), id="structured"),
            pytest.param(
                [ToolOutputText(text="3 rows"), ToolOutputText(text="2 orders")], id="structured-list"
            ),
            pytest.param({"type": "text", "text": "3 rows"}, id="typed-dict"),
        ],
    )
    def test_a_tool_result_replays_as_the_model_saw_it(self, result):
        """The SDK sends most results as str(); a replay must turn into the same next request."""

        @function_tool
        def lookup(key: str) -> Any:
            """Look a key up."""
            return result

        storage = MemoryStorage()

        def attempt(model: _ScriptedModel) -> DurableJournal:
            agent = Agent(name="analyst", model=model, tools=[lookup])
            journal = DurableJournal(storage, clean_up_after_run=True)
            with journal_scope(journal):
                run_durably_sync(agent, "go", run_config=NO_TRACING)
            return journal

        with pytest.raises(RuntimeError):
            attempt(_ScriptedModel(lookups=2, fail_on_answer=True))
        journal = attempt(_ScriptedModel(lookups=2))

        assert journal.stats.replayed == {"model": 2, "tool": 2}

    @pytest.mark.parametrize(
        "change",
        [
            pytest.param({"instructions": "Answer in French."}, id="instructions"),
            pytest.param({"model_settings": ModelSettings(temperature=0.1)}, id="settings"),
            pytest.param({"output_type": _Report}, id="output-type"),
            pytest.param({"handoffs": [Agent(name="auditor")]}, id="handoffs"),
            pytest.param({"tools": []}, id="tools"),
        ],
    )
    def test_a_request_that_changed_between_attempts_runs_live(self, change):
        storage, tool = MemoryStorage(), _CountingTool()
        with pytest.raises(RuntimeError):
            _attempt(storage, _ScriptedModel(fail_on_answer=True), tool)

        retry_model = _ScriptedModel()
        agent = Agent(name="analyst", model=retry_model, tools=AirflowTools(tool.tool())).clone(**change)
        # Some changes make the scripted answers invalid for the agent; only the first request matters.
        with journal_scope(DurableJournal(storage, clean_up_after_run=True)), contextlib.suppress(Exception):
            run_durably_sync(agent, "go", run_config=NO_TRACING)

        # The first request reached the model, rather than replaying the failed try's answer.
        assert retry_model.tool_results_seen[0] == 0

    def test_a_request_the_sdk_retried_keeps_its_position(self):
        """The SDK's model retry makes the same call again; the next try must still line up."""
        settings = ModelSettings(
            retry=ModelRetrySettings(
                max_retries=1,
                backoff=ModelRetryBackoffSettings(initial_delay=0, max_delay=0, jitter=False),
                policy=lambda context: True,
            )
        )
        storage, tool = MemoryStorage(), _CountingTool()

        def attempt(model: _ScriptedModel) -> DurableJournal:
            agent = Agent(
                name="analyst", model=model, tools=AirflowTools(tool.tool()), model_settings=settings
            )
            journal = DurableJournal(storage, clean_up_after_run=True)
            with journal_scope(journal):
                run_durably_sync(agent, "go", run_config=NO_TRACING)
            return journal

        with pytest.raises(RuntimeError, match="worker lost"):
            attempt(_ScriptedModel(fail_first_request=True, fail_on_answer=True))
        journal = attempt(_ScriptedModel())

        assert journal.stats.replayed == {"model": 1, "tool": 1}

    def test_a_tool_whose_approval_depends_on_its_arguments_is_left_to_the_sdk(self):
        async def policy(context, arguments, call_id) -> bool:
            return arguments.get("amount", 0) > 100

        @function_tool(needs_approval=policy)
        def pay(amount: int = 1000) -> str:
            """Pay an amount."""
            return "paid"

        assert _get_durable_tool(pay, DurableJournal(MemoryStorage()).start_run()) is pay

    def test_a_tool_that_always_needs_approval_is_recorded(self):
        @function_tool(needs_approval=True)
        def pay(amount: int = 1000) -> str:
            """Pay an amount."""
            return "paid"

        assert _get_durable_tool(pay, DurableJournal(MemoryStorage()).start_run()) is not pay

    @pytest.mark.parametrize(
        ("option", "value"),
        [
            ("previous_response_id", "resp_1"),
            ("conversation_id", "conv_1"),
            ("auto_previous_response_id", True),
        ],
    )
    def test_refuses_a_conversation_kept_on_openais_servers(self, option, value):
        with pytest.raises(ValueError, match=option):
            _attempt(MemoryStorage(), _ScriptedModel(), _CountingTool(), **{option: value})

    def test_a_recorded_answer_that_cannot_be_read_runs_live_with_every_call_after_it(self):
        storage, tool = MemoryStorage(), _CountingTool()
        with pytest.raises(RuntimeError):
            _attempt(storage, _ScriptedModel(fail_on_answer=True), tool)
        storage.entries[build_step_key(0, 0)]["payload"]["output"] = [{"type": "not an output item"}]

        retry_model = _ScriptedModel()
        _, journal = _attempt(storage, retry_model, tool)

        assert retry_model.calls == 2
        assert tool.calls == 2
        assert journal.stats.replayed == {"model": 0}

    @pytest.mark.parametrize(
        "failure_error_function",
        [
            pytest.param({}, id="default-formatter"),
            pytest.param(
                {"failure_error_function": lambda ctx, error: f"lookup failed: {error}"}, id="custom"
            ),
        ],
    )
    def test_a_tool_call_that_failed_runs_again_on_retry(self, failure_error_function):
        """The SDK hands the model the error as text; that text must not replay as the tool's result."""
        calls = []

        @function_tool(**failure_error_function)
        def lookup(key: str) -> str:
            """Look a key up."""
            calls.append(key)
            if len(calls) == 1:
                raise ConnectionError("warehouse unreachable")
            return f"{key}=42"

        storage = MemoryStorage()

        def attempt(model: _ScriptedModel) -> DurableJournal:
            journal = DurableJournal(storage, clean_up_after_run=True)
            with journal_scope(journal):
                run_durably_sync(
                    Agent(name="analyst", model=model, tools=[lookup]), "go", run_config=NO_TRACING
                )
            return journal

        with pytest.raises(RuntimeError, match="worker lost"):
            attempt(_ScriptedModel(fail_on_answer=True))
        retry_model = _ScriptedModel()
        attempt(retry_model)

        assert len(calls) == 2
        assert retry_model.calls == 1

    def test_a_model_behavior_error_a_tool_raises_reruns_the_tool_and_replays_the_answer_before_it(self):
        calls = []

        @function_tool(failure_error_function=None)
        def lookup(key: str) -> str:
            """Look a key up."""
            calls.append(key)
            if len(calls) == 1:
                raise ModelBehaviorError("lookup was called with a key that does not exist")
            return f"{key}=42"

        storage = MemoryStorage()

        def attempt(model: _ScriptedModel) -> None:
            with journal_scope(DurableJournal(storage, clean_up_after_run=True)):
                run_durably_sync(
                    Agent(name="analyst", model=model, tools=[lookup]), "go", run_config=NO_TRACING
                )

        with pytest.raises(ModelBehaviorError):
            attempt(_ScriptedModel())
        retry_model = _ScriptedModel()
        attempt(retry_model)

        assert len(calls) == 2
        # The answer that asked for the lookup replays; only the final answer is asked for.
        assert retry_model.calls == 1

    def test_a_replayed_correctable_failure_counts_against_the_tools_retry_limit(self, caplog):
        """Across attempts the tool gets max_retries corrections in all, not max_retries per attempt."""
        storage = MemoryStorage()
        tool = _ToolsetLookup(max_retries=2, retry={"k0", "k1", "k2"})
        with pytest.raises(RuntimeError, match="worker lost"):
            _attempt(storage, _ScriptedModel(fail_on_answer=True), tool)
        tool.calls = 0

        # The replayed failure of k0 is the first correction, k1 the second, and k2 one too many.
        with (
            caplog.at_level(logging.INFO, logger="airflow.providers.common.ai.tools._from_toolset"),
            pytest.raises(ToolCallError, match="lookup kept failing after 2 correction"),
        ):
            _attempt(storage, _ScriptedModel(lookups=3), tool)
        assert tool.calls == 2
        # The line the guide quotes, so a task log shows why the limit ran out early.
        assert [
            record.getMessage()
            for record in caplog.records
            if record.name == "airflow.providers.common.ai.tools._from_toolset"
        ] == ["Tool lookup: an earlier attempt left 1 of its 2 correction(s) used"]

    def test_a_count_recorded_for_a_tool_since_swapped_for_a_custom_one_replays_without_running_it(self):
        """A Dag edit between tries put a tool of the same name and schema, with no retry budget, in its place."""
        storage, tool = MemoryStorage(), _ToolsetLookup(max_retries=1, retry=set())
        with pytest.raises(RuntimeError, match="worker lost"):
            _attempt(storage, _ScriptedModel(fail_on_answer=True), tool)
        custom_calls = []

        async def custom(arguments: dict[str, Any]) -> ToolResult:
            custom_calls.append(arguments["key"])
            return ToolResult(f"{arguments['key']}=custom")

        agent = Agent(
            name="analyst",
            model=_ScriptedModel(),
            tools=AirflowTools(dataclasses.replace(tool.tool(), function=custom)),
        )
        with journal_scope(DurableJournal(storage, clean_up_after_run=True)):
            result = run_durably_sync(agent, "go", run_config=NO_TRACING)

        assert custom_calls == []
        assert result.final_output == "answer: k0=42"

    def test_a_run_that_used_up_a_tools_retry_limit_asks_the_model_again_on_retry(self):
        """The model's answers used the limit up, so a retry asks it again rather than replaying them."""
        storage = MemoryStorage()
        tool = _ToolsetLookup(max_retries=1, retry={"k0", "k1"})
        with pytest.raises(ToolCallError, match="lookup kept failing after 1 correction"):
            _attempt(storage, _ScriptedModel(lookups=2), tool)
        tool.calls = 0
        retry_model = _ScriptedModel(lookups=1)

        result, _ = _attempt(storage, retry_model, tool)

        # The answer that asked for k1 is asked for again, and this time the model answers.
        assert retry_model.calls == 1
        assert tool.calls == 0
        assert result.final_output.startswith("answer: ")

    def test_a_replayed_success_starts_the_tools_retry_count_over(self):
        storage = MemoryStorage()
        tool = _ToolsetLookup(max_retries=1, retry={"k0", "k2"})
        with pytest.raises(RuntimeError, match="worker lost"):
            _attempt(storage, _ScriptedModel(lookups=2, fail_on_answer=True), tool)
        tool.calls = 0

        # k1 succeeded after k0 failed, so k2 is the one correction the limit allows.
        result, _ = _attempt(storage, _ScriptedModel(lookups=3), tool)

        assert tool.calls == 1
        assert result.final_output.startswith("answer: ")

    def test_parallel_calls_replay_the_count_they_left_in_the_order_they_finished(self):
        """
        Live, b fails and then a, still running, succeeds, so d is the first correction again.

        A replay reaches a and b in the order they started, which would count d as the second.
        """
        storage = MemoryStorage()
        tool = _ToolsetLookup(max_retries=1, retry={"b", "d"}, delays={"a": 0.05})
        with pytest.raises(RuntimeError, match="worker lost"):
            _attempt(storage, _TurnsModel([["a", "b"]], fail_on_answer=True), tool)
        tool.calls = 0

        result, _ = _attempt(storage, _TurnsModel([["a", "b"], ["d"]]), tool)

        assert result.final_output == "answer"
        assert tool.calls == 1

    def test_a_third_attempt_counts_the_corrections_of_both_earlier_attempts(self):
        storage = MemoryStorage()
        tool = _ToolsetLookup(max_retries=2, retry={"k0", "k1", "k2"})
        with pytest.raises(RuntimeError, match="worker lost"):
            _attempt(storage, _ScriptedModel(lookups=1, fail_on_answer=True), tool)
        with pytest.raises(RuntimeError, match="worker lost"):
            _attempt(storage, _ScriptedModel(lookups=2, fail_on_answer=True), tool)
        tool.calls = 0

        # k0 from the first attempt and k1 from the second are both corrections already used.
        with pytest.raises(ToolCallError, match="lookup kept failing after 2 correction"):
            _attempt(storage, _ScriptedModel(lookups=3), tool)
        assert tool.calls == 1

    @pytest.mark.parametrize("sequential", [False, True], ids=["parallel", "sequential"])
    def test_a_live_failure_alongside_a_replayed_one_counts_once(self, sequential):
        """b failed the first try outright; on the retry it fails in a way the model can correct, as a did."""
        storage = MemoryStorage()
        tool = _ToolsetLookup(max_retries=1, retry={"a", "b"}, broken={"b"}, sequential=sequential)
        with pytest.raises(ToolCallError, match="warehouse dropped the connection"):
            _attempt(storage, _TurnsModel([["a", "b"]]), tool)
        tool.broken, tool.calls = (), 0

        result, _ = _attempt(storage, _TurnsModel([["a", "b"]]), tool)

        assert result.final_output == "answer"
        assert tool.calls == 1

    def test_a_replayed_success_does_not_hide_a_live_failure_alongside_it(self):
        storage = MemoryStorage()
        tool = _ToolsetLookup(max_retries=1, retry={"b", "d"}, broken={"b"})
        with pytest.raises(ToolCallError, match="warehouse dropped the connection"):
            _attempt(storage, _TurnsModel([["a", "b"]]), tool)
        tool.broken, tool.calls = (), 0

        # b fails live before a's replay restores its success, which must not wipe b's correction:
        # b is the first correction and d one too many.
        with pytest.raises(ToolCallError, match="lookup kept failing after 1 correction"):
            _attempt(storage, _TurnsModel([["a", "b"], ["d"]]), tool)
        assert tool.calls == 2

    def test_each_agent_run_in_a_task_restores_its_own_count(self):
        """A second run_durably call in the task replays its count after a first one ran live."""
        storage, tool = MemoryStorage(), _ToolsetLookup(max_retries=1, retry={"k1", "k2"})

        def attempt(second: _ScriptedModel) -> None:
            tools = AirflowTools(tool.tool())
            with journal_scope(DurableJournal(storage, clean_up_after_run=True)):
                run_durably_sync(
                    Agent(name="a", model=_ScriptedModel(), tools=tools), "1", run_config=NO_TRACING
                )
                run_durably_sync(Agent(name="b", model=second, tools=tools), "2", run_config=NO_TRACING)

        with pytest.raises(RuntimeError, match="worker lost"):
            attempt(_ScriptedModel(lookups=2, fail_on_answer=True))
        tool.calls = 0

        # The first run succeeded, so it runs live again; the second's k1 is its one correction.
        with pytest.raises(ToolCallError, match="lookup kept failing after 1 correction"):
            attempt(_ScriptedModel(lookups=3))
        assert tool.calls == 2

    def test_a_custom_tool_that_calls_a_toolset_tool_replays_without_running(self):
        storage = MemoryStorage()
        inner = _ToolsetLookup(max_retries=1, retry=set())
        outer_calls = []

        def custom_tool() -> AirflowTool:
            lookup = inner.tool()

            async def function(arguments: dict[str, Any]) -> ToolResult:
                outer_calls.append(arguments["key"])
                return await lookup.call(arguments)

            return dataclasses.replace(lookup, function=function)

        def attempt(model: Model) -> Any:
            agent = Agent(name="analyst", model=model, tools=AirflowTools(custom_tool()))
            with journal_scope(DurableJournal(storage, clean_up_after_run=True)):
                return run_durably_sync(agent, "go", run_config=NO_TRACING)

        with pytest.raises(RuntimeError, match="worker lost"):
            attempt(_ScriptedModel(fail_on_answer=True))

        result = attempt(_ScriptedModel())

        assert outer_calls == ["k0"]
        assert result.final_output == "answer: k0=42"

    def test_a_tool_rebuilt_without_the_replay_hook_replays_its_result_as_text(self):
        storage, tool = MemoryStorage(), _ToolsetLookup(max_retries=1, retry=set())

        def attempt(model: Model) -> Any:
            tools = [dataclasses.replace(function_tool) for function_tool in AirflowTools(tool.tool())]
            with journal_scope(DurableJournal(storage, clean_up_after_run=True)):
                return run_durably_sync(
                    Agent(name="analyst", model=model, tools=tools), "go", run_config=NO_TRACING
                )

        with pytest.raises(RuntimeError, match="worker lost"):
            attempt(_ScriptedModel(fail_on_answer=True))

        assert attempt(_ScriptedModel()).final_output == "answer: k0=42"

    def test_a_used_up_limit_inside_an_agent_used_as_a_tool_fails_that_call_not_the_model(self):
        """The outer model's answer was fine: the retry replays it and runs the inner agent again."""
        storage, tool = MemoryStorage(), _ToolsetLookup(max_retries=1, retry={"k0", "k1"})

        def attempt(outer: _ScriptedModel) -> None:
            specialist = Agent(name="specialist", model="inner", tools=AirflowTools(tool.tool()))
            lead = Agent(
                name="lead",
                model=outer,
                tools=[specialist.as_tool("lookup", "Ask the specialist.", failure_error_function=None)],
            )
            config = RunConfig(tracing_disabled=True, model_provider=_Provider(_ScriptedModel(lookups=2)))
            with journal_scope(DurableJournal(storage, clean_up_after_run=True)):
                run_durably_sync(lead, "go", run_config=config)

        with pytest.raises(ToolCallError, match="lookup kept failing after 1 correction"):
            attempt(_ScriptedModel(argument="input"))
        outer = _ScriptedModel(argument="input")

        with pytest.raises(ToolCallError, match="lookup kept failing after 1 correction"):
            attempt(outer)

        assert outer.calls == 0

    def test_a_run_with_a_session_runs_live_on_retry(self, tmp_path):
        """The session already holds the failed try's prompt, so the first request differs."""
        storage, tool = MemoryStorage(), _CountingTool()
        session = SQLiteSession("analyst", str(tmp_path / "session.db"))
        with pytest.raises(RuntimeError):
            _attempt(storage, _ScriptedModel(fail_on_answer=True), tool, session=session)

        _, journal = _attempt(storage, _ScriptedModel(), tool, session=session)

        assert journal.stats.replayed == {}

    def test_outside_a_task_it_runs_the_agent_as_runner_run_does(self):
        model, tool = _ScriptedModel(), _CountingTool()
        agent = Agent(name="analyst", model=model, tools=AirflowTools(tool.tool()))

        result = run_durably_sync(agent, "go", run_config=NO_TRACING)

        assert result.final_output == "answer: k0=42"
        assert model.calls == 2


class _SlowRunHooks(RunHooks):
    """Makes every tool call wait ``delays[call id]`` seconds before it is invoked."""

    def __init__(self, delays: dict[str, float]) -> None:
        self.delays = delays

    async def on_tool_start(self, context, agent, tool) -> None:
        await asyncio.sleep(self.delays[context.tool_call_id])


class _SlowAgentHooks(AgentHooks):
    """Makes every tool call of the agent wait ``delays[call id]`` seconds before it is invoked."""

    def __init__(self, delays: dict[str, float]) -> None:
        self.delays = delays

    async def on_tool_start(self, context, agent, tool) -> None:
        await asyncio.sleep(self.delays[context.tool_call_id])


def _attempt_parallel(
    storage: MemoryStorage,
    model: Model,
    tools: _SleepingTools,
    *,
    agent_hooks: AgentHooks | None = None,
    guardrail_delays: dict[str, float] | None = None,
    rejected_calls: set[str] | None = None,
    **kwargs: Any,
):
    """Run one attempt of an agent whose model asks for several tools at once."""
    function_tools = tools.tools()
    if guardrail_delays is not None or rejected_calls is not None:

        @tool_input_guardrail
        async def wait(data):
            await asyncio.sleep((guardrail_delays or {}).get(data.context.tool_call_id, 0))
            if rejected_calls and data.context.tool_call_id in rejected_calls:
                return ToolGuardrailFunctionOutput.reject_content("stopped by a guardrail")
            return ToolGuardrailFunctionOutput.allow()

        for function_tool_ in function_tools:
            function_tool_.tool_input_guardrails = [wait]
    agent = Agent(name="analyst", model=model, tools=function_tools, hooks=agent_hooks)
    journal = DurableJournal(storage, clean_up_after_run=True)
    with journal_scope(journal):
        return run_durably_sync(agent, "go", run_config=NO_TRACING, **kwargs), journal


class TestParallelToolCalls:
    def test_parallel_tool_calls_replay_when_their_bodies_finish_in_a_different_order(self):
        """
        Characterizes the documented claim that parallel calls keep the order they started in.

        The model's response reserves a position for each tool call, so how long a tool body takes does not matter.
        """
        storage = MemoryStorage()
        tools = _SleepingTools({"a": 0.05, "b": 0.01})
        with pytest.raises(RuntimeError, match="worker lost"):
            _attempt_parallel(storage, _ParallelModel(fail_on_answer=True), tools)
        assert tools.calls == {"a": 1, "b": 1}

        tools.body_delays = {"a": 0.01, "b": 0.05}
        retry_model = _ParallelModel()
        _, journal = _attempt_parallel(storage, retry_model, tools)

        assert journal.stats.replayed == {"model": 1, "tool": 2}
        assert tools.calls == {"a": 1, "b": 1}
        assert retry_model.calls == 1

    def test_only_the_parallel_tool_call_that_failed_runs_again_on_retry(self):
        storage = MemoryStorage()
        tools = _SleepingTools(failing={"b"})
        with pytest.raises(Exception, match="lookup b failed"):
            _attempt_parallel(storage, _ParallelModel(("a", "b", "c")), tools)
        assert tools.calls == {"a": 1, "b": 1, "c": 1}

        tools.failing = set()
        retry_model = _ParallelModel(("a", "b", "c"))
        _, journal = _attempt_parallel(storage, retry_model, tools)

        assert tools.calls == {"a": 1, "b": 2, "c": 1}
        assert journal.stats.replayed == {"model": 1, "tool": 2}
        assert retry_model.calls == 1

    def test_a_replayed_tool_call_a_guardrail_stops_is_not_counted_as_replayed(self):
        storage = MemoryStorage()
        tools = _SleepingTools()
        rejected: set[str] = set()
        with pytest.raises(RuntimeError, match="worker lost"):
            _attempt_parallel(storage, _ParallelModel(fail_on_answer=True), tools, rejected_calls=rejected)
        assert tools.calls == {"a": 1, "b": 1}

        rejected.add("call-b")
        retry_model = _ParallelModel()
        _, journal = _attempt_parallel(storage, retry_model, tools, rejected_calls=rejected)

        assert journal.stats.replayed == {"model": 1, "tool": 1}
        assert tools.calls == {"a": 1, "b": 1}
        assert retry_model.calls == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("call_id", ["call-shared", ""], ids=["duplicate-id", "empty-id"])
    async def test_tool_calls_sharing_a_call_id_each_replay_their_own_result(self, call_id):
        """The SDK rejects a repeated id itself, but a model that emitted one must not make the calls trade steps."""
        storage = MemoryStorage()
        tools = _SleepingTools()
        response = ModelResponse(
            output=[
                ResponseFunctionToolCall(
                    id=f"fc-{key}",
                    call_id=call_id,
                    name="lookup",
                    arguments=json.dumps({"key": key}),
                    type="function_call",
                )
                for key in ("a", "b")
            ],
            usage=Usage(),
            response_id="resp",
        )

        async def attempt(order: tuple[str, ...]) -> tuple[dict[str, Any], DurableJournal]:
            journal = DurableJournal(storage)
            durable_run = journal.start_run()
            durable_tools = [_get_durable_tool(tool, durable_run) for tool in tools.tools()]
            _DurableModel(mock.create_autospec(Model, instance=True), durable_run)._reserve_tool_steps(
                response, durable_tools
            )
            outputs = {}
            for key in order:
                context = ToolContext(
                    context=None,
                    usage=Usage(),
                    tool_name="lookup",
                    tool_call_id=call_id,
                    tool_arguments=json.dumps({"key": key}),
                )
                outputs[key] = await durable_tools[0].on_invoke_tool(context, json.dumps({"key": key}))
            return outputs, journal

        await attempt(("a", "b"))
        assert tools.calls == {"a": 1, "b": 1}

        outputs, journal = await attempt(("b", "a"))

        assert tools.calls == {"a": 1, "b": 1}
        assert outputs == {"a": "a=42", "b": "b=42"}
        assert journal.stats.replayed["tool"] == 2

    def test_only_wrapped_tools_get_a_reserved_step(self):
        """
        A tool the model is handed but the durable run does not wrap never takes a reserved step.

        MCP tools and a handoff agent's tools reach the model this way, so reserving for them would leave
        positions nobody takes.
        """

        @function_tool
        def plain_lookup(key: str) -> str:
            """Look a key up."""
            return key

        journal = DurableJournal(MemoryStorage())
        durable_run = journal.start_run()
        durable_tool = _get_durable_tool(_SleepingTools().tools()[0], durable_run)
        fingerprint = _build_tool_fingerprint("lookup", json.dumps({"key": "a"}), "call-durable")
        response = ModelResponse(
            output=[
                ResponseFunctionToolCall(
                    id=f"fc-{call_id}",
                    call_id=call_id,
                    name=name,
                    arguments=json.dumps({"key": "a"}),
                    type="function_call",
                )
                for call_id, name in (("call-durable", "lookup"), ("call-plain", "plain_lookup"))
            ],
            usage=Usage(),
            response_id="resp",
        )

        _DurableModel(mock.create_autospec(Model, instance=True), durable_run)._reserve_tool_steps(
            response, [durable_tool, plain_lookup]
        )

        assert durable_run.position == 1
        durable = durable_run.claim_reserved(
            "call-durable", "tool:lookup", kind="tool", fingerprint=fingerprint
        )
        plain = durable_run.claim_reserved("call-plain", "tool:plain_lookup", kind="tool", fingerprint=None)
        assert (durable.position, plain.position) == (0, 1)

    @pytest.mark.asyncio
    async def test_a_later_response_reserves_a_new_step_for_a_call_with_the_same_id_and_arguments(self):
        storage = MemoryStorage()
        tools = _SleepingTools()
        response = ModelResponse(
            output=[
                ResponseFunctionToolCall(
                    id="fc-x",
                    call_id="call-x",
                    name="lookup",
                    arguments=json.dumps({"key": "a"}),
                    type="function_call",
                )
            ],
            usage=Usage(),
            response_id="resp",
        )
        durable_run = DurableJournal(storage).start_run()
        durable_tools = [_get_durable_tool(tool, durable_run) for tool in tools.tools()]
        model = _DurableModel(mock.create_autospec(Model, instance=True), durable_run)
        # No tool takes the first response's call; only the second response's is invoked.
        model._reserve_tool_steps(response, durable_tools)
        model._reserve_tool_steps(response, durable_tools)
        context = ToolContext(
            context=None,
            usage=Usage(),
            tool_name="lookup",
            tool_call_id="call-x",
            tool_arguments=json.dumps({"key": "a"}),
        )

        await durable_tools[0].on_invoke_tool(context, json.dumps({"key": "a"}))

        assert build_step_key(durable_run.key, 1) in storage.entries
        assert build_step_key(durable_run.key, 0) not in storage.entries

    @pytest.mark.parametrize("source", ["run-hook", "agent-hook", "tool-input-guardrail"])
    @pytest.mark.parametrize(
        "first_attempt_delays",
        [{"call-a": 0.01, "call-b": 0.1}, {"call-a": 0.1, "call-b": 0.01}],
        ids=["a-first", "b-first"],
    )
    def test_parallel_tool_calls_replay_when_what_runs_before_them_takes_a_different_time(
        self, source, first_attempt_delays
    ):
        """A hook or guardrail the SDK awaits before invoking a tool can let a later call claim first."""
        storage = MemoryStorage()
        tools = _SleepingTools()

        def attempt(model: Model, delays: dict[str, float]):
            kwargs: dict[str, Any] = {}
            if source == "run-hook":
                kwargs["hooks"] = _SlowRunHooks(delays)
            elif source == "agent-hook":
                kwargs["agent_hooks"] = _SlowAgentHooks(delays)
            else:
                kwargs["guardrail_delays"] = delays
            return _attempt_parallel(storage, model, tools, **kwargs)

        with pytest.raises(RuntimeError, match="worker lost"):
            attempt(_ParallelModel(fail_on_answer=True), first_attempt_delays)
        assert tools.calls == {"a": 1, "b": 1}

        _, journal = attempt(_ParallelModel(), {"call-a": 0.1, "call-b": 0.01})

        assert journal.stats.replayed == {"model": 1, "tool": 2}
        assert tools.calls == {"a": 1, "b": 1}


class TestDurableExceptions:
    def test_a_static_handoff_agent_replays_its_calls(self):
        storage, tool = MemoryStorage(), _CountingTool()

        def attempt(specialist_model: _ScriptedModel) -> DurableJournal:
            specialist = Agent(name="specialist", model=specialist_model, tools=AirflowTools(tool.tool()))
            lead = Agent(name="lead", model=_TransferModel(), handoffs=[specialist])
            journal = DurableJournal(storage, clean_up_after_run=True)
            with journal_scope(journal):
                run_durably_sync(lead, "go", run_config=NO_TRACING)
            return journal

        # The transfer's own result is the first function output the specialist sees.
        with pytest.raises(RuntimeError, match="worker lost"):
            attempt(_ScriptedModel(lookups=2, fail_on_answer=True))
        assert tool.calls == 1

        specialist_model = _ScriptedModel(lookups=2)
        journal = attempt(specialist_model)

        assert journal.stats.replayed == {"model": 2, "tool": 1}
        assert specialist_model.calls == 1
        assert tool.calls == 1

    @pytest.mark.parametrize(
        ("by_name", "replayed_models"),
        [
            pytest.param(False, 1, id="model-instance"),
            pytest.param(True, 2, id="model-name"),
        ],
    )
    def test_a_handoff_object_agent_runs_its_own_tools_live(self, by_name, replayed_models):
        storage, tool = MemoryStorage(), _CountingTool()

        def attempt(specialist_model: _ScriptedModel) -> DurableJournal:
            specialist = Agent(
                name="specialist",
                model="specialist-model" if by_name else specialist_model,
                tools=AirflowTools(tool.tool()),
            )
            lead = Agent(name="lead", model=_TransferModel(), handoffs=[handoff(specialist)])
            config = RunConfig(tracing_disabled=True, model_provider=_Provider(specialist_model))
            journal = DurableJournal(storage, clean_up_after_run=True)
            with journal_scope(journal):
                run_durably_sync(lead, "go", run_config=config)
            return journal

        with pytest.raises(RuntimeError, match="worker lost"):
            attempt(_ScriptedModel(lookups=2, fail_on_answer=True))
        assert tool.calls == 1

        journal = attempt(_ScriptedModel(lookups=2))

        assert journal.stats.replayed == {"model": replayed_models}
        assert tool.calls == 2

    @pytest.mark.asyncio
    async def test_a_streamed_model_call_is_not_recorded(self):
        events = [object(), object()]

        async def stream(*args: Any, **kwargs: Any):
            for event in events:
                yield event

        wrapped = mock.create_autospec(Model, instance=True)
        wrapped.stream_response.side_effect = stream
        storage = MemoryStorage()
        durable_run = DurableJournal(storage).start_run()
        model = _DurableModel(wrapped, durable_run)

        streamed = [
            event
            async for event in model.stream_response(
                None,
                "go",
                ModelSettings(),
                [],
                None,
                [],
                ModelTracing.DISABLED,
                previous_response_id=None,
                conversation_id=None,
                prompt=None,
            )
        ]

        assert streamed == events
        wrapped.get_response.assert_not_called()
        assert durable_run.position == 0
        assert storage.steps() == []

    @pytest.mark.parametrize(
        "needs_approval",
        [True, lambda context, arguments, call_id: True],
        ids=["always", "by-rule"],
    )
    def test_without_request_input_a_run_that_stops_for_approval_fails_naming_the_tool(self, needs_approval):
        calls = []

        @function_tool(needs_approval=needs_approval)
        def lookup(key: str) -> str:
            """Look a key up."""
            calls.append(key)
            return f"{key}=42"

        with (
            mock.patch(
                "airflow.providers.common.ai.durable.openai_agents._get_request_input",
                autospec=True,
                return_value=None,
            ),
            journal_scope(DurableJournal(MemoryStorage(), clean_up_after_run=True)),
            pytest.raises(
                UnsupportedToolDeferralError,
                match=r"approval to call lookup \(analyst\).*needs Airflow 3\.4\+ and a standard provider",
            ),
        ):
            run_durably_sync(
                Agent(name="analyst", model=_ScriptedModel(), tools=[lookup]), "go", run_config=NO_TRACING
            )

        assert calls == []

    def test_outside_a_task_a_run_that_stops_for_approval_fails_naming_the_tool(self):
        """A journal set in a unit test has no task to pause, so the run fails as it does without request_input()."""
        calls: list[str] = []

        with (
            journal_scope(DurableJournal(MemoryStorage(), clean_up_after_run=True)),
            pytest.raises(
                UnsupportedToolDeferralError,
                match=r"approval to call lookup \(analyst\).*there is no running task to pause",
            ),
        ):
            run_durably_sync(
                Agent(name="analyst", model=_ScriptedModel(), tools=[_lookup_tool(calls)]),
                "go",
                run_config=NO_TRACING,
            )

        assert calls == []

    def test_a_clear_after_an_agent_tool_stopped_for_approval_asks_again_from_that_turn(self):
        """Earlier turns replay; the agent tool's answer, given while its own run waited, does not."""
        storage, calls = MemoryStorage(), []

        def attempt(needs_approval: Any, call_id_prefix: str) -> DurableJournal:
            @function_tool(needs_approval=needs_approval)
            def lookup(key: str) -> str:
                """Look a key up."""
                calls.append(key)
                return f"{key}=42"

            specialist = Agent(name="specialist", model="inner", tools=[lookup])
            # The model asked again answers with new call ids, as a real one does. Reusing them
            # would resume the SDK's in-process record of the first try's nested run instead.
            lead = Agent(
                name="lead",
                model=_ScriptedModel(lookups=2, argument="input", call_id_prefix=call_id_prefix),
                tools=[specialist.as_tool("lookup", "Ask the specialist.")],
            )
            config = RunConfig(tracing_disabled=True, model_provider=_Provider(_ScriptedModel()))
            journal = DurableJournal(storage, clean_up_after_run=True)
            with journal_scope(journal):
                run_durably_sync(lead, "go", run_config=config)
            return journal

        # The specialist answers the first time it is asked, and waits for approval the second time.
        with pytest.raises(UnsupportedToolDeferralError, match=r"lookup \(specialist\).*through as_tool\(\)"):
            attempt(lambda context, arguments, call_id: bool(calls), call_id_prefix="first")
        journal = attempt(False, call_id_prefix="second")

        # The first turn and the specialist's first answer replay; the second turn runs again.
        assert journal.stats.replayed == {"model": 1, "tool": 1}
        assert calls == ["k0", "k0"]


def _answer(option: str, reason: str = "") -> dict[str, Any]:
    """The answer event the Core API puts in the paused task's next_kwargs."""
    return {
        "chosen_options": [option],
        "params_input": {"reason": reason},
        "responded_at": datetime.datetime(2026, 11, 5, tzinfo=datetime.UTC),
        "responded_by_user": {"id": "1", "name": "reviewer"},
        "timedout": False,
    }


def _sent(mock_supervisor_comms, msg_type: type) -> list[Any]:
    messages = (
        call.kwargs.get("msg", call.args[0] if call.args else None)
        for call in mock_supervisor_comms.send.call_args_list
    )
    return [msg for msg in messages if isinstance(msg, msg_type)]


class _Reviewer:
    """
    Runs a task through the task runner and answers each Required Actions request it parks on.

    A resumed run is handed the parked kwargs plus the answer, as the Core API response
    handler hands them back, so ``PythonOperator.resume_with_input`` runs for real.
    """

    def __init__(self, create_runtime_ti, mock_supervisor_comms) -> None:
        self.create_runtime_ti = create_runtime_ti
        self.comms = mock_supervisor_comms
        self.decide: Any = lambda request: _answer("Approve")
        self.keys: list[str] = []

    @property
    def requests(self) -> list[CreateHITLDetailPayload]:
        return _sent(self.comms, CreateHITLDetailPayload)

    def run(self, op) -> tuple[Any, BaseException | None]:
        ti = self.create_runtime_ti(task=op)
        state, msg, error = _run_ti(ti)
        while isinstance(msg, AwaitInputTask):
            parked = deserialize(msg.next_kwargs)
            assert isinstance(parked, dict)
            self.keys.append(parked["key"])
            resumed = self.create_runtime_ti(task=op, ti_id=ti.id)
            resumed._ti_context_from_server.next_method = msg.next_method
            resumed._ti_context_from_server.next_kwargs = serialize(
                {**parked, "event": self.decide(self.requests[-1])}
            )
            state, msg, error = _run_ti(resumed)
        return state, error

    def returned(self) -> list[Any]:
        return [msg.value for msg in _sent(self.comms, SetXCom) if msg.key == "return_value"]


def _run_ti(ti) -> tuple[Any, Any, BaseException | None]:
    return run(ti, context=ti.get_template_context(), log=structlog.get_logger())


@pytest.fixture
def reviewer(create_runtime_ti, mock_supervisor_comms):
    return _Reviewer(create_runtime_ti, mock_supervisor_comms)


def _lookup_tool(calls: list[str], needs_approval: Any = True) -> Any:
    @function_tool(needs_approval=needs_approval)
    def lookup(key: str) -> str:
        """Look a key up."""
        calls.append(key)
        return f"{key}=42"

    return lookup


def _agent_task(storage: MemoryStorage, agent: Any, journals: list[DurableJournal], **kwargs: Any):
    """
    A ``@task`` that runs an agent durably; each run, first or resumed, gets a fresh journal over ``storage``.

    ``agent`` is the agent, or a function of the run's number that returns the agent that run uses.
    """
    with DAG(dag_id="agent_with_approvals"):

        @task
        def handle_ticket() -> str:
            run_agent = agent(len(journals)) if callable(agent) else agent
            journal = DurableJournal(storage, clean_up_after_run=True)
            journals.append(journal)
            with journal_scope(journal):
                return run_durably_sync(run_agent, "go", run_config=NO_TRACING, **kwargs).final_output

        return handle_ticket().operator


class TestToolApproval:
    @pytest.mark.parametrize(
        "needs_approval",
        [True, lambda context, arguments, call_id: True],
        ids=["always", "by-rule"],
    )
    def test_an_approved_call_runs_once_and_no_model_call_is_made_twice(self, reviewer, needs_approval):
        storage, calls, journals, model = MemoryStorage(), [], [], _ScriptedModel()
        agent = Agent(name="analyst", model=model, tools=[_lookup_tool(calls, needs_approval)])

        state, error = reviewer.run(_agent_task(storage, agent, journals))

        assert (state, error) == (TaskInstanceState.SUCCESS, None)
        assert reviewer.returned() == ["answer: k0=42"]
        assert calls == ["k0"]
        # The request that asked for the call replayed on resume; only the one that answers is new.
        assert model.calls == 2
        assert journals[-1].stats.replayed == {"model": 1}
        [request] = reviewer.requests
        assert request.subject == "analyst wants to call lookup"
        assert request.options == ["Approve", "Reject"]
        assert "**Tool:** lookup — Look a key up." in request.body
        assert '"key": "k0"' in request.body
        assert reviewer.keys[0].split(":")[1] == "call-0"
        assert storage.steps() == []

    @pytest.mark.parametrize(
        ("reason", "told_to_model"),
        [
            ("Refunds over 40 need a manager.", "Refunds over 40 need a manager."),
            ("   ", "Tool execution was not approved."),
        ],
        ids=["with-reason", "blank-reason"],
    )
    def test_a_rejected_call_tells_the_model_the_reviewers_reason(self, reviewer, reason, told_to_model):
        storage, calls, journals, model = MemoryStorage(), [], [], _ScriptedModel()
        reviewer.decide = lambda request: _answer("Reject", reason)
        agent = Agent(name="analyst", model=model, tools=[_lookup_tool(calls)])

        state, _ = reviewer.run(_agent_task(storage, agent, journals))

        assert state == TaskInstanceState.SUCCESS
        assert calls == []
        assert reviewer.returned() == [f"answer: {told_to_model}"]
        assert model.calls == 2

    def test_a_later_approval_replays_the_calls_before_it(self, reviewer):
        storage, calls, journals, model = MemoryStorage(), [], [], _ScriptedModel(lookups=2)
        agent = Agent(name="analyst", model=model, tools=[_lookup_tool(calls)])

        state, _ = reviewer.run(_agent_task(storage, agent, journals))

        assert state == TaskInstanceState.SUCCESS
        assert reviewer.returned() == ["answer: k1=42"]
        assert len(reviewer.requests) == 2
        # Across three runs of the task, every model call and every tool call happened once.
        assert model.calls == 3
        assert calls == ["k0", "k1"]
        assert journals[-1].stats.replayed == {"model": 2, "tool": 1}

    def test_two_calls_in_one_answer_are_asked_one_at_a_time(self, reviewer):
        storage, calls, journals, model = MemoryStorage(), [], [], _ParallelModel()
        reviewer.decide = lambda request: (
            _answer("Approve") if '"key": "a"' in request.body else _answer("Reject", "Not b.")
        )
        agent = Agent(name="analyst", model=model, tools=[_lookup_tool(calls)])

        state, _ = reviewer.run(_agent_task(storage, agent, journals))

        assert state == TaskInstanceState.SUCCESS
        assert [key.split(":")[1] for key in reviewer.keys] == ["call-a", "call-b"]
        assert calls == ["a"]
        assert reviewer.returned() == ["answer: ['Not b.', 'a=42']"]
        assert model.calls == 2

    def test_a_tool_of_an_agent_handed_off_to_pauses_the_same_way(self, reviewer):
        storage, calls, journals = MemoryStorage(), [], []
        # The transfer's own result is the first function output the specialist sees.
        specialist = Agent(name="specialist", model=_ScriptedModel(lookups=2), tools=[_lookup_tool(calls)])
        lead = Agent(name="lead", model=_TransferModel(), handoffs=[specialist])

        state, _ = reviewer.run(_agent_task(storage, lead, journals))

        assert state == TaskInstanceState.SUCCESS
        assert reviewer.returned() == ["answer: k1=42"]
        assert [request.subject for request in reviewer.requests] == ["specialist wants to call lookup"]
        assert calls == ["k1"]

    def test_a_retry_approves_without_asking_a_call_that_already_ran(self, reviewer):
        storage, calls, journals = MemoryStorage(), [], []
        lookup = _lookup_tool(calls)
        failing = Agent(name="analyst", model=_ScriptedModel(fail_on_answer=True), tools=[lookup])
        state, error = reviewer.run(_agent_task(storage, failing, journals))
        assert state == TaskInstanceState.FAILED
        assert isinstance(error, RuntimeError)
        assert len(reviewer.requests) == 1

        retry_model = _ScriptedModel()
        retry = Agent(name="analyst", model=retry_model, tools=[lookup])
        state, _ = reviewer.run(_agent_task(storage, retry, journals))

        assert state == TaskInstanceState.SUCCESS
        assert reviewer.returned() == ["answer: k0=42"]
        assert len(reviewer.requests) == 1
        assert calls == ["k0"]
        assert retry_model.calls == 1

    def test_a_run_with_a_session_fails_instead_of_pausing(self, reviewer):
        agent = Agent(name="analyst", model=_ScriptedModel(), tools=[_lookup_tool([])])

        state, error = reviewer.run(
            _agent_task(MemoryStorage(), agent, [], session=SQLiteSession("approval"))
        )

        assert state == TaskInstanceState.FAILED
        assert isinstance(error, UnsupportedToolDeferralError)
        assert "a run with a session" in str(error)
        assert reviewer.requests == []

    @pytest.mark.parametrize("fresh_call_ids", [True, False], ids=["fresh-ids", "reused-ids"])
    @pytest.mark.parametrize(
        "decisions",
        [("Approve",), ("Approve", "Approve"), ("Reject", "Approve")],
        ids=["one-approval", "two-approvals", "reject-then-approve"],
    )
    def test_a_retry_after_the_model_failed_pauses_and_resumes_on_its_own_answers(
        self, reviewer, fresh_call_ids, decisions
    ):
        """A failed try's entries must neither replay into the retry nor make its resumes ask the model again."""
        storage, calls, journals = MemoryStorage(), [], []
        model = _ScriptedModel(lookups=len(decisions), fresh_call_ids=fresh_call_ids)
        # The first try runs out of turns after its first answer, which fails it because of the model.
        unapproved = Agent(name="analyst", model=model, tools=[_lookup_tool(calls, needs_approval=False)])
        state, error = reviewer.run(_agent_task(storage, unapproved, journals, max_turns=1))
        assert (state, type(error)) == (TaskInstanceState.FAILED, MaxTurnsExceeded)
        assert calls == ["k0"]

        reviewer.decide = lambda request: _answer(decisions[len(reviewer.requests) - 1], "No.")
        approved = Agent(name="analyst", model=model, tools=[_lookup_tool(calls)])
        state, error = reviewer.run(_agent_task(storage, approved, journals))

        assert (state, error) == (TaskInstanceState.SUCCESS, None)
        assert len(reviewer.requests) == len(decisions)
        # The retry ran each approved call once, its answer to the first try's call included.
        assert calls == ["k0"] + [f"k{i}" for i, decision in enumerate(decisions) if decision == "Approve"]
        # The retry asked the model once per call and once for the answer; every resume replayed.
        assert model.calls == 1 + len(decisions) + 1
        replayed_tools = sum(decision == "Approve" for decision in decisions[:-1])
        assert journals[-1].stats.replayed == (
            {"model": len(decisions), "tool": replayed_tools} if replayed_tools else {"model": len(decisions)}
        )

    def test_a_resume_that_does_not_come_back_to_the_answered_call_fails(self, reviewer):
        """An answer must not apply to a call the reviewer never saw, even one reusing its id."""
        storage, calls, journals = MemoryStorage(), [], []

        def agent_for_run(run: int) -> Agent[Any]:
            # From the resume on, the request differs, so the model is asked again and answers
            # with a call that reuses the paused call's id but not its arguments.
            return Agent(
                name="analyst",
                model=_ScriptedModel(argument="key" if run == 0 else "order"),
                instructions=None if run == 0 else "Changed while the task waited.",
                tools=[_lookup_tool(calls)],
            )

        state, error = reviewer.run(_agent_task(storage, agent_for_run, journals))

        assert state == TaskInstanceState.FAILED
        assert isinstance(error, DurableJournalError)
        assert len(reviewer.requests) == 1
        assert calls == []

    def test_a_resume_that_finishes_without_the_answered_call_fails(self, reviewer):
        """Finishing would leave the answer unread and the journal deleted, and a retry would start over."""
        storage, calls, journals = MemoryStorage(), [], []

        def agent_for_run(run: int) -> Agent[Any]:
            # From the resume on, the request differs and the model asks for a lookup that needs
            # no approval, so the run finishes without asking about the call that was answered.
            return Agent(
                name="analyst",
                model=_ScriptedModel(value_prefix="k" if run == 0 else "x"),
                instructions=None if run == 0 else "Changed while the task waited.",
                tools=[
                    _lookup_tool(calls, lambda context, arguments, call_id: arguments["key"].startswith("k"))
                ],
            )

        state, error = reviewer.run(_agent_task(storage, agent_for_run, journals))

        assert state == TaskInstanceState.FAILED
        assert isinstance(error, DurableJournalError)
        assert len(reviewer.requests) == 1
        assert calls == ["x0"]
        # Kept for a clear, which replays the finished run instead of starting it over.
        assert storage.steps() != []

    def test_an_error_asking_for_approval_fails_the_task_with_that_error(self, reviewer):
        """Only an answer the run did not come back to fails it for good; any other error is retried."""
        calls: list[str] = []
        failing = mock.create_autospec(
            request_input, side_effect=RuntimeError("the API server dropped the connection")
        )
        agent = Agent(name="analyst", model=_ScriptedModel(), tools=[_lookup_tool(calls)])

        with mock.patch(
            "airflow.providers.common.ai.durable.openai_agents._get_request_input",
            autospec=True,
            return_value=failing,
        ):
            state, error = reviewer.run(_agent_task(MemoryStorage(), agent, []))

        assert state == TaskInstanceState.FAILED
        assert type(error) is RuntimeError
        assert str(error) == "the API server dropped the connection"
        assert calls == []

    def test_a_task_that_runs_another_agent_first_resumes_past_it(self, reviewer):
        """The answer belongs to the second agent's run, so the first one finishing does not claim it."""
        storage, calls = MemoryStorage(), []
        researcher = Agent(name="researcher", model=_ScriptedModel(), tools=[_lookup_tool(calls, False)])
        analyst = Agent(name="analyst", model=_ScriptedModel(value_prefix="x"), tools=[_lookup_tool(calls)])
        with DAG(dag_id="two_agents"):

            @task
            def research_then_analyse() -> str:
                with journal_scope(DurableJournal(storage, clean_up_after_run=True)):
                    run_durably_sync(researcher, "go", run_config=NO_TRACING)
                    return run_durably_sync(analyst, "go", run_config=NO_TRACING).final_output

            op = research_then_analyse().operator

        state, error = reviewer.run(op)

        assert (state, error) == (TaskInstanceState.SUCCESS, None)
        assert len(reviewer.requests) == 1
        # The first agent's record is deleted when it finishes, so it runs again on the resume.
        assert calls == ["k0", "k0", "x0"]

    def test_an_operator_that_cannot_resume_fails_instead_of_pausing(self, reviewer):
        class _TicketOperator(BaseOperator):
            def execute(self, context):
                agent = Agent(name="analyst", model=_ScriptedModel(), tools=[_lookup_tool([])])
                with journal_scope(DurableJournal(MemoryStorage(), clean_up_after_run=True)):
                    return run_durably_sync(agent, "go", run_config=NO_TRACING).final_output

        with DAG(dag_id="agent_in_an_operator"):
            op = _TicketOperator(task_id="handle_ticket")

        state, error = reviewer.run(op)

        assert state == TaskInstanceState.FAILED
        assert isinstance(error, UnsupportedToolDeferralError)
        assert "_TicketOperator" in str(error)
        assert reviewer.requests == []

    def test_a_tool_of_an_agent_reached_through_handoff_fails_instead_of_pausing(self, reviewer):
        specialist = Agent(name="specialist", model=_ScriptedModel(lookups=2), tools=[_lookup_tool([])])
        lead = Agent(name="lead", model=_TransferModel(), handoffs=[handoff(specialist)])

        state, error = reviewer.run(_agent_task(MemoryStorage(), lead, []))

        assert state == TaskInstanceState.FAILED
        assert isinstance(error, UnsupportedToolDeferralError)
        assert "reached through handoff()" in str(error)
        assert reviewer.requests == []

    @pytest.mark.enable_redact
    def test_a_secret_in_the_arguments_is_masked_for_the_reviewer(self, reviewer, registered_secret):
        calls: list[str] = []
        model = _ScriptedModel(value_prefix=registered_secret)
        agent = Agent(name="analyst", model=model, tools=[_lookup_tool(calls)])

        state, _ = reviewer.run(_agent_task(MemoryStorage(), agent, []))

        assert state == TaskInstanceState.SUCCESS
        [request] = reviewer.requests
        assert registered_secret not in request.body
        assert "***" in request.body
        assert len(calls) == 1
