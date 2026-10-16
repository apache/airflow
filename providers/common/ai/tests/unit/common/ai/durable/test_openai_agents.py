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

import contextlib
import datetime
import json
from typing import Any

import pytest

pytest.importorskip("agents")

from agents import (
    Agent,
    Model,
    ModelProvider,
    ModelResponse,
    ModelSettings,
    RunConfig,
    SQLiteSession,
    ToolOutputText,
    Usage,
    function_tool,
)
from agents.exceptions import MaxTurnsExceeded, ModelBehaviorError
from agents.retry import ModelRetryBackoffSettings, ModelRetrySettings
from openai.types.responses import ResponseFunctionToolCall, ResponseOutputMessage, ResponseOutputText
from pydantic import BaseModel

from airflow.providers.common.ai.durable.base import build_step_key
from airflow.providers.common.ai.durable.journal import DurableJournal, journal_scope
from airflow.providers.common.ai.durable.openai_agents import _get_durable_tool, run_durably_sync
from airflow.providers.common.ai.tools import AirflowTool, ToolResult
from airflow.providers.common.ai.tools.openai_agents import AirflowTools

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
    ) -> None:
        self.lookups = lookups
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
                    call_id=f"call-{len(outputs)}",
                    name="lookup",
                    arguments=json.dumps({self.argument: f"k{len(outputs)}"}),
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


class _Provider(ModelProvider):
    """Resolves every model name to one model."""

    def __init__(self, model: Model) -> None:
        self.model = model

    def get_model(self, model_name: str | None) -> Model:
        return self.model


class _Report(BaseModel):
    rows: int


def _attempt(storage: MemoryStorage, model: Model, tool: _CountingTool, prompt: str = "go", **kwargs: Any):
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
