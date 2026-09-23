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
import time
from typing import Any
from unittest import mock

import pytest
from pydantic_ai import Agent
from pydantic_ai._run_context import RunContext
from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.messages import ModelResponse, RetryPromptPart, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import FunctionModel
from pydantic_core import ValidationError

from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError, ManagedAgentRejected
from airflow.providers.common.ai.managed_agents import (
    FailoverManagedAgentClient,
    ManagedAgentCapabilities,
    ManagedAgentRef,
    ManagedAgentRequest,
    ManagedAgentResponse,
)
from airflow.providers.common.ai.toolsets.managed_agent import BaseManagedAgentToolset, ManagedAgentToolset

FAKE_REF = ManagedAgentRef(platform="fake.cloud", name="specialist-1")


class FakeManagedAgentToolset(BaseManagedAgentToolset):
    """Minimal direct subclass standing in for an agent that has no hook."""

    def __init__(self, *, result: Any = "the answer", raises: Exception | None = None, **kwargs):
        kwargs.setdefault("tool_name", "ask_specialist")
        kwargs.setdefault("description", "Answers questions about the thing.")
        super().__init__(**kwargs)
        self._result = result
        self._raises = raises
        self.prompts: list[str] = []

    @property
    def agent_ref(self) -> ManagedAgentRef:
        return FAKE_REF

    async def invoke(self, prompt: str) -> Any:
        self.prompts.append(prompt)
        if self._raises is not None:
            raise self._raises
        return self._result


class FakeClient:
    """A ManagedAgentClient with no hook behind it."""

    def __init__(
        self,
        name: str = "specialist-1",
        *,
        answer: str = "the answer",
        raises: Exception | None = None,
        raise_times: int = 1,
        ref_raises: Exception | None = None,
        sessions: bool = False,
    ):
        self._name = name
        self._answer = answer
        self._raises = raises
        self._raise_times = raise_times
        self._ref_raises = ref_raises
        self.capabilities = ManagedAgentCapabilities(sessions=sessions)
        self.requests: list[ManagedAgentRequest] = []

    @property
    def ref(self) -> ManagedAgentRef:
        if self._ref_raises is not None:
            raise self._ref_raises
        return ManagedAgentRef(platform="fake.cloud", name=self._name)

    def invoke(self, request: ManagedAgentRequest) -> ManagedAgentResponse:
        self.requests.append(request)
        if self._raises is not None and len(self.requests) <= self._raise_times:
            raise self._raises
        return ManagedAgentResponse(text=f"{self._answer}: {request.prompt}", raw={"envelope": True})


async def call(toolset: BaseManagedAgentToolset, prompt: str = "what is the number?") -> Any:
    ctx = mock.MagicMock(spec=RunContext)
    tools = await toolset.get_tools(ctx)
    return await toolset.call_tool(toolset._tool_name, {"prompt": prompt}, ctx, tools[toolset._tool_name])


class TestBaseManagedAgentToolsetConstruction:
    def test_is_abstract(self):
        with pytest.raises(TypeError, match="abstract"):
            BaseManagedAgentToolset(tool_name="x", description="y")  # type: ignore[abstract]

    @pytest.mark.parametrize(
        "description", [None, "", "   ", "\n"], ids=["none", "empty", "whitespace", "newline"]
    )
    def test_absent_description_falls_back_to_the_tool_name(self, description):
        toolset = FakeManagedAgentToolset(tool_name="ask_bookings_analyst", description=description)
        assert toolset._description == "Ask bookings analyst"

    def test_supplied_description_is_kept_verbatim(self):
        toolset = FakeManagedAgentToolset(description="Knows bookings. Cannot see support tickets.")
        assert toolset._description == "Knows bookings. Cannot see support tickets."

    def test_empty_tool_name_rejected(self):
        with pytest.raises(ValueError, match="tool_name must be a non-empty string"):
            FakeManagedAgentToolset(tool_name="")

    def test_timeout_is_public_so_implementations_can_honor_it(self):
        assert FakeManagedAgentToolset().timeout is None
        assert FakeManagedAgentToolset(timeout=600.0).timeout == 600.0

    def test_id_is_derived_from_tool_name(self):
        assert FakeManagedAgentToolset(tool_name="ask_bookings").id == "managed-agent-ask_bookings"

    def test_not_replayable_by_default(self):
        assert FakeManagedAgentToolset().replayable is False

    @pytest.mark.asyncio
    async def test_max_retries_defaults_to_one_and_is_configurable(self):
        default = await FakeManagedAgentToolset().get_tools(ctx=None)
        assert default["ask_specialist"].max_retries == 1
        tuned = await FakeManagedAgentToolset(max_retries=3).get_tools(ctx=None)
        assert tuned["ask_specialist"].max_retries == 3

    def test_negative_max_retries_rejected(self):
        with pytest.raises(ValueError, match="max_retries must not be negative"):
            FakeManagedAgentToolset(max_retries=-1)

    def test_subclass_implementing_neither_invoke_hook_is_rejected(self):
        class Neither(BaseManagedAgentToolset):
            @property
            def agent_ref(self) -> ManagedAgentRef:
                return FAKE_REF

        with pytest.raises(TypeError, match="must implement invoke_sync"):
            Neither(tool_name="ask_nothing")


class TestSyncInvocation:
    """A blocking vendor SDK must not run on the agent's event loop."""

    class _Blocking(BaseManagedAgentToolset):
        @property
        def agent_ref(self) -> ManagedAgentRef:
            return FAKE_REF

        def invoke_sync(self, prompt: str) -> Any:
            time.sleep(0.2)
            return f"slept for {prompt}"

    @pytest.mark.asyncio
    async def test_invoke_sync_is_offloaded_so_the_loop_keeps_running(self):
        ticks = 0

        async def ticker():
            nonlocal ticks
            while True:
                await asyncio.sleep(0.01)
                ticks += 1

        task = asyncio.create_task(ticker())
        result = await self._Blocking(tool_name="ask_slow").invoke("q")
        task.cancel()
        assert result == "slept for q"
        assert ticks >= 3, "the event loop was blocked while invoke_sync ran"

    @pytest.mark.asyncio
    async def test_invoke_sync_result_reaches_the_model_through_call_tool(self):
        assert await call(self._Blocking(tool_name="ask_slow"), "q") == "slept for q"

    @pytest.mark.asyncio
    async def test_an_async_override_is_used_as_is(self):
        assert await FakeManagedAgentToolset(result="async answer").invoke("q") == "async answer"


class TestGetTools:
    @pytest.mark.asyncio
    async def test_exposes_exactly_one_tool_under_its_name(self):
        tools = await FakeManagedAgentToolset(tool_name="ask_bookings").get_tools(ctx=None)
        assert list(tools) == ["ask_bookings"]

    @pytest.mark.asyncio
    async def test_tool_definition_carries_name_description_and_prompt_schema(self):
        toolset = FakeManagedAgentToolset(tool_name="ask_bookings", description="Knows bookings.")
        tool_def = (await toolset.get_tools(ctx=None))["ask_bookings"].tool_def
        assert tool_def.name == "ask_bookings"
        assert tool_def.description == "Knows bookings."
        assert tool_def.parameters_json_schema["required"] == ["prompt"]
        assert tool_def.parameters_json_schema["properties"]["prompt"]["type"] == "string"

    @pytest.mark.asyncio
    async def test_not_sequential_so_specialists_can_be_consulted_concurrently(self):
        tool_def = (await FakeManagedAgentToolset().get_tools(ctx=None))["ask_specialist"].tool_def
        assert tool_def.sequential is False

    @pytest.mark.asyncio
    async def test_args_validator_rejects_a_missing_prompt(self):
        validator = (await FakeManagedAgentToolset().get_tools(ctx=None))["ask_specialist"].args_validator
        with pytest.raises(ValidationError):
            validator.validate_json("{}")


class TestCallTool:
    @pytest.mark.asyncio
    async def test_passes_the_prompt_through_to_invoke(self):
        toolset = FakeManagedAgentToolset()
        await call(toolset, prompt="how many widgets?")
        assert toolset.prompts == ["how many widgets?"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("result", "expected"),
        [("plain text", "plain text"), (None, "null"), ({"total": 42}, '{"total": 42}'), ([1, 2], "[1, 2]")],
        ids=["str", "none", "dict", "list"],
    )
    async def test_result_is_serialized_for_the_model(self, result, expected):
        assert await call(FakeManagedAgentToolset(result=result)) == expected

    @pytest.mark.asyncio
    async def test_logs_the_agent_it_consulted(self, caplog):
        with caplog.at_level("INFO"):
            await call(FakeManagedAgentToolset())
        assert "specialist-1" in caplog.text
        assert "fake.cloud" in caplog.text

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.common.ai.toolsets.managed_agent.Stats.incr", autospec=True)
    async def test_every_answer_is_counted_by_tool_and_platform(self, mock_incr):
        await call(FakeManagedAgentToolset(tool_name="ask_bookings"))
        mock_incr.assert_called_once_with(
            "managed_agent.served", tags={"tool": "ask_bookings", "platform": "fake.cloud"}
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.common.ai.toolsets.managed_agent.Stats.incr", autospec=True)
    async def test_a_failed_call_is_not_counted_as_served(self, mock_incr):
        with pytest.raises(RuntimeError):
            await call(FakeManagedAgentToolset(raises=RuntimeError("503")))
        mock_incr.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [
            ModelRetry("rephrase that"),
            ManagedAgentInvocationError("bad credentials"),
            RuntimeError("503 from upstream"),
        ],
        ids=["model_retry", "terminal", "transient"],
    )
    async def test_invoke_errors_propagate_unchanged(self, error):
        # The base class must not reclassify what invoke() raised: the three buckets are
        # handled by different layers (model, task failure, task retry).
        with pytest.raises(type(error), match=str(error)):
            await call(FakeManagedAgentToolset(raises=error))


class TestManagedAgentToolset:
    """The client-backed toolset, which is the one Dags are expected to use."""

    @pytest.mark.asyncio
    async def test_model_gets_text_never_the_envelope(self):
        client = FakeClient()
        assert await call(ManagedAgentToolset(client, tool_name="ask"), "q") == "the answer: q"

    @pytest.mark.asyncio
    async def test_timeout_travels_on_the_request(self):
        client = FakeClient()
        await call(ManagedAgentToolset(client, tool_name="ask", timeout=42.0), "q")
        assert client.requests == [ManagedAgentRequest(prompt="q", timeout=42.0)]

    def test_something_that_is_not_a_client_is_refused_at_construction(self):
        # Passing the hook instead of hook.agent(...) is the likely mistake; it must not wait for the first call.
        with pytest.raises(TypeError, match="Pass hook.agent"):
            ManagedAgentToolset(object(), tool_name="ask")  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_vendor_options_travel_on_every_request(self):
        client = FakeClient()
        await call(ManagedAgentToolset(client, tool_name="ask", vendor_options={"class_method": "plan"}), "q")
        assert client.requests[0].vendor_options == {"class_method": "plan"}

    def test_identity_and_replayable_come_from_the_caller(self):
        client = FakeClient("analyst")
        toolset = ManagedAgentToolset(client, tool_name="ask", replayable=True)
        assert toolset.agent_ref == ManagedAgentRef(platform="fake.cloud", name="analyst")
        assert toolset.client is client
        assert toolset.replayable is True

    @pytest.mark.asyncio
    async def test_rejection_becomes_a_model_retry_at_the_boundary(self):
        client = FakeClient(raises=ManagedAgentRejected("too vague"))
        with pytest.raises(ModelRetry, match="too vague"):
            await call(ManagedAgentToolset(client, tool_name="ask"), "q")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error", [ManagedAgentInvocationError("gone"), RuntimeError("503")], ids=["terminal", "transient"]
    )
    async def test_other_errors_propagate_unchanged(self, error):
        with pytest.raises(type(error), match=str(error)):
            await call(ManagedAgentToolset(FakeClient(raises=error), tool_name="ask"), "q")

    @pytest.mark.asyncio
    async def test_a_broken_identity_does_not_fail_the_call(self, caplog):
        # Identity is for the log line and the metric tag; the answer must not depend on it.
        client = FakeClient(ref_raises=RuntimeError("Connection 'standby' not found"))
        with caplog.at_level("WARNING"):
            assert await call(ManagedAgentToolset(client, tool_name="ask"), "q") == "the answer: q"
        assert "identity could not be resolved" in caplog.text
        assert "Connection 'standby' not found" in caplog.text

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.common.ai.toolsets.managed_agent.Stats.incr", autospec=True)
    async def test_a_failover_group_is_counted_under_the_failover_platform(self, mock_incr):
        group = FailoverManagedAgentClient([FakeClient("a"), FakeClient("b")])
        await call(ManagedAgentToolset(group, tool_name="ask"), "q")
        mock_incr.assert_called_once_with(
            "managed_agent.served", tags={"tool": "ask", "platform": "failover"}
        )

    def test_rephrase_loop_end_to_end_with_a_scripted_model(self):
        """The rejected first prompt is re-asked once, then the answer reaches the model."""
        client = FakeClient(raises=ManagedAgentRejected("Which quarter?"), raise_times=1)
        seen: list[Any] = []

        def scripted(messages, info):
            if len(messages) == 1:
                return ModelResponse(
                    parts=[ToolCallPart(info.function_tools[0].name, {"prompt": "Revenue?"})]
                )
            parts = messages[-1].parts
            if any(isinstance(p, RetryPromptPart) for p in parts):
                seen.extend(p for p in parts if isinstance(p, RetryPromptPart))
                return ModelResponse(
                    parts=[ToolCallPart(info.function_tools[0].name, {"prompt": "Q3 revenue?"})]
                )
            seen.extend(p for p in parts if isinstance(p, ToolReturnPart))
            return ModelResponse(parts=[TextPart("Done")])

        toolset = ManagedAgentToolset(client, tool_name="ask_analyst", max_retries=1)
        result = Agent(FunctionModel(scripted), toolsets=[toolset]).run_sync("go")

        assert result.output == "Done"
        assert [type(p).__name__ for p in seen] == ["RetryPromptPart", "ToolReturnPart"]
        assert "Which quarter?" in seen[0].content
        assert seen[1].content == "the answer: Q3 revenue?"
        assert [r.prompt for r in client.requests] == ["Revenue?", "Q3 revenue?"]
