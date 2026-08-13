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

from typing import Any

import pytest
from pydantic_ai.exceptions import ModelRetry
from pydantic_core import ValidationError

from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
from airflow.providers.common.ai.toolsets.managed_agent import BaseManagedAgentToolset


class FakeManagedAgentToolset(BaseManagedAgentToolset):
    """Minimal implementation standing in for a provider's concrete toolset."""

    def __init__(self, *, result: Any = "the answer", raises: Exception | None = None, **kwargs):
        kwargs.setdefault("tool_name", "ask_specialist")
        kwargs.setdefault("description", "Answers questions about the thing.")
        super().__init__(**kwargs)
        self._result = result
        self._raises = raises
        self.prompts: list[str] = []

    @property
    def agent_ref(self) -> dict[str, str]:
        return {"platform": "fake.cloud", "name": "specialist-1"}

    async def invoke(self, prompt: str) -> Any:
        self.prompts.append(prompt)
        if self._raises is not None:
            raise self._raises
        return self._result


class TestBaseManagedAgentToolsetConstruction:
    def test_is_abstract(self):
        with pytest.raises(TypeError, match="abstract"):
            BaseManagedAgentToolset(tool_name="x", description="y")  # type: ignore[abstract]

    @pytest.mark.parametrize(
        "description",
        ["", "   ", "\n"],
        ids=["empty", "whitespace", "newline"],
    )
    def test_blank_description_rejected(self, description):
        with pytest.raises(ValueError, match="description is required"):
            FakeManagedAgentToolset(description=description)

    def test_empty_tool_name_rejected(self):
        with pytest.raises(ValueError, match="tool_name must be a non-empty string"):
            FakeManagedAgentToolset(tool_name="")

    def test_timeout_defaults_to_none_so_subclasses_supply_it(self):
        assert FakeManagedAgentToolset()._timeout is None

    def test_subclass_may_default_its_own_timeout(self):
        class WithPlatformDefault(FakeManagedAgentToolset):
            def __init__(self, **kwargs):
                kwargs.setdefault("timeout", 600.0)
                super().__init__(**kwargs)

        assert WithPlatformDefault()._timeout == 600.0

    def test_id_is_derived_from_tool_name(self):
        assert FakeManagedAgentToolset(tool_name="ask_bookings").id == "managed-agent-ask_bookings"

    def test_not_replayable_by_default(self):
        assert FakeManagedAgentToolset().replayable is False


class TestGetTools:
    @pytest.mark.asyncio
    async def test_exposes_exactly_one_tool_under_its_name(self):
        toolset = FakeManagedAgentToolset(tool_name="ask_bookings")
        tools = await toolset.get_tools(ctx=None)
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
        toolset = FakeManagedAgentToolset()
        tool_def = (await toolset.get_tools(ctx=None))["ask_specialist"].tool_def
        assert tool_def.sequential is False

    @pytest.mark.asyncio
    async def test_args_validator_rejects_a_missing_prompt(self):
        toolset = FakeManagedAgentToolset()
        validator = (await toolset.get_tools(ctx=None))["ask_specialist"].args_validator
        with pytest.raises(ValidationError):
            validator.validate_json("{}")


class TestCallTool:
    async def _call(self, toolset, prompt="what is the number?"):
        tools = await toolset.get_tools(ctx=None)
        tool = tools[toolset._tool_name]
        return await toolset.call_tool(toolset._tool_name, {"prompt": prompt}, None, tool)

    @pytest.mark.asyncio
    async def test_passes_the_prompt_through_to_invoke(self):
        toolset = FakeManagedAgentToolset()
        await self._call(toolset, prompt="how many widgets?")
        assert toolset.prompts == ["how many widgets?"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("result", "expected"),
        [
            ("plain text", "plain text"),
            (None, "null"),
            ({"total": 42}, '{"total": 42}'),
            ([1, 2], "[1, 2]"),
        ],
        ids=["str", "none", "dict", "list"],
    )
    async def test_result_is_serialised_for_the_model(self, result, expected):
        assert await self._call(FakeManagedAgentToolset(result=result)) == expected

    @pytest.mark.asyncio
    async def test_logs_the_agent_it_consulted(self, caplog):
        await self._call(FakeManagedAgentToolset())
        assert "specialist-1" in caplog.text
        assert "fake.cloud" in caplog.text

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
        # The base class must not reclassify what invoke() raised: the three
        # buckets are handled by different layers (model, task failure, task retry).
        with pytest.raises(type(error), match=str(error)):
            await self._call(FakeManagedAgentToolset(raises=error))
