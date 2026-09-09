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
from pydantic_ai.exceptions import ModelRetry
from pydantic_core import ValidationError

from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
from airflow.providers.common.ai.toolsets.managed_agent import (
    BaseManagedAgentToolset,
    FailoverManagedAgentToolset,
)


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
        [None, "", "   ", "\n"],
        ids=["none", "empty", "whitespace", "newline"],
    )
    def test_absent_description_falls_back_to_the_tool_name(self, description):
        # Matches HookToolset, which derives a description from the method name
        # when there is no docstring. The name is the required identifier; the
        # description is guidance the author may omit.
        toolset = FakeManagedAgentToolset(tool_name="ask_bookings_analyst", description=description)
        assert toolset._description == "Ask bookings analyst"

    def test_supplied_description_is_kept_verbatim(self):
        toolset = FakeManagedAgentToolset(description="Knows bookings. Cannot see support tickets.")
        assert toolset._description == "Knows bookings. Cannot see support tickets."

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

    @pytest.mark.asyncio
    async def test_max_retries_defaults_to_one_and_is_configurable(self):
        # One rephrase by default; 0 disables the ModelRetry path entirely, so it
        # is a deliberate choice rather than the default.
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
            def agent_ref(self) -> dict[str, str]:
                return {"platform": "fake.cloud", "name": "neither"}

        with pytest.raises(TypeError, match="must implement invoke_sync"):
            Neither(tool_name="ask_nothing")


class TestSyncInvocation:
    """A blocking vendor SDK must not run on the agent's event loop."""

    class _Blocking(BaseManagedAgentToolset):
        @property
        def agent_ref(self) -> dict[str, str]:
            return {"platform": "fake.cloud", "name": "blocking"}

        def invoke_sync(self, prompt: str) -> Any:
            time.sleep(0.2)
            return f"slept for {prompt}"

    @pytest.mark.asyncio
    async def test_invoke_sync_is_offloaded_so_the_loop_keeps_running(self):
        # A synchronous client would freeze every other tool call for the
        # duration of the remote call if it ran on the loop, so the base class
        # runs invoke_sync in a worker thread.
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
        toolset = self._Blocking(tool_name="ask_slow")
        tools = await toolset.get_tools(ctx=None)
        result = await toolset.call_tool("ask_slow", {"prompt": "q"}, None, tools["ask_slow"])
        assert result == "slept for q"

    @pytest.mark.asyncio
    async def test_an_async_override_is_used_as_is(self):
        # FakeManagedAgentToolset overrides invoke() directly, the path a vendor
        # with a natively async client takes.
        assert await FakeManagedAgentToolset(result="async answer").invoke("q") == "async answer"


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


class TestFailoverManagedAgentToolset:
    @staticmethod
    def _group(*members, **kwargs):
        kwargs.setdefault("tool_name", "ask_resilient")
        kwargs.setdefault("description", "Answers questions, on whichever cloud is up.")
        return FailoverManagedAgentToolset(members=list(members), **kwargs)

    async def _call(self, group, prompt="what is the number?"):
        tools = await group.get_tools(ctx=None)
        return await group.call_tool(group._tool_name, {"prompt": prompt}, None, tools[group._tool_name])

    @pytest.mark.parametrize("count", [0, 1], ids=["none", "one"])
    def test_needs_at_least_two_members(self, count):
        members = [FakeManagedAgentToolset() for _ in range(count)]
        with pytest.raises(ValueError, match="at least two members"):
            self._group(*members)

    @pytest.mark.asyncio
    async def test_members_are_copied_so_the_caller_cannot_empty_the_group(self):
        # invoke() relies on the group being non-empty; aliasing the caller's list
        # would let it be emptied after construction and make invoke return None,
        # which reaches the model as the string "null".
        # Constructed directly, not via _group(), which copies the list itself.
        members = [FakeManagedAgentToolset(result="from primary"), FakeManagedAgentToolset()]
        group = FailoverManagedAgentToolset(
            members=members,
            tool_name="ask_resilient",
            description="Answers questions, on whichever cloud is up.",
        )
        members.clear()
        assert await self._call(group) == "from primary"

    @pytest.mark.asyncio
    async def test_primary_answer_wins_and_standby_is_untouched(self):
        primary = FakeManagedAgentToolset(result="from primary")
        standby = FakeManagedAgentToolset(result="from standby")
        assert await self._call(self._group(primary, standby)) == "from primary"
        assert standby.prompts == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [
            ManagedAgentInvocationError("region is down"),
            RuntimeError("503 from upstream"),
            TimeoutError("read timeout"),
        ],
        ids=["terminal", "transient", "timeout"],
    )
    async def test_fails_over_when_primary_fails(self, error):
        primary = FakeManagedAgentToolset(raises=error)
        standby = FakeManagedAgentToolset(result="from standby")
        assert await self._call(self._group(primary, standby)) == "from standby"
        assert standby.prompts == ["what is the number?"]

    @pytest.mark.asyncio
    async def test_model_retry_does_not_burn_the_standby(self):
        # A prompt the primary could not parse will not parse on the standby
        # either, so the model must get the chance to rephrase instead.
        primary = FakeManagedAgentToolset(raises=ModelRetry("rephrase that"))
        standby = FakeManagedAgentToolset(result="from standby")
        with pytest.raises(ModelRetry, match="rephrase that"):
            await self._call(self._group(primary, standby))
        assert standby.prompts == []

    @pytest.mark.asyncio
    async def test_last_members_error_propagates_when_all_fail(self):
        primary = FakeManagedAgentToolset(raises=RuntimeError("primary down"))
        standby = FakeManagedAgentToolset(raises=ManagedAgentInvocationError("standby down"))
        with pytest.raises(ManagedAgentInvocationError, match="standby down"):
            await self._call(self._group(primary, standby))

    @pytest.mark.asyncio
    async def test_narrowed_failover_on_lets_other_errors_through(self):
        primary = FakeManagedAgentToolset(raises=RuntimeError("a bug, not an outage"))
        standby = FakeManagedAgentToolset(result="from standby")
        group = self._group(primary, standby, failover_on=(ManagedAgentInvocationError,))
        with pytest.raises(RuntimeError, match="a bug, not an outage"):
            await self._call(group)
        assert standby.prompts == []

    @pytest.mark.asyncio
    async def test_warns_on_failover_and_names_the_standby(self, caplog):
        primary = FakeManagedAgentToolset(raises=ManagedAgentInvocationError("down"))
        standby = FakeManagedAgentToolset(result="ok")
        await self._call(self._group(primary, standby))
        assert "failing over" in caplog.text
        assert "served by standby" in caplog.text

    @pytest.mark.asyncio
    async def test_groups_nest(self):
        inner = self._group(
            FakeManagedAgentToolset(raises=ManagedAgentInvocationError("a down")),
            FakeManagedAgentToolset(raises=ManagedAgentInvocationError("b down")),
        )
        outer = self._group(inner, FakeManagedAgentToolset(result="from outer standby"))
        assert await self._call(outer) == "from outer standby"

    @pytest.mark.parametrize(
        ("primary_replayable", "standby_replayable", "expected"),
        [(True, True, True), (True, False, False), (False, False, False)],
        ids=["both", "one", "neither"],
    )
    def test_replayable_only_when_every_member_is(self, primary_replayable, standby_replayable, expected):
        # The durable cache cannot know which member produced the answer it
        # holds, so a single non-replayable member makes the group unsafe.
        primary, standby = FakeManagedAgentToolset(), FakeManagedAgentToolset()
        primary.replayable, standby.replayable = primary_replayable, standby_replayable
        assert self._group(primary, standby).replayable is expected

    def test_agent_ref_describes_the_group(self):
        ref = self._group(FakeManagedAgentToolset(), FakeManagedAgentToolset()).agent_ref
        assert ref["platform"] == "failover"
        assert ref["name"] == "specialist-1 -> specialist-1"


class TestFailoverMetrics:
    """A failover is a success-shaped event, so the counters are the only signal
    distinguishing a healthy primary from one that has been down for a week."""

    @staticmethod
    def _group(*members):
        return FailoverManagedAgentToolset(
            members=list(members),
            tool_name="ask_resilient",
            description="Answers questions, on whichever cloud is up.",
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.common.ai.toolsets.managed_agent.Stats")
    async def test_primary_success_counts_as_primary(self, mock_stats):
        await self._group(FakeManagedAgentToolset(), FakeManagedAgentToolset()).invoke("q")
        mock_stats.incr.assert_called_once_with(
            "managed_agent.served",
            tags={
                "tool": "ask_resilient",
                "platform": "fake.cloud",
                "role": "primary",
                "position": "0",
            },
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.common.ai.toolsets.managed_agent.Stats")
    async def test_failover_emits_both_counters(self, mock_stats):
        primary = FakeManagedAgentToolset(raises=ManagedAgentInvocationError("down"))
        await self._group(primary, FakeManagedAgentToolset()).invoke("q")

        assert mock_stats.incr.call_args_list == [
            mock.call(
                "managed_agent.failover",
                tags={
                    "tool": "ask_resilient",
                    "from_platform": "fake.cloud",
                    "to_platform": "fake.cloud",
                },
            ),
            mock.call(
                "managed_agent.served",
                tags={
                    "tool": "ask_resilient",
                    "platform": "fake.cloud",
                    "role": "standby",
                    # The member that answered, not just that a standby did.
                    "position": "1",
                },
            ),
        ]

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.common.ai.toolsets.managed_agent.Stats")
    async def test_total_outage_records_the_failover_but_no_answer(self, mock_stats):
        group = self._group(
            FakeManagedAgentToolset(raises=ManagedAgentInvocationError("a down")),
            FakeManagedAgentToolset(raises=ManagedAgentInvocationError("b down")),
        )
        with pytest.raises(ManagedAgentInvocationError):
            await group.invoke("q")

        emitted = [c.args[0] for c in mock_stats.incr.call_args_list]
        assert emitted == ["managed_agent.failover"], "no answer means no served counter"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.common.ai.toolsets.managed_agent.Stats")
    async def test_model_retry_is_not_a_failover(self, mock_stats):
        group = self._group(FakeManagedAgentToolset(raises=ModelRetry("rephrase")), FakeManagedAgentToolset())
        with pytest.raises(ModelRetry):
            await group.invoke("q")
        mock_stats.incr.assert_not_called()
