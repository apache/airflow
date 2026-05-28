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

from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from airflow.providers.common.ai.hooks.base_ai import (
    AgentRunRequest,
    AgentRunResult,
    AgentUsage,
    BaseAIHook,
    BaseToolset,
    DurableContext,
    DurableStats,
    SkillSpec,
    ToolSpec,
    tool_identifier,
)
from airflow.providers.common.compat.sdk import BaseHook


class TestBaseAIHookGetAgentHook:
    @patch("airflow.providers.common.ai.hooks.base_ai.BaseHook.get_hook", autospec=True)
    def test_returns_hook_when_instance_is_base_ai_hook(self, mock_get_hook):
        mock_hook = MagicMock(spec=BaseAIHook)
        mock_get_hook.return_value = mock_hook

        result = BaseAIHook.get_agent_hook("my_conn")

        assert result is mock_hook
        mock_get_hook.assert_called_once_with("my_conn", hook_params=None)

    @patch("airflow.providers.common.ai.hooks.base_ai.BaseHook.get_hook", autospec=True)
    def test_raises_when_hook_is_not_base_ai_hook(self, mock_get_hook):
        mock_get_hook.return_value = MagicMock(spec=BaseHook)

        with pytest.raises(TypeError, match="not a BaseAIHook"):
            BaseAIHook.get_agent_hook("my_conn")


class TestBaseAIHookInit:
    def test_stores_model_id_and_conn_id(self):
        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec.fn

        hook = ConcreteHook(llm_conn_id="my_conn", model_id="openai:gpt-5")
        assert hook.llm_conn_id == "my_conn"
        assert hook.model_id == "openai:gpt-5"


class TestBaseAIHookAgentDurable:
    def test_bind_pop_round_trip(self):
        agent = Agent(TestModel())
        storage = MagicMock()
        counter = MagicMock()

        BaseAIHook._bind_agent_durable(agent, storage, counter)
        assert agent._airflow_durable_state == (storage, counter)
        assert BaseAIHook._pop_agent_durable(agent) == (storage, counter)
        assert BaseAIHook._pop_agent_durable(agent) is None
        assert not hasattr(agent, "_airflow_durable_state")


class TestValidateRunRequest:
    def test_rejects_toolsets_when_unsupported(self):
        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"
            supports_toolsets = False
            supports_usage_limits = True
            supports_durable = True

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec.fn

        hook = ConcreteHook(llm_conn_id="test_conn")
        request = AgentRunRequest(prompt="hi", toolsets=[MagicMock()])
        with pytest.raises(ValueError, match="toolsets are not supported"):
            hook.validate_run_request(request)

    def test_rejects_usage_limits_when_unsupported(self):
        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"
            supports_toolsets = True
            supports_usage_limits = False
            supports_durable = True

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec.fn

        hook = ConcreteHook(llm_conn_id="test_conn")
        request = AgentRunRequest(prompt="hi", usage_limits=MagicMock())
        with pytest.raises(ValueError, match="usage_limits are not supported"):
            hook.validate_run_request(request)


class TestAgentRunResult:
    def test_dataclass_fields(self):
        usage = AgentUsage(requests=1, tool_calls=2, total_tokens=10)
        result = AgentRunResult(
            output="answer",
            message_history=["msg"],
            model_name="test-model",
            usage=usage,
            tool_names=["query"],
        )
        assert result.output == "answer"
        assert result.message_history == ["msg"]
        assert result.model_name == "test-model"
        assert result.usage == usage
        assert result.tool_names == ["query"]
        assert result.durable_stats is None

    def test_durable_stats_field(self):
        stats = DurableStats(replayed_model=2, cached_model=3)
        result = AgentRunResult(output="x", durable_stats=stats)
        assert result.durable_stats is stats


class TestAgentRunRequest:
    def test_defaults(self):
        req = AgentRunRequest(prompt="hello")
        assert req.prompt == "hello"
        assert req.output_type is str
        assert req.instructions == ""
        assert req.toolsets is None
        assert req.skills is None
        assert req.skills_params == {}
        assert req.usage_limits is None
        assert req.message_history is None
        assert req.enable_tool_logging is True
        assert req.durable_context is None
        assert req.agent_params == {}

    def test_with_all_fields(self):
        ctx = DurableContext(dag_id="d", task_id="t", run_id="r", map_index=2)
        req = AgentRunRequest(
            prompt="test",
            output_type=int,
            instructions="sys",
            toolsets=["ts"],
            skills=["/skills/"],
            skills_params={"strict": True},
            usage_limits="limits",
            message_history=["h"],
            enable_tool_logging=False,
            durable_context=ctx,
            agent_params={"retries": 3},
        )
        assert req.output_type is int
        assert req.instructions == "sys"
        assert req.skills == ["/skills/"]
        assert req.skills_params == {"strict": True}
        assert req.durable_context is ctx
        assert req.agent_params == {"retries": 3}


class TestBaseAIHookResolveTools:
    def test_resolve_tools_calls_spec_to_native(self):
        """_resolve_tools converts each ToolSpec via _tool_spec_to_native."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return {"name": spec.name, "fn": spec.fn}

        hook = ConcreteHook.__new__(ConcreteHook)

        def my_tool(x: int) -> str:
            return str(x)

        class MyToolset(BaseToolset):
            def as_tools(self):
                return [ToolSpec(name="my_tool", description="desc", parameters={}, fn=my_tool)]

        result = hook._resolve_tools([MyToolset()], enable_logging=False, storage=None, counter=None)

        assert len(result) == 1
        assert result[0]["name"] == "my_tool"

    def test_resolve_tools_wraps_with_logging(self):
        """When enable_logging=True, callable is wrapped."""
        mock_log = MagicMock()

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            @property
            def log(self):
                return mock_log

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec.fn

        hook = ConcreteHook.__new__(ConcreteHook)

        calls = []

        def original():
            calls.append("original")
            return "result"

        class SimpleToolset(BaseToolset):
            def as_tools(self):
                return [ToolSpec(name="original", description="", parameters={}, fn=original)]

        [wrapped_fn] = hook._resolve_tools([SimpleToolset()], enable_logging=True, storage=None, counter=None)
        wrapped_fn()

        assert calls == ["original"]
        mock_log.info.assert_called()

    def test_resolve_tools_wraps_plain_callable(self):
        """A bare Python function is auto-wrapped using __name__ and __doc__."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return {"name": spec.name, "description": spec.description}

        hook = ConcreteHook.__new__(ConcreteHook)

        def roll_dice() -> str:
            """Roll a six-sided die and return the result."""
            return "4"

        result = hook._resolve_tools([roll_dice], enable_logging=False, storage=None, counter=None)

        assert len(result) == 1
        assert result[0]["name"] == "roll_dice"
        assert result[0]["description"] == "Roll a six-sided die and return the result."

    def test_resolve_tools_passes_non_function_non_toolset_through(self):
        """Items that are not BaseToolset and not plain functions are passed through unchanged."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec.fn

        hook = ConcreteHook.__new__(ConcreteHook)

        native_tool_obj = object()  # not a function, not a BaseToolset
        result = hook._resolve_tools([native_tool_obj], enable_logging=True, storage=None, counter=None)

        assert result == [native_tool_obj]

    def test_resolve_tools_mixes_base_toolset_and_native(self):
        """BaseToolset items are converted; non-function native items are passed through in order."""

        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return f"converted:{spec.name}"

        hook = ConcreteHook.__new__(ConcreteHook)

        native_tool = object()  # not a function, passes through unchanged

        class MyToolset(BaseToolset):
            def as_tools(self):
                return [ToolSpec(name="greet", description="", parameters={}, fn=lambda: "hi")]

        result = hook._resolve_tools(
            [MyToolset(), native_tool], enable_logging=False, storage=None, counter=None
        )

        assert result == ["converted:greet", native_tool]


class TestToolIdentifier:
    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("my-tool", "my_tool"),
            ("my tool", "my_tool"),
            ("ns.tool", "ns_tool"),
            ("123-tool", "_123_tool"),
        ],
    )
    def test_normalizes_to_valid_identifier(self, name, expected):
        assert tool_identifier(name) == expected

    @pytest.mark.parametrize("name", ["", "   ", "---"])
    def test_rejects_invalid_names(self, name):
        with pytest.raises(ValueError, match="ToolSpec.name"):
            tool_identifier(name)


class TestSkillSpecValidation:
    def test_path_only(self):
        spec = SkillSpec(path="/skills/pdf")
        assert spec.path == "/skills/pdf"
        assert spec.name is None

    def test_inline_only(self):
        spec = SkillSpec(name="greet", description="Greet", instructions="Say hi.")
        assert spec.path is None
        assert spec.name == "greet"

    def test_rejects_path_with_inline_fields(self):
        with pytest.raises(ValueError, match="cannot set 'path' together"):
            SkillSpec(
                path="/skills/",
                name="greet",
                description="Greet",
                instructions="Say hi.",
            )

    def test_rejects_partial_inline(self):
        with pytest.raises(ValueError, match="inline skills require"):
            SkillSpec(name="only-name")

    def test_rejects_empty_spec(self):
        with pytest.raises(ValueError, match="must set 'path' or all of"):
            SkillSpec()


class TestBaseAIHookSkills:
    def test_default_capability_flags(self):
        assert BaseAIHook.supports_toolsets is False
        assert BaseAIHook.supports_skills is False
        assert BaseAIHook.supports_durable is False
        assert BaseAIHook.supports_usage_limits is False

    def test_skill_spec_to_native_path_string(self):
        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec

        hook = ConcreteHook.__new__(ConcreteHook)
        assert hook._skill_spec_to_native("/skills/pdf") == "/skills/pdf"
        assert hook._skill_spec_to_native(SkillSpec(path="/skills/")) == "/skills/"

    def test_skill_spec_to_native_inline_raises_by_default(self):
        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec

        hook = ConcreteHook.__new__(ConcreteHook)
        with pytest.raises(ValueError, match="inline skills require"):
            SkillSpec(name="only-name")
        with pytest.raises(ValueError, match="SkillSpec must set 'path'"):
            hook._skill_spec_to_native(
                SkillSpec(name="greet", description="Greet users", instructions="Say hello.")
            )

    def test_resolve_skill_sources_from_request(self):
        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec

        hook = ConcreteHook.__new__(ConcreteHook)
        request = AgentRunRequest(prompt="q", skills=["/a/", SkillSpec(path="/b/")])
        assert hook._resolve_skill_sources(request) == ["/a/", "/b/"]

    def test_resolve_skill_sources_empty_when_not_set(self):
        class ConcreteHook(BaseAIHook):
            conn_type = "test"
            hook_name = "Test"

            def get_model(self):
                return None

            def create_agent(self, request):
                return None

            def run_agent(self, agent, request):
                return AgentRunResult(output="")

            def _tool_spec_to_native(self, spec):
                return spec

        hook = ConcreteHook.__new__(ConcreteHook)
        request = AgentRunRequest(prompt="q")
        assert hook._resolve_skill_sources(request) == []


class TestBaseAIHookLoggedCallable:
    def test_logged_callable_logs_and_returns(self):
        logger = MagicMock()
        calls = []

        def fn(x):
            calls.append(x)
            return x * 2

        wrapped = BaseAIHook._logged_callable(fn, logger)
        result = wrapped(x=5)

        assert result == 10
        assert calls == [5]
        logger.info.assert_called()

    def test_logged_callable_logs_exception(self):
        logger = MagicMock()

        def failing():
            raise RuntimeError("boom")

        wrapped = BaseAIHook._logged_callable(failing, logger)
        with pytest.raises(RuntimeError, match="boom"):
            wrapped()

        logger.exception.assert_called_once()


class TestBaseAIHookCachedCallable:
    def test_cached_callable_saves_and_returns(self):
        storage = MagicMock()
        counter = MagicMock()
        counter.next_step.return_value = 1
        storage.load_tool_result.return_value = (False, None)

        calls = []

        def fn():
            calls.append(1)
            return "computed"

        wrapped = BaseAIHook._cached_callable(fn, storage, counter)
        result = wrapped()

        assert result == "computed"
        assert calls == [1]
        storage.save_tool_result.assert_called_once_with("tool_step_1", "computed")

    def test_cached_callable_replays_on_hit(self):
        storage = MagicMock()
        counter = MagicMock()
        counter.replayed_tool = 0
        counter.next_step.return_value = 1
        storage.load_tool_result.return_value = (True, "cached_value")

        calls = []

        def fn():
            calls.append(1)
            return "computed"

        wrapped = BaseAIHook._cached_callable(fn, storage, counter)
        result = wrapped()

        assert result == "cached_value"
        assert calls == []
        assert counter.replayed_tool == 1
        storage.save_tool_result.assert_not_called()
