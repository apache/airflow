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

import json
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from strands import Agent as StrandsAgent
from strands.models.gemini import GeminiModel

from airflow.models.connection import Connection
from airflow.providers.common.ai.hooks.base import (
    AgentRunRequest,
    AgentRunResult,
    BaseAIHook,
    BaseToolset,
    SkillSpec,
    ToolSpec,
)
from airflow.providers.common.ai.hooks.strands_ai import StrandsGeminiHook, StrandsHook

STRANDS_AI = "airflow.providers.common.ai.hooks.strands_ai"


@pytest.fixture
def strands_agent():
    return create_autospec(StrandsAgent, instance=True)


@pytest.fixture
def gemini_model():
    return MagicMock(spec=GeminiModel)


@pytest.fixture
def gemini_hook() -> StrandsGeminiHook:
    return StrandsGeminiHook(llm_conn_id="test", model_id="gemini-2.5-flash")


class TestStrandsHookContract:
    def test_strands_hook_is_abstract(self):
        """StrandsHook cannot be instantiated — get_model is abstract."""
        with pytest.raises(TypeError):
            StrandsHook()  # type: ignore[abstract]

    def test_strands_gemini_hook_is_base_ai_hook(self):
        assert issubclass(StrandsGeminiHook, BaseAIHook)

    def test_strands_gemini_hook_is_strands_hook(self):
        assert issubclass(StrandsGeminiHook, StrandsHook)

    def test_capability_flags(self):
        assert StrandsGeminiHook.supports_toolsets is True
        assert StrandsGeminiHook.supports_durable is False
        assert StrandsGeminiHook.supports_usage_limits is False
        assert StrandsGeminiHook.supports_skills is True

    def test_conn_type(self):
        assert StrandsGeminiHook.conn_type == "strands-gemini"

    def test_hook_name(self):
        assert "Gemini" in StrandsGeminiHook.hook_name

    def test_ui_field_behaviour(self):
        behaviour = StrandsGeminiHook.get_ui_field_behaviour()
        assert "host" in behaviour["hidden_fields"]
        assert behaviour["relabeling"]["password"] == "API Key"


class TestStrandsGeminiHookInit:
    def test_default_conn_id(self):
        hook = StrandsGeminiHook()
        assert hook.llm_conn_id == "strands_default"
        assert hook.model_id is None

    def test_custom_conn_id_and_model(self):
        hook = StrandsGeminiHook(llm_conn_id="my_conn", model_id="gemini-2.5-flash")
        assert hook.llm_conn_id == "my_conn"
        assert hook.model_id == "gemini-2.5-flash"


class TestStrandsGeminiHookGetModel:
    @patch(f"{STRANDS_AI}.GeminiModel", autospec=True)
    def test_get_model_with_api_key(self, mock_gemini_model_cls):
        conn = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            password="google-api-key",
            extra=json.dumps({"model": "gemini-2.5-flash"}),
        )
        mock_gemini_model = MagicMock(spec=GeminiModel)
        mock_gemini_model_cls.return_value = mock_gemini_model

        hook = StrandsGeminiHook(llm_conn_id="test", model_id="gemini-2.5-flash")
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_model()

        assert result is mock_gemini_model
        assert hook._resolved_model_id == "gemini-2.5-flash"
        mock_gemini_model_cls.assert_called_once_with(
            model_id="gemini-2.5-flash",
            client_args={"api_key": "google-api-key"},
        )

    @patch(f"{STRANDS_AI}.GeminiModel", autospec=True)
    def test_get_model_with_env_auth(self, mock_gemini_model_cls):
        """No explicit API key — delegates to GOOGLE_API_KEY env var via Gemini client."""
        conn = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            extra=json.dumps({"model": "gemini-2.5-flash", "params": {"temperature": 0.5}}),
        )
        mock_gemini_model_cls.return_value = MagicMock(spec=GeminiModel)

        hook = StrandsGeminiHook(llm_conn_id="test")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        call_kwargs = mock_gemini_model_cls.call_args.kwargs
        assert call_kwargs == {"model_id": "gemini-2.5-flash", "params": {"temperature": 0.5}}
        assert "client_args" not in call_kwargs

    @patch(f"{STRANDS_AI}.GeminiModel", autospec=True)
    def test_get_model_with_api_key_and_params(self, mock_gemini_model_cls):
        conn = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            password="google-api-key",
            extra=json.dumps(
                {"model": "gemini-2.5-flash", "params": {"temperature": 0.7, "max_output_tokens": 2048}}
            ),
        )
        mock_gemini_model_cls.return_value = MagicMock(spec=GeminiModel)

        hook = StrandsGeminiHook(llm_conn_id="test")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        mock_gemini_model_cls.assert_called_once_with(
            model_id="gemini-2.5-flash",
            client_args={"api_key": "google-api-key"},
            params={"temperature": 0.7, "max_output_tokens": 2048},
        )

    @patch(f"{STRANDS_AI}.GeminiModel", autospec=True)
    def test_get_model_with_model_from_extra(self, mock_gemini_model_cls):
        conn = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            extra=json.dumps({"model": "gemini-2.5-flash"}),
        )
        mock_gemini_model = MagicMock(spec=GeminiModel)
        mock_gemini_model_cls.return_value = mock_gemini_model

        hook = StrandsGeminiHook(llm_conn_id="test")
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_model()

        assert result is mock_gemini_model
        mock_gemini_model_cls.assert_called_once_with(model_id="gemini-2.5-flash")

    @patch(f"{STRANDS_AI}.GeminiModel", autospec=True)
    def test_model_id_param_overrides_extra(self, mock_gemini_model_cls):
        conn = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            extra=json.dumps({"model": "gemini-2.0-flash"}),
        )
        mock_gemini_model_cls.return_value = MagicMock(spec=GeminiModel)

        hook = StrandsGeminiHook(llm_conn_id="test", model_id="gemini-2.5-flash")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_model()

        mock_gemini_model_cls.assert_called_once_with(model_id="gemini-2.5-flash")

    def test_get_model_raises_when_no_model(self):
        conn = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            extra=json.dumps({}),
        )
        hook = StrandsGeminiHook(llm_conn_id="test")

        with (
            patch.object(hook, "get_connection", return_value=conn),
            pytest.raises(ValueError, match="No model specified"),
        ):
            hook.get_model()


class TestStrandsGeminiHookTestConnection:
    @patch(f"{STRANDS_AI}.GeminiModel", autospec=True)
    def test_successful_connection(self, mock_gemini_model_cls):
        conn = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            extra=json.dumps({"model": "gemini-2.5-flash"}),
        )
        mock_gemini_model_cls.return_value = MagicMock(spec=GeminiModel)

        hook = StrandsGeminiHook(llm_conn_id="test")
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is True
        assert message == "StrandsGeminiHook resolved successfully."

    def test_failed_connection_no_model(self):
        conn = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            extra=json.dumps({}),
        )
        hook = StrandsGeminiHook(llm_conn_id="test")

        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is False
        assert "No model specified" in message


class TestStrandsHookToolSpecToNative:
    @patch(f"{STRANDS_AI}.strands_tool", autospec=True)
    def test_tool_spec_to_native_wraps_fn_as_strands_tool(self, mock_strands_tool):
        mock_strands_tool.side_effect = lambda fn: fn

        def my_fn(x: int) -> str:
            """Does x."""
            return str(x)

        spec = ToolSpec(
            name="my_fn",
            description="Does x.",
            parameters={"type": "object", "properties": {"x": {"type": "integer"}}},
            fn=my_fn,
        )

        hook = StrandsGeminiHook.__new__(StrandsGeminiHook)
        result = hook._tool_spec_to_native(spec)

        mock_strands_tool.assert_called_once()
        wrapped = mock_strands_tool.call_args[0][0]
        assert wrapped.__name__ == "my_fn"
        assert wrapped.__doc__ == "Does x."
        assert wrapped(42) == "42"
        assert result is wrapped

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("my-tool", "my_tool"),
            ("my tool", "my_tool"),
            ("ns.tool", "ns_tool"),
            ("123-tool", "_123_tool"),
        ],
    )
    @patch(f"{STRANDS_AI}.strands_tool", autospec=True)
    def test_tool_spec_to_native_sanitises_names(self, mock_strands_tool, name, expected):
        mock_strands_tool.side_effect = lambda fn: fn
        spec = ToolSpec(name=name, description="desc", parameters={}, fn=lambda: None)
        hook = StrandsGeminiHook.__new__(StrandsGeminiHook)

        hook._tool_spec_to_native(spec)

        wrapped = mock_strands_tool.call_args[0][0]
        assert wrapped.__name__ == expected


class TestStrandsHookCreateAndRunAgent:
    @patch(f"{STRANDS_AI}.Agent", autospec=True)
    def test_create_agent_builds_strands_agent(self, mock_agent_cls, gemini_hook, gemini_model):
        with patch.object(gemini_hook, "get_model", return_value=gemini_model):
            request = AgentRunRequest(prompt="hello")
            agent = gemini_hook.create_agent(request)

        mock_agent_cls.assert_called_once()
        _, kwargs = mock_agent_cls.call_args
        assert kwargs["model"] is gemini_model
        assert kwargs["tools"] == []
        assert "system_prompt" not in kwargs
        assert agent is mock_agent_cls.return_value

    @patch(f"{STRANDS_AI}.Agent", autospec=True)
    def test_create_agent_passes_instructions_as_system_prompt(
        self, mock_agent_cls, gemini_hook, gemini_model
    ):
        with patch.object(gemini_hook, "get_model", return_value=gemini_model):
            request = AgentRunRequest(prompt="q", instructions="Be concise.")
            gemini_hook.create_agent(request)

        _, kwargs = mock_agent_cls.call_args
        assert kwargs["system_prompt"] == "Be concise."

    @patch(f"{STRANDS_AI}.Agent", autospec=True)
    def test_create_agent_forwards_agent_params(self, mock_agent_cls, gemini_hook, gemini_model):
        callback_handler = MagicMock()

        with patch.object(gemini_hook, "get_model", return_value=gemini_model):
            request = AgentRunRequest(
                prompt="q",
                agent_params={"callback_handler": callback_handler, "record_direct_tool_call": True},
            )
            gemini_hook.create_agent(request)

        _, kwargs = mock_agent_cls.call_args
        assert kwargs["callback_handler"] is callback_handler
        assert kwargs["record_direct_tool_call"] is True

    @patch(f"{STRANDS_AI}.strands_tool", autospec=True)
    @patch(f"{STRANDS_AI}.Agent", autospec=True)
    def test_create_agent_resolves_toolsets(
        self, mock_agent_cls, mock_strands_tool, gemini_hook, gemini_model
    ):
        mock_strands_tool.side_effect = lambda fn: fn
        spec = ToolSpec(name="t", description="d", parameters={}, fn=lambda: None)

        class MyToolset(BaseToolset):
            def as_tools(self):
                return [spec]

        with patch.object(gemini_hook, "get_model", return_value=gemini_model):
            request = AgentRunRequest(prompt="q", toolsets=[MyToolset()], enable_tool_logging=False)
            gemini_hook.create_agent(request)

        mock_strands_tool.assert_called_once()
        _, kwargs = mock_agent_cls.call_args
        native_tool = mock_strands_tool.call_args[0][0]
        assert kwargs["tools"] == [native_tool]
        assert native_tool.__name__ == "t"

    def test_create_agent_rejects_usage_limits(self, gemini_hook):
        with pytest.raises(ValueError, match="usage_limits are not supported"):
            gemini_hook.create_agent(AgentRunRequest(prompt="hi", usage_limits=MagicMock()))

    def test_create_agent_rejects_durable_context(self, gemini_hook):
        with pytest.raises(ValueError, match="durable execution requires"):
            gemini_hook.create_agent(AgentRunRequest(prompt="hi", durable_context=MagicMock()))

    def test_run_agent_returns_agent_run_result(self, gemini_hook, strands_agent):
        mock_response = MagicMock()
        mock_response.configure_mock(**{"__str__.return_value": "the answer"})
        strands_agent.return_value = mock_response

        gemini_hook._resolved_model_id = "gemini-2.5-flash"

        request = AgentRunRequest(prompt="hello")
        result = gemini_hook.run_agent(strands_agent, request)

        assert isinstance(result, AgentRunResult)
        assert result.output == "the answer"
        assert result.model_name == "gemini-2.5-flash"
        assert result.usage is None
        assert result.tool_names is None
        assert result.message_history is None
        strands_agent.assert_called_once_with("hello")

    def test_run_agent_falls_back_to_hook_model_id(self, gemini_hook, strands_agent):
        strands_agent.return_value = MagicMock(__str__=lambda self: "ok")

        request = AgentRunRequest(prompt="hello")
        result = gemini_hook.run_agent(strands_agent, request)

        assert result.model_name == "gemini-2.5-flash"
        strands_agent.assert_called_once_with("hello")


class TestStrandsHookSkills:
    def test_skill_spec_path_passthrough(self):
        hook = StrandsGeminiHook.__new__(StrandsGeminiHook)
        assert hook._skill_spec_to_native("./skills/pdf-processing") == "./skills/pdf-processing"
        assert hook._skill_spec_to_native(SkillSpec(path="./skills/")) == "./skills/"

    @patch(f"{STRANDS_AI}.Skill", autospec=True)
    def test_skill_spec_inline_becomes_strands_skill(self, mock_skill_cls):
        hook = StrandsGeminiHook.__new__(StrandsGeminiHook)
        spec = SkillSpec(name="greet", description="Greet users", instructions="Say hello.")
        mock_skill_cls.return_value = "native-skill"

        result = hook._skill_spec_to_native(spec)

        assert result == "native-skill"
        mock_skill_cls.assert_called_once_with(
            name="greet",
            description="Greet users",
            instructions="Say hello.",
        )

    def test_resolve_skill_sources_empty_when_not_set(self, gemini_hook):
        request = AgentRunRequest(prompt="q")
        assert gemini_hook._resolve_skill_sources(request) == []

    def test_build_skills_plugin_returns_none_when_no_skills(self, gemini_hook):
        request = AgentRunRequest(prompt="q")
        assert gemini_hook._build_skills_plugin(request) is None

    @patch(f"{STRANDS_AI}.AgentSkills", autospec=True)
    def test_build_skills_plugin_forwards_skills_params(self, mock_plugin_cls, gemini_hook):
        mock_plugin_cls.return_value = "skills-plugin"

        request = AgentRunRequest(
            prompt="q",
            skills=["./skills/"],
            skills_params={"strict": True, "max_resource_files": 5},
        )
        result = gemini_hook._build_skills_plugin(request)

        assert result == "skills-plugin"
        mock_plugin_cls.assert_called_once_with(
            skills="./skills/",
            strict=True,
            max_resource_files=5,
        )

    @patch(f"{STRANDS_AI}.AgentSkills", autospec=True)
    @patch(f"{STRANDS_AI}.Agent", autospec=True)
    def test_create_agent_attaches_agentskills_plugin(
        self, mock_agent_cls, mock_plugin_cls, gemini_hook, gemini_model
    ):
        mock_plugin_cls.return_value = "skills-plugin"

        with patch.object(gemini_hook, "get_model", return_value=gemini_model):
            request = AgentRunRequest(prompt="q", skills=["./skills/"])
            gemini_hook.create_agent(request)

        mock_plugin_cls.assert_called_once_with(skills="./skills/")
        _, kwargs = mock_agent_cls.call_args
        assert kwargs["plugins"] == ["skills-plugin"]

    @patch(f"{STRANDS_AI}.AgentSkills", autospec=True)
    @patch(f"{STRANDS_AI}.Agent", autospec=True)
    def test_create_agent_attaches_agentskills_plugin_multiple_sources(
        self, mock_agent_cls, mock_plugin_cls, gemini_hook, gemini_model
    ):
        """When multiple skill sources are given they are passed as a list."""
        mock_plugin_cls.return_value = "skills-plugin"

        with patch.object(gemini_hook, "get_model", return_value=gemini_model):
            request = AgentRunRequest(prompt="q", skills=["./skills/a/", "./skills/b/"])
            gemini_hook.create_agent(request)

        mock_plugin_cls.assert_called_once_with(skills=["./skills/a/", "./skills/b/"])
        _, kwargs = mock_agent_cls.call_args
        assert kwargs["plugins"] == ["skills-plugin"]

    @patch(f"{STRANDS_AI}.Skill", autospec=True)
    @patch(f"{STRANDS_AI}.AgentSkills", autospec=True)
    @patch(f"{STRANDS_AI}.Agent", autospec=True)
    def test_create_agent_resolves_inline_skill_spec(
        self, mock_agent_cls, mock_plugin_cls, mock_skill_cls, gemini_hook, gemini_model
    ):
        mock_skill_cls.return_value = "inline-native-skill"
        mock_plugin_cls.return_value = "skills-plugin"

        with patch.object(gemini_hook, "get_model", return_value=gemini_model):
            request = AgentRunRequest(
                prompt="q",
                skills=[SkillSpec(name="greet", description="Greet users", instructions="Say hello.")],
            )
            gemini_hook.create_agent(request)

        mock_skill_cls.assert_called_once_with(
            name="greet",
            description="Greet users",
            instructions="Say hello.",
        )
        mock_plugin_cls.assert_called_once_with(skills="inline-native-skill")
        _, kwargs = mock_agent_cls.call_args
        assert kwargs["plugins"] == ["skills-plugin"]

    @patch(f"{STRANDS_AI}.AgentSkills", autospec=True)
    @patch(f"{STRANDS_AI}.Agent", autospec=True)
    def test_create_agent_merges_existing_plugins(
        self, mock_agent_cls, mock_plugin_cls, gemini_hook, gemini_model
    ):
        mock_plugin_cls.return_value = "skills-plugin"

        with patch.object(gemini_hook, "get_model", return_value=gemini_model):
            request = AgentRunRequest(
                prompt="q",
                skills=["./skills/"],
                agent_params={"plugins": ["existing-plugin"]},
            )
            gemini_hook.create_agent(request)

        _, kwargs = mock_agent_cls.call_args
        assert kwargs["plugins"] == ["existing-plugin", "skills-plugin"]
