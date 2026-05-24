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
from unittest.mock import MagicMock, patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.ai.hooks.base_ai import (
    AgentRunRequest,
    AgentRunResult,
    BaseAIHook,
    SkillSpec,
    ToolSpec,
)
from airflow.providers.common.ai.hooks.strands_ai import StrandsGeminiHook, StrandsHook


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
    @patch("strands.models.gemini.GeminiModel")
    @patch("airflow.providers.common.ai.hooks.strands_ai.StrandsGeminiHook.get_connection")
    def test_get_model_with_api_key(self, mock_get_connection, mock_gemini_model_cls):
        mock_get_connection.return_value = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            password="google-api-key",
            extra=json.dumps({"model": "gemini-2.5-flash"}),
        )

        mock_gemini_model = MagicMock()
        mock_gemini_model_cls.return_value = mock_gemini_model

        hook = StrandsGeminiHook(llm_conn_id="test", model_id="gemini-2.5-flash")
        result = hook.get_model()

        assert result is mock_gemini_model
        mock_gemini_model_cls.assert_called_once_with(
            model_id="gemini-2.5-flash",
            client_args={"api_key": "google-api-key"},
        )

    @patch("strands.models.gemini.GeminiModel")
    @patch("airflow.providers.common.ai.hooks.strands_ai.StrandsGeminiHook.get_connection")
    def test_get_model_with_env_auth(self, mock_get_connection, mock_gemini_model_cls):
        """No explicit API key — delegates to GOOGLE_API_KEY env var via Gemini client."""
        mock_get_connection.return_value = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            extra=json.dumps({"model": "gemini-2.5-flash", "params": {"temperature": 0.5}}),
        )
        mock_gemini_model = MagicMock()
        mock_gemini_model_cls.return_value = mock_gemini_model

        hook = StrandsGeminiHook(llm_conn_id="test")
        hook.get_model()

        mock_gemini_model_cls.assert_called_once_with(
            model_id="gemini-2.5-flash",
            params={"temperature": 0.5},
        )

    @patch("airflow.providers.common.ai.hooks.strands_ai.StrandsGeminiHook.get_connection")
    def test_get_model_raises_when_no_model(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            extra=json.dumps({}),
        )
        hook = StrandsGeminiHook(llm_conn_id="test")

        with (
            patch.dict(
                "sys.modules",
                {"strands": MagicMock(), "strands.models": MagicMock(), "strands.models.gemini": MagicMock()},
            ),
            pytest.raises(ValueError, match="No model specified"),
        ):
            hook.get_model()

    def test_get_model_raises_on_missing_strands(self):
        from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

        hook = StrandsGeminiHook(llm_conn_id="test", model_id="gemini-2.5-flash")
        with patch.dict(
            "sys.modules", {"strands": None, "strands.models": None, "strands.models.gemini": None}
        ):
            with pytest.raises(AirflowOptionalProviderFeatureException, match="strands-agents"):
                hook.get_model()


class TestStrandsHookToolSpecToNative:
    def test_tool_spec_to_native_wraps_fn_as_strands_tool(self):
        mock_strands_tool = MagicMock(side_effect=lambda fn: fn)

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

        with patch.dict("sys.modules", {"strands": MagicMock(tool=mock_strands_tool)}):
            import sys

            sys.modules["strands"].tool = mock_strands_tool
            hook._tool_spec_to_native(spec)

        mock_strands_tool.assert_called_once()
        wrapped = mock_strands_tool.call_args[0][0]
        assert wrapped.__name__ == "my_fn"
        assert wrapped.__doc__ == "Does x."

    def test_tool_spec_to_native_sanitises_name(self):
        """Hyphens in tool name are replaced with underscores for Python identifiers."""
        mock_strands_tool = MagicMock(side_effect=lambda fn: fn)
        spec = ToolSpec(
            name="my-tool",
            description="desc",
            parameters={},
            fn=lambda: None,
        )

        hook = StrandsGeminiHook.__new__(StrandsGeminiHook)

        with patch.dict("sys.modules", {"strands": MagicMock(tool=mock_strands_tool)}):
            import sys

            sys.modules["strands"].tool = mock_strands_tool
            hook._tool_spec_to_native(spec)

        wrapped = mock_strands_tool.call_args[0][0]
        assert wrapped.__name__ == "my_tool"


class TestStrandsHookCreateAndRunAgent:
    def test_create_agent_builds_strands_agent(self):
        mock_agent_cls = MagicMock()
        mock_model = MagicMock()

        hook = StrandsGeminiHook(llm_conn_id="test", model_id="gemini-2.5-flash")

        with (
            patch.object(hook, "get_model", return_value=mock_model),
            patch.dict("sys.modules", {"strands": MagicMock(Agent=mock_agent_cls)}),
        ):
            import sys

            sys.modules["strands"].Agent = mock_agent_cls

            request = AgentRunRequest(prompt="hello")
            agent = hook.create_agent(request)

        mock_agent_cls.assert_called_once()
        assert agent is mock_agent_cls.return_value

    def test_run_agent_returns_agent_run_result(self):
        mock_response = MagicMock()
        mock_response.__str__ = MagicMock(return_value="the answer")
        mock_agent_instance = MagicMock(return_value=mock_response)

        hook = StrandsGeminiHook(llm_conn_id="test", model_id="gemini-2.5-flash")
        hook._resolved_model_id = "gemini-2.5-flash"

        request = AgentRunRequest(prompt="hello")
        result = hook.run_agent(mock_agent_instance, request)

        assert isinstance(result, AgentRunResult)
        assert result.output == "the answer"
        assert result.model_name == "gemini-2.5-flash"
        mock_agent_instance.assert_called_once_with("hello")

    def test_create_agent_passes_instructions_as_system_prompt(self):
        mock_agent_cls = MagicMock()
        mock_agent_cls.return_value = MagicMock(return_value=MagicMock(__str__=lambda s: "ok"))
        mock_model = MagicMock()

        hook = StrandsGeminiHook(llm_conn_id="test", model_id="gemini-2.5-flash")

        with (
            patch.object(hook, "get_model", return_value=mock_model),
            patch.dict("sys.modules", {"strands": MagicMock(Agent=mock_agent_cls)}),
        ):
            import sys

            sys.modules["strands"].Agent = mock_agent_cls

            request = AgentRunRequest(prompt="q", instructions="Be concise.")
            hook.create_agent(request)

        _, kwargs = mock_agent_cls.call_args
        assert kwargs.get("system_prompt") == "Be concise."

    def test_create_agent_resolves_toolsets(self):
        mock_agent_cls = MagicMock()
        mock_model = MagicMock()

        hook = StrandsGeminiHook(llm_conn_id="test", model_id="gemini-2.5-flash")

        from airflow.providers.common.ai.hooks.base_ai import BaseToolset

        spec = ToolSpec(name="t", description="d", parameters={}, fn=lambda: None)

        class MyToolset(BaseToolset):
            def as_tools(self):
                return [spec]

        with (
            patch.object(hook, "get_model", return_value=mock_model),
            patch.object(hook, "_tool_spec_to_native", return_value="native_tool") as mock_native,
            patch.dict("sys.modules", {"strands": MagicMock(Agent=mock_agent_cls)}),
        ):
            import sys

            sys.modules["strands"].Agent = mock_agent_cls

            request = AgentRunRequest(prompt="q", toolsets=[MyToolset()], enable_tool_logging=False)
            hook.create_agent(request)

        mock_native.assert_called_once()
        _, kwargs = mock_agent_cls.call_args
        assert kwargs.get("tools") == ["native_tool"]


class TestStrandsHookSkills:
    def test_skill_spec_path_passthrough(self):
        hook = StrandsGeminiHook.__new__(StrandsGeminiHook)
        assert hook._skill_spec_to_native("./skills/pdf-processing") == "./skills/pdf-processing"
        assert hook._skill_spec_to_native(SkillSpec(path="./skills/")) == "./skills/"

    def test_skill_spec_inline_becomes_strands_skill(self):
        hook = StrandsGeminiHook.__new__(StrandsGeminiHook)
        spec = SkillSpec(name="greet", description="Greet users", instructions="Say hello.")
        mock_skill_cls = MagicMock(return_value="native-skill")

        with patch.dict("sys.modules", {"strands": MagicMock(Skill=mock_skill_cls)}):
            import sys

            sys.modules["strands"].Skill = mock_skill_cls
            result = hook._skill_spec_to_native(spec)

        assert result == "native-skill"
        mock_skill_cls.assert_called_once_with(
            name="greet",
            description="Greet users",
            instructions="Say hello.",
        )

    def test_skill_spec_invalid_raises(self):
        hook = StrandsGeminiHook.__new__(StrandsGeminiHook)
        with pytest.raises(ValueError, match="SkillSpec must set"):
            hook._skill_spec_to_native(SkillSpec(name="only-name"))

    def test_resolve_skill_sources_empty_when_not_set(self):
        hook = StrandsGeminiHook(llm_conn_id="test")
        request = AgentRunRequest(prompt="q")
        assert hook._resolve_skill_sources(request) == []

    @patch("airflow.providers.common.ai.hooks.strands_ai.StrandsGeminiHook.get_connection")
    def test_create_agent_attaches_agentskills_plugin(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            extra=json.dumps({}),
        )
        mock_agent_cls = MagicMock()
        mock_plugin_cls = MagicMock(return_value="skills-plugin")
        hook = StrandsGeminiHook(llm_conn_id="test")

        with (
            patch.object(hook, "get_model", return_value=MagicMock()),
            patch.dict(
                "sys.modules", {"strands": MagicMock(Agent=mock_agent_cls, AgentSkills=mock_plugin_cls)}
            ),
        ):
            import sys

            sys.modules["strands"].Agent = mock_agent_cls
            sys.modules["strands"].AgentSkills = mock_plugin_cls

            request = AgentRunRequest(prompt="q", skills=["./skills/"])
            hook.create_agent(request)

        mock_plugin_cls.assert_called_once_with(skills="./skills/")
        _, kwargs = mock_agent_cls.call_args
        assert kwargs["plugins"] == ["skills-plugin"]

    @patch("airflow.providers.common.ai.hooks.strands_ai.StrandsGeminiHook.get_connection")
    def test_create_agent_merges_existing_plugins(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id="test",
            conn_type="strands-gemini",
            extra=json.dumps({}),
        )
        mock_agent_cls = MagicMock()
        mock_plugin_cls = MagicMock(return_value="skills-plugin")
        hook = StrandsGeminiHook(llm_conn_id="test")

        with (
            patch.object(hook, "get_model", return_value=MagicMock()),
            patch.dict(
                "sys.modules", {"strands": MagicMock(Agent=mock_agent_cls, AgentSkills=mock_plugin_cls)}
            ),
        ):
            import sys

            sys.modules["strands"].Agent = mock_agent_cls
            sys.modules["strands"].AgentSkills = mock_plugin_cls

            request = AgentRunRequest(
                prompt="q",
                skills=["./skills/"],
                agent_params={"plugins": ["existing-plugin"]},
            )
            hook.create_agent(request)

        _, kwargs = mock_agent_cls.call_args
        assert kwargs["plugins"] == ["existing-plugin", "skills-plugin"]

    def test_build_skills_plugin_forwards_skills_params(self):
        mock_plugin_cls = MagicMock(return_value="skills-plugin")
        hook = StrandsGeminiHook(llm_conn_id="test")

        with patch.dict("sys.modules", {"strands": MagicMock(AgentSkills=mock_plugin_cls)}):
            import sys

            sys.modules["strands"].AgentSkills = mock_plugin_cls

            request = AgentRunRequest(
                prompt="q",
                skills=["./skills/"],
                skills_params={"strict": True, "max_resource_files": 5},
            )
            hook._build_skills_plugin(request)

        mock_plugin_cls.assert_called_once_with(
            skills="./skills/",
            strict=True,
            max_resource_files=5,
        )

    def test_skill_spec_path_uses_base_helper(self):
        hook = StrandsGeminiHook.__new__(StrandsGeminiHook)
        assert hook._skill_spec_to_native("./skills/") == "./skills/"
        assert hook._skill_spec_to_native(SkillSpec(path="./skills/")) == "./skills/"
