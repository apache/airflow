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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("google.adk")

from google.adk.agents import BaseAgent, LoopAgent, ParallelAgent, SequentialAgent
from google.genai import types as genai_types
from pydantic import BaseModel

from airflow.models.connection import Connection
from airflow.providers.common.ai.hooks.adk import AdkHook, _GeminiWithApiKey

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _hook_with_conn(model_id="gemini-2.5-flash", password=None, extra=None):
    """Return an AdkHook wired to a fake connection."""
    hook = AdkHook(llm_conn_id="test_conn", model_id=model_id)
    conn = Connection(
        conn_id="test_conn",
        conn_type="adk",
        password=password,
        extra=extra,
    )
    return hook, conn


# =========================================================================
# Init
# =========================================================================
class TestAdkHookInit:
    def test_default_conn_id(self):
        hook = AdkHook()
        assert hook.llm_conn_id == "adk_default"
        assert hook.model_id is None

    def test_custom_conn_id(self):
        hook = AdkHook(llm_conn_id="my_adk", model_id="gemini-2.5-flash")
        assert hook.llm_conn_id == "my_adk"
        assert hook.model_id == "gemini-2.5-flash"

    def test_custom_session_service(self):
        mock_svc = MagicMock()
        hook = AdkHook(session_service=mock_svc)
        assert hook._session_service is mock_svc


# =========================================================================
# get_conn
# =========================================================================
class TestAdkHookGetConn:
    def test_get_conn_with_api_key(self):
        """API key from connection is stored on the hook instance, not in env."""
        hook, conn = _hook_with_conn(password="test-google-api-key")
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()
            assert result == "gemini-2.5-flash"
            assert hook._api_key == "test-google-api-key"

    def test_get_conn_with_model_from_extra(self):
        """Model is read from connection extra when model_id is not set."""
        hook = AdkHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="adk",
            password="test-key",
            extra='{"model": "gemini-2.5-pro"}',
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        assert result == "gemini-2.5-pro"
        assert hook.model_id == "gemini-2.5-pro"

    def test_model_id_param_overrides_extra(self):
        """model_id parameter takes priority over connection extra."""
        hook, conn = _hook_with_conn(password="test-key")
        conn.extra = '{"model": "gemini-2.5-pro"}'
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        assert result == "gemini-2.5-flash"

    def test_get_conn_raises_when_no_model(self):
        hook = AdkHook(llm_conn_id="test_conn")
        conn = Connection(conn_id="test_conn", conn_type="adk", password="test-key")
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="No model specified"):
                hook.get_conn()

    def test_get_conn_without_credentials_uses_env(self):
        """No API key in connection means env-based auth (ADC, etc.)."""
        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        assert result == "gemini-2.5-flash"

    def test_get_conn_caches_result(self):
        """get_conn() should resolve the model once and cache it."""
        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn) as mock_get_conn:
            first = hook.get_conn()
            second = hook.get_conn()

        assert first == second
        mock_get_conn.assert_called_once()

    def test_get_conn_falls_back_when_connection_not_found(self):
        """Falls back to model_id when connection cannot be found."""
        hook = AdkHook(llm_conn_id="nonexistent", model_id="gemini-2.5-flash")
        with patch.object(hook, "get_connection", side_effect=Exception("Connection not found")):
            result = hook.get_conn()

        assert result == "gemini-2.5-flash"

    def test_get_conn_does_not_leak_api_key_to_env(self):
        """API key must never be written to the process-wide environment."""
        import os

        hook, conn = _hook_with_conn(password="secret-key-should-not-leak")
        env_before = os.environ.get("GOOGLE_API_KEY")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()
        assert os.environ.get("GOOGLE_API_KEY") == env_before


# =========================================================================
# create_agent — LLM (default)
# =========================================================================
class TestAdkHookCreateAgentLlm:
    @patch("airflow.providers.common.ai.hooks.adk.ADKAgent", autospec=True)
    def test_create_agent_defaults(self, mock_agent_cls):
        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(instructions="You are a helpful assistant.")

        mock_agent_cls.assert_called_once_with(
            name="airflow_agent",
            model="gemini-2.5-flash",
            instruction="You are a helpful assistant.",
            tools=[],
        )

    @patch("airflow.providers.common.ai.hooks.adk.ADKAgent", autospec=True)
    def test_create_agent_with_api_key_passes_gemini_model(self, mock_agent_cls):
        """When an API key is present, a _GeminiWithApiKey instance is passed."""
        hook, conn = _hook_with_conn(password="my-api-key")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(instructions="Help me.")

        call_kwargs = mock_agent_cls.call_args[1]
        model_arg = call_kwargs["model"]
        assert isinstance(model_arg, _GeminiWithApiKey)
        assert model_arg.model == "gemini-2.5-flash"
        assert model_arg._api_key == "my-api-key"

    @patch("airflow.providers.common.ai.hooks.adk.ADKAgent", autospec=True)
    def test_create_agent_with_tools_and_params(self, mock_agent_cls):
        def my_tool():
            """A test tool."""
            return "ok"

        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                name="custom_agent",
                instructions="Be helpful.",
                tools=[my_tool],
                description="A custom agent",
            )

        mock_agent_cls.assert_called_once_with(
            name="custom_agent",
            model="gemini-2.5-flash",
            instruction="Be helpful.",
            tools=[my_tool],
            description="A custom agent",
        )

    @patch("airflow.providers.common.ai.hooks.adk.ADKAgent", autospec=True)
    @patch("airflow.providers.common.ai.utils.toolset_bridge.toolsets_to_adk_tools")
    def test_create_agent_bridges_toolsets(self, mock_bridge, mock_agent_cls):
        """When toolsets are provided, they are bridged to ADK tools."""
        mock_bridge.return_value = [MagicMock(name="bridged_tool")]

        hook, conn = _hook_with_conn()
        mock_toolset = MagicMock()
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(instructions="Be helpful.", toolsets=[mock_toolset])

        mock_bridge.assert_called_once_with([mock_toolset])
        call_kwargs = mock_agent_cls.call_args[1]
        assert len(call_kwargs["tools"]) == 1

    @patch("airflow.providers.common.ai.hooks.adk.ADKAgent", autospec=True)
    @patch("airflow.providers.common.ai.utils.toolset_bridge.toolsets_to_adk_tools")
    def test_create_agent_merges_toolsets_and_plain_tools(self, mock_bridge, mock_agent_cls):
        """Bridged toolsets and plain ADK tools are merged together."""
        bridged = MagicMock(name="bridged")
        mock_bridge.return_value = [bridged]

        def plain_tool():
            """A plain tool."""

        hook, conn = _hook_with_conn()
        mock_toolset = MagicMock()
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                instructions="Be helpful.",
                toolsets=[mock_toolset],
                tools=[plain_tool],
            )

        call_kwargs = mock_agent_cls.call_args[1]
        assert plain_tool in call_kwargs["tools"]
        assert bridged in call_kwargs["tools"]

    # -- input_schema / output_schema -----------------------------------
    @patch("airflow.providers.common.ai.hooks.adk.ADKAgent", autospec=True)
    def test_create_agent_with_input_output_schema(self, mock_agent_cls):
        class InSchema(BaseModel):
            query: str

        class OutSchema(BaseModel):
            answer: str

        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                instructions="Respond.",
                input_schema=InSchema,
                output_schema=OutSchema,
                output_key="result",
            )

        kw = mock_agent_cls.call_args[1]
        assert kw["input_schema"] is InSchema
        assert kw["output_schema"] is OutSchema
        assert kw["output_key"] == "result"

    # -- generate_content_config ----------------------------------------
    @patch("airflow.providers.common.ai.hooks.adk.ADKAgent", autospec=True)
    def test_create_agent_with_generate_content_config(self, mock_agent_cls):
        config = genai_types.GenerateContentConfig(temperature=0.2)
        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(instructions="Hi.", generate_content_config=config)

        kw = mock_agent_cls.call_args[1]
        assert kw["generate_content_config"] is config

    # -- callbacks ------------------------------------------------------
    @patch("airflow.providers.common.ai.hooks.adk.ADKAgent", autospec=True)
    def test_create_agent_with_callbacks(self, mock_agent_cls):
        bm = MagicMock(name="before_model")
        am = MagicMock(name="after_model")
        bt = MagicMock(name="before_tool")
        at = MagicMock(name="after_tool")
        ba = MagicMock(name="before_agent")
        aa = MagicMock(name="after_agent")

        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                instructions="Go.",
                before_model_callback=bm,
                after_model_callback=am,
                before_tool_callback=bt,
                after_tool_callback=at,
                before_agent_callback=ba,
                after_agent_callback=aa,
            )

        kw = mock_agent_cls.call_args[1]
        assert kw["before_model_callback"] is bm
        assert kw["after_model_callback"] is am
        assert kw["before_tool_callback"] is bt
        assert kw["after_tool_callback"] is at
        assert kw["before_agent_callback"] is ba
        assert kw["after_agent_callback"] is aa


# =========================================================================
# create_agent — composite types
# =========================================================================
class TestAdkHookCreateAgentComposite:
    @staticmethod
    def _make_sub_agent(name: str) -> BaseAgent:
        """Create a minimal real BaseAgent for use as a sub-agent."""
        return SequentialAgent(name=name, sub_agents=[])

    def test_create_sequential_agent(self):
        sub_a = self._make_sub_agent("a")
        sub_b = self._make_sub_agent("b")
        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            agent = hook.create_agent(
                name="seq",
                agent_type="sequential",
                sub_agents=[sub_a, sub_b],
            )
        assert isinstance(agent, SequentialAgent)
        assert agent.name == "seq"
        assert len(agent.sub_agents) == 2

    def test_create_parallel_agent(self):
        sub_a = self._make_sub_agent("a")
        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            agent = hook.create_agent(
                name="par",
                agent_type="parallel",
                sub_agents=[sub_a],
            )
        assert isinstance(agent, ParallelAgent)

    def test_create_loop_agent_with_max_iterations(self):
        sub_a = self._make_sub_agent("a")
        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            agent = hook.create_agent(
                name="loop",
                agent_type="loop",
                sub_agents=[sub_a],
                max_iterations=10,
            )
        assert isinstance(agent, LoopAgent)
        assert agent.max_iterations == 10

    def test_unknown_agent_type_raises(self):
        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="Unknown agent_type"):
                hook.create_agent(name="bad", agent_type="nonexistent")

    def test_composite_agent_with_before_after_callbacks(self):
        ba = MagicMock(name="before_agent")
        aa = MagicMock(name="after_agent")
        sub_a = self._make_sub_agent("a")
        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            agent = hook.create_agent(
                name="seq_cb",
                agent_type="sequential",
                sub_agents=[sub_a],
                before_agent_callback=ba,
                after_agent_callback=aa,
            )
        assert isinstance(agent, SequentialAgent)
        assert agent.before_agent_callback is ba
        assert agent.after_agent_callback is aa


# =========================================================================
# run_agent
# =========================================================================
class TestAdkHookRunAgent:
    @patch("airflow.providers.common.ai.hooks.adk.ADKRunner", autospec=True)
    @patch("airflow.providers.common.ai.hooks.adk.InMemorySessionService", autospec=True)
    def test_run_agent_returns_final_text(self, mock_session_service_cls, mock_runner_cls):
        """run_agent collects final response text from the event stream."""
        mock_session = MagicMock()
        mock_session.id = "session-123"
        mock_session_service = mock_session_service_cls.return_value
        mock_session_service.create_session = AsyncMock(return_value=mock_session)

        mock_part = MagicMock()
        mock_part.text = "The answer is 42."
        mock_event = MagicMock()
        mock_event.is_final_response.return_value = True
        mock_event.content.parts = [mock_part]

        async def mock_run_async(**kwargs):
            yield mock_event

        mock_runner = mock_runner_cls.return_value
        mock_runner.run_async = mock_run_async

        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        result = hook.run_agent(agent=MagicMock(), prompt="What is the answer?")

        assert result == "The answer is 42."

    @patch("airflow.providers.common.ai.hooks.adk.ADKRunner", autospec=True)
    @patch("airflow.providers.common.ai.hooks.adk.InMemorySessionService", autospec=True)
    def test_run_agent_skips_non_final_events(self, mock_session_service_cls, mock_runner_cls):
        """Non-final events are ignored; only final response text is collected."""
        mock_session = MagicMock()
        mock_session.id = "session-456"
        mock_session_service = mock_session_service_cls.return_value
        mock_session_service.create_session = AsyncMock(return_value=mock_session)

        non_final_event = MagicMock()
        non_final_event.is_final_response.return_value = False

        mock_part = MagicMock()
        mock_part.text = "Done."
        final_event = MagicMock()
        final_event.is_final_response.return_value = True
        final_event.content.parts = [mock_part]

        async def mock_run_async(**kwargs):
            yield non_final_event
            yield final_event

        mock_runner = mock_runner_cls.return_value
        mock_runner.run_async = mock_run_async

        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        result = hook.run_agent(agent=MagicMock(), prompt="Do something")

        assert result == "Done."

    @patch("airflow.providers.common.ai.hooks.adk.ADKRunner", autospec=True)
    def test_run_agent_with_custom_session_service(self, mock_runner_cls):
        """When a session_service is provided, it is used instead of InMemorySessionService."""
        mock_session = MagicMock()
        mock_session.id = "db-session-1"
        mock_svc = MagicMock()
        mock_svc.create_session = AsyncMock(return_value=mock_session)

        mock_part = MagicMock()
        mock_part.text = "Ok."
        mock_event = MagicMock()
        mock_event.is_final_response.return_value = True
        mock_event.content.parts = [mock_part]

        async def mock_run_async(**kwargs):
            yield mock_event

        mock_runner = mock_runner_cls.return_value
        mock_runner.run_async = mock_run_async

        hook = AdkHook(
            llm_conn_id="test_conn",
            model_id="gemini-2.5-flash",
            session_service=mock_svc,
        )
        result = hook.run_agent(agent=MagicMock(), prompt="Hello")

        assert result == "Ok."
        # Verify the custom session service was passed to Runner
        runner_call_kwargs = mock_runner_cls.call_args[1]
        assert runner_call_kwargs["session_service"] is mock_svc


# =========================================================================
# test_connection
# =========================================================================
class TestAdkHookTestConnection:
    def test_successful_connection(self):
        hook, conn = _hook_with_conn()
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is True
        assert "gemini-2.5-flash" in message

    def test_failed_connection_no_model(self):
        hook = AdkHook(llm_conn_id="test_conn")
        conn = Connection(conn_id="test_conn", conn_type="adk")
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is False
        assert "No model specified" in message


# =========================================================================
# UI field behaviour
# =========================================================================
class TestAdkHookUIFieldBehaviour:
    def test_ui_field_behaviour(self):
        result = AdkHook.get_ui_field_behaviour()
        assert "schema" in result["hidden_fields"]
        assert "port" in result["hidden_fields"]
        assert "login" in result["hidden_fields"]
        assert result["relabeling"]["password"] == "API Key"
