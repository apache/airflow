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
"""Tests for AgentOperator when the connection type is ``adk`` (Google ADK backend)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.ai.operators.agent import AgentOperator


def _adk_connection() -> Connection:
    """Return a fake ADK connection."""
    return Connection(conn_id="adk_llm", conn_type="adk")


def _mock_adk_hook():
    """Return a mock AdkHook."""
    hook = MagicMock()
    hook.run_agent.return_value = "ADK response"
    return hook


# =========================================================================
# Hook resolution — conn_type routing
# =========================================================================
class TestAgentOperatorAdkHookResolution:
    """Verify that ``llm_hook`` produces an AdkHook when conn_type is ``adk``."""

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook.__init__", return_value=None)
    def test_llm_hook_returns_adk_hook_for_adk_conn_type(self, mock_init, mock_get_conn):
        mock_get_conn.return_value = _adk_connection()

        op = AgentOperator(
            task_id="test",
            prompt="Hello",
            llm_conn_id="adk_llm",
            model_id="gemini-2.5-flash",
        )
        hook = op.llm_hook

        from airflow.providers.common.ai.hooks.adk import AdkHook

        assert isinstance(hook, AdkHook)
        mock_init.assert_called_once_with(llm_conn_id="adk_llm", model_id="gemini-2.5-flash")

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_llm_hook_falls_back_to_pydanticai_for_other_types(self, mock_pydantic_cls, mock_get_conn):
        mock_get_conn.return_value = Connection(conn_id="llm", conn_type="pydanticai")

        op = AgentOperator(
            task_id="test",
            prompt="Hello",
            llm_conn_id="llm",
        )
        hook = op.llm_hook

        mock_pydantic_cls.get_hook.assert_called_once_with("llm", hook_params={"model_id": None})
        assert hook is mock_pydantic_cls.get_hook.return_value

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_llm_hook_defaults_to_pydanticai_on_connection_error(self, mock_pydantic_cls, mock_get_conn):
        """When the connection lookup fails, default to pydanticai."""
        mock_get_conn.side_effect = Exception("Connection not found")

        op = AgentOperator(
            task_id="test",
            prompt="Hello",
            llm_conn_id="missing",
        )
        hook = op.llm_hook

        mock_pydantic_cls.get_hook.assert_called_once()
        assert hook is mock_pydantic_cls.get_hook.return_value


# =========================================================================
# Execute — ADK path
# =========================================================================
class TestAgentOperatorAdkExecute:
    """Tests for execute() when the ADK backend is selected."""

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_creates_agent_and_runs_via_hook(self, mock_adk_hook_cls, mock_get_conn):
        """Standard (non-HITL) path: create_agent + run_agent."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_agent = MagicMock()
        mock_hook.create_agent.return_value = mock_agent
        mock_hook.run_agent.return_value = "The answer is 42."

        op = AgentOperator(
            task_id="test",
            prompt="What is the answer?",
            llm_conn_id="adk_llm",
            model_id="gemini-2.5-flash",
            system_prompt="You are helpful.",
        )
        result = op.execute(context=MagicMock())

        assert result == "The answer is 42."
        mock_adk_hook_cls.assert_called_once_with(llm_conn_id="adk_llm", model_id="gemini-2.5-flash")
        mock_hook.create_agent.assert_called_once_with(
            output_type=str, instructions="You are helpful.", toolsets=None
        )
        mock_hook.run_agent.assert_called_once_with(agent=mock_agent, prompt="What is the answer?")

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_passes_agent_params(self, mock_adk_hook_cls, mock_get_conn):
        """agent_params are forwarded to create_agent (e.g. ``tools``)."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "ok"

        def my_tool():
            """A tool."""

        op = AgentOperator(
            task_id="test",
            prompt="use tools",
            llm_conn_id="adk_llm",
            agent_params={"tools": [my_tool], "description": "An agent"},
        )
        op.execute(context=MagicMock())

        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["tools"] == [my_tool]
        assert create_call[1]["description"] == "An agent"

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_passes_toolsets_without_logging_wrapping(self, mock_adk_hook_cls, mock_get_conn):
        """ADK path skips LoggingToolset wrapping — toolsets are passed raw to the hook."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "ok"

        mock_toolset = MagicMock()
        op = AgentOperator(
            task_id="test",
            prompt="do it",
            llm_conn_id="adk_llm",
            toolsets=[mock_toolset],
            enable_tool_logging=True,  # should be ignored for ADK
        )
        op.execute(context=MagicMock())

        create_call = mock_hook.create_agent.call_args
        # Toolsets should NOT be wrapped in LoggingToolset for ADK
        assert create_call[1]["toolsets"] == [mock_toolset]

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_with_model_id(self, mock_adk_hook_cls, mock_get_conn):
        """model_id is passed to AdkHook."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "ok"

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="adk_llm",
            model_id="gemini-2.5-pro",
        )
        op.execute(context=MagicMock())

        mock_adk_hook_cls.assert_called_once_with(llm_conn_id="adk_llm", model_id="gemini-2.5-pro")

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_returns_string_output_directly(self, mock_adk_hook_cls, mock_get_conn):
        """ADK run_agent returns a string, which is returned as-is (no BaseModel handling)."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "plain text response"

        op = AgentOperator(
            task_id="test",
            prompt="Tell me something",
            llm_conn_id="adk_llm",
        )
        result = op.execute(context=MagicMock())

        assert result == "plain text response"
        assert isinstance(result, str)


# =========================================================================
# _resolve_conn_type
# =========================================================================
class TestResolveConnType:
    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    def test_returns_adk_for_adk_connection(self, mock_get_conn):
        mock_get_conn.return_value = _adk_connection()
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="adk_llm")
        assert op._resolve_conn_type() == "adk"

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    def test_returns_pydanticai_for_pydanticai_connection(self, mock_get_conn):
        mock_get_conn.return_value = Connection(conn_id="llm", conn_type="pydanticai")
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="llm")
        assert op._resolve_conn_type() == "pydanticai"

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    def test_returns_pydanticai_on_error(self, mock_get_conn):
        mock_get_conn.side_effect = Exception("Connection not found")
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="missing")
        assert op._resolve_conn_type() == "pydanticai"

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    def test_returns_pydanticai_azure(self, mock_get_conn):
        mock_get_conn.return_value = Connection(conn_id="azure", conn_type="pydanticai-azure")
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="azure")
        assert op._resolve_conn_type() == "pydanticai-azure"


# =========================================================================
# _build_agent — ADK-specific behaviour
# =========================================================================
class TestBuildAgentAdk:
    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_build_agent_does_not_wrap_toolsets_for_adk(self, mock_adk_hook_cls, mock_get_conn):
        """For ADK, toolsets should not be wrapped in LoggingToolset even if enable_tool_logging=True."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()

        mock_toolset = MagicMock()
        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="adk_llm",
            toolsets=[mock_toolset],
            enable_tool_logging=True,
        )
        op._build_agent()

        # Verify toolsets were passed without LoggingToolset wrapping
        create_call = mock_hook.create_agent.call_args
        passed_toolsets = create_call[1]["toolsets"]
        assert passed_toolsets == [mock_toolset]

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_build_agent_passes_none_toolsets(self, mock_adk_hook_cls, mock_get_conn):
        """When no toolsets, None is passed through."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="adk_llm",
        )
        op._build_agent()

        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["toolsets"] is None

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_build_agent_forwards_system_prompt_as_instructions(self, mock_adk_hook_cls, mock_get_conn):
        """system_prompt is passed as instructions to create_agent."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="adk_llm",
            system_prompt="You are a helpful analyst.",
        )
        op._build_agent()

        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["instructions"] == "You are a helpful analyst."

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_build_agent_merges_agent_params(self, mock_adk_hook_cls, mock_get_conn):
        """agent_params are unpacked into create_agent call."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()

        def my_tool():
            """Tool."""

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="adk_llm",
            agent_params={"tools": [my_tool], "name": "custom_agent"},
        )
        op._build_agent()

        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["tools"] == [my_tool]
        assert create_call[1]["name"] == "custom_agent"


# =========================================================================
# Edge cases
# =========================================================================
class TestAgentOperatorAdkEdgeCases:
    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_without_model_id_uses_connection_model(self, mock_adk_hook_cls, mock_get_conn):
        """When model_id is not specified, it falls through to the hook to resolve from connection."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "ok"

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="adk_llm",
            # model_id intentionally omitted
        )
        op.execute(context=MagicMock())

        mock_adk_hook_cls.assert_called_once_with(llm_conn_id="adk_llm", model_id=None)

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_with_empty_agent_params(self, mock_adk_hook_cls, mock_get_conn):
        """Empty agent_params doesn't add extra kwargs."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "done"

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="adk_llm",
            agent_params={},
        )
        op.execute(context=MagicMock())

        create_call = mock_hook.create_agent.call_args
        # Only the base kwargs should be present
        assert create_call[1]["output_type"] is str
        assert create_call[1]["instructions"] == ""
        assert create_call[1]["toolsets"] is None

    @pytest.mark.parametrize("enable_tool_logging", [True, False])
    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_toolsets_never_wrapped_for_adk_regardless_of_flag(
        self, mock_adk_hook_cls, mock_get_conn, enable_tool_logging
    ):
        """No LoggingToolset wrapping for ADK regardless of enable_tool_logging value."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "ok"

        mock_toolset = MagicMock()
        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="adk_llm",
            toolsets=[mock_toolset],
            enable_tool_logging=enable_tool_logging,
        )
        op.execute(context=MagicMock())

        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["toolsets"] == [mock_toolset]
