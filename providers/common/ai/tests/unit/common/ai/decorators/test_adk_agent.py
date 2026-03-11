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
"""Tests for @task.agent decorator when the connection type is ``adk`` (Google ADK backend)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.ai.decorators.agent import _AgentDecoratedOperator


def _adk_connection() -> Connection:
    return Connection(conn_id="adk_llm", conn_type="adk")


class TestAgentDecoratedOperatorAdk:
    """Tests for the @task.agent decorator when using the ADK backend."""

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_calls_callable_returns_adk_output(self, mock_adk_hook_cls, mock_get_conn):
        """The callable's return becomes the prompt; ADK run_agent output is returned."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "ADK generated answer"

        def my_prompt():
            return "Analyze the data"

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="adk_llm",
        )
        result = op.execute(context={})

        assert result == "ADK generated answer"
        assert op.prompt == "Analyze the data"
        mock_hook.run_agent.assert_called_once_with(
            agent=mock_hook.create_agent.return_value, prompt="Analyze the data"
        )

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None],
        ids=["non-string", "empty", "whitespace", "none"],
    )
    def test_execute_raises_on_invalid_prompt_regardless_of_backend(self, return_value):
        """TypeError is raised before the backend is invoked, so it applies to ADK too."""
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            llm_conn_id="adk_llm",
        )
        with pytest.raises(TypeError, match="non-empty string"):
            op.execute(context={})

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_merges_op_kwargs_into_callable(self, mock_adk_hook_cls, mock_get_conn):
        """op_kwargs are resolved and used to build the prompt."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "done"

        def my_prompt(topic):
            return f"Analyze {topic}"

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="adk_llm",
            op_kwargs={"topic": "cost trends"},
        )
        op.execute(context={"task_instance": MagicMock()})

        assert op.prompt == "Analyze cost trends"
        mock_hook.run_agent.assert_called_once_with(
            agent=mock_hook.create_agent.return_value, prompt="Analyze cost trends"
        )

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_toolsets_not_wrapped_for_adk(self, mock_adk_hook_cls, mock_get_conn):
        """ADK path does NOT wrap toolsets in LoggingToolset."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "result"

        mock_toolset = MagicMock()
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Go",
            llm_conn_id="adk_llm",
            toolsets=[mock_toolset],
            enable_tool_logging=True,
        )
        op.execute(context={})

        create_call = mock_hook.create_agent.call_args
        # Toolsets passed as-is (no LoggingToolset wrapping)
        assert create_call[1]["toolsets"] == [mock_toolset]

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_agent_params_forwarded(self, mock_adk_hook_cls, mock_get_conn):
        """agent_params are forwarded to create_agent for ADK tools."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "ok"

        def my_tool():
            """A tool."""

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Use tools",
            llm_conn_id="adk_llm",
            agent_params={"tools": [my_tool]},
        )
        op.execute(context={})

        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["tools"] == [my_tool]

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_model_id_passed_to_adk_hook(self, mock_adk_hook_cls, mock_get_conn):
        """model_id is passed through to the AdkHook constructor."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "ok"

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Test",
            llm_conn_id="adk_llm",
            model_id="gemini-2.5-pro",
        )
        op.execute(context={})

        mock_adk_hook_cls.assert_called_once_with(llm_conn_id="adk_llm", model_id="gemini-2.5-pro")

    @patch("airflow.providers.common.ai.operators.agent.BaseHook.get_connection")
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_system_prompt_forwarded_as_instructions(self, mock_adk_hook_cls, mock_get_conn):
        """system_prompt becomes the instructions parameter for create_agent."""
        mock_get_conn.return_value = _adk_connection()
        mock_hook = mock_adk_hook_cls.return_value
        mock_hook.create_agent.return_value = MagicMock()
        mock_hook.run_agent.return_value = "answer"

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Question",
            llm_conn_id="adk_llm",
            system_prompt="You are an expert data analyst.",
        )
        op.execute(context={})

        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["instructions"] == "You are an expert data analyst."
