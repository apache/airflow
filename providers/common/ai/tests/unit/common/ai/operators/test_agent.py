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

from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.logging import LoggingToolset


def _mock_hook(output="The answer is 42."):
    """Create a mock BaseAIHook that returns the given output."""
    hook = MagicMock()
    hook.create_agent.return_value = MagicMock(name="mock_agent")
    hook.run_agent.return_value = output
    return hook


class TestAgentOperatorValidation:
    def test_requires_llm_conn_id(self):
        with pytest.raises(TypeError):
            AgentOperator(task_id="test", prompt="hello")


class TestAgentOperatorTemplateFields:
    def test_template_fields(self):
        expected = {"prompt", "llm_conn_id", "model_id", "system_prompt", "agent_params"}
        assert set(AgentOperator.template_fields) == expected


class TestAgentOperatorResolveConnType:
    @patch("airflow.providers.common.ai.operators.agent.BaseHook", autospec=True)
    def test_resolve_pydanticai_conn_type(self, mock_base_hook):
        conn = MagicMock()
        conn.conn_type = "pydanticai"
        mock_base_hook.get_connection.return_value = conn

        op = AgentOperator(task_id="test", prompt="hello", llm_conn_id="my_llm")
        assert op._resolve_conn_type() == "pydanticai"

    @patch("airflow.providers.common.ai.operators.agent.BaseHook", autospec=True)
    def test_resolve_adk_conn_type(self, mock_base_hook):
        conn = MagicMock()
        conn.conn_type = "adk"
        mock_base_hook.get_connection.return_value = conn

        op = AgentOperator(task_id="test", prompt="hello", llm_conn_id="my_adk")
        assert op._resolve_conn_type() == "adk"

    @patch("airflow.providers.common.ai.operators.agent.BaseHook", autospec=True)
    def test_resolve_defaults_to_pydanticai_on_error(self, mock_base_hook):
        mock_base_hook.get_connection.side_effect = Exception("Not found")

        op = AgentOperator(task_id="test", prompt="hello", llm_conn_id="missing")
        assert op._resolve_conn_type() == "pydanticai"


class TestAgentOperatorLlmHook:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_creates_pydanticai_hook(self, mock_hook_cls):
        op = AgentOperator(task_id="test", prompt="hello", llm_conn_id="my_llm")
        with patch.object(op, "_resolve_conn_type", return_value="pydanticai"):
            hook = op.llm_hook

        assert hook is not None

    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_creates_adk_hook(self, mock_hook_cls):
        op = AgentOperator(task_id="test", prompt="hello", llm_conn_id="my_adk")
        with patch.object(op, "_resolve_conn_type", return_value="adk"):
            hook = op.llm_hook

        assert hook is not None


class TestAgentOperatorExecute:
    def test_execute_creates_agent_and_runs(self):
        mock_hook = _mock_hook("The answer is 42.")
        op = AgentOperator(
            task_id="test",
            prompt="What is the answer?",
            llm_conn_id="my_llm",
            system_prompt="You are helpful.",
        )
        with patch.object(type(op), "llm_hook", new_callable=PropertyMock, return_value=mock_hook):
            with patch.object(op, "_resolve_conn_type", return_value="pydanticai"):
                result = op.execute(context=MagicMock())

        assert result == "The answer is 42."
        mock_hook.create_agent.assert_called_once_with(
            output_type=str,
            instructions="You are helpful.",
            toolsets=None,
        )
        mock_hook.run_agent.assert_called_once_with(
            agent=mock_hook.create_agent.return_value,
            prompt="What is the answer?",
        )

    def test_execute_passes_toolsets_wrapped_for_logging(self):
        """Toolsets are wrapped in LoggingToolset for pydantic-ai backend."""
        mock_hook = _mock_hook("done")
        mock_toolset = MagicMock()

        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            llm_conn_id="my_llm",
            toolsets=[mock_toolset],
        )
        with patch.object(type(op), "llm_hook", new_callable=PropertyMock, return_value=mock_hook):
            with patch.object(op, "_resolve_conn_type", return_value="pydanticai"):
                op.execute(context=MagicMock())

        create_call = mock_hook.create_agent.call_args
        passed_toolsets = create_call[1]["toolsets"]
        assert len(passed_toolsets) == 1
        assert isinstance(passed_toolsets[0], LoggingToolset)
        assert passed_toolsets[0].wrapped is mock_toolset

    def test_enable_tool_logging_false_skips_wrapping(self):
        """enable_tool_logging=False passes toolsets through unwrapped."""
        mock_hook = _mock_hook("done")
        mock_toolset = MagicMock()

        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            llm_conn_id="my_llm",
            toolsets=[mock_toolset],
            enable_tool_logging=False,
        )
        with patch.object(type(op), "llm_hook", new_callable=PropertyMock, return_value=mock_hook):
            with patch.object(op, "_resolve_conn_type", return_value="pydanticai"):
                op.execute(context=MagicMock())

        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["toolsets"] == [mock_toolset]

    def test_adk_backend_skips_logging_wrapping(self):
        """ADK backend does not wrap toolsets in LoggingToolset."""
        mock_hook = _mock_hook("done")
        mock_toolset = MagicMock()

        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            llm_conn_id="my_adk",
            toolsets=[mock_toolset],
        )
        with patch.object(type(op), "llm_hook", new_callable=PropertyMock, return_value=mock_hook):
            with patch.object(op, "_resolve_conn_type", return_value="adk"):
                op.execute(context=MagicMock())

        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["toolsets"] == [mock_toolset]

    def test_execute_passes_agent_params(self):
        """agent_params are unpacked into create_agent."""
        mock_hook = _mock_hook("ok")
        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            agent_params={"retries": 3, "model_settings": {"temperature": 0}},
        )
        with patch.object(type(op), "llm_hook", new_callable=PropertyMock, return_value=mock_hook):
            with patch.object(op, "_resolve_conn_type", return_value="pydanticai"):
                op.execute(context=MagicMock())

        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["retries"] == 3
        assert create_call[1]["model_settings"] == {"temperature": 0}

    def test_execute_structured_output(self):
        """Structured output via BaseModel is serialized with model_dump."""

        class Summary(BaseModel):
            text: str
            score: float

        mock_hook = _mock_hook(Summary(text="Great", score=0.95))
        op = AgentOperator(
            task_id="test",
            prompt="Analyze this",
            llm_conn_id="my_llm",
            output_type=Summary,
        )
        with patch.object(type(op), "llm_hook", new_callable=PropertyMock, return_value=mock_hook):
            with patch.object(op, "_resolve_conn_type", return_value="pydanticai"):
                result = op.execute(context=MagicMock())

        assert result == {"text": "Great", "score": 0.95}

    def test_execute_with_adk_tools_via_agent_params(self):
        """ADK-specific tools can be passed via agent_params."""
        mock_hook = _mock_hook("adk result")

        def my_tool():
            """A test tool."""
            return {"result": "ok"}

        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            llm_conn_id="my_adk",
            agent_params={"tools": [my_tool]},
        )
        with patch.object(type(op), "llm_hook", new_callable=PropertyMock, return_value=mock_hook):
            with patch.object(op, "_resolve_conn_type", return_value="adk"):
                result = op.execute(context=MagicMock())

        assert result == "adk result"
        create_call = mock_hook.create_agent.call_args
        assert create_call[1]["tools"] == [my_tool]
