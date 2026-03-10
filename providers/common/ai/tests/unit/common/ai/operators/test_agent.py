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
from pydantic import BaseModel

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.logging import LoggingToolset


def _make_mock_run_result(output):
    """Create a mock AgentRunResult compatible with log_run_summary."""
    mock_result = MagicMock()
    mock_result.output = output
    mock_result.usage.return_value = MagicMock(
        requests=1, tool_calls=0, input_tokens=0, output_tokens=0, total_tokens=0
    )
    mock_result.response = MagicMock(model_name="test-model")
    mock_result.all_messages.return_value = []
    return mock_result


def _make_mock_agent(output):
    """Create a mock agent that returns the given output."""
    mock_agent = MagicMock(spec=["run_sync"])
    mock_agent.run_sync.return_value = _make_mock_run_result(output)
    return mock_agent


class TestAgentOperatorValidation:
    def test_requires_llm_conn_id(self):
        with pytest.raises(ValueError, match="llm_conn_id is required"):
            AgentOperator(task_id="test", prompt="hello")


class TestAgentOperatorTemplateFields:
    def test_template_fields(self):
        expected = {"prompt", "llm_conn_id", "model_id", "system_prompt", "agent_params"}
        assert set(AgentOperator.template_fields) == expected


class TestAgentOperatorExecute:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_execute_creates_agent_from_hook(self, mock_hook_cls):
        mock_agent = _make_mock_agent("The answer is 42.")
        mock_hook_cls.return_value.create_agent.return_value = mock_agent

        op = AgentOperator(
            task_id="test",
            prompt="What is the answer?",
            llm_conn_id="my_llm",
            system_prompt="You are helpful.",
        )
        result = op.execute(context=MagicMock())

        assert result == "The answer is 42."
        mock_hook_cls.assert_called_once_with(llm_conn_id="my_llm", model_id=None)
        mock_hook_cls.return_value.create_agent.assert_called_once_with(
            output_type=str, instructions="You are helpful."
        )
        mock_agent.run_sync.assert_called_once_with("What is the answer?")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_execute_passes_toolsets_in_agent_kwargs(self, mock_hook_cls):
        """Toolsets are passed through to the agent constructor."""
        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent("done")

        mock_toolset = MagicMock()
        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            llm_conn_id="my_llm",
            toolsets=[mock_toolset],
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.return_value.create_agent.call_args
        passed_toolsets = create_call[1]["toolsets"]
        assert len(passed_toolsets) == 1
        assert isinstance(passed_toolsets[0], LoggingToolset)
        assert passed_toolsets[0].wrapped is mock_toolset

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_enable_tool_logging_false_skips_wrapping(self, mock_hook_cls):
        """enable_tool_logging=False passes toolsets through unwrapped."""
        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent("done")

        mock_toolset = MagicMock()
        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            llm_conn_id="my_llm",
            toolsets=[mock_toolset],
            enable_tool_logging=False,
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.return_value.create_agent.call_args
        assert create_call[1]["toolsets"] == [mock_toolset]

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_execute_passes_agent_params(self, mock_hook_cls):
        """agent_params are unpacked into create_agent."""
        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent("ok")

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            agent_params={"retries": 3, "model_settings": {"temperature": 0}},
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.return_value.create_agent.call_args
        assert create_call[1]["retries"] == 3
        assert create_call[1]["model_settings"] == {"temperature": 0}

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_execute_structured_output(self, mock_hook_cls):
        """Structured output via BaseModel is serialized with model_dump."""

        class Summary(BaseModel):
            text: str
            score: float

        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent(
            Summary(text="Great", score=0.95)
        )

        op = AgentOperator(
            task_id="test",
            prompt="Analyze this",
            llm_conn_id="my_llm",
            output_type=Summary,
        )
        result = op.execute(context=MagicMock())

        assert result == {"text": "Great", "score": 0.95}

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_execute_with_model_id(self, mock_hook_cls):
        """model_id is passed to PydanticAIHook."""
        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent("ok")

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
            model_id="openai:gpt-5",
        )
        op.execute(context=MagicMock())

        mock_hook_cls.assert_called_once_with(llm_conn_id="my_llm", model_id="openai:gpt-5")


class TestAgentOperatorFrameworkValidation:
    def test_invalid_framework_raises(self):
        with pytest.raises(ValueError, match="Unsupported agent_framework"):
            AgentOperator(task_id="test", prompt="hello", llm_conn_id="x", agent_framework="unknown")

    def test_pydantic_ai_requires_llm_conn_id(self):
        with pytest.raises(ValueError, match="llm_conn_id is required"):
            AgentOperator(task_id="test", prompt="hello", agent_framework="pydantic_ai")

    def test_adk_requires_model_id(self):
        with pytest.raises(ValueError, match="model_id is required"):
            AgentOperator(task_id="test", prompt="hello", agent_framework="adk")

    def test_adk_accepts_model_id_without_llm_conn_id(self):
        """ADK framework should not require llm_conn_id."""
        op = AgentOperator(
            task_id="test",
            prompt="hello",
            agent_framework="adk",
            model_id="gemini-2.5-flash",
        )
        assert op.agent_framework == "adk"
        assert op.model_id == "gemini-2.5-flash"


class TestAgentOperatorADKExecute:
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_creates_agent_from_hook(self, mock_hook_cls):
        """execute() routes to _execute_adk and creates agent via AdkHook."""
        mock_agent = MagicMock()
        mock_hook_cls.return_value.create_agent.return_value = mock_agent
        mock_hook_cls.return_value.run_agent_sync.return_value = "ADK result text"

        op = AgentOperator(
            task_id="test",
            prompt="What is the answer?",
            agent_framework="adk",
            model_id="gemini-2.5-flash",
            system_prompt="You are helpful.",
        )
        result = op.execute(context=MagicMock())

        assert result == "ADK result text"
        mock_hook_cls.assert_called_once_with(llm_conn_id="", model_id="gemini-2.5-flash")
        mock_hook_cls.return_value.create_agent.assert_called_once_with(
            instruction="You are helpful.",
            description="Airflow AgentOperator ADK agent",
        )
        mock_hook_cls.return_value.run_agent_sync.assert_called_once_with(
            agent=mock_agent, prompt="What is the answer?"
        )

    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_passes_tools_in_agent_kwargs(self, mock_hook_cls):
        """Tools are passed through to the agent constructor."""
        mock_hook_cls.return_value.run_agent_sync.return_value = "done"

        def my_tool():
            """A test tool."""
            return {"result": "ok"}

        op = AgentOperator(
            task_id="test",
            prompt="Do something",
            agent_framework="adk",
            model_id="gemini-2.5-flash",
            tools=[my_tool],
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.return_value.create_agent.call_args
        assert create_call[1]["tools"] == [my_tool]

    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_passes_agent_params(self, mock_hook_cls):
        """agent_params are unpacked into create_agent."""
        mock_hook_cls.return_value.run_agent_sync.return_value = "ok"

        op = AgentOperator(
            task_id="test",
            prompt="test",
            agent_framework="adk",
            model_id="gemini-2.5-flash",
            agent_params={"description": "custom description"},
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.return_value.create_agent.call_args
        assert create_call[1]["description"] == "custom description"

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", autospec=True)
    def test_pydantic_ai_dispatches_as_default(self, mock_hook_cls):
        """Default framework routes to _execute_pydantic_ai, not _execute_adk."""
        mock_hook_cls.return_value.create_agent.return_value = _make_mock_agent("pydantic result")

        op = AgentOperator(
            task_id="test",
            prompt="test",
            llm_conn_id="my_llm",
        )
        result = op.execute(context=MagicMock())

        assert result == "pydantic result"
        mock_hook_cls.return_value.create_agent.assert_called_once()
