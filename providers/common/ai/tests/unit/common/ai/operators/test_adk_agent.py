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

from airflow.providers.common.ai.operators.adk_agent import AdkAgentOperator


class TestAdkAgentOperatorValidation:
    def test_requires_model_id(self):
        with pytest.raises(TypeError):
            AdkAgentOperator(task_id="test", prompt="hello")

    def test_accepts_model_id_without_llm_conn_id(self):
        """ADK operator should not require llm_conn_id."""
        op = AdkAgentOperator(
            task_id="test",
            prompt="hello",
            model_id="gemini-2.5-flash",
        )
        assert op.model_id == "gemini-2.5-flash"
        assert op.llm_conn_id == ""


class TestAdkAgentOperatorTemplateFields:
    def test_template_fields(self):
        expected = {"prompt", "llm_conn_id", "model_id", "system_prompt", "agent_params"}
        assert set(AdkAgentOperator.template_fields) == expected


class TestAdkAgentOperatorExecute:
    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_creates_agent_from_hook(self, mock_hook_cls):
        """execute() creates agent via AdkHook and runs it."""
        mock_agent = MagicMock()
        mock_hook_cls.return_value.create_agent.return_value = mock_agent
        mock_hook_cls.return_value.run_agent_sync.return_value = "ADK result text"

        op = AdkAgentOperator(
            task_id="test",
            prompt="What is the answer?",
            model_id="gemini-2.5-flash",
            system_prompt="You are helpful.",
        )
        result = op.execute(context=MagicMock())

        assert result == "ADK result text"
        mock_hook_cls.assert_called_once_with(llm_conn_id="", model_id="gemini-2.5-flash")
        mock_hook_cls.return_value.create_agent.assert_called_once_with(
            instruction="You are helpful.",
            description="Airflow AdkAgentOperator agent",
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

        op = AdkAgentOperator(
            task_id="test",
            prompt="Do something",
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

        op = AdkAgentOperator(
            task_id="test",
            prompt="test",
            model_id="gemini-2.5-flash",
            agent_params={"description": "custom description"},
        )
        op.execute(context=MagicMock())

        create_call = mock_hook_cls.return_value.create_agent.call_args
        assert create_call[1]["description"] == "custom description"

    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_structured_output(self, mock_hook_cls):
        """Structured output via BaseModel is serialized with model_dump."""

        class Summary(BaseModel):
            text: str
            score: float

        mock_hook_cls.return_value.run_agent_sync.return_value = Summary(text="Great", score=0.95)

        op = AdkAgentOperator(
            task_id="test",
            prompt="Analyze this",
            model_id="gemini-2.5-flash",
            output_type=Summary,
        )
        result = op.execute(context=MagicMock())

        assert result == {"text": "Great", "score": 0.95}

    @patch("airflow.providers.common.ai.hooks.adk.AdkHook", autospec=True)
    def test_execute_with_llm_conn_id(self, mock_hook_cls):
        """llm_conn_id is passed to AdkHook when provided."""
        mock_hook_cls.return_value.run_agent_sync.return_value = "ok"

        op = AdkAgentOperator(
            task_id="test",
            prompt="test",
            model_id="gemini-2.5-flash",
            llm_conn_id="my_adk_conn",
        )
        op.execute(context=MagicMock())

        mock_hook_cls.assert_called_once_with(llm_conn_id="my_adk_conn", model_id="gemini-2.5-flash")
