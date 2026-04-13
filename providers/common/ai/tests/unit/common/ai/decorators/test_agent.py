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

from airflow.providers.common.ai.decorators.agent import _AgentDecoratedOperator
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


class TestAgentDecoratedOperator:
    def test_custom_operator_name(self):
        assert _AgentDecoratedOperator.custom_operator_name == "@task.agent"

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_calls_callable_and_returns_output(self, mock_hook_cls):
        """The callable's return value becomes the agent prompt."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("The top customer is Acme Corp.")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        def my_prompt():
            return "Who is our top customer?"

        op = _AgentDecoratedOperator(task_id="test", python_callable=my_prompt, llm_conn_id="my_llm")
        result = op.execute(context={})

        assert result == "The top customer is Acme Corp."
        assert op.prompt == "Who is our top customer?"
        mock_agent.run_sync.assert_called_once_with("Who is our top customer?")

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None],
        ids=["non-string", "empty", "whitespace", "none"],
    )
    def test_execute_raises_on_invalid_prompt(self, return_value):
        """TypeError when the callable returns a non-string or blank string."""
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            llm_conn_id="my_llm",
        )
        with pytest.raises(TypeError, match="non-empty string"):
            op.execute(context={})

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_merges_op_kwargs_into_callable(self, mock_hook_cls):
        """op_kwargs are resolved by the callable to build the prompt."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("done")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        def my_prompt(topic):
            return f"Analyze {topic}"

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
            op_kwargs={"topic": "revenue trends"},
        )
        op.execute(context={"task_instance": MagicMock()})

        assert op.prompt == "Analyze revenue trends"
        mock_agent.run_sync.assert_called_once_with("Analyze revenue trends")

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_passes_toolsets_through(self, mock_hook_cls):
        """Toolsets passed to the decorator are forwarded to the agent."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("result")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        mock_toolset = MagicMock()

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Do something",
            llm_conn_id="my_llm",
            toolsets=[mock_toolset],
        )
        op.execute(context={})

        create_call = mock_hook_cls.get_hook.return_value.create_agent.call_args
        passed_toolsets = create_call[1]["toolsets"]
        assert len(passed_toolsets) == 1
        assert isinstance(passed_toolsets[0], LoggingToolset)
        assert passed_toolsets[0].wrapped is mock_toolset

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_structured_output(self, mock_hook_cls):
        """BaseModel output is serialized with model_dump."""

        class Summary(BaseModel):
            text: str

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(Summary(text="Great results"))
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Summarize",
            llm_conn_id="my_llm",
            output_type=Summary,
        )
        result = op.execute(context={})

        assert result == {"text": "Great results"}

    def test_durable_kwarg_passes_through_to_operator(self):
        """durable=True is forwarded to AgentOperator via **kwargs."""
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "prompt",
            llm_conn_id="my_llm",
            durable=True,
        )
        assert op.durable is True

    def test_durable_default_false_through_decorator(self):
        """durable defaults to False when not specified."""
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "prompt",
            llm_conn_id="my_llm",
        )
        assert op.durable is False
