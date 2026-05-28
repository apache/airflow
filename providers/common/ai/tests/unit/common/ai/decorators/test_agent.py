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
from pydantic_ai.messages import ImageUrl

from airflow.providers.common.ai.decorators.agent import _AgentDecoratedOperator
from airflow.providers.common.ai.hooks.base_ai import AgentRunResult, AgentUsage, BaseAIHook


def _make_run_result(output):
    return AgentRunResult(output=output, model_name="test-model", usage=AgentUsage(requests=1))


def _make_mock_hook(run_result):
    mock_hook = MagicMock(spec=BaseAIHook)
    mock_hook.supports_toolsets = True
    mock_hook.supports_durable = False
    mock_hook.supports_usage_limits = True
    mock_hook.create_agent.return_value = MagicMock()
    mock_hook.run_agent.return_value = run_result
    return mock_hook


class TestAgentDecoratedOperator:
    def test_custom_operator_name(self):
        assert _AgentDecoratedOperator.custom_operator_name == "@task.agent"

    def test_execute_calls_callable_and_returns_output(self):
        mock_hook = _make_mock_hook(_make_run_result("The top customer is Acme Corp."))

        def my_prompt():
            return "Who is our top customer?"

        op = _AgentDecoratedOperator(task_id="test", python_callable=my_prompt, llm_conn_id="my_llm")
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            result = op.execute(context={})

        assert result == "The top customer is Acme Corp."
        assert op.prompt == "Who is our top customer?"

        request = mock_hook.create_agent.call_args[0][0]
        assert request.prompt == "Who is our top customer?"

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None, b"bytes", bytearray(b"x"), [], ()],
        ids=["non-string", "empty", "whitespace", "none", "bytes", "bytearray", "empty-list", "empty-tuple"],
    )
    def test_execute_raises_on_invalid_prompt(self, return_value):
        """TypeError when the callable returns an unsupported prompt shape."""
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            llm_conn_id="my_llm",
        )
        with pytest.raises(TypeError, match="must be"):
            op.execute(context={})

    def test_execute_accepts_sequence_prompt(self):
        """A non-empty Sequence[UserContent] return value is forwarded as-is."""

        image = ImageUrl(url="https://example.com/x.png")
        prompt = ["Describe this:", image]
        mock_hook = _make_mock_hook(_make_run_result("ok"))

        def my_prompt():
            return prompt

        op = _AgentDecoratedOperator(task_id="test", python_callable=my_prompt, llm_conn_id="my_llm")
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            op.execute(context={})

        assert op.prompt == prompt
        request = mock_hook.create_agent.call_args[0][0]
        assert request.prompt == prompt

    def test_sequence_prompt_with_hitl_review_raises(self):
        """Sequence prompt + enable_hitl_review=True fails before the agent runs."""
        from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

        if not AIRFLOW_V_3_1_PLUS:
            pytest.skip("enable_hitl_review requires Airflow >= 3.1.0")

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: ["x", ImageUrl(url="https://example.com/x.png")],
            llm_conn_id="my_llm",
            enable_hitl_review=True,
        )
        with pytest.raises(TypeError, match="enable_hitl_review=True"):
            op.execute(context={})

    def test_execute_merges_op_kwargs_into_callable(self):
        mock_hook = _make_mock_hook(_make_run_result("done"))

        def my_prompt(topic):
            return f"Analyze {topic}"

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
            op_kwargs={"topic": "revenue trends"},
        )
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            op.execute(context={"task_instance": MagicMock()})

        assert op.prompt == "Analyze revenue trends"
        request = mock_hook.create_agent.call_args[0][0]
        assert request.prompt == "Analyze revenue trends"

    def test_execute_passes_toolsets_through(self):
        """Toolsets passed to the decorator are forwarded verbatim in AgentRunRequest."""
        mock_hook = _make_mock_hook(_make_run_result("result"))
        mock_toolset = MagicMock()

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Do something",
            llm_conn_id="my_llm",
            toolsets=[mock_toolset],
        )
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            op.execute(context={})

        request = mock_hook.create_agent.call_args[0][0]
        assert request.toolsets == [mock_toolset]

    def test_execute_structured_output(self):
        class Summary(BaseModel):
            text: str

        mock_hook = _make_mock_hook(_make_run_result(Summary(text="Great results")))

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "Summarize",
            llm_conn_id="my_llm",
            output_type=Summary,
        )
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            result = op.execute(context={})

        assert result == {"text": "Great results"}

    def test_durable_kwarg_passes_through_to_operator(self):
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "prompt",
            llm_conn_id="my_llm",
            durable=True,
        )
        assert op.durable is True

    def test_durable_default_false_through_decorator(self):
        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: "prompt",
            llm_conn_id="my_llm",
        )
        assert op.durable is False
