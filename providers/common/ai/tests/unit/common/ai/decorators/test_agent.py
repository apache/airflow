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
from airflow.providers.common.ai.toolsets.logging import LoggingToolset

try:
    from airflow.sdk.serde import SUPPORTS_OPERATOR_DESERIALIZATION_WALKER as _CORE_WALKER
except ImportError:
    _CORE_WALKER = False

requires_typed_xcom = pytest.mark.skipif(
    not _CORE_WALKER,
    reason="Requires a core with the worker-side deserialization-class walk.",
)


class Summary(BaseModel):
    text: str


def _make_mock_run_result(output):
    """Create a mock AgentRunResult compatible with log_run_summary."""
    mock_result = MagicMock()
    mock_result.output = output
    mock_result.usage = MagicMock(requests=1, tool_calls=0, input_tokens=0, output_tokens=0, total_tokens=0)
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
        mock_agent.run_sync.assert_called_once_with("Who is our top customer?", usage_limits=None)

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

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_accepts_sequence_prompt(self, mock_hook_cls):
        """A non-empty Sequence[UserContent] return value is forwarded to run_sync as-is."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        image = ImageUrl(url="https://example.com/x.png")
        prompt = ["Describe this:", image]

        def my_prompt():
            return prompt

        op = _AgentDecoratedOperator(task_id="test", python_callable=my_prompt, llm_conn_id="my_llm")
        op.execute(context={})

        assert op.prompt == prompt
        mock_agent.run_sync.assert_called_once_with(prompt, usage_limits=None)

    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_sequence_prompt_with_hitl_review_raises_before_run_sync(self, mock_hook_cls):
        """Sequence prompt + enable_hitl_review=True fails before the agent runs."""
        from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

        if not AIRFLOW_V_3_1_PLUS:
            pytest.skip("enable_hitl_review requires Airflow >= 3.1.0")

        mock_agent = MagicMock(spec=["run_sync"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = _AgentDecoratedOperator(
            task_id="test",
            python_callable=lambda: ["x", ImageUrl(url="https://example.com/x.png")],
            llm_conn_id="my_llm",
            enable_hitl_review=True,
        )
        with pytest.raises(TypeError, match="enable_hitl_review=True"):
            op.execute(context={})

        mock_agent.run_sync.assert_not_called()

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
        mock_agent.run_sync.assert_called_once_with("Analyze revenue trends", usage_limits=None)

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

    @requires_typed_xcom
    @patch("airflow.providers.common.ai.operators.agent.PydanticAIHook", autospec=True)
    def test_execute_structured_output(self, mock_hook_cls):
        """BaseModel output flows through XCom as the Pydantic instance."""
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

        assert isinstance(result, Summary)
        assert result.text == "Great results"

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
