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

from enum import Enum
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.ai.decorators.llm_branch import _LLMBranchDecoratedOperator
from airflow.providers.common.ai.operators.llm_branch import LLMBranchOperator


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


class TestLLMBranchDecoratedOperator:
    def test_custom_operator_name(self):
        assert _LLMBranchDecoratedOperator.custom_operator_name == "@task.llm_branch"

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_calls_callable_and_branches(self, mock_hook_cls, mock_do_branch):
        """The callable's return value becomes the LLM prompt, LLM output goes through do_branch."""
        downstream_enum = Enum("DownstreamTasks", {"positive": "positive", "negative": "negative"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(downstream_enum.positive)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        mock_do_branch.return_value = "positive"

        def my_prompt():
            return "Route this review"

        op = _LLMBranchDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
        )
        op.downstream_task_ids = {"positive", "negative"}

        result = op.execute(context={})

        assert result == "positive"
        assert op.prompt == "Route this review"
        mock_agent.run_sync.assert_called_once_with("Route this review")
        mock_do_branch.assert_called_once()

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None],
        ids=["non-string", "empty", "whitespace", "none"],
    )
    def test_execute_raises_on_invalid_prompt(self, return_value):
        """TypeError when the callable returns a non-string or blank string."""
        op = _LLMBranchDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            llm_conn_id="my_llm",
        )
        with pytest.raises(TypeError, match="non-empty string"):
            op.execute(context={})

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_merges_op_kwargs_into_callable(self, mock_hook_cls, mock_do_branch):
        """op_kwargs are resolved by the callable to build the prompt."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(downstream_enum.task_a)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        def my_prompt(ticket_type):
            return f"Route this {ticket_type} ticket"

        op = _LLMBranchDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
            op_kwargs={"ticket_type": "billing"},
        )
        op.downstream_task_ids = {"task_a"}

        op.execute(context={"task_instance": MagicMock()})

        assert op.prompt == "Route this billing ticket"
