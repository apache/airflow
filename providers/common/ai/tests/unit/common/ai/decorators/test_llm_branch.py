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
from pydantic_ai.messages import ImageUrl

from airflow.providers.common.ai.decorators.llm_branch import _LLMBranchDecoratedOperator
from airflow.providers.common.ai.hooks.base import AgentRunResult, AgentUsage, BaseAIHook
from airflow.providers.common.ai.operators.llm_branch import LLMBranchOperator


def _make_run_result(output):
    return AgentRunResult(
        output=output,
        model_name="test-model",
        usage=AgentUsage(requests=1),
    )


def _make_mock_hook(run_result):
    mock_hook = MagicMock()
    mock_hook.create_agent.return_value = MagicMock()
    mock_hook.run_agent.return_value = run_result
    return mock_hook


class TestLLMBranchDecoratedOperator:
    def test_custom_operator_name(self):
        assert _LLMBranchDecoratedOperator.custom_operator_name == "@task.llm_branch"

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_calls_callable_and_branches(self, mock_do_branch):
        """The callable's return value becomes the LLM prompt, LLM output goes through do_branch."""
        downstream_enum = Enum("DownstreamTasks", {"positive": "positive", "negative": "negative"})
        mock_hook = _make_mock_hook(_make_run_result(downstream_enum.positive))
        mock_do_branch.return_value = "positive"

        def my_prompt():
            return "Route this review"

        op = _LLMBranchDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
        )
        op.downstream_task_ids = {"positive", "negative"}

        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            result = op.execute(context={})

        assert result == "positive"
        assert op.prompt == "Route this review"
        mock_do_branch.assert_called_once()

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None, b"bytes", bytearray(b"x"), [], ()],
        ids=["non-string", "empty", "whitespace", "none", "bytes", "bytearray", "empty-list", "empty-tuple"],
    )
    def test_execute_raises_on_invalid_prompt(self, return_value):
        """TypeError when the callable returns an unsupported prompt shape."""
        op = _LLMBranchDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            llm_conn_id="my_llm",
        )
        with pytest.raises(TypeError, match="must be"):
            op.execute(context={})

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_accepts_sequence_prompt(self, mock_do_branch):
        """A non-empty Sequence[UserContent] return value is forwarded as-is."""
        downstream_enum = Enum("DownstreamTasks", {"positive": "positive"})
        image = ImageUrl(url="https://example.com/x.png")
        prompt = ["Route based on this image:", image]
        mock_hook = _make_mock_hook(_make_run_result(downstream_enum.positive))
        mock_do_branch.return_value = "positive"

        def my_prompt():
            return prompt

        op = _LLMBranchDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
        )
        op.downstream_task_ids = {"positive"}

        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            op.execute(context={})

        assert op.prompt == prompt
        request = mock_hook.create_agent.call_args[0][0]
        assert request.prompt == prompt

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_merges_op_kwargs_into_callable(self, mock_do_branch):
        """op_kwargs are resolved by the callable to build the prompt."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a"})
        mock_hook = _make_mock_hook(_make_run_result(downstream_enum.task_a))

        def my_prompt(ticket_type):
            return f"Route this {ticket_type} ticket"

        op = _LLMBranchDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
            op_kwargs={"ticket_type": "billing"},
        )
        op.downstream_task_ids = {"task_a"}

        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            op.execute(context={"task_instance": MagicMock()})

        assert op.prompt == "Route this billing ticket"
