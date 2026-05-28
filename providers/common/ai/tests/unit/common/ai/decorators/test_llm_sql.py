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
from pydantic_ai.messages import ImageUrl

from airflow.providers.common.ai.decorators.llm_sql import _LLMSQLDecoratedOperator
from airflow.providers.common.ai.hooks.base_ai import AgentRunResult, AgentUsage, BaseAIHook


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


class TestLLMSQLDecoratedOperator:
    def test_custom_operator_name(self):
        assert _LLMSQLDecoratedOperator.custom_operator_name == "@task.llm_sql"

    def test_execute_calls_callable_and_uses_result_as_prompt(self):
        """The user's callable return value becomes the LLM prompt."""
        mock_hook = _make_mock_hook(_make_run_result("SELECT 1"))

        def my_prompt_fn():
            return "Get all users"

        op = _LLMSQLDecoratedOperator(task_id="test", python_callable=my_prompt_fn, llm_conn_id="my_llm")
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            result = op.execute(context={})

        assert result == "SELECT 1"
        assert op.prompt == "Get all users"
        request = mock_hook.create_agent.call_args[0][0]
        assert request.prompt == "Get all users"

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None, b"bytes", bytearray(b"x"), [], ()],
        ids=["non-string", "empty", "whitespace", "none", "bytes", "bytearray", "empty-list", "empty-tuple"],
    )
    def test_execute_raises_on_invalid_prompt(self, return_value):
        """TypeError when the callable returns an unsupported prompt shape."""
        op = _LLMSQLDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            llm_conn_id="my_llm",
        )
        with pytest.raises(TypeError, match="must be"):
            op.execute(context={})

    def test_execute_accepts_sequence_prompt(self):
        """A non-empty Sequence[UserContent] return value is forwarded as-is."""
        image = ImageUrl(url="https://example.com/x.png")
        prompt = ["Write SQL for this diagram:", image]
        mock_hook = _make_mock_hook(_make_run_result("SELECT 1"))

        def my_prompt_fn():
            return prompt

        op = _LLMSQLDecoratedOperator(task_id="test", python_callable=my_prompt_fn, llm_conn_id="my_llm")
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            op.execute(context={})

        assert op.prompt == prompt
        request = mock_hook.create_agent.call_args[0][0]
        assert request.prompt == prompt

    def test_sequence_prompt_with_require_approval_raises(self):
        """Sequence prompt + require_approval=True fails before the agent runs."""
        op = _LLMSQLDecoratedOperator(
            task_id="test",
            python_callable=lambda: ["x", ImageUrl(url="https://example.com/x.png")],
            llm_conn_id="my_llm",
            require_approval=True,
        )
        with pytest.raises(TypeError, match="require_approval=True"):
            op.execute(context={})

    def test_execute_merges_op_kwargs_into_callable(self):
        """op_kwargs are resolved by the callable to build the prompt."""
        mock_hook = _make_mock_hook(_make_run_result("SELECT 1"))

        def my_prompt_fn(table_name):
            return f"Get all rows from {table_name}"

        op = _LLMSQLDecoratedOperator(
            task_id="test",
            python_callable=my_prompt_fn,
            llm_conn_id="my_llm",
            op_kwargs={"table_name": "users"},
        )
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            op.execute(context={"task_instance": MagicMock()})

        assert op.prompt == "Get all rows from users"
