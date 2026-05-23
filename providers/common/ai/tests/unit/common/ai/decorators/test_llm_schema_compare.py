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

from airflow.providers.common.ai.decorators.llm_schema_compare import _LLMSchemaCompareDecoratedOperator
from airflow.providers.common.ai.hooks.base_ai import AgentRunResult, AgentUsage, BaseAIHook
from airflow.providers.common.ai.operators.llm_schema_compare import (
    LLMSchemaCompareOperator,
    SchemaCompareResult,
)


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


def _make_compare_result():
    return SchemaCompareResult(
        mismatches=[],
        summary="Schemas are compatible.",
        compatible=True,
    )


class TestLLMSchemaCompareDecoratedOperator:
    def test_custom_operator_name(self):
        assert _LLMSchemaCompareDecoratedOperator.custom_operator_name == "@task.llm_schema_compare"

    @patch.object(LLMSchemaCompareOperator, "_build_schema_context", return_value="mocked schema")
    def test_execute_calls_callable_and_uses_result_as_prompt(self, mock_build_ctx):
        """The user's callable return value becomes the LLM prompt."""
        mock_hook = _make_mock_hook(_make_run_result(_make_compare_result()))

        def my_prompt_fn():
            return "Compare schemas and flag breaking changes"

        op = _LLMSchemaCompareDecoratedOperator(
            task_id="test",
            python_callable=my_prompt_fn,
            llm_conn_id="llm_conn",
            db_conn_ids=["postgres_default", "snowflake_default"],
            table_names=["test_table"],
        )
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            result = op.execute(context={})

        assert result["compatible"] is True
        assert op.prompt == "Compare schemas and flag breaking changes"

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None, b"bytes", bytearray(b"x"), [], ()],
        ids=["non-string", "empty", "whitespace", "none", "bytes", "bytearray", "empty-list", "empty-tuple"],
    )
    def test_execute_raises_on_invalid_prompt(self, return_value):
        """TypeError when the callable returns an unsupported prompt shape."""
        op = _LLMSchemaCompareDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            llm_conn_id="llm_conn",
            db_conn_ids=["postgres_default", "snowflake_default"],
            table_names=["test_table"],
        )
        with pytest.raises(TypeError, match="must be"):
            op.execute(context={})

    @patch.object(LLMSchemaCompareOperator, "_build_schema_context", return_value="mocked schema")
    def test_execute_accepts_sequence_prompt(self, mock_build_ctx):
        """A non-empty Sequence[UserContent] return value is forwarded as-is."""
        image = ImageUrl(url="https://example.com/x.png")
        prompt = ["Compare these schemas:", image]
        mock_hook = _make_mock_hook(_make_run_result(_make_compare_result()))

        def my_prompt_fn():
            return prompt

        op = _LLMSchemaCompareDecoratedOperator(
            task_id="test",
            python_callable=my_prompt_fn,
            llm_conn_id="llm_conn",
            db_conn_ids=["postgres_default", "snowflake_default"],
            table_names=["test_table"],
        )
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            op.execute(context={})

        assert op.prompt == prompt
        request = mock_hook.create_agent.call_args[0][0]
        assert request.prompt == prompt

    @patch.object(LLMSchemaCompareOperator, "_build_schema_context", return_value="mocked schema")
    def test_execute_merges_op_kwargs_into_callable(self, mock_build_ctx):
        """op_kwargs are resolved by the callable to build the prompt."""
        mock_hook = _make_mock_hook(_make_run_result(_make_compare_result()))

        def my_prompt_fn(target_env):
            return f"Compare schemas for {target_env} environment"

        op = _LLMSchemaCompareDecoratedOperator(
            task_id="test",
            python_callable=my_prompt_fn,
            llm_conn_id="llm_conn",
            op_kwargs={"target_env": "production"},
            db_conn_ids=["postgres_default", "snowflake_default"],
            table_names=["test_table"],
        )
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            op.execute(context={"task_instance": MagicMock()})

        assert op.prompt == "Compare schemas for production environment"
