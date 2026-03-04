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

from airflow.providers.common.ai.decorators.llm_sql import _LLMSQLDecoratedOperator


class TestLLMSQLDecoratedOperator:
    def test_custom_operator_name(self):
        assert _LLMSQLDecoratedOperator.custom_operator_name == "@task.llm_sql"

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_calls_callable_and_uses_result_as_prompt(self, mock_hook_cls):
        """The user's callable return value becomes the LLM prompt."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_result = MagicMock(spec=["output"])
        mock_result.output = "SELECT 1"
        mock_agent.run_sync.return_value = mock_result
        mock_hook_cls.return_value.create_agent.return_value = mock_agent

        def my_prompt_fn():
            return "Get all users"

        op = _LLMSQLDecoratedOperator(task_id="test", python_callable=my_prompt_fn, llm_conn_id="my_llm")
        result = op.execute(context={})

        assert result == "SELECT 1"
        assert op.prompt == "Get all users"
        mock_agent.run_sync.assert_called_once_with("Get all users")

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None],
        ids=["non-string", "empty", "whitespace", "none"],
    )
    def test_execute_raises_on_invalid_prompt(self, return_value):
        """TypeError when the callable returns a non-string or blank string."""
        op = _LLMSQLDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            llm_conn_id="my_llm",
        )
        with pytest.raises(TypeError, match="non-empty string"):
            op.execute(context={})

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_merges_op_kwargs_into_callable(self, mock_hook_cls):
        """op_kwargs are resolved by the callable to build the prompt."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_result = MagicMock(spec=["output"])
        mock_result.output = "SELECT 1"
        mock_agent.run_sync.return_value = mock_result
        mock_hook_cls.return_value.create_agent.return_value = mock_agent

        def my_prompt_fn(table_name):
            return f"Get all rows from {table_name}"

        op = _LLMSQLDecoratedOperator(
            task_id="test",
            python_callable=my_prompt_fn,
            llm_conn_id="my_llm",
            op_kwargs={"table_name": "users"},
        )
        op.execute(context={"task_instance": MagicMock()})

        assert op.prompt == "Get all rows from users"
