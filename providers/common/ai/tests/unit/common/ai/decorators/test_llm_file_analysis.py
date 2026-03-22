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

from airflow.providers.common.ai.decorators.llm_file_analysis import _LLMFileAnalysisDecoratedOperator
from airflow.providers.common.ai.utils.file_analysis import FileAnalysisRequest


def _make_mock_run_result(output):
    mock_result = MagicMock(spec=["output", "usage", "response", "all_messages"])
    mock_result.output = output
    mock_result.usage.return_value = MagicMock(
        spec=["requests", "tool_calls", "input_tokens", "output_tokens", "total_tokens"],
        requests=1,
        tool_calls=0,
        input_tokens=0,
        output_tokens=0,
        total_tokens=0,
    )
    mock_result.response = MagicMock(spec=["model_name"], model_name="test-model")
    mock_result.all_messages.return_value = []
    return mock_result


class TestLLMFileAnalysisDecoratedOperator:
    def test_custom_operator_name(self):
        assert _LLMFileAnalysisDecoratedOperator.custom_operator_name == "@task.llm_file_analysis"

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    @patch(
        "airflow.providers.common.ai.operators.llm_file_analysis.build_file_analysis_request", autospec=True
    )
    def test_execute_calls_callable_and_returns_output(self, mock_build_request, mock_hook_cls):
        mock_build_request.return_value = FileAnalysisRequest(
            user_content="prepared prompt",
            resolved_paths=["/tmp/app.log"],
            total_size_bytes=10,
        )
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("This is a summary.")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        def my_prompt():
            return "Summarize this text"

        op = _LLMFileAnalysisDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
        )
        result = op.execute(context={})

        assert result == "This is a summary."
        assert op.prompt == "Summarize this text"
        mock_agent.run_sync.assert_called_once_with("prepared prompt")

    @pytest.mark.parametrize(
        "return_value",
        [42, "", "   ", None],
        ids=["non-string", "empty", "whitespace", "none"],
    )
    def test_execute_raises_on_invalid_prompt(self, return_value):
        op = _LLMFileAnalysisDecoratedOperator(
            task_id="test",
            python_callable=lambda: return_value,
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
        )
        with pytest.raises(TypeError, match="non-empty string"):
            op.execute(context={})

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    @patch(
        "airflow.providers.common.ai.operators.llm_file_analysis.build_file_analysis_request", autospec=True
    )
    def test_execute_merges_op_kwargs_into_callable(self, mock_build_request, mock_hook_cls):
        mock_build_request.return_value = FileAnalysisRequest(
            user_content="prepared prompt",
            resolved_paths=["/tmp/app.log"],
            total_size_bytes=10,
        )
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("done")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        def my_prompt(topic):
            return f"Summarize {topic}"

        op = _LLMFileAnalysisDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
            op_kwargs={"topic": "system logs"},
        )
        op.execute(context={"task_instance": MagicMock(spec=["task_id"])})

        assert op.prompt == "Summarize system logs"
        mock_agent.run_sync.assert_called_once_with("prepared prompt")
