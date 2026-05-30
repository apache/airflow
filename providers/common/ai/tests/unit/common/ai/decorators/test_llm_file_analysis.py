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
from airflow.providers.common.ai.hooks.base import AgentRunResult, AgentUsage, BaseAIHook
from airflow.providers.common.ai.utils.file_analysis import FileAnalysisRequest


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


class TestLLMFileAnalysisDecoratedOperator:
    def test_custom_operator_name(self):
        assert _LLMFileAnalysisDecoratedOperator.custom_operator_name == "@task.llm_file_analysis"

    @patch(
        "airflow.providers.common.ai.operators.llm_file_analysis.build_file_analysis_request", autospec=True
    )
    def test_execute_calls_callable_and_returns_output(self, mock_build_request):
        mock_build_request.return_value = FileAnalysisRequest(
            user_content="prepared prompt",
            resolved_paths=["/tmp/app.log"],
            total_size_bytes=10,
        )
        mock_hook = _make_mock_hook(_make_run_result("This is a summary."))

        def my_prompt():
            return "Summarize this text"

        op = _LLMFileAnalysisDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
        )
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            result = op.execute(context={})

        assert result == "This is a summary."
        assert op.prompt == "Summarize this text"
        request = mock_hook.create_agent.call_args[0][0]
        assert request.prompt == "prepared prompt"

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

    @patch(
        "airflow.providers.common.ai.operators.llm_file_analysis.build_file_analysis_request", autospec=True
    )
    def test_execute_merges_op_kwargs_into_callable(self, mock_build_request):
        mock_build_request.return_value = FileAnalysisRequest(
            user_content="prepared prompt",
            resolved_paths=["/tmp/app.log"],
            total_size_bytes=10,
        )
        mock_hook = _make_mock_hook(_make_run_result("done"))

        def my_prompt(topic):
            return f"Summarize {topic}"

        op = _LLMFileAnalysisDecoratedOperator(
            task_id="test",
            python_callable=my_prompt,
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
            op_kwargs={"topic": "system logs"},
        )
        with patch.object(BaseAIHook, "get_agent_hook", return_value=mock_hook):
            op.execute(context={"task_instance": MagicMock(spec=["task_id"])})

        assert op.prompt == "Summarize system logs"
