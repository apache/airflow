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

from datetime import timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import BaseModel

from airflow.providers.common.ai.hooks.base_ai import AgentRunResult, AgentUsage
from airflow.providers.common.ai.operators.llm_file_analysis import LLMFileAnalysisOperator
from airflow.providers.common.ai.utils.file_analysis import FileAnalysisRequest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS


def _make_mock_run_result(output):
    return AgentRunResult(
        output=output,
        model_name="test-model",
        usage=AgentUsage(requests=1, tool_calls=0, input_tokens=0, output_tokens=0, total_tokens=0),
    )


def _make_context(ti_id=None):
    ti_id = ti_id or uuid4()
    ti = MagicMock(spec=["id"])
    ti.id = ti_id
    context = MagicMock(spec=["__getitem__"])
    context.__getitem__.side_effect = lambda key: {"task_instance": ti}[key]
    return context


class TestLLMFileAnalysisOperator:
    def test_template_fields(self):
        expected = {
            "prompt",
            "llm_conn_id",
            "model_id",
            "system_prompt",
            "agent_params",
            "file_path",
            "file_conn_id",
        }
        assert set(LLMFileAnalysisOperator.template_fields) == expected

    @patch("airflow.providers.common.ai.operators.llm.BaseAIHook", autospec=True)
    @patch(
        "airflow.providers.common.ai.operators.llm_file_analysis.build_file_analysis_request", autospec=True
    )
    def test_execute_returns_string_output(self, mock_build_request, mock_hook_cls):
        mock_build_request.return_value = FileAnalysisRequest(
            user_content="prepared prompt",
            resolved_paths=["/tmp/app.log"],
            total_size_bytes=10,
        )
        mock_agent = MagicMock()
        mock_hook_cls.get_agent_hook.return_value.create_agent.return_value = mock_agent
        mock_hook_cls.get_agent_hook.return_value.run_agent.return_value = _make_mock_run_result(
            "Analysis complete"
        )

        op = LLMFileAnalysisOperator(
            task_id="test",
            prompt="Summarize the file",
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
        )
        result = op.execute(context={})

        assert result == "Analysis complete"
        mock_build_request.assert_called_once_with(
            file_path="/tmp/app.log",
            file_conn_id=None,
            prompt="Summarize the file",
            multi_modal=False,
            max_files=20,
            max_file_size_bytes=5 * 1024 * 1024,
            max_total_size_bytes=20 * 1024 * 1024,
            max_text_chars=100_000,
            sample_rows=10,
        )
        mock_hook = mock_hook_cls.get_agent_hook.return_value
        request = mock_hook.create_agent.call_args[0][0]
        assert request.prompt == "prepared prompt"
        mock_hook.run_agent.assert_called_once_with(mock_agent, request)

    @patch("airflow.providers.common.ai.operators.llm.BaseAIHook", autospec=True)
    @patch(
        "airflow.providers.common.ai.operators.llm_file_analysis.build_file_analysis_request", autospec=True
    )
    def test_execute_structured_output_serializes_model(self, mock_build_request, mock_hook_cls):
        class Summary(BaseModel):
            findings: list[str]

        mock_build_request.return_value = FileAnalysisRequest(
            user_content="prepared prompt",
            resolved_paths=["/tmp/app.log"],
            total_size_bytes=10,
        )
        mock_agent = MagicMock()
        mock_hook_cls.get_agent_hook.return_value.create_agent.return_value = mock_agent
        mock_hook_cls.get_agent_hook.return_value.run_agent.return_value = _make_mock_run_result(
            Summary(findings=["error spike"])
        )

        op = LLMFileAnalysisOperator(
            task_id="test",
            prompt="Summarize the file",
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
            output_type=Summary,
        )
        result = op.execute(context={})

        assert result == {"findings": ["error spike"]}

    @patch(
        "airflow.providers.common.ai.operators.llm_file_analysis.build_file_analysis_request", autospec=True
    )
    def test_parameter_validation(self, mock_build_request):
        with pytest.raises(ValueError, match="max_files"):
            LLMFileAnalysisOperator(
                task_id="test",
                prompt="p",
                llm_conn_id="my_llm",
                file_path="/tmp/app.log",
                max_files=0,
            )
        with pytest.raises(ValueError, match="sample_rows"):
            LLMFileAnalysisOperator(
                task_id="test",
                prompt="p",
                llm_conn_id="my_llm",
                file_path="/tmp/app.log",
                sample_rows=0,
            )
        mock_build_request.assert_not_called()


@pytest.mark.skipif(
    not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
)
class TestLLMFileAnalysisOperatorApproval:
    class Summary(BaseModel):
        findings: list[str]

    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.BaseAIHook", autospec=True)
    @patch(
        "airflow.providers.common.ai.operators.llm_file_analysis.build_file_analysis_request", autospec=True
    )
    def test_execute_with_approval_defers(
        self, mock_build_request, mock_hook_cls, mock_upsert, mock_trigger_cls
    ):
        from airflow.providers.common.compat.sdk import TaskDeferred

        mock_build_request.return_value = FileAnalysisRequest(
            user_content="prepared prompt",
            resolved_paths=["/tmp/app.log"],
            total_size_bytes=10,
        )
        mock_agent = MagicMock()
        mock_hook_cls.get_agent_hook.return_value.create_agent.return_value = mock_agent
        mock_hook_cls.get_agent_hook.return_value.run_agent.return_value = _make_mock_run_result(
            "LLM response"
        )

        op = LLMFileAnalysisOperator(
            task_id="approval_test",
            prompt="Summarize this",
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
            require_approval=True,
        )
        ctx = _make_context()

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(context=ctx)

        assert exc_info.value.method_name == "execute_complete"
        assert exc_info.value.kwargs["generated_output"] == "LLM response"
        mock_upsert.assert_called_once()

    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.BaseAIHook", autospec=True)
    @patch(
        "airflow.providers.common.ai.operators.llm_file_analysis.build_file_analysis_request", autospec=True
    )
    def test_execute_with_approval_defers_structured_output_as_json(
        self, mock_build_request, mock_hook_cls, mock_upsert, mock_trigger_cls
    ):
        from airflow.providers.common.compat.sdk import TaskDeferred

        mock_build_request.return_value = FileAnalysisRequest(
            user_content="prepared prompt",
            resolved_paths=["/tmp/app.log"],
            total_size_bytes=10,
        )
        mock_agent = MagicMock()
        mock_hook_cls.get_agent_hook.return_value.create_agent.return_value = mock_agent
        mock_hook_cls.get_agent_hook.return_value.run_agent.return_value = _make_mock_run_result(
            self.Summary(findings=["error spike"])
        )

        op = LLMFileAnalysisOperator(
            task_id="approval_structured_test",
            prompt="Summarize this",
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
            output_type=self.Summary,
            require_approval=True,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(context=_make_context())

        assert exc_info.value.kwargs["generated_output"] == '{"findings":["error spike"]}'
        mock_upsert.assert_called_once()

    def test_execute_complete_with_approval_restores_structured_output(self):
        op = LLMFileAnalysisOperator(
            task_id="approval_complete_test",
            prompt="Summarize this",
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
            output_type=self.Summary,
            require_approval=True,
        )
        event = {"chosen_options": [op.APPROVE], "params_input": {}, "responded_by_user": "reviewer"}

        result = op.execute_complete({}, generated_output='{"findings":["error spike"]}', event=event)

        assert result == {"findings": ["error spike"]}

    def test_execute_complete_with_approval_restores_modified_structured_output(self):
        op = LLMFileAnalysisOperator(
            task_id="approval_complete_modified_test",
            prompt="Summarize this",
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
            output_type=self.Summary,
            require_approval=True,
            allow_modifications=True,
        )
        event = {
            "chosen_options": [op.APPROVE],
            "params_input": {"output": '{"findings":["reviewed output"]}'},
            "responded_by_user": "reviewer",
        }

        result = op.execute_complete({}, generated_output='{"findings":["error spike"]}', event=event)

        assert result == {"findings": ["reviewed output"]}

    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.BaseAIHook", autospec=True)
    @patch(
        "airflow.providers.common.ai.operators.llm_file_analysis.build_file_analysis_request", autospec=True
    )
    def test_execute_with_approval_timeout(
        self, mock_build_request, mock_hook_cls, mock_upsert, mock_trigger_cls
    ):
        from airflow.providers.common.compat.sdk import TaskDeferred

        mock_build_request.return_value = FileAnalysisRequest(
            user_content="prepared prompt",
            resolved_paths=["/tmp/app.log"],
            total_size_bytes=10,
        )
        mock_agent = MagicMock()
        mock_hook_cls.get_agent_hook.return_value.create_agent.return_value = mock_agent
        mock_hook_cls.get_agent_hook.return_value.run_agent.return_value = _make_mock_run_result("output")

        timeout = timedelta(hours=1)
        op = LLMFileAnalysisOperator(
            task_id="timeout_test",
            prompt="p",
            llm_conn_id="my_llm",
            file_path="/tmp/app.log",
            require_approval=True,
            approval_timeout=timeout,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(context=_make_context())

        assert exc_info.value.timeout == timeout
