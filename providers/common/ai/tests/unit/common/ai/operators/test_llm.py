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

from airflow.providers.common.ai.mixins.approval import (
    LLMApprovalMixin,
)
from airflow.providers.common.ai.operators.llm import LLMOperator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS


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


class TestLLMOperator:
    def test_template_fields(self):
        expected = {"prompt", "llm_conn_id", "model_id", "system_prompt", "agent_params"}
        assert set(LLMOperator.template_fields) == expected

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_returns_string_output(self, mock_hook_cls):
        """Default output_type=str returns the LLM string directly."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("Paris is the capital of France.")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(task_id="test", prompt="What is the capital of France?", llm_conn_id="my_llm")
        result = op.execute(context=MagicMock())

        assert result == "Paris is the capital of France."
        mock_agent.run_sync.assert_called_once_with("What is the capital of France?")
        mock_hook_cls.get_hook.return_value.create_agent.assert_called_once_with(
            output_type=str, instructions=""
        )
        mock_hook_cls.get_hook.assert_called_once_with("my_llm", hook_params={"model_id": None})

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_structured_output_with_all_params(self, mock_hook_cls):
        """Structured output via model_dump(), with model_id, system_prompt, and agent_params."""

        class Entities(BaseModel):
            names: list[str]

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(Entities(names=["Alice", "Bob"]))
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="test",
            prompt="Extract entities",
            llm_conn_id="my_llm",
            model_id="openai:gpt-5",
            system_prompt="You are an extractor.",
            output_type=Entities,
            agent_params={"retries": 3, "model_settings": {"temperature": 0.9}},
        )
        result = op.execute(context=MagicMock())

        assert result == {"names": ["Alice", "Bob"]}
        mock_hook_cls.get_hook.assert_called_once_with("my_llm", hook_params={"model_id": "openai:gpt-5"})
        mock_hook_cls.get_hook.return_value.create_agent.assert_called_once_with(
            output_type=Entities,
            instructions="You are an extractor.",
            retries=3,
            model_settings={"temperature": 0.9},
        )


def _make_context(ti_id=None):
    ti_id = ti_id or uuid4()
    ti = MagicMock()
    ti.id = ti_id
    return MagicMock(**{"__getitem__": lambda self, key: {"task_instance": ti}[key]})


@pytest.mark.skipif(
    not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
)
class TestLLMOperatorApproval:
    """Tests for LLMOperator with require_approval=True (LLMApprovalMixin integration)."""

    def test_inherits_llm_approval_mixin(self):
        assert issubclass(LLMOperator, LLMApprovalMixin)

    def test_default_approval_flags(self):
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c")
        assert op.require_approval is False
        assert op.allow_modifications is False
        assert op.approval_timeout is None

    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_defers(self, mock_hook_cls, mock_upsert, mock_trigger_cls):
        """When require_approval=True, execute() defers instead of returning output."""
        from airflow.providers.common.compat.sdk import TaskDeferred

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("LLM response")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="approval_test",
            prompt="Summarize this",
            llm_conn_id="my_llm",
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
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_and_modifications(self, mock_hook_cls, mock_upsert, mock_trigger_cls):
        """allow_modifications=True passes an editable 'output' param."""
        from airflow.providers.common.compat.sdk import TaskDeferred

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("draft output")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="mod_test",
            prompt="Write a draft",
            llm_conn_id="my_llm",
            require_approval=True,
            allow_modifications=True,
        )
        ctx = _make_context()

        with pytest.raises(TaskDeferred):
            op.execute(context=ctx)

        upsert_kwargs = mock_upsert.call_args[1]
        assert "output" in upsert_kwargs["params"]

    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_and_timeout(self, mock_hook_cls, mock_upsert, mock_trigger_cls):
        """approval_timeout is passed to the trigger."""
        from airflow.providers.common.compat.sdk import TaskDeferred

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("output")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        timeout = timedelta(hours=1)
        op = LLMOperator(
            task_id="timeout_test",
            prompt="p",
            llm_conn_id="my_llm",
            require_approval=True,
            approval_timeout=timeout,
        )
        ctx = _make_context()

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(context=ctx)

        assert exc_info.value.timeout == timeout

    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_structured_output(self, mock_hook_cls, mock_upsert, mock_trigger_cls):
        """Structured (BaseModel) output is serialized before deferring."""
        from airflow.providers.common.compat.sdk import TaskDeferred

        class Summary(BaseModel):
            text: str

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(Summary(text="hello"))
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="struct_test",
            prompt="Summarize",
            llm_conn_id="my_llm",
            output_type=Summary,
            require_approval=True,
        )
        ctx = _make_context()

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(context=ctx)

        assert exc_info.value.kwargs["generated_output"] == '{"text":"hello"}'

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_without_approval_returns_normally(self, mock_hook_cls):
        """When require_approval=False, execute() returns output directly."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result("plain output")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(task_id="no_approval", prompt="p", llm_conn_id="my_llm", require_approval=False)
        result = op.execute(context={})

        assert result == "plain output"

    def test_execute_complete_approved(self):
        """execute_complete returns output when approved."""
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c")
        event = {"chosen_options": ["Approve"], "responded_by_user": "admin"}

        result = op.execute_complete({}, generated_output="the output", event=event)

        assert result == "the output"

    def test_execute_complete_rejected(self):
        """execute_complete raises HITLRejectException when rejected."""
        from airflow.providers.standard.exceptions import HITLRejectException

        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c")
        event = {"chosen_options": ["Reject"], "responded_by_user": "admin"}

        with pytest.raises(HITLRejectException):
            op.execute_complete({}, generated_output="output", event=event)

    def test_execute_complete_with_error(self):
        """execute_complete raises HITLTriggerEventError on error event."""
        from airflow.providers.standard.exceptions import HITLTriggerEventError

        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c")
        event = {"error": "oops", "error_type": "unknown"}

        with pytest.raises(HITLTriggerEventError, match="oops"):
            op.execute_complete({}, generated_output="output", event=event)

    def test_execute_complete_with_modified_output(self):
        """execute_complete returns modified output when reviewer edits it."""
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c", allow_modifications=True)
        event = {
            "chosen_options": ["Approve"],
            "responded_by_user": "editor",
            "params_input": {"output": "edited"},
        }

        result = op.execute_complete({}, generated_output="original", event=event)

        assert result == "edited"
