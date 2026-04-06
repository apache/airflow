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

from airflow.providers.common.ai.operators.llm import LLMOperator
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


class TestLLMBranchOperator:
    def test_inherits_from_skipmixin_is_true(self):
        assert LLMBranchOperator.inherits_from_skipmixin is True

    def test_template_fields(self):
        assert set(LLMBranchOperator.template_fields) == set(LLMOperator.template_fields)

    def test_output_type_ignored(self):
        """Passing output_type= doesn't break anything; it's silently dropped."""
        op = LLMBranchOperator(
            task_id="test",
            prompt="pick a branch",
            llm_conn_id="my_llm",
            output_type=int,
        )
        # output_type is overridden to str (the LLMOperator default) since
        # the real output_type is built dynamically from downstream_task_ids
        assert op.output_type is str

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_single_branch(self, mock_hook_cls, mock_do_branch):
        """LLM returns a single enum member → do_branch receives a string."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_b": "task_b"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(downstream_enum.task_a)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        mock_do_branch.return_value = "task_a"

        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick a branch",
            llm_conn_id="my_llm",
        )
        op.downstream_task_ids = {"task_a", "task_b"}

        ctx = MagicMock()
        result = op.execute(ctx)

        assert result == "task_a"
        mock_do_branch.assert_called_once_with(ctx, "task_a")
        mock_agent.run_sync.assert_called_once_with("Pick a branch")

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_multi_branch(self, mock_hook_cls, mock_do_branch):
        """allow_multiple_branches=True → LLM returns list of enums → do_branch receives list."""
        downstream_enum = Enum(
            "DownstreamTasks", {"task_a": "task_a", "task_b": "task_b", "task_c": "task_c"}
        )

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(
            [downstream_enum.task_a, downstream_enum.task_c]
        )
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        mock_do_branch.return_value = ["task_a", "task_c"]

        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick branches",
            llm_conn_id="my_llm",
            allow_multiple_branches=True,
        )
        op.downstream_task_ids = {"task_a", "task_b", "task_c"}

        ctx = MagicMock()
        result = op.execute(ctx)

        assert result == ["task_a", "task_c"]
        mock_do_branch.assert_called_once_with(ctx, ["task_a", "task_c"])

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_system_prompt_forwarded(self, mock_hook_cls, mock_do_branch):
        """system_prompt is passed to create_agent(instructions=...)."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(downstream_enum.task_a)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick",
            llm_conn_id="my_llm",
            system_prompt="Route tickets to the right team.",
        )
        op.downstream_task_ids = {"task_a"}

        op.execute(MagicMock())

        call_kwargs = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert call_kwargs.kwargs["instructions"] == "Route tickets to the right team."

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_downstream_task_ids_used_for_enum(self, mock_hook_cls, mock_do_branch):
        """The dynamic enum is built from self.downstream_task_ids."""
        downstream_enum = Enum(
            "DownstreamTasks", {"billing": "billing", "auth": "auth", "general": "general"}
        )

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(downstream_enum.billing)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick",
            llm_conn_id="my_llm",
        )
        op.downstream_task_ids = {"billing", "auth", "general"}

        op.execute(MagicMock())

        output_type = mock_hook_cls.get_hook.return_value.create_agent.call_args.kwargs["output_type"]
        assert {m.value for m in output_type} == {"billing", "auth", "general"}

    def test_execute_raises_on_no_downstream_tasks(self):
        """ValueError when the operator has no downstream tasks."""
        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick",
            llm_conn_id="my_llm",
        )
        with pytest.raises(ValueError, match="no downstream tasks"):
            op.execute(MagicMock())
