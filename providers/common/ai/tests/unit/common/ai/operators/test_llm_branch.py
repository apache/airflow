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
from uuid import uuid4

import pytest

from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.operators.llm_branch import LLMBranchOperator
from airflow.providers.common.compat.sdk import Param, ParamValidationError, TaskDeferred
from airflow.providers.standard.exceptions import HITLRejectException

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_3_PLUS

if AIRFLOW_V_3_3_PLUS:
    # On 3.3+ cores require_approval pauses the task in AWAITING_INPUT; older cores defer to
    # HITLTrigger. Both signals carry method_name/kwargs/timeout, so the approval tests assert
    # against whichever pause signal the running core uses.
    from airflow.sdk.exceptions import TaskAwaitingInput as ApprovalPauseSignal
else:
    ApprovalPauseSignal = TaskDeferred  # type: ignore[assignment, misc]


def _make_mock_run_result(output):
    """Create a mock AgentRunResult compatible with log_run_summary."""
    mock_result = MagicMock()
    mock_result.output = output
    mock_result.usage = MagicMock(requests=1, tool_calls=0, input_tokens=0, output_tokens=0, total_tokens=0)
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
        mock_agent.run_sync.assert_called_once_with("Pick a branch", usage_limits=None)

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
    def test_execute_rejects_empty_branch_selection(self, mock_hook_cls, mock_do_branch):
        """LLM returning an empty list fails instead of skipping every downstream task."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result([])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick branches",
            llm_conn_id="my_llm",
            allow_multiple_branches=True,
        )
        op.downstream_task_ids = {"task_a", "task_b"}

        with pytest.raises(ValueError, match="selected no branches"):
            op.execute(MagicMock())
        mock_do_branch.assert_not_called()

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


def _make_context(ti_id=None):
    ti_id = ti_id or uuid4()
    ti = MagicMock()
    ti.id = ti_id
    return MagicMock(**{"__getitem__": lambda self, key: {"task_instance": ti}[key]})


@pytest.mark.skipif(
    not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
)
class TestLLMBranchOperatorApproval:
    """Tests for LLMBranchOperator with require_approval=True (LLMApprovalMixin integration)."""

    def test_inherits_llm_approval_mixin(self):
        assert issubclass(LLMBranchOperator, LLMApprovalMixin)

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_pauses_before_branching(
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, mock_do_branch
    ):
        """When require_approval=True, execute() pauses after the LLM choice, before do_branch."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_b": "task_b"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(downstream_enum.task_a)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="branch_approval",
            prompt="Pick a branch",
            llm_conn_id="my_llm",
            require_approval=True,
        )
        op.downstream_task_ids = {"task_a", "task_b"}

        with pytest.raises(ApprovalPauseSignal) as exc_info:
            op.execute(_make_context())

        assert exc_info.value.method_name == "execute_complete"
        assert exc_info.value.kwargs["generated_output"] == "task_a"
        mock_upsert.assert_called_once()
        mock_do_branch.assert_not_called()

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_serializes_multiple_branches(
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, mock_do_branch
    ):
        """With allow_multiple_branches=True the choice is deferred as a JSON list."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_c": "task_c"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(
            [downstream_enum.task_a, downstream_enum.task_c]
        )
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="branch_approval_multi",
            prompt="Pick branches",
            llm_conn_id="my_llm",
            allow_multiple_branches=True,
            require_approval=True,
        )
        op.downstream_task_ids = {"task_a", "task_b", "task_c"}

        with pytest.raises(ApprovalPauseSignal) as exc_info:
            op.execute(_make_context())

        assert exc_info.value.kwargs["generated_output"] == '["task_a","task_c"]'
        mock_do_branch.assert_not_called()

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_review_form_lists_choices_and_renders_enum_dropdown(
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, mock_do_branch
    ):
        """The review body lists the valid branches and the editable param is an enum dropdown."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_b": "task_b"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result(downstream_enum.task_a)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="branch_approval",
            prompt="Pick a branch",
            llm_conn_id="my_llm",
            require_approval=True,
            allow_modifications=True,
        )
        op.downstream_task_ids = {"task_b", "task_a"}

        with pytest.raises(ApprovalPauseSignal):
            op.execute(_make_context())

        call_kwargs = mock_upsert.call_args.kwargs
        assert call_kwargs["body"].startswith("Valid branches: `task_a`, `task_b`")
        assert call_kwargs["params"]["output"]["schema"] == {
            "type": "string",
            "enum": ["task_a", "task_b"],
        }

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_review_form_multi_branch_renders_multiselect(
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, mock_do_branch
    ):
        """With allow_multiple_branches the editable param is an array enum (multi-select)."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_b": "task_b"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _make_mock_run_result([downstream_enum.task_a])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="branch_approval_multi",
            prompt="Pick branches",
            llm_conn_id="my_llm",
            allow_multiple_branches=True,
            require_approval=True,
            allow_modifications=True,
        )
        op.downstream_task_ids = {"task_a", "task_b"}

        with pytest.raises(ApprovalPauseSignal):
            op.execute(_make_context())

        call_kwargs = mock_upsert.call_args.kwargs
        assert "Valid branches: `task_a`, `task_b`" in call_kwargs["body"]
        assert call_kwargs["params"]["output"]["schema"] == {
            "type": "array",
            "items": {"type": "string", "enum": ["task_a", "task_b"]},
            "examples": ["task_a", "task_b"],
        }
        assert call_kwargs["params"]["output"]["value"] == ["task_a"]

        schema = call_kwargs["params"]["output"]["schema"]
        assert Param(schema=schema).resolve(["task_a"]) == ["task_a"]
        with pytest.raises(ParamValidationError):
            Param(schema=schema).resolve(["task_x"])

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_rejects_sequence_prompt_with_require_approval(self, mock_hook_cls):
        """Non-string prompt + require_approval=True fails before the agent runs."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="test",
            prompt=["describe", b"bytes"],  # type: ignore[arg-type]
            llm_conn_id="my_llm",
            require_approval=True,
        )
        op.downstream_task_ids = {"task_a"}

        with pytest.raises(TypeError, match="require_approval=True"):
            op.execute(_make_context())

        mock_agent.run_sync.assert_not_called()

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_approved_single_branch(self, mock_do_branch):
        """execute_complete branches into the approved task."""
        mock_do_branch.return_value = "task_a"
        op = LLMBranchOperator(task_id="t", prompt="p", llm_conn_id="c")
        op.downstream_task_ids = {"task_a", "task_b"}
        event = {"chosen_options": ["Approve"], "responded_by_user": "admin"}
        ctx = _make_context()

        result = op.execute_complete(ctx, generated_output="task_a", event=event)

        assert result == "task_a"
        mock_do_branch.assert_called_once_with(ctx, "task_a")

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_approved_multiple_branches(self, mock_do_branch):
        """execute_complete parses the JSON list back before branching."""
        mock_do_branch.return_value = ["task_a", "task_c"]
        op = LLMBranchOperator(task_id="t", prompt="p", llm_conn_id="c", allow_multiple_branches=True)
        op.downstream_task_ids = {"task_a", "task_b", "task_c"}
        event = {"chosen_options": ["Approve"], "responded_by_user": "admin"}
        ctx = _make_context()

        result = op.execute_complete(ctx, generated_output='["task_a","task_c"]', event=event)

        assert result == ["task_a", "task_c"]
        mock_do_branch.assert_called_once_with(ctx, ["task_a", "task_c"])

    @patch.object(LLMBranchOperator, "skip")
    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_reject_skips_downstream_except_teardowns(self, mock_do_branch, mock_skip):
        op = LLMBranchOperator(task_id="t", prompt="p", llm_conn_id="c")
        op.downstream_task_ids = {"task_a", "cleanup"}
        event = {"chosen_options": ["Reject"], "responded_by_user": "admin"}
        task_a = MagicMock(is_teardown=False)
        cleanup = MagicMock(is_teardown=True)
        task = MagicMock()
        task.get_direct_relatives.return_value = [task_a, cleanup]
        ti = MagicMock()
        ctx = MagicMock(**{"__getitem__": lambda self, key: {"task": task, "ti": ti}[key]})

        result = op.execute_complete(ctx, generated_output="task_a", event=event)

        assert result is None
        task.get_direct_relatives.assert_called_once_with(upstream=False)
        mock_skip.assert_called_once()
        assert mock_skip.call_args.kwargs["ti"] is ti
        assert list(mock_skip.call_args.kwargs["tasks"]) == [task_a]
        mock_do_branch.assert_not_called()

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_reject_fails_with_fail_on_reject(self, mock_do_branch):
        op = LLMBranchOperator(task_id="t", prompt="p", llm_conn_id="c", fail_on_reject=True)
        op.downstream_task_ids = {"task_a", "task_b"}
        event = {"chosen_options": ["Reject"], "responded_by_user": "admin"}

        with pytest.raises(HITLRejectException, match="rejected"):
            op.execute_complete(_make_context(), generated_output="task_a", event=event)

        mock_do_branch.assert_not_called()

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_with_modified_branch(self, mock_do_branch):
        """A reviewer-modified branch is used when it is a valid downstream task."""
        mock_do_branch.return_value = "task_b"
        op = LLMBranchOperator(task_id="t", prompt="p", llm_conn_id="c", allow_modifications=True)
        op.downstream_task_ids = {"task_a", "task_b"}
        event = {
            "chosen_options": ["Approve"],
            "responded_by_user": "admin",
            "params_input": {"output": "task_b"},
        }
        ctx = _make_context()

        result = op.execute_complete(ctx, generated_output="task_a", event=event)

        assert result == "task_b"
        mock_do_branch.assert_called_once_with(ctx, "task_b")

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_with_multiselect_modified_branches(self, mock_do_branch):
        """A list submitted by the multi-select review form branches into those tasks."""
        mock_do_branch.return_value = ["task_b", "task_c"]
        op = LLMBranchOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            allow_multiple_branches=True,
            allow_modifications=True,
        )
        op.downstream_task_ids = {"task_a", "task_b", "task_c"}
        event = {
            "chosen_options": ["Approve"],
            "responded_by_user": "admin",
            "params_input": {"output": ["task_b", "task_c"]},
        }
        ctx = _make_context()

        result = op.execute_complete(ctx, generated_output='["task_a"]', event=event)

        assert result == ["task_b", "task_c"]
        mock_do_branch.assert_called_once_with(ctx, ["task_b", "task_c"])

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_rejects_invalid_modified_branch(self, mock_do_branch):
        """A reviewer-modified branch outside downstream_task_ids fails validation."""
        op = LLMBranchOperator(task_id="t", prompt="p", llm_conn_id="c", allow_modifications=True)
        op.downstream_task_ids = {"task_a", "task_b"}
        event = {
            "chosen_options": ["Approve"],
            "responded_by_user": "admin",
            "params_input": {"output": "task_x"},
        }

        with pytest.raises(ValueError, match="not downstream tasks"):
            op.execute_complete(_make_context(), generated_output="task_a", event=event)

        mock_do_branch.assert_not_called()

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_rejects_empty_branch_list(self, mock_do_branch):
        """A reviewed empty list would skip every downstream task and must be rejected."""
        op = LLMBranchOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            allow_multiple_branches=True,
            allow_modifications=True,
        )
        op.downstream_task_ids = {"task_a", "task_b"}
        event = {
            "chosen_options": ["Approve"],
            "responded_by_user": "admin",
            "params_input": {"output": "[]"},
        }

        with pytest.raises(ValueError, match="selects no branches"):
            op.execute_complete(_make_context(), generated_output='["task_a"]', event=event)

        mock_do_branch.assert_not_called()

    @pytest.mark.parametrize(
        "modified",
        ["not json", '{"task_a": 1}', '["task_a", 2]'],
        ids=["malformed-json", "not-a-list", "non-string-item"],
    )
    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_rejects_invalid_multi_branch_shapes(self, mock_do_branch, modified):
        """With allow_multiple_branches=True the reviewed output must be a JSON list of strings."""
        op = LLMBranchOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            allow_multiple_branches=True,
            allow_modifications=True,
        )
        op.downstream_task_ids = {"task_a", "task_b"}
        event = {
            "chosen_options": ["Approve"],
            "responded_by_user": "admin",
            "params_input": {"output": modified},
        }

        with pytest.raises(ValueError, match="JSON list"):
            op.execute_complete(_make_context(), generated_output='["task_a"]', event=event)

        mock_do_branch.assert_not_called()
