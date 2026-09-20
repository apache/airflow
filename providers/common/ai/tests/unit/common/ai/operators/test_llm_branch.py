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

from decimal import Decimal
from enum import Enum
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import TypeAdapter
from pydantic_ai import Agent
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import FunctionModel

from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.operators.llm_branch import LLMBranchOperator
from airflow.providers.common.compat.sdk import Param, ParamValidationError, TaskDeferred
from airflow.providers.standard.exceptions import HITLRejectException
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_3_PLUS

if AIRFLOW_V_3_3_PLUS:
    # On 3.3+ cores require_approval pauses the task in AWAITING_INPUT; older cores defer to
    # HITLTrigger. Both signals carry method_name/kwargs/timeout, so the approval tests assert
    # against whichever pause signal the running core uses.
    from airflow.sdk.exceptions import TaskAwaitingInput as ApprovalPauseSignal
else:
    ApprovalPauseSignal = TaskDeferred  # type: ignore[assignment, misc]


class TestLLMBranchOperator:
    def test_inherits_from_skipmixin_is_true(self):
        assert LLMBranchOperator.inherits_from_skipmixin is True

    def test_template_fields(self):
        assert set(LLMBranchOperator.template_fields) == {*LLMOperator.template_fields, "branch_descriptions"}

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
    def test_execute_single_branch(self, mock_hook_cls, mock_do_branch, make_mock_run_result):
        """LLM returns a single enum member → do_branch receives a string."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_b": "task_b"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(downstream_enum.task_a)
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
    def test_execute_coerces_usage_limits_dict_before_run_sync(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        """A dict ``usage_limits`` is coerced into a real ``UsageLimits`` before ``run_sync``."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_b": "task_b"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(downstream_enum.task_a)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        mock_do_branch.return_value = "task_a"

        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick a branch",
            llm_conn_id="my_llm",
            usage_limits={"cost_limit": "0.5"},
        )
        op.downstream_task_ids = {"task_a", "task_b"}

        op.execute(MagicMock())

        _, kwargs = mock_agent.run_sync.call_args
        assert kwargs["usage_limits"].cost_limit == Decimal("0.5")

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_multi_branch(self, mock_hook_cls, mock_do_branch, make_mock_run_result):
        """allow_multiple_branches=True → LLM returns list of enums → do_branch receives list."""
        downstream_enum = Enum(
            "DownstreamTasks", {"task_a": "task_a", "task_b": "task_b", "task_c": "task_c"}
        )

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(
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
    def test_execute_rejects_empty_branch_selection(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        """LLM returning an empty list fails instead of skipping every downstream task."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result([])
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
    def test_system_prompt_forwarded(self, mock_hook_cls, mock_do_branch, make_mock_run_result):
        """system_prompt is passed to create_agent(instructions=...)."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(downstream_enum.task_a)
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
    def test_downstream_task_ids_used_for_enum(self, mock_hook_cls, mock_do_branch, make_mock_run_result):
        """The dynamic enum is built from self.downstream_task_ids."""
        downstream_enum = Enum(
            "DownstreamTasks", {"billing": "billing", "auth": "auth", "general": "general"}
        )

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(downstream_enum.billing)
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

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_enum_options_are_sorted_regardless_of_downstream_order(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        """The option order the model sees is sorted, not whatever order downstream_task_ids iterates in.

        ``downstream_task_ids`` is a set, so its iteration order depends on string hashing and
        differs between worker processes. Option order is part of the question for a classifier
        model, so it has to be the same on every worker. A reverse-sorted list stands in for an
        unlucky set order; without ``sorted()`` the enum comes out reversed and this fails.
        """
        downstream_enum = Enum(
            "DownstreamTasks", {"task_a": "task_a", "task_b": "task_b", "task_c": "task_c"}
        )

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(downstream_enum.task_a)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(task_id="test", prompt="Pick", llm_conn_id="my_llm")
        op.downstream_task_ids = ["task_c", "task_b", "task_a"]

        op.execute(MagicMock())

        output_type = mock_hook_cls.get_hook.return_value.create_agent.call_args.kwargs["output_type"]
        assert [m.value for m in output_type] == ["task_a", "task_b", "task_c"]

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_branch_descriptions_land_in_the_output_schema(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        """Each described option carries its description in the schema; an undescribed one carries none.

        This is the shape both a text model's tool schema and pydantic-ai's TypeSafe adapter read
        a per-option description from: ``anyOf`` of ``{const, description}``, not a bare ``enum``.
        """
        mock_agent = MagicMock(spec=["run_sync"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="route",
            prompt="Pick",
            llm_conn_id="my_llm",
            branch_descriptions={
                "handle_auth": "Sign-in, passwords, 2FA. Owns missing reset emails.",
                "handle_billing": "Invoices, charges, refunds.",
            },
        )
        op.downstream_task_ids = {"handle_general", "handle_billing", "handle_auth"}
        output_type = None

        def capture(**kwargs):
            nonlocal output_type
            output_type = kwargs["output_type"]
            mock_agent.run_sync.return_value = make_mock_run_result(output_type.handle_auth)
            return mock_agent

        mock_hook_cls.get_hook.return_value.create_agent.side_effect = capture

        op.execute(MagicMock())

        schema = TypeAdapter(output_type).json_schema()
        assert "enum" not in schema
        assert schema["anyOf"] == [
            {
                "const": "handle_auth",
                "type": "string",
                "description": "Sign-in, passwords, 2FA. Owns missing reset emails.",
            },
            {"const": "handle_billing", "type": "string", "description": "Invoices, charges, refunds."},
            {"const": "handle_general", "type": "string"},
        ]
        # Validation is still the enum: the output handling downstream is unchanged.
        assert [m.value for m in output_type] == ["handle_auth", "handle_billing", "handle_general"]
        mock_do_branch.assert_called_once_with(mock_do_branch.call_args.args[0], "handle_auth")

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_branch_descriptions_reach_the_model(self, mock_hook_cls, mock_do_branch):
        """Through a real pydantic-ai Agent, the descriptions are in the request the model receives.

        ``FunctionModel`` sits where every provider adapter sits and is handed the same
        ``output_tools`` schema, so this is the request as a model sees it, not the operator's
        view of it.
        """
        seen: dict = {}

        def model_fn(messages, info):
            tool = info.output_tools[0]
            seen["schema"] = tool.parameters_json_schema
            return ModelResponse(parts=[ToolCallPart(tool.name, {"response": "handle_billing"})])

        def create_agent(*, output_type, instructions, **_):
            return Agent(FunctionModel(model_fn), output_type=output_type, instructions=instructions)

        mock_hook_cls.get_hook.return_value.create_agent.side_effect = create_agent

        op = LLMBranchOperator(
            task_id="route",
            prompt="I was charged twice.",
            llm_conn_id="my_llm",
            system_prompt="Route the ticket.",
            branch_descriptions={"handle_billing": "Invoices, charges, refunds."},
        )
        op.downstream_task_ids = {"handle_auth", "handle_billing"}

        op.execute(MagicMock())

        options = seen["schema"]["$defs"]["DownstreamTasks"]["anyOf"]
        assert {o["const"]: o.get("description") for o in options} == {
            "handle_auth": None,
            "handle_billing": "Invoices, charges, refunds.",
        }
        mock_do_branch.assert_called_once_with(mock_do_branch.call_args.args[0], "handle_billing")

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_branch_descriptions_with_multiple_branches(self, mock_hook_cls, mock_do_branch):
        """With allow_multiple_branches the descriptions sit on the list's items."""
        seen: dict = {}

        def model_fn(messages, info):
            tool = info.output_tools[0]
            seen["schema"] = tool.parameters_json_schema
            return ModelResponse(
                parts=[ToolCallPart(tool.name, {"response": ["handle_shipping", "handle_packaging"]})]
            )

        def create_agent(*, output_type, instructions, **_):
            return Agent(FunctionModel(model_fn), output_type=output_type, instructions=instructions)

        mock_hook_cls.get_hook.return_value.create_agent.side_effect = create_agent

        op = LLMBranchOperator(
            task_id="classify",
            prompt="Shipping was slow and the box was damaged.",
            llm_conn_id="my_llm",
            allow_multiple_branches=True,
            branch_descriptions={"handle_shipping": "Late or lost deliveries."},
        )
        op.downstream_task_ids = {"handle_shipping", "handle_packaging"}

        op.execute(MagicMock())

        items = seen["schema"]["properties"]["response"]["items"]
        assert items == {"$ref": "#/$defs/DownstreamTasks"}
        options = seen["schema"]["$defs"]["DownstreamTasks"]["anyOf"]
        assert [o["const"] for o in options] == ["handle_packaging", "handle_shipping"]
        assert options[1]["description"] == "Late or lost deliveries."
        mock_do_branch.assert_called_once_with(
            mock_do_branch.call_args.args[0], ["handle_shipping", "handle_packaging"]
        )

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_branch_descriptions_unknown_key_fails_before_the_model_call(self, mock_hook_cls):
        """A key that is not a downstream task is a ValueError naming it and the valid choices."""
        op = LLMBranchOperator(
            task_id="route",
            prompt="Pick",
            llm_conn_id="my_llm",
            branch_descriptions={"handle_genral": "typo", "handle_auth": "ok"},
        )
        op.downstream_task_ids = {"handle_auth", "handle_general"}

        with pytest.raises(
            ValueError, match=r"'route' names \['handle_genral'\].*\['handle_auth', 'handle_general'\]"
        ):
            op.execute(MagicMock())

        mock_hook_cls.get_hook.return_value.create_agent.assert_not_called()

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_no_branch_descriptions_keeps_the_plain_enum_schema(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        """Without descriptions the schema is the bare enum it always was."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = LLMBranchOperator(task_id="route", prompt="Pick", llm_conn_id="my_llm")
        op.downstream_task_ids = {"task_b", "task_a"}
        output_type = None

        def capture(**kwargs):
            nonlocal output_type
            output_type = kwargs["output_type"]
            mock_agent.run_sync.return_value = make_mock_run_result(output_type.task_a)
            return mock_agent

        mock_hook_cls.get_hook.return_value.create_agent.side_effect = capture

        op.execute(MagicMock())

        schema = TypeAdapter(output_type).json_schema()
        assert schema["enum"] == ["task_a", "task_b"]
        assert "anyOf" not in schema

    def test_branch_descriptions_is_a_template_field(self):
        assert "branch_descriptions" in LLMBranchOperator.template_fields

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
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, mock_do_branch, make_mock_run_result
    ):
        """When require_approval=True, execute() pauses after the LLM choice, before do_branch."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_b": "task_b"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(downstream_enum.task_a)
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
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, mock_do_branch, make_mock_run_result
    ):
        """With allow_multiple_branches=True the choice is deferred as a JSON list."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_c": "task_c"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(
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
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, mock_do_branch, make_mock_run_result
    ):
        """The review body lists the valid branches and the editable param is an enum dropdown."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_b": "task_b"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(downstream_enum.task_a)
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
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, mock_do_branch, make_mock_run_result
    ):
        """With allow_multiple_branches the editable param is an array enum (multi-select)."""
        downstream_enum = Enum("DownstreamTasks", {"task_a": "task_a", "task_b": "task_b"})

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result([downstream_enum.task_a])
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
        event = {"chosen_options": ["Approve"], "responded_by_user": {"id": "u1", "name": "admin"}}
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
        event = {"chosen_options": ["Approve"], "responded_by_user": {"id": "u1", "name": "admin"}}
        ctx = _make_context()

        result = op.execute_complete(ctx, generated_output='["task_a","task_c"]', event=event)

        assert result == ["task_a", "task_c"]
        mock_do_branch.assert_called_once_with(ctx, ["task_a", "task_c"])

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("ignore_downstream_trigger_rules", "with_teardown", "expected"),
        [
            (False, True, {"op2"}),
            (False, False, {"op2"}),
            (True, True, {"op2", "op3"}),
            (True, False, {"op2", "op3", "op4"}),
        ],
    )
    @patch.object(LLMBranchOperator, "skip")
    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_reject_skips_downstream_except_teardowns(
        self, mock_do_branch, mock_skip, dag_maker, ignore_downstream_trigger_rules, with_teardown, expected
    ):
        with dag_maker(serialized=True):
            op1 = LLMBranchOperator(
                task_id="op1",
                prompt="p",
                llm_conn_id="c",
                require_approval=True,
                ignore_downstream_trigger_rules=ignore_downstream_trigger_rules,
            )
            op2 = EmptyOperator(task_id="op2")
            op3 = EmptyOperator(task_id="op3")
            op4 = EmptyOperator(task_id="op4")
            if with_teardown:
                op4.as_teardown()
            op1 >> op2 >> op3 >> op4
        event = {"chosen_options": ["Reject"], "responded_by_user": {"id": "u1", "name": "admin"}}
        ti = MagicMock()
        ctx = MagicMock(**{"__getitem__": lambda self, key: {"task": op1, "ti": ti}[key]})

        result = op1.execute_complete(ctx, generated_output="op2", event=event)

        assert result is None
        mock_skip.assert_called_once()
        assert mock_skip.call_args.kwargs["ti"] is ti
        assert {t.task_id for t in mock_skip.call_args.kwargs["tasks"]} == expected
        mock_do_branch.assert_not_called()

    @patch.object(LLMBranchOperator, "log")
    @patch.object(LLMBranchOperator, "skip")
    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_timed_out_reject_names_the_timeout_default(
        self, mock_do_branch, mock_skip, mock_log
    ):
        op = LLMBranchOperator(task_id="t", prompt="p", llm_conn_id="c")
        op.downstream_task_ids = {"task_a"}
        event = {"chosen_options": ["Reject"], "responded_by_user": None, "timedout": True}
        task = MagicMock()
        task.get_direct_relatives.return_value = []
        ctx = MagicMock(**{"__getitem__": lambda self, key: {"task": task, "ti": MagicMock()}[key]})

        op.execute_complete(ctx, generated_output="task_a", event=event)

        mock_log.info.assert_called_once_with(
            "Rejected by %s. Skipping downstream tasks...", "the approval timeout default"
        )

    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_reject_fails_with_fail_on_reject(self, mock_do_branch):
        op = LLMBranchOperator(task_id="t", prompt="p", llm_conn_id="c", fail_on_reject=True)
        op.downstream_task_ids = {"task_a", "task_b"}
        event = {"chosen_options": ["Reject"], "responded_by_user": {"id": "u1", "name": "admin"}}

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
            "responded_by_user": {"id": "u1", "name": "admin"},
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
            "responded_by_user": {"id": "u1", "name": "admin"},
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
            "responded_by_user": {"id": "u1", "name": "admin"},
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
            "responded_by_user": {"id": "u1", "name": "admin"},
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
            "responded_by_user": {"id": "u1", "name": "admin"},
            "params_input": {"output": modified},
        }

        with pytest.raises(ValueError, match="JSON list"):
            op.execute_complete(_make_context(), generated_output='["task_a"]', event=event)

        mock_do_branch.assert_not_called()
