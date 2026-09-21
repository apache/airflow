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
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import TypeAdapter, ValidationError
from pydantic_ai import Agent
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import FunctionModel

from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.operators.llm_branch import BranchOption, DecisionPolicy, LLMBranchOperator
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
        assert set(LLMBranchOperator.template_fields) == {*LLMOperator.template_fields, "branches"}

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

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("task_a")
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

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("task_a")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        mock_do_branch.return_value = "task_a"

        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick a branch",
            llm_conn_id="my_llm",
            usage_limits={"cost_limit": "0.5"},
        )
        op.downstream_task_ids = {"task_a", "task_b"}

        op.execute(MagicMock(spec=dict))

        _, kwargs = mock_agent.run_sync.call_args
        assert kwargs["usage_limits"].cost_limit == Decimal("0.5")

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_multi_branch(self, mock_hook_cls, mock_do_branch, make_mock_run_result):
        """allow_multiple_branches=True → LLM returns list of enums → do_branch receives list."""

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(["task_a", "task_c"])
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
            op.execute(MagicMock(spec=dict))
        mock_do_branch.assert_not_called()

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_system_prompt_forwarded(self, mock_hook_cls, mock_do_branch, make_mock_run_result):
        """system_prompt is passed to create_agent(instructions=...)."""

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("task_a")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick",
            llm_conn_id="my_llm",
            system_prompt="Route tickets to the right team.",
        )
        op.downstream_task_ids = {"task_a"}

        op.execute(MagicMock(spec=dict))

        call_kwargs = mock_hook_cls.get_hook.return_value.create_agent.call_args
        assert call_kwargs.kwargs["instructions"] == "Route tickets to the right team."

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_downstream_task_ids_used_for_enum(self, mock_hook_cls, mock_do_branch, make_mock_run_result):
        """The dynamic enum is built from self.downstream_task_ids."""

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("billing")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick",
            llm_conn_id="my_llm",
        )
        op.downstream_task_ids = {"billing", "auth", "general"}

        op.execute(MagicMock(spec=dict))

        output_type = mock_hook_cls.get_hook.return_value.create_agent.call_args.kwargs["output_type"]
        assert set(TypeAdapter(output_type).json_schema()["enum"]) == {"billing", "auth", "general"}

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

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("task_a")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(task_id="test", prompt="Pick", llm_conn_id="my_llm")
        op.downstream_task_ids = ["task_c", "task_b", "task_a"]

        op.execute(MagicMock(spec=dict))

        output_type = mock_hook_cls.get_hook.return_value.create_agent.call_args.kwargs["output_type"]
        assert TypeAdapter(output_type).json_schema()["enum"] == ["task_a", "task_b", "task_c"]

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_branch_descriptions_land_in_the_output_schema(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        """Each described option carries its description in the schema; an undescribed one carries none.

        This is the shape both a text model's tool schema and pydantic-ai's TypeSafe adapter read
        a per-option description from: ``anyOf`` of ``{const, description}``, not a bare ``enum``.
        pydantic-ai's ``Choices`` builds it; a bare string and a ``BranchOption`` describe alike.
        """
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("handle_auth")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMBranchOperator(
            task_id="route",
            prompt="Pick",
            llm_conn_id="my_llm",
            branches={
                "handle_auth": BranchOption("Sign-in, passwords, 2FA. Owns missing reset emails."),
                "handle_billing": "Invoices, charges, refunds.",
            },
        )
        op.downstream_task_ids = {"handle_general", "handle_billing", "handle_auth"}

        op.execute(MagicMock(spec=dict))

        output_type = mock_hook_cls.get_hook.return_value.create_agent.call_args.kwargs["output_type"]
        schema = TypeAdapter(output_type).json_schema()
        assert "enum" not in schema
        assert [(o["const"], o.get("description")) for o in schema["anyOf"]] == [
            ("handle_auth", "Sign-in, passwords, 2FA. Owns missing reset emails."),
            ("handle_billing", "Invoices, charges, refunds."),
            ("handle_general", None),
        ]
        # Validation is the Choices type: an answer outside the task ids is rejected by pydantic-ai.
        with pytest.raises(ValidationError, match="handle_nothing"):
            TypeAdapter(output_type).validate_python("handle_nothing")
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
            branches={"handle_billing": "Invoices, charges, refunds."},
        )
        op.downstream_task_ids = {"handle_auth", "handle_billing"}

        op.execute(MagicMock(spec=dict))

        options = _resolve(seen["schema"], seen["schema"]["properties"]["response"])["anyOf"]
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
            branches={"handle_shipping": "Late or lost deliveries."},
        )
        op.downstream_task_ids = {"handle_shipping", "handle_packaging"}

        op.execute(MagicMock(spec=dict))

        items = _resolve(seen["schema"], seen["schema"]["properties"]["response"]["items"])
        options = items["anyOf"]
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
            branches={"handle_genral": "typo", "handle_auth": "ok"},
        )
        op.downstream_task_ids = {"handle_auth", "handle_general"}

        with pytest.raises(
            ValueError,
            match=r"branches for 'route' names \['handle_genral'\].*\['handle_auth', 'handle_general'\]",
        ):
            op.execute(MagicMock(spec=dict))

        mock_hook_cls.get_hook.return_value.create_agent.assert_not_called()

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_no_branch_descriptions_keeps_the_plain_enum_schema(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        """Without descriptions the schema is the bare enum it always was."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        mock_agent.run_sync.return_value = make_mock_run_result("task_a")
        op = LLMBranchOperator(task_id="route", prompt="Pick", llm_conn_id="my_llm")
        op.downstream_task_ids = {"task_b", "task_a"}

        op.execute(MagicMock(spec=dict))

        output_type = mock_hook_cls.get_hook.return_value.create_agent.call_args.kwargs["output_type"]
        schema = TypeAdapter(output_type).json_schema()
        assert schema["enum"] == ["task_a", "task_b"]
        assert "anyOf" not in schema

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    def test_branch_descriptions_render_as_nested_template_fields(self):
        """Both the string shorthand and a BranchOption render their description; the bar is untouched."""
        op = LLMBranchOperator(
            task_id="route",
            prompt="Pick",
            llm_conn_id="my_llm",
            branches={"a": "Owner: {{ ds }}", "b": BranchOption("Other {{ ds }}", min_confidence=0.9)},
            decision_policy=DecisionPolicy(min_confidence=0.5),
        )

        op.render_template_fields({"ds": "2026-09-20"})

        assert op.branches == {
            "a": BranchOption("Owner: 2026-09-20"),
            "b": BranchOption("Other 2026-09-20", min_confidence=0.9),
        }
        assert "template_fields" not in {f.name for f in __import__("dataclasses").fields(BranchOption)}

    @patch("airflow.providers.common.ai.operators.llm_branch.Choices", None)
    @patch("airflow.providers.common.ai.operators.llm_branch.Choice", None)
    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_enum_fallback_emits_the_same_schema_and_branches_on_the_member(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        """On pydantic-ai without ``Choices`` the Enum fallback carries the same anyOf and the pick unwraps."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = LLMBranchOperator(
            task_id="route",
            prompt="Pick",
            llm_conn_id="my_llm",
            branches={"handle_billing": "Invoices, charges, refunds."},
        )
        op.downstream_task_ids = {"handle_auth", "handle_billing"}

        def capture(**kwargs):
            mock_agent.run_sync.return_value = make_mock_run_result(kwargs["output_type"].handle_billing)
            return mock_agent

        mock_hook_cls.get_hook.return_value.create_agent.side_effect = capture

        op.execute(MagicMock(spec=dict))

        output_type = mock_hook_cls.get_hook.return_value.create_agent.call_args.kwargs["output_type"]
        schema = TypeAdapter(output_type).json_schema()
        assert [(o["const"], o.get("description")) for o in schema["anyOf"]] == [
            ("handle_auth", None),
            ("handle_billing", "Invoices, charges, refunds."),
        ]
        mock_do_branch.assert_called_once_with(mock_do_branch.call_args.args[0], "handle_billing")

    def test_branches_must_be_a_mapping(self):
        with pytest.raises(TypeError, match="branches must be a mapping"):
            LLMBranchOperator(
                task_id="route", prompt="Pick", llm_conn_id="my_llm", branches="{{ var.json.routes }}"
            )

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    def test_branches_are_normalised_to_options(self):
        op = LLMBranchOperator(
            task_id="route",
            prompt="Pick",
            llm_conn_id="my_llm",
            branches={"a": "Plain.", "b": BranchOption(min_confidence=0.9)},
            decision_policy=DecisionPolicy(min_confidence=0.5),
        )
        assert op.branches == {"a": BranchOption("Plain."), "b": BranchOption(None, 0.9)}
        assert op._branch_bars == {"b": 0.9}

    def test_execute_raises_on_no_downstream_tasks(self):
        """ValueError when the operator has no downstream tasks."""
        op = LLMBranchOperator(
            task_id="test",
            prompt="Pick",
            llm_conn_id="my_llm",
        )
        with pytest.raises(ValueError, match="no downstream tasks"):
            op.execute(MagicMock(spec=dict))


def _resolve(schema, node):
    """Follow a ``$ref`` into the schema's ``$defs``; pydantic names the def after the Choices type."""
    if "$ref" in node:
        return schema["$defs"][node["$ref"].rsplit("/", 1)[1]]
    return node


def _jev_result(make_mock_run_result, output, *, confidence=None, probabilities=None, model="jev-1.13.0"):
    """A run result shaped like pydantic-ai's TypeSafe adapter returns: provider_details on the response."""
    result = make_mock_run_result(output)
    details = None
    if confidence is not None:
        details = {
            "confidence": {"response": confidence},
            "probabilities": {"response": probabilities or {}},
            "scores": {},
        }
    result.response = ModelResponse(parts=[], model_name=model, provider_details=details)
    return result


def _decision_pushes(context):
    return [
        c.kwargs["value"]
        for c in context["task_instance"].xcom_push.call_args_list
        if c.kwargs.get("key") == "decision"
    ]


def _make_context(ti_id=None):
    ti_id = ti_id or uuid4()
    ti = MagicMock()
    ti.id = ti_id
    return MagicMock(**{"__getitem__": lambda self, key: {"task_instance": ti}[key]})


class TestLLMBranchOperatorConfidenceGate:
    """decision_policy and the decision XCom. The review itself reuses the approval flow tested below."""

    def _op(self, **kwargs):
        op = LLMBranchOperator(task_id="triage", prompt="traceback", llm_conn_id="my_llm", **kwargs)
        op.downstream_task_ids = {"rerun", "page_oncall", "ignore"}
        return op

    def test_branch_bars_need_a_policy_bar_to_inherit(self):
        with pytest.raises(
            ValueError, match=r"branches \['page_oncall'\] set min_confidence but decision_policy"
        ):
            self._op(branches={"page_oncall": BranchOption("Page.", min_confidence=0.9)})

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_branch_bar_for_an_unknown_task_fails_before_the_model_call(self, mock_hook_cls):
        op = self._op(
            decision_policy=DecisionPolicy(min_confidence=0.6),
            branches={"page_oncal": BranchOption(min_confidence=0.9)},
        )

        with pytest.raises(ValueError, match=r"branches for 'triage' names \['page_oncal'\]"):
            op.execute(_make_context())

        mock_hook_cls.get_hook.return_value.create_agent.assert_not_called()

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_confident_pick_branches_and_records_the_decision(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(
            make_mock_run_result,
            "rerun",
            confidence=0.94,
            probabilities={"rerun": 0.94, "page_oncall": 0.05, "ignore": 0.01},
        )
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op(
            decision_policy=DecisionPolicy(min_confidence=0.7), branches={"rerun": "Transient failure."}
        )
        context = _make_context()

        op.execute(context)

        mock_do_branch.assert_called_once_with(context, "rerun")
        (record,) = _decision_pushes(context)
        assert record == {
            "model": "jev-1.13.0",
            "proposed": "rerun",
            "action": "rerun",
            "confidence": {"response": 0.94},
            "probabilities": {"response": {"rerun": 0.94, "page_oncall": 0.05, "ignore": 0.01}},
            "min_confidence": 0.7,
            "review": None,
            "decided_by": "model",
            "policy": {"min_confidence": 0.7, "on_uncertain": "review", "branches": None},
            "descriptions": {"rerun": "Transient failure."},
        }

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="review needs the HITL flow, Airflow >= 3.1")
    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail", autospec=True)
    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_pick_below_the_bar_goes_to_review_before_branching(
        self, mock_hook_cls, mock_do_branch, mock_upsert, mock_trigger_cls, make_mock_run_result
    ):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(
            make_mock_run_result,
            "page_oncall",
            confidence=0.52,
            probabilities={"page_oncall": 0.52, "rerun": 0.46, "ignore": 0.02},
        )
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op(
            decision_policy=DecisionPolicy(min_confidence=0.6),
            branches={"page_oncall": BranchOption(min_confidence=0.9)},
        )
        context = _make_context()

        with pytest.raises(ApprovalPauseSignal) as exc_info:
            op.execute(context)

        assert exc_info.value.kwargs["generated_output"] == "page_oncall"
        mock_do_branch.assert_not_called()
        body = mock_upsert.call_args.kwargs["body"]
        assert "Confidence: 0.52 (minimum 0.90)" in body
        assert "page_oncall 0.52, rerun 0.46, ignore 0.02" in body
        (record,) = _decision_pushes(context)
        assert record["proposed"] == "page_oncall"
        assert record["action"] is None
        assert record["min_confidence"] == 0.9
        assert record["review"] == "below_threshold"
        assert record["decided_by"] is None
        assert record["policy"] == {
            "min_confidence": 0.6,
            "on_uncertain": "review",
            "branches": {"page_oncall": 0.9},
        }
        # The same record rides the continuation, so the resume does not depend on the XCom.
        assert exc_info.value.kwargs["decision"] == record

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_per_branch_bar_applies_to_the_picked_branch_only(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        """0.52 is under page_oncall's 0.9 bar but rerun's bar is 0.6, so a rerun at 0.65 branches."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(make_mock_run_result, "rerun", confidence=0.65)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op(
            decision_policy=DecisionPolicy(min_confidence=0.6),
            branches={"page_oncall": BranchOption(min_confidence=0.9)},
        )
        context = _make_context()

        op.execute(context)

        mock_do_branch.assert_called_once_with(context, "rerun")
        (record,) = _decision_pushes(context)
        assert record["min_confidence"] == 0.6
        assert record["review"] is None

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="review needs the HITL flow, Airflow >= 3.1")
    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail", autospec=True)
    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_picked_branch_absent_from_the_overrides_inherits_the_default(
        self, mock_hook_cls, mock_do_branch, mock_upsert, mock_trigger_cls, make_mock_run_result
    ):
        """A branch the overrides do not name is still gated, by min_confidence."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(make_mock_run_result, "ignore", confidence=0.3)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op(
            decision_policy=DecisionPolicy(min_confidence=0.6),
            branches={"page_oncall": BranchOption(min_confidence=0.9)},
        )
        context = _make_context()

        with pytest.raises(ApprovalPauseSignal):
            op.execute(context)

        mock_do_branch.assert_not_called()
        (record,) = _decision_pushes(context)
        assert record["min_confidence"] == 0.6
        assert record["review"] == "below_threshold"

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="review needs the HITL flow, Airflow >= 3.1")
    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail", autospec=True)
    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_text_model_with_a_bar_goes_to_review_by_default(
        self, mock_hook_cls, mock_do_branch, mock_upsert, mock_trigger_cls, make_mock_run_result
    ):
        """A model that reports no confidence must not silently bypass a configured bar."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(make_mock_run_result, "rerun", model="claude-sonnet-5")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op(decision_policy=DecisionPolicy(min_confidence=0.7))
        context = _make_context()

        with pytest.raises(ApprovalPauseSignal):
            op.execute(context)

        mock_do_branch.assert_not_called()
        assert "Confidence: not reported by the model (minimum 0.70)" in mock_upsert.call_args.kwargs["body"]
        (record,) = _decision_pushes(context)
        assert record["model"] == "claude-sonnet-5"
        assert record["confidence"] == {}
        assert record["review"] == "missing_confidence"

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_low_confidence_can_fail_the_task(self, mock_hook_cls, mock_do_branch, make_mock_run_result):
        """on_uncertain="fail": the record is written first so the failure is explained."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(make_mock_run_result, "rerun", confidence=0.4)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op(decision_policy=DecisionPolicy(min_confidence=0.7, on_uncertain="fail"))
        context = _make_context()

        with pytest.raises(ValueError, match="Confidence 0.40 for branch task 'triage' is below"):
            op.execute(context)

        mock_do_branch.assert_not_called()
        (record,) = _decision_pushes(context)
        assert record["review"] == "below_threshold"
        assert record["action"] is None
        assert record["decided_by"] == "policy"
        assert record["policy"]["on_uncertain"] == "fail"

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_text_model_with_a_bar_can_fail(self, mock_hook_cls, mock_do_branch, make_mock_run_result):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(make_mock_run_result, "rerun", model="claude-sonnet-5")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op(decision_policy=DecisionPolicy(min_confidence=0.7, on_uncertain="fail"))

        with pytest.raises(ValueError, match="reported no confidence"):
            op.execute(_make_context())

        mock_do_branch.assert_not_called()

    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_no_bar_and_no_confidence_is_todays_behaviour(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(make_mock_run_result, "rerun", model="claude-sonnet-5")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op()
        context = _make_context()

        op.execute(context)

        mock_do_branch.assert_called_once_with(context, "rerun")
        (record,) = _decision_pushes(context)
        assert record == {
            "model": "claude-sonnet-5",
            "proposed": "rerun",
            "action": "rerun",
            "confidence": {},
            "probabilities": {},
            "min_confidence": None,
            "review": None,
            "decided_by": "model",
            "policy": {"min_confidence": None, "on_uncertain": "review", "branches": None},
            "descriptions": None,
        }

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_do_xcom_push_false_suppresses_the_record(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(make_mock_run_result, "rerun", confidence=0.9)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op(decision_policy=DecisionPolicy(min_confidence=0.7), do_xcom_push=False)
        context = _make_context()

        op.execute(context)

        assert _decision_pushes(context) == []

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_multiple_branches_use_the_strictest_picked_bar(
        self, mock_hook_cls, mock_do_branch, make_mock_run_result
    ):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(
            make_mock_run_result, ["rerun", "page_oncall"], confidence=0.95
        )
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op(
            allow_multiple_branches=True,
            decision_policy=DecisionPolicy(min_confidence=0.6),
            branches={"page_oncall": BranchOption(min_confidence=0.9)},
        )
        context = _make_context()

        op.execute(context)

        mock_do_branch.assert_called_once_with(context, ["rerun", "page_oncall"])
        (record,) = _decision_pushes(context)
        assert record["min_confidence"] == 0.9
        assert record["proposed"] == ["rerun", "page_oncall"]

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="review needs the HITL flow, Airflow >= 3.1")
    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail", autospec=True)
    @patch.object(LLMBranchOperator, "do_branch")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_require_approval_still_asks_at_high_confidence(
        self, mock_hook_cls, mock_do_branch, mock_upsert, mock_trigger_cls, make_mock_run_result
    ):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = _jev_result(make_mock_run_result, "rerun", confidence=0.99)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = self._op(require_approval=True, decision_policy=DecisionPolicy(min_confidence=0.5))
        context = _make_context()

        with pytest.raises(ApprovalPauseSignal):
            op.execute(context)

        mock_do_branch.assert_not_called()
        (record,) = _decision_pushes(context)
        assert record["review"] == "require_approval"

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_records_the_human_decision(self, mock_do_branch):
        """The final record is built from the continuation; the task instance has no xcom_pull at all."""
        op = self._op(decision_policy=DecisionPolicy(min_confidence=0.7), allow_modifications=True)
        ti = MagicMock(spec=["id", "xcom_push"])
        pending = {
            "model": "jev-1.13.0",
            "proposed": "page_oncall",
            "action": None,
            "confidence": {"response": 0.52},
            "probabilities": {"response": {"page_oncall": 0.52, "rerun": 0.46}},
            "min_confidence": 0.9,
            "review": "below_threshold",
            "decided_by": None,
            "policy": {"min_confidence": 0.7, "on_uncertain": "review", "branches": None},
        }
        context = MagicMock(spec=dict, **{"__getitem__": lambda self, key: {"task_instance": ti}[key]})
        event = {
            "chosen_options": ["Approve"],
            "params_input": {"output": "rerun"},
            "responded_by_user": {"id": "u1", "name": "Sam"},
        }

        op.execute_complete(context, generated_output="page_oncall", event=event, decision=pending)

        mock_do_branch.assert_called_once_with(context, "rerun")
        final = ti.xcom_push.call_args.kwargs["value"]
        assert final == {**pending, "action": "rerun", "decided_by": "human"}

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @patch.object(LLMBranchOperator, "skip")
    def test_execute_complete_records_a_rejection_before_skipping(self, mock_skip):
        """skip() raises on the Task SDK to hand the skip to the supervisor, so the record must be written first."""
        mock_skip.side_effect = RuntimeError("DownstreamTasksSkipped stands in here")
        op = self._op(decision_policy=DecisionPolicy(min_confidence=0.7))
        ti = MagicMock(spec=["id", "xcom_push"])
        task = MagicMock(spec=["get_direct_relatives", "get_flat_relatives"])
        task.get_direct_relatives.return_value = []
        context = {"task_instance": ti, "ti": ti, "task": task}
        event = {"chosen_options": ["Reject"], "responded_by_user": {"id": "u1", "name": "Sam"}}
        pending = {"proposed": "rerun", "action": None, "decided_by": None}

        with pytest.raises(RuntimeError):
            op.execute_complete(context, generated_output="rerun", event=event, decision=pending)

        final = ti.xcom_push.call_args.kwargs["value"]
        assert final["action"] is None
        assert final["decided_by"] == "human"

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_records_a_timeout_default(self, mock_do_branch):
        op = self._op(decision_policy=DecisionPolicy(min_confidence=0.7))
        ti = MagicMock(spec=["id", "xcom_push"])
        context = MagicMock(spec=dict, **{"__getitem__": lambda self, key: {"task_instance": ti}[key]})

        op.execute_complete(
            context,
            generated_output="rerun",
            event={"chosen_options": ["Approve"], "timedout": True},
            decision={"proposed": "rerun", "action": None, "decided_by": None},
        )

        final = ti.xcom_push.call_args.kwargs["value"]
        assert final["action"] == "rerun"
        assert final["decided_by"] == "timeout_default"

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    def test_fail_on_reject_finalises_the_record_before_raising(self):
        op = self._op(decision_policy=DecisionPolicy(min_confidence=0.7), fail_on_reject=True)
        ti = MagicMock(spec=["id", "xcom_push"])
        context = MagicMock(spec=dict, **{"__getitem__": lambda self, key: {"task_instance": ti}[key]})
        context.get.side_effect = lambda key, default=None: {"task_instance": ti}.get(key, default)
        pending = {"proposed": "rerun", "action": None, "decided_by": None}
        event = {"chosen_options": ["Reject"], "responded_by_user": {"id": "u1", "name": "Sam"}}

        with pytest.raises(HITLRejectException):
            op.execute_complete(context, generated_output="rerun", event=event, decision=pending)

        assert ti.xcom_push.call_args.kwargs["value"] == {**pending, "decided_by": "human"}

    @pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="require_approval needs the HITL flow, Airflow >= 3.1")
    @patch.object(LLMBranchOperator, "do_branch")
    def test_execute_complete_without_a_carried_decision_writes_no_record(self, mock_do_branch):
        """A pause that carried no record (require_approval on an older checkpoint) resumes without one."""
        op = self._op(require_approval=True)
        ti = MagicMock(spec=["id", "xcom_push"])
        context = MagicMock(spec=dict, **{"__getitem__": lambda self, key: {"task_instance": ti}[key]})
        event = {"chosen_options": ["Approve"], "responded_by_user": {"id": "u1", "name": "Sam"}}

        op.execute_complete(context, generated_output="rerun", event=event)

        mock_do_branch.assert_called_once_with(context, "rerun")
        ti.xcom_push.assert_not_called()


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

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("task_a")
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

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(["task_a", "task_c"])
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

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("task_a")
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

        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(["task_a"])
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
