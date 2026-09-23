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

import logging
from datetime import timedelta
from decimal import Decimal
from unittest.mock import ANY, MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.usage import RequestUsage, UsageLimits

from airflow.providers.common.ai.mixins.approval import (
    LLMApprovalMixin,
)
from airflow.providers.common.ai.operators import llm as llm_module
from airflow.providers.common.ai.operators.llm import DecisionPolicy, LLMOperator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_3_PLUS

try:
    from airflow.sdk.serde import SUPPORTS_OPERATOR_DESERIALIZATION_WALKER as _CORE_WALKER
except ImportError:
    _CORE_WALKER = False

from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, TaskDeferred
from airflow.providers.standard.exceptions import HITLRejectException, HITLTimeoutError

if AIRFLOW_V_3_3_PLUS:
    # On 3.3+ cores require_approval pauses the task in AWAITING_INPUT; older cores defer
    # to HITLTrigger. Both exceptions carry method_name/kwargs/timeout, so the approval
    # tests assert against whichever pause signal the running core uses.
    from airflow.sdk.exceptions import TaskAwaitingInput as ApprovalPauseSignal
else:
    ApprovalPauseSignal = TaskDeferred  # type: ignore[assignment, misc]

AWAIT_INPUT_FLAG_PATH = "airflow.providers.common.ai.mixins.approval.AIRFLOW_V_3_3_PLUS"

# Returning the Pydantic instance through XCom (rather than a dict) only happens
# on cores that register declared ``output_type`` classes from the worker-side
# DAG walk. On older cores the operator dumps to a dict, so these tests skip.
requires_typed_xcom = pytest.mark.skipif(
    not _CORE_WALKER,
    reason="Requires a core with the worker-side deserialization-class walk.",
)


class Entities(BaseModel):
    names: list[str]


class Summary(BaseModel):
    text: str


class Assessment(BaseModel):
    category: str
    score: float


PRICED_COST = Decimal("0.10")


def _build_priced_response(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    return ModelResponse(
        parts=[TextPart(content="the answer")],
        usage=RequestUsage(input_tokens=100, output_tokens=50, cost=PRICED_COST),
    )


class TestLLMOperator:
    def test_template_fields(self):
        expected = {
            "prompt",
            "llm_conn_id",
            "model_id",
            "fallback_conn_ids",
            "system_prompt",
            "agent_params",
            "usage_limits",
        }
        assert set(LLMOperator.template_fields) == expected

    @pytest.mark.parametrize(
        "bad",
        [pytest.param([], id="list"), pytest.param("0.5", id="str"), pytest.param(5, id="int")],
    )
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_rejects_non_container_usage_limits(self, mock_hook_cls, bad):
        """A non-UsageLimits/dict/None value (e.g. a str migrated from ``max_cost``,
        or the whole field written as a single un-rendered Jinja expression) is
        rejected by ``coerce_usage_limits`` at execute time, not at ``__init__`` --
        checking it at ``__init__`` would raise on a still-templated string before
        it is ever rendered (the ``validate-operators-init`` hook forbids exactly
        that: value-dependent validation of a template field before render)."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c", usage_limits=bad)
        with pytest.raises(TypeError, match="usage_limits must be a UsageLimits, a dict, or None"):
            op.execute(context=MagicMock())
        mock_agent.run_sync.assert_not_called()

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_returns_string_output(self, mock_hook_cls, make_mock_run_result):
        """Default output_type=str returns the LLM string directly."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("Paris is the capital of France.")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(task_id="test", prompt="What is the capital of France?", llm_conn_id="my_llm")
        result = op.execute(context=MagicMock())

        assert result == "Paris is the capital of France."
        mock_agent.run_sync.assert_called_once_with(
            "What is the capital of France?", usage_limits=None, cancellation_token=ANY
        )
        mock_hook_cls.get_hook.return_value.create_agent.assert_called_once_with(
            output_type=str, instructions=""
        )
        mock_hook_cls.get_hook.assert_called_once_with(
            "my_llm", hook_params={"model_id": None, "fallback_conn_ids": None}
        )

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_forwards_usage_limits_to_run_sync(self, mock_hook_cls, make_mock_run_result):
        """``usage_limits`` is forwarded verbatim to ``agent.run_sync``."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        limits = UsageLimits(request_limit=2, output_tokens_limit=100)
        op = LLMOperator(
            task_id="test",
            prompt="Summarize",
            llm_conn_id="my_llm",
            usage_limits=limits,
        )
        op.execute(context=MagicMock())

        mock_agent.run_sync.assert_called_once_with("Summarize", usage_limits=limits, cancellation_token=ANY)

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_coerces_usage_limits_dict_before_run_sync(self, mock_hook_cls, make_mock_run_result):
        """A dict ``usage_limits`` is coerced into a real ``UsageLimits`` before ``run_sync``."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="test",
            prompt="Summarize",
            llm_conn_id="my_llm",
            usage_limits={"cost_limit": "0.5"},
        )
        op.execute(context=MagicMock())

        _, kwargs = mock_agent.run_sync.call_args
        assert kwargs["usage_limits"] == UsageLimits(cost_limit=Decimal("0.5"))

    @pytest.mark.parametrize(
        ("field", "rendered", "expected"),
        [
            pytest.param("cost_limit", "0.05", Decimal("0.05"), id="decimal"),
            pytest.param("request_limit", "7", 7, id="int"),
            pytest.param("count_tokens_before_request", "true", True, id="bool"),
        ],
    )
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_renders_templated_usage_limits_dict_then_coerces(
        self, mock_hook_cls, field, rendered, expected, make_mock_run_result
    ):
        """The template chain end to end, for each ``_COERCERS`` type: Jinja renders
        the dict's string leaf (still a string -- Jinja never converts type), then
        ``execute`` coerces it to the field's real type. @amoghrajesh's review asked
        whether *every* field can be templated, not just ``cost_limit``."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="test",
            prompt="Summarize",
            llm_conn_id="my_llm",
            usage_limits={field: "{{ params.value }}"},
        )
        op.render_template_fields({"params": {"value": rendered}})
        assert op.usage_limits == {field: rendered}

        op.execute(context=MagicMock())

        _, kwargs = mock_agent.run_sync.call_args
        assert getattr(kwargs["usage_limits"], field) == expected

    def test_render_template_fields_leaves_usage_limits_object_unchanged(self):
        """A ``UsageLimits`` instance has no ``resolve``/``template_fields``, so Jinja's
        nested-template-field walk is a no-op and the exact same object comes back."""
        limits = UsageLimits(cost_limit=Decimal("1"))
        op = LLMOperator(task_id="test", prompt="Summarize", llm_conn_id="my_llm", usage_limits=limits)
        op.render_template_fields({})
        assert op.usage_limits is limits

    @pytest.mark.parametrize(
        "cap_kwargs",
        [
            pytest.param({"usage_limits": {"cost_limit": "0.05"}}, id="usage_limits_dict"),
            pytest.param({"usage_limits": UsageLimits(cost_limit=Decimal("0.05"))}, id="usage_limits_object"),
        ],
    )
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_raises_when_cost_cap_exceeded(self, mock_hook_cls, cap_kwargs):
        """A real run whose cost exceeds the configured cap raises ``UsageLimitExceeded``."""
        mock_hook_cls.get_hook.return_value.create_agent.side_effect = lambda **kw: Agent(
            FunctionModel(_build_priced_response), **kw
        )

        op = LLMOperator(task_id="test", prompt="Summarize", llm_conn_id="my_llm", **cap_kwargs)
        with pytest.raises(UsageLimitExceeded, match=r"cost_limit.*0\.05"):
            op.execute(context=MagicMock())

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_completes_when_cost_stays_under_cap(self, mock_hook_cls):
        """A real run costing less than the configured ``cost_limit`` completes and returns the model output."""
        mock_hook_cls.get_hook.return_value.create_agent.side_effect = lambda **kw: Agent(
            FunctionModel(_build_priced_response), **kw
        )

        op = LLMOperator(
            task_id="test",
            prompt="Summarize",
            llm_conn_id="my_llm",
            usage_limits={"cost_limit": str(PRICED_COST * 2)},
        )

        assert op.execute(context=MagicMock()) == "the answer"

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_raises_valueerror_for_unparsable_usage_limits_value(self, mock_hook_cls):
        """An unparsable templated ``usage_limits`` value surfaces as ``ValueError`` before any model call."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="test", prompt="Summarize", llm_conn_id="my_llm", usage_limits={"cost_limit": ""}
        )

        with pytest.raises(ValueError, match=r"usage_limits\['cost_limit'\]"):
            op.execute(context=MagicMock())
        mock_agent.run_sync.assert_not_called()

    @requires_typed_xcom
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_structured_output_with_all_params(self, mock_hook_cls, make_mock_run_result):
        """Structured output returns the Pydantic instance unchanged so downstream tasks keep the type."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(Entities(names=["Alice", "Bob"]))
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

        assert isinstance(result, Entities)
        assert result.names == ["Alice", "Bob"]
        mock_hook_cls.get_hook.assert_called_once_with(
            "my_llm", hook_params={"model_id": "openai:gpt-5", "fallback_conn_ids": None}
        )
        mock_hook_cls.get_hook.return_value.create_agent.assert_called_once_with(
            output_type=Entities,
            instructions="You are an extractor.",
            retries=3,
            model_settings={"temperature": 0.9},
        )

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_forwards_fallback_conn_ids_to_hook(self, mock_hook_cls, make_mock_run_result):
        """``fallback_conn_ids`` on the operator overrides the connection's own extra field."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="test",
            prompt="p",
            llm_conn_id="my_llm",
            fallback_conn_ids=["conn_a", "conn_b"],
        )
        op.execute(context=MagicMock())

        mock_hook_cls.get_hook.assert_called_once_with(
            "my_llm", hook_params={"model_id": None, "fallback_conn_ids": ["conn_a", "conn_b"]}
        )

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_forwards_empty_fallback_conn_ids_to_hook(self, mock_hook_cls, make_mock_run_result):
        """An explicit ``[]`` disables a chain configured on the connection, not just an override."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("ok")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="test",
            prompt="p",
            llm_conn_id="my_llm",
            fallback_conn_ids=[],
        )
        op.execute(context=MagicMock())

        mock_hook_cls.get_hook.assert_called_once_with(
            "my_llm", hook_params={"model_id": None, "fallback_conn_ids": []}
        )

    def test_declares_output_type_for_deserialization(self):
        """Declares ``output_type`` so the worker-side DAG walk registers it for deserialization.

        Registration happens in the core walk over the loaded DAG (covered by the
        task-runner tests), not as an ``__init__`` side effect.
        """
        assert "output_type" in LLMOperator.deserialization_allowed_class_fields

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_serialize_output_returns_dict(self, mock_hook_cls, make_mock_run_result):
        """serialize_output=True dumps the BaseModel to a dict on the wire."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(Entities(names=["A", "B"]))
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            output_type=Entities,
            serialize_output=True,
        )
        result = op.execute(context=MagicMock())

        assert result == {"names": ["A", "B"]}
        assert not isinstance(result, Entities)


def _make_context(ti_id=None):
    ti_id = ti_id or uuid4()
    ti = MagicMock()
    ti.id = ti_id
    return MagicMock(**{"__getitem__": lambda self, key: {"task_instance": ti}[key]})


class TestLLMOperatorConfidenceGate:
    """decision_policy on a structured output, and the decision XCom."""

    def _result(self, make_mock_run_result, output, details):
        result = make_mock_run_result(output)
        result.response = ModelResponse(parts=[], model_name="jev-1.13.0", provider_details=details)
        return result

    @pytest.mark.parametrize(
        "context",
        [
            pytest.param({}, id="no-task-instance"),
            pytest.param({"task_instance": {"id": "not-a-ti"}}, id="task-instance-is-a-dict"),
            pytest.param({"task_instance": None}, id="task-instance-is-none"),
        ],
    )
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_hand_built_context_skips_the_decision_push_with_a_warning(
        self, mock_hook_cls, make_mock_run_result, context, caplog
    ):
        """A dict-shaped or missing task instance (old tests, custom runners) must not fail the run."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = self._result(make_mock_run_result, Summary(text="t"), None)
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c", output_type=Summary)

        with caplog.at_level(logging.WARNING):
            output = op.execute(context)

        assert Summary.model_validate(output).text == "t"
        assert "the decision record was not pushed to XCom" in caplog.text

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_confident_output_returns_and_records_per_field_confidence(
        self, mock_hook_cls, make_mock_run_result
    ):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = self._result(
            make_mock_run_result,
            Summary(text="t"),
            {"confidence": {"text": 0.9}, "probabilities": {}, "scores": {}},
        )
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = LLMOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            output_type=Summary,
            decision_policy=DecisionPolicy(min_confidence=0.7),
        )
        context = MagicMock(spec=dict)

        output = op.execute(context)

        assert isinstance(output, (Summary, dict))
        pushes = {
            c.kwargs["key"]: c.kwargs["value"] for c in context["task_instance"].xcom_push.call_args_list
        }
        assert pushes["decision"]["confidence"] == {"text": 0.9}
        assert pushes["decision"]["min_confidence"] == 0.7
        assert pushes["decision"]["review"] is None
        assert pushes["decision"]["decided_by"] == "model"
        assert pushes["decision"]["proposed"] is None

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="review needs the HITL flow, Airflow >= 3.1")
    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail", autospec=True)
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_one_weak_field_sends_the_output_to_review(
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, make_mock_run_result
    ):
        """The least confident of the fields that reported a confidence is what the bar is compared against."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = self._result(
            make_mock_run_result,
            Assessment(category="billing", score=0.3),
            {"confidence": {"category": 0.9, "score": 0.4}, "probabilities": {}, "scores": {}},
        )
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = LLMOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            output_type=Assessment,
            decision_policy=DecisionPolicy(min_confidence=0.7),
        )
        context = MagicMock(spec=dict)
        context["task_instance"].id = uuid4()

        with pytest.raises(ApprovalPauseSignal) as exc_info:
            op.execute(context)

        body = mock_upsert.call_args.kwargs["body"]
        assert "Confidence: 0.40 (minimum 0.70)" in body
        pushes = {
            c.kwargs["key"]: c.kwargs["value"] for c in context["task_instance"].xcom_push.call_args_list
        }
        assert pushes["decision"]["review"] == "below_threshold"
        assert pushes["decision"]["decided_by"] is None
        assert pushes["decision"]["policy"] == {
            "min_confidence": 0.7,
            "on_uncertain": "review",
            "branches": None,
        }
        assert exc_info.value.kwargs["decision"] == pushes["decision"]

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_a_field_that_reports_no_confidence_is_not_gated(self, mock_hook_cls, make_mock_run_result):
        """A bounded float field reports no confidence (the probability is the answer); only reported fields count."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = self._result(
            make_mock_run_result,
            Assessment(category="billing", score=0.3),
            {"confidence": {"category": 0.9}, "probabilities": {}, "scores": {"score": 0.3}},
        )
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = LLMOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            output_type=Assessment,
            decision_policy=DecisionPolicy(min_confidence=0.7),
        )
        context = MagicMock(spec=dict)

        op.execute(context)

        pushes = {
            c.kwargs["key"]: c.kwargs["value"] for c in context["task_instance"].xcom_push.call_args_list
        }
        assert pushes["decision"]["review"] is None
        assert pushes["decision"]["confidence"] == {"category": 0.9}

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_nan_confidence_counts_as_not_reported(self, mock_hook_cls, make_mock_run_result):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = self._result(
            make_mock_run_result,
            Summary(text="t"),
            {"confidence": {"text": float("nan")}, "probabilities": {}, "scores": {}},
        )
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = LLMOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            output_type=Summary,
            decision_policy=DecisionPolicy(min_confidence=0.7, on_uncertain="fail"),
        )

        with pytest.raises(ValueError, match="reported no confidence"):
            op.execute(MagicMock(spec=dict))

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="review needs the HITL flow, Airflow >= 3.1")
    def test_a_rejected_review_finalises_the_record_before_raising(self):
        """The mixin raises on rejection; the record must still say a human decided, not stay pending."""
        op = LLMOperator(
            task_id="t", prompt="p", llm_conn_id="c", decision_policy=DecisionPolicy(min_confidence=0.7)
        )
        ti = MagicMock(spec=["id", "xcom_push"])
        context = MagicMock(spec=dict, **{"__getitem__": lambda self, key: {"task_instance": ti}[key]})
        context.get.side_effect = lambda key, default=None: {"task_instance": ti}.get(key, default)
        pending = {"proposed": None, "action": None, "review": "below_threshold", "decided_by": None}
        event = {"chosen_options": ["Reject"], "responded_by_user": {"id": "u1", "name": "Sam"}}

        with pytest.raises(HITLRejectException):
            op.execute_complete(context, generated_output="the output", event=event, decision=pending)

        assert ti.xcom_push.call_args.kwargs["value"] == {**pending, "decided_by": "human"}

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    @pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="review needs the HITL flow, Airflow >= 3.1")
    def test_a_timed_out_review_with_no_default_finalises_the_record_before_raising(self):
        op = LLMOperator(
            task_id="t", prompt="p", llm_conn_id="c", decision_policy=DecisionPolicy(min_confidence=0.7)
        )
        ti = MagicMock(spec=["id", "xcom_push"])
        context = MagicMock(spec=dict, **{"__getitem__": lambda self, key: {"task_instance": ti}[key]})
        context.get.side_effect = lambda key, default=None: {"task_instance": ti}.get(key, default)
        pending = {"proposed": None, "action": None, "review": "below_threshold", "decided_by": None}
        event = {"error": "approval_timeout expired", "error_type": "timeout"}

        with pytest.raises(HITLTimeoutError):
            op.execute_complete(context, generated_output="the output", event=event, decision=pending)

        assert ti.xcom_push.call_args.kwargs["value"] == {**pending, "action": None, "decided_by": "timeout"}

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    def test_policy_review_can_take_a_timeout_default_without_require_approval(self):
        op = LLMOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            decision_policy=DecisionPolicy(min_confidence=0.7),
            approval_timeout=timedelta(hours=1),
            on_approval_timeout="approve",
        )
        assert op.on_approval_timeout == "approve"

    def test_policy_fail_does_not_open_the_review_path(self):
        with pytest.raises(ValueError, match="needs a review path"):
            LLMOperator(
                task_id="t",
                prompt="p",
                llm_conn_id="c",
                decision_policy=DecisionPolicy(min_confidence=0.7, on_uncertain="fail"),
                approval_timeout=timedelta(hours=1),
                on_approval_timeout="approve",
            )

    @patch("airflow.providers.common.ai.operators.llm.AIRFLOW_V_3_1_PLUS", False)
    def test_policy_review_is_rejected_on_an_old_core_at_construction(self):
        with pytest.raises(
            AirflowOptionalProviderFeatureException, match="on_uncertain='review'.*Airflow 3.1"
        ):
            LLMOperator(
                task_id="t", prompt="p", llm_conn_id="c", decision_policy=DecisionPolicy(min_confidence=0.7)
            )
        # "fail" never opens a review, so it builds on any core.
        LLMOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            decision_policy=DecisionPolicy(min_confidence=0.7, on_uncertain="fail"),
        )

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    def test_policy_review_rejects_a_sequence_prompt_before_the_model_call(self):
        """The multimodal-prompt guard applies to any review path, not only require_approval."""
        op = LLMOperator(
            task_id="t",
            prompt=[{"type": "text", "text": "p"}],  # type: ignore[arg-type]
            llm_conn_id="c",
            decision_policy=DecisionPolicy(min_confidence=0.7),
        )
        with pytest.raises(TypeError, match="non-string prompt"):
            op.execute(MagicMock(spec=dict))

    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="a reviewing decision_policy needs the HITL flow, Airflow >= 3.1"
    )
    def test_execute_complete_finalizes_the_carried_decision(self):
        op = LLMOperator(
            task_id="t", prompt="p", llm_conn_id="c", decision_policy=DecisionPolicy(min_confidence=0.7)
        )
        ti = MagicMock(spec=["id", "xcom_push"])
        context = MagicMock(spec=dict, **{"__getitem__": lambda self, key: {"task_instance": ti}[key]})
        pending = {"proposed": None, "action": None, "review": "below_threshold", "decided_by": None}
        event = {"chosen_options": ["Approve"], "responded_by_user": {"id": "u1", "name": "Sam"}}

        result = op.execute_complete(context, generated_output="the output", event=event, decision=pending)

        assert result == "the output"
        assert ti.xcom_push.call_args.kwargs == {
            "key": "decision",
            "value": {**pending, "decided_by": "human"},
        }

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_no_bar_keeps_todays_behaviour_and_still_records(self, mock_hook_cls, make_mock_run_result):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("plain text")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c")
        context = MagicMock(spec=dict)

        assert op.execute(context) == "plain text"

        pushes = {
            c.kwargs["key"]: c.kwargs["value"] for c in context["task_instance"].xcom_push.call_args_list
        }
        assert pushes["decision"] == {
            "model": "test-model",
            "proposed": None,
            "action": None,
            "confidence": {},
            "probabilities": {},
            "min_confidence": None,
            "review": None,
            "decided_by": "model",
            "policy": {"min_confidence": None, "on_uncertain": "review", "branches": None},
        }

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_text_model_with_a_bar_can_fail(self, mock_hook_cls, make_mock_run_result):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("plain text")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent
        op = LLMOperator(
            task_id="t",
            prompt="p",
            llm_conn_id="c",
            decision_policy=DecisionPolicy(min_confidence=0.7, on_uncertain="fail"),
        )
        context = MagicMock(spec=dict)

        with pytest.raises(ValueError, match="reported no confidence"):
            op.execute(context)

        pushes = {
            c.kwargs["key"]: c.kwargs["value"] for c in context["task_instance"].xcom_push.call_args_list
        }
        assert pushes["decision"]["review"] == "missing_confidence"
        assert pushes["decision"]["decided_by"] == "policy"

    def test_policy_must_be_a_decision_policy(self):
        with pytest.raises(TypeError, match="decision_policy must be a DecisionPolicy"):
            LLMOperator(task_id="t", prompt="p", llm_conn_id="c", decision_policy={"min_confidence": 0.7})

    def test_public_import_path(self):
        assert llm_module.DecisionPolicy is DecisionPolicy
        assert "DecisionPolicy" in llm_module.__all__


class TestLLMOperatorApprovalVersionGate:
    """__init__ rejects require_approval on cores without human-in-the-loop support.

    Deliberately carries no class-level 3.1 skipif. These tests simulate an old core by
    patching the flag, so they must not inherit the sibling class's skip -- and on a
    genuine pre-3.1 core, such as the 3.0.6 providers-compatibility job, they are the
    only tests that exercise the gate natively.
    """

    @pytest.mark.parametrize(
        ("kwargs", "expected_exception", "match"),
        [
            pytest.param(
                {"require_approval": True},
                AirflowOptionalProviderFeatureException,
                "Airflow 3.1",
                id="require-approval-rejected",
            ),
            pytest.param(
                {"require_approval": True, "on_approval_timeout": "approve"},
                AirflowOptionalProviderFeatureException,
                "Airflow 3.1",
                id="version-beats-combination-rule",
            ),
            pytest.param(
                {"require_approval": True, "on_approval_timeout": "nope"},
                ValueError,
                "on_approval_timeout must be",
                id="literal-check-keeps-precedence",
            ),
        ],
    )
    @patch("airflow.providers.common.ai.operators.llm.AIRFLOW_V_3_1_PLUS", False)
    def test_old_core_reports_the_blocking_argument(self, kwargs, expected_exception, match):
        """Which of two applicable errors __init__ reports, and in which order.

        Dropping on_approval_timeout would not make the operator work on an older core,
        so the version has to beat the combination rule. A bad literal is wrong on every
        core, so it keeps its own precise message -- which also pins the guard below the
        literal check, since hoisting it would swap that message for the version one.
        """
        with pytest.raises(expected_exception, match=match):
            LLMOperator(task_id="t", prompt="p", llm_conn_id="c", **kwargs)

    @patch("airflow.providers.common.ai.operators.llm.AIRFLOW_V_3_1_PLUS", False)
    def test_operator_without_approval_builds_on_old_core(self):
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c")
        assert op.require_approval is False

    @patch("airflow.providers.common.ai.operators.llm.AIRFLOW_V_3_1_PLUS", False)
    def test_approval_assigned_users_rejected_on_old_core(self):
        with pytest.raises(AirflowOptionalProviderFeatureException, match="needs Airflow 3.1"):
            LLMOperator(
                task_id="t",
                prompt="p",
                llm_conn_id="c",
                approval_assigned_users={"id": "u1", "name": "alice"},
            )


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
        assert op.on_approval_timeout == "fail"
        assert op.approval_notifiers == []
        assert op.approval_assigned_users == []

    def test_unknown_on_approval_timeout_raises(self):
        with pytest.raises(ValueError, match="on_approval_timeout must be"):
            LLMOperator(
                task_id="t",
                prompt="p",
                llm_conn_id="c",
                approval_timeout=timedelta(hours=1),
                on_approval_timeout="skip",
            )

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"require_approval": True},
            {"require_approval": True, "approval_timeout": timedelta(0)},
            {"require_approval": True, "approval_timeout": timedelta(hours=-1)},
            {"approval_timeout": timedelta(hours=1)},
        ],
        ids=[
            "no_approval_timeout",
            "zero_approval_timeout",
            "negative_approval_timeout",
            "no_require_approval",
        ],
    )
    def test_on_approval_timeout_without_prerequisites_raises(self, kwargs):
        with pytest.raises(
            ValueError, match="needs a review path .* and a positive approval_timeout to fire"
        ):
            LLMOperator(task_id="t", prompt="p", llm_conn_id="c", on_approval_timeout="approve", **kwargs)

    def test_single_approval_notifier_normalized_to_list(self):
        notifier = MagicMock(spec=BaseNotifier)
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c", approval_notifiers=notifier)
        assert op.approval_notifiers == [notifier]

    @pytest.mark.parametrize(
        ("notifiers", "match"),
        [
            (print, r"iterable of BaseNotifier instances, got <built-in function print>"),
            (5, r"iterable of BaseNotifier instances, got 5"),
            ("not-a-notifier", r"iterable of BaseNotifier instances, got 'not-a-notifier'"),
            ({"a": 1}, r"must contain BaseNotifier instances, got 'a'"),
            ([object()], r"must contain BaseNotifier instances, got <object object"),
        ],
        ids=["callable", "int", "str", "dict", "list_of_object"],
    )
    def test_rejects_non_notifier_approval_notifiers(self, notifiers, match):
        with pytest.raises(TypeError, match=match):
            LLMOperator(task_id="t", prompt="p", llm_conn_id="c", approval_notifiers=notifiers)

    def test_accepts_generator_of_approval_notifiers(self):
        notifiers = [MagicMock(spec=BaseNotifier), MagicMock(spec=BaseNotifier)]
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c", approval_notifiers=iter(notifiers))
        assert op.approval_notifiers == notifiers

    @pytest.mark.parametrize(
        "assigned_users",
        [
            {"id": "u1", "name": "alice"},
            [{"id": "u1", "name": "alice"}],
            iter([{"id": "u1", "name": "alice"}]),
        ],
        ids=["single", "list", "generator"],
    )
    def test_approval_assigned_users_normalized_to_list(self, assigned_users):
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c", approval_assigned_users=assigned_users)
        assert op.approval_assigned_users == [{"id": "u1", "name": "alice"}]

    @pytest.mark.parametrize(
        ("assigned_users", "match"),
        [
            ("alice", r"dict or an iterable of them, got 'alice'"),
            (5, r"dict or an iterable of them, got 5"),
            ({}, r"entries must be \{'id': str, 'name': str\} dicts, got \{\}"),
            ([{"id": "u1"}], r"entries must be .* got \{'id': 'u1'\}"),
            ([{"id": 1, "name": "alice"}], r"entries must be .* got \{'id': 1, 'name': 'alice'\}"),
            (["alice"], r"entries must be .* got 'alice'"),
        ],
        ids=["str", "int", "empty_dict", "missing_name", "non_str_id", "list_of_str"],
    )
    def test_rejects_malformed_approval_assigned_users(self, assigned_users, match):
        with pytest.raises(TypeError, match=match):
            LLMOperator(task_id="t", prompt="p", llm_conn_id="c", approval_assigned_users=assigned_users)

    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_defers(
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, make_mock_run_result
    ):
        """When require_approval=True, execute() defers instead of returning output."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("LLM response")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="approval_test",
            prompt="Summarize this",
            llm_conn_id="my_llm",
            require_approval=True,
        )
        ctx = _make_context()

        with pytest.raises(ApprovalPauseSignal) as exc_info:
            op.execute(context=ctx)

        assert exc_info.value.method_name == "execute_complete"
        assert exc_info.value.kwargs["generated_output"] == "LLM response"
        mock_upsert.assert_called_once()

    @patch(AWAIT_INPUT_FLAG_PATH, False)
    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_defers_on_legacy_core(
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, make_mock_run_result
    ):
        """On cores < 3.3 (flag pinned), execute() falls back to deferring to HITLTrigger."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("LLM response")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="legacy_approval_test",
            prompt="Summarize this",
            llm_conn_id="my_llm",
            require_approval=True,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            op.execute(context=_make_context())

        assert exc_info.value.method_name == "execute_complete"
        assert exc_info.value.kwargs["generated_output"] == "LLM response"
        mock_trigger_cls.assert_called_once()
        mock_upsert.assert_called_once()

    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_and_modifications(
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, make_mock_run_result
    ):
        """allow_modifications=True passes an editable 'output' param."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("draft output")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="mod_test",
            prompt="Write a draft",
            llm_conn_id="my_llm",
            require_approval=True,
            allow_modifications=True,
        )
        ctx = _make_context()

        with pytest.raises(ApprovalPauseSignal):
            op.execute(context=ctx)

        upsert_kwargs = mock_upsert.call_args[1]
        assert "output" in upsert_kwargs["params"]

    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_and_timeout(
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, make_mock_run_result
    ):
        """approval_timeout is passed to the trigger."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("output")
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

        with pytest.raises(ApprovalPauseSignal) as exc_info:
            op.execute(context=ctx)

        if AIRFLOW_V_3_3_PLUS:
            assert exc_info.value.timeout == timeout
        else:
            assert mock_trigger_cls.call_args[1]["timeout_datetime"] is not None

    @patch("airflow.providers.standard.triggers.hitl.HITLTrigger", autospec=True)
    @patch("airflow.sdk.execution_time.hitl.upsert_hitl_detail")
    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_with_approval_structured_output(
        self, mock_hook_cls, mock_upsert, mock_trigger_cls, make_mock_run_result
    ):
        """Structured (BaseModel) output is serialized before deferring."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result(Summary(text="hello"))
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="struct_test",
            prompt="Summarize",
            llm_conn_id="my_llm",
            output_type=Summary,
            require_approval=True,
        )
        ctx = _make_context()

        with pytest.raises(ApprovalPauseSignal) as exc_info:
            op.execute(context=ctx)

        assert exc_info.value.kwargs["generated_output"] == '{"text":"hello"}'

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_without_approval_returns_normally(self, mock_hook_cls, make_mock_run_result):
        """When require_approval=False, execute() returns output directly."""
        mock_agent = MagicMock(spec=["run_sync"])
        mock_agent.run_sync.return_value = make_mock_run_result("plain output")
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(task_id="no_approval", prompt="p", llm_conn_id="my_llm", require_approval=False)
        result = op.execute(context={})

        assert result == "plain output"

    def test_execute_complete_approved(self):
        """execute_complete returns output when approved."""
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c")
        event = {"chosen_options": ["Approve"], "responded_by_user": {"id": "u1", "name": "admin"}}

        result = op.execute_complete({}, generated_output="the output", event=event)

        assert result == "the output"

    def test_execute_complete_rejected(self):
        """execute_complete raises HITLRejectException when rejected."""
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c")
        event = {"chosen_options": ["Reject"], "responded_by_user": {"id": "u1", "name": "admin"}}

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
            "responded_by_user": {"id": "u1", "name": "editor"},
            "params_input": {"output": "edited"},
        }

        result = op.execute_complete({}, generated_output="original", event=event)

        assert result == "edited"

    @requires_typed_xcom
    def test_execute_complete_rehydrates_pydantic_for_structured_output(self):
        """When output_type is a BaseModel, execute_complete returns the model, not the JSON string."""
        op = LLMOperator(task_id="t", prompt="p", llm_conn_id="c", output_type=Summary)
        event = {"chosen_options": ["Approve"], "responded_by_user": {"id": "u1", "name": "admin"}}

        result = op.execute_complete({}, generated_output='{"text":"hello"}', event=event)

        assert isinstance(result, Summary)
        assert result.text == "hello"

    @pytest.mark.parametrize(
        ("output_type", "generated_output", "expected"),
        [
            (int, "5", 5),
            (list[str], '["a","b"]', ["a", "b"]),
            pytest.param(Summary, '{"text":"hello"}', Summary(text="hello"), marks=requires_typed_xcom),
        ],
        ids=["int", "list", "basemodel"],
    )
    def test_execute_complete_restores_non_str_output_type(self, output_type, generated_output, expected):
        op = LLMOperator(
            task_id="t", prompt="p", llm_conn_id="c", output_type=output_type, require_approval=True
        )
        event = {"chosen_options": ["Approve"], "responded_by_user": {"id": "u1", "name": "admin"}}

        result = op.execute_complete({}, generated_output=generated_output, event=event)

        assert result == expected


@pytest.mark.skipif(
    not AIRFLOW_V_3_1_PLUS, reason="Human in the loop is only compatible with Airflow >= 3.1.0"
)
class TestLLMOperatorMultimodalPromptGuard:
    """LLMOperator.execute raises before agent.run_sync when require_approval is True
    and self.prompt is not a string -- covering direct-operator construction and the
    native template rendering escape (where a string template renders to a Sequence)."""

    @patch("airflow.providers.common.ai.operators.llm.PydanticAIHook", autospec=True)
    def test_execute_rejects_sequence_prompt_with_require_approval(self, mock_hook_cls):
        mock_agent = MagicMock(spec=["run_sync"])
        mock_hook_cls.get_hook.return_value.create_agent.return_value = mock_agent

        op = LLMOperator(
            task_id="t",
            prompt="placeholder",
            llm_conn_id="c",
            require_approval=True,
        )
        op.prompt = ["x", object()]  # simulate post-template-render value

        with pytest.raises(TypeError, match="require_approval=True"):
            op.execute(context=_make_context())

        mock_agent.run_sync.assert_not_called()
