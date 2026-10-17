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
"""Per-tool approval: AgentOperator pauses before a tool that requires approval and resumes."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai import Agent, CallDeferred, DeferredToolRequests, Tool
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.toolsets.function import FunctionToolset
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.exceptions import (
    ToolApprovalAlreadyRequestedError,
    ToolApprovalError,
    UnsupportedToolDeferralError,
)
from airflow.providers.common.ai.operators.agent import (
    _TOOL_APPROVAL_REQUESTED_KEY,
    _TOOL_APPROVAL_TRANSCRIPT_KEY,
    AgentOperator,
)
from airflow.providers.common.ai.sandbox.base import SandboxBackend
from airflow.providers.common.ai.toolsets.sandbox import SandboxToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.providers.common.ai.utils.usage_budget import USAGE_BUDGET_KEY, dump_run_usage
from airflow.providers.standard.exceptions import HITLTimeoutError

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS, AIRFLOW_V_3_4_PLUS

pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="Per-tool approval needs Airflow 3.3+.")

if AIRFLOW_V_3_3_PLUS:
    from airflow.sdk.exceptions import TaskAwaitingInput

UPSERT = "airflow.providers.common.ai.operators.agent.upsert_hitl_detail"
REPEAT_SUPPORTED = "airflow.providers.common.ai.operators.agent._REPEAT_APPROVAL_SUPPORTED"


class _FakeTaskStateStore:
    """Dict-backed stand-in for the task state store accessor."""

    def __init__(self):
        self.data: dict[str, Any] = {}

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value, *, retention=None):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)


class _Shop:
    """Two tools: ``lookup`` runs freely, ``refund`` needs a human."""

    def __init__(self):
        self.calls: list[tuple[str, int]] = []

    def toolset(self):
        def lookup(order_id: int) -> str:
            self.calls.append(("lookup", order_id))
            return f"order {order_id} costs $10"

        def refund(order_id: int) -> str:
            self.calls.append(("refund", order_id))
            return f"refunded order {order_id}"

        return FunctionToolset(tools=[lookup, refund]).approval_required(
            lambda ctx, tool_def, args: tool_def.name == "refund"
        )


def _returns(messages) -> list[str]:
    return [str(p.content) for m in messages for p in m.parts if isinstance(p, ToolReturnPart)]


def _lookup_and_refund(messages, info: AgentInfo) -> ModelResponse:
    """Call both tools in parallel, then report every tool result."""
    if not _returns(messages):
        return ModelResponse(
            parts=[
                ToolCallPart("lookup", {"order_id": 1}, tool_call_id="c-lookup"),
                ToolCallPart("refund", {"order_id": 1}, tool_call_id="c-refund"),
            ]
        )
    return ModelResponse(parts=[TextPart(" | ".join(_returns(messages)))])


def _refunds_one_per_step(count: int):
    """Refund orders 1..count, one call per step, so each refund needs its own approval."""

    def model(messages, info: AgentInfo) -> ModelResponse:
        done = _returns(messages)
        if len(done) < count:
            n = len(done) + 1
            return ModelResponse(parts=[ToolCallPart("refund", {"order_id": n}, tool_call_id=f"c-{n}")])
        return ModelResponse(parts=[TextPart(" | ".join(done))])

    return model


_two_refunds = _refunds_one_per_step(2)


def _operator(model_fn, toolsets, **kwargs) -> AgentOperator:
    op = AgentOperator(task_id="t", prompt="Refund order 1", llm_conn_id="llm", toolsets=toolsets, **kwargs)
    hook = MagicMock(spec=["create_agent"])
    hook.create_agent.side_effect = lambda **kw: Agent(FunctionModel(model_fn), **kw)
    op.llm_hook = hook
    return op


def _context(store: _FakeTaskStateStore, *, try_number: int = 1) -> Any:  # a Context stand-in
    ti = MagicMock(spec=["id", "dag_id", "task_id", "run_id", "map_index", "try_number", "xcom_push"])
    ti.configure_mock(
        id="ti-1", dag_id="d", task_id="t", run_id="r", map_index=-1, try_number=try_number, max_tries=0
    )
    return {"task_instance": ti, "task_state_store": store}


APPROVE = {"chosen_options": ["Approve"], "params_input": {}, "responded_by_user": {"name": "alice"}}


def _reject(reason: str = "") -> dict[str, Any]:
    return {
        "chosen_options": ["Reject"],
        "params_input": {"reason": reason},
        "responded_by_user": {"name": "bob"},
    }


def _pause(op: AgentOperator, ctx: Any) -> TaskAwaitingInput:
    with patch(UPSERT, autospec=True):
        with pytest.raises(TaskAwaitingInput) as exc:
            op.execute(ctx)
    return exc.value


class TestPause:
    def test_pauses_before_the_gated_tool_and_runs_the_ungated_one(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        op = _operator(
            _lookup_and_refund,
            [shop.toolset()],
            tool_approval_timeout=timedelta(hours=1),
            tool_approval_assigned_users={"id": "u1", "name": "alice"},
        )

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput) as exc:
                op.execute(_context(store))

        assert shop.calls == [("lookup", 1)]
        assert exc.value.method_name == "resume_after_tool_approval"
        assert exc.value.kwargs["tool_call_ids"] == ["c-refund"]
        assert _TOOL_APPROVAL_TRANSCRIPT_KEY in store.data
        body = upsert.call_args.kwargs["body"]
        assert "**refund**" in body
        assert '"order_id": 1' in body
        assert "lookup" not in body
        assert upsert.call_args.kwargs["options"] == ["Approve", "Reject"]
        assert upsert.call_args.kwargs["defaults"] is None
        assert upsert.call_args.kwargs["subject"] == "Approve refund for task `t` (request 1)"
        assert upsert.call_args.kwargs["multiple"] is False
        # Optional in the review form: without "null" the UI requires a reason even to approve.
        assert upsert.call_args.kwargs["params"]["reason"]["schema"] == {"type": ["string", "null"]}
        # Hidden in the review form ("const"), and sent back with the answer.
        assert upsert.call_args.kwargs["params"]["request"]["schema"] == {"type": "integer", "const": 1}
        assert upsert.call_args.kwargs["assigned_users"] == [{"id": "u1", "name": "alice"}]
        assert exc.value.timeout == timedelta(hours=1)

    def test_a_function_tool_marked_requires_approval_pauses_too(self):
        refunds = []

        def refund(order_id: int) -> str:
            refunds.append(order_id)
            return "refunded"

        def model(messages, info):
            if _returns(messages):
                return ModelResponse(parts=[TextPart("done")])
            return ModelResponse(parts=[ToolCallPart("refund", {"order_id": 3})])

        toolset = FunctionToolset(tools=[Tool(refund, requires_approval=True)])
        with patch(UPSERT, autospec=True):
            with pytest.raises(TaskAwaitingInput):
                _operator(model, [toolset]).execute(_context(_FakeTaskStateStore()))

        assert refunds == []

    @pytest.mark.enable_redact
    def test_tool_arguments_are_masked_in_the_review_body(self):
        def call_api(url: str, api_key: str) -> str:
            return "ok"

        toolset = FunctionToolset(tools=[call_api]).approval_required()

        def model(messages, info):
            return ModelResponse(parts=[ToolCallPart("call_api", {"url": "https://x", "api_key": "s3cr3t"})])

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput):
                _operator(model, [toolset]).execute(_context(_FakeTaskStateStore()))

        assert "s3cr3t" not in upsert.call_args.kwargs["body"]

    @patch(REPEAT_SUPPORTED, False)
    def test_before_airflow_3_4_a_later_try_fails_closed_once_an_approval_was_requested(self):
        """Before 3.4, core keeps one approval request per task instance across retries and clears,
        with the first request's subject and body, so a retry's request would show the earlier call."""
        shop, store = _Shop(), _FakeTaskStateStore()
        _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))
        assert store.data[_TOOL_APPROVAL_REQUESTED_KEY] is True

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(ToolApprovalAlreadyRequestedError, match="already asked once"):
                _operator(_lookup_and_refund, [shop.toolset()]).execute(_context(store, try_number=2))

        upsert.assert_not_called()

    @patch(REPEAT_SUPPORTED, True)
    def test_from_airflow_3_4_no_once_per_run_marker_is_written(self):
        store = _FakeTaskStateStore()
        _pause(_operator(_lookup_and_refund, [_Shop().toolset()]), _context(store))
        assert _TOOL_APPROVAL_REQUESTED_KEY not in store.data

    @pytest.mark.skipif(AIRFLOW_V_3_4_PLUS, reason="The once-per-run rule applies before Airflow 3.4.")
    def test_on_the_installed_airflow_before_3_4_a_second_request_fails(self):
        """No patch: checks the real version gate on the Airflow this suite runs against."""
        shop, store = _Shop(), _FakeTaskStateStore()
        _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))
        with patch(UPSERT, autospec=True):
            with pytest.raises(ToolApprovalAlreadyRequestedError):
                _operator(_lookup_and_refund, [shop.toolset()]).execute(_context(store, try_number=2))

    @pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Repeat requests need Airflow 3.4.")
    def test_on_the_installed_airflow_3_4_a_second_request_pauses(self):
        """No patch: checks the real version gate on the Airflow this suite runs against."""
        shop, store = _Shop(), _FakeTaskStateStore()
        _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))
        with patch(UPSERT, autospec=True):
            with pytest.raises(TaskAwaitingInput):
                _operator(_lookup_and_refund, [shop.toolset()]).execute(_context(store, try_number=2))

    @patch(REPEAT_SUPPORTED, True)
    def test_a_later_try_asks_again(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput):
                _operator(_lookup_and_refund, [shop.toolset()]).execute(_context(store, try_number=2))

        upsert.assert_called_once()
        assert '"order_id": 1' in upsert.call_args.kwargs["body"]

    @patch(REPEAT_SUPPORTED, False)
    def test_a_failed_request_does_not_block_the_retry(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        with patch(UPSERT, autospec=True, side_effect=RuntimeError("api down")):
            with pytest.raises(RuntimeError, match="api down"):
                _operator(_lookup_and_refund, [shop.toolset()]).execute(_context(store))

        assert _TOOL_APPROVAL_REQUESTED_KEY not in store.data

    def test_a_fresh_run_deletes_a_stale_transcript(self):
        store = _FakeTaskStateStore()
        store.data[_TOOL_APPROVAL_TRANSCRIPT_KEY] = "left over from a try that ended while waiting"

        def answer(messages, info):
            return ModelResponse(parts=[TextPart("done")])

        assert _operator(answer, [_Shop().toolset()]).execute(_context(store)) == "done"
        assert _TOOL_APPROVAL_TRANSCRIPT_KEY not in store.data

    def test_deny_on_timeout_sets_reject_as_the_timeout_default(self):
        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput):
                _operator(
                    _lookup_and_refund,
                    [_Shop().toolset()],
                    on_tool_approval_timeout="deny",
                    tool_approval_timeout=timedelta(hours=1),
                ).execute(_context(_FakeTaskStateStore()))

        assert upsert.call_args.kwargs["defaults"] == ["Reject"]

    def test_a_user_set_deferred_output_type_fails_where_approval_is_unsupported(self):
        """Adding DeferredToolRequests by hand must not open a pause that durable, HITL review,
        code mode or a sandbox cannot survive."""
        op = _operator(
            _lookup_and_refund,
            [_Shop().toolset()],
            output_type=[str, DeferredToolRequests],
            enable_hitl_review=True,
        )

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(UnsupportedToolDeferralError, match="not available with durable"):
                op.execute(_context(_FakeTaskStateStore()))

        upsert.assert_not_called()

    def test_external_execution_calls_are_refused(self):
        def slow_job() -> str:
            raise CallDeferred

        def model(messages, info):
            return ModelResponse(parts=[ToolCallPart("slow_job", {})])

        with patch(UPSERT, autospec=True):
            with pytest.raises(UnsupportedToolDeferralError, match="need external execution"):
                _operator(model, [FunctionToolset(tools=[slow_job])]).execute(_context(_FakeTaskStateStore()))


class TestResume:
    def test_approve_runs_the_call_once_and_finishes(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))

        # A resume is a fresh process: a new operator, same task state store.
        output = _operator(_lookup_and_refund, [shop.toolset()]).resume_after_tool_approval(
            _context(store), **paused.kwargs, event=APPROVE
        )

        assert shop.calls == [("lookup", 1), ("refund", 1)]
        assert "refunded order 1" in output
        assert "order 1 costs $10" in output
        assert _TOOL_APPROVAL_TRANSCRIPT_KEY not in store.data

    def test_the_resumed_run_can_be_cancelled_by_on_kill(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))
        tokens = []

        def recording_model(messages, info: AgentInfo) -> ModelResponse:
            tokens.append(resumed._cancellation_token)
            return _lookup_and_refund(messages, info)

        resumed = _operator(recording_model, [shop.toolset()])
        resumed.resume_after_tool_approval(_context(store), **paused.kwargs, event=APPROVE)

        assert tokens
        assert all(token is not None for token in tokens)
        assert resumed._cancellation_token is None

    def test_reject_tells_the_agent_why_and_skips_the_call(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))

        output = _operator(_lookup_and_refund, [shop.toolset()]).resume_after_tool_approval(
            _context(store), **paused.kwargs, event=_reject("refunds need a ticket")
        )

        assert shop.calls == [("lookup", 1)]
        assert "refunds need a ticket" in output

    @pytest.mark.parametrize(
        "untouched",
        [
            pytest.param("", id="empty"),
            pytest.param(None, id="null"),
            pytest.param({"value": None, "schema": {"type": ["string", "null"]}}, id="param-spec"),
        ],
    )
    def test_reject_without_a_typed_reason_sends_a_default_message(self, untouched):
        """An untouched reason field comes back as "", None, or (from some UI versions) the spec."""
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))
        event = {
            "chosen_options": ["Reject"],
            "params_input": {"reason": untouched},
            "responded_by_user": None,
        }

        output = _operator(_lookup_and_refund, [shop.toolset()]).resume_after_tool_approval(
            _context(store), **paused.kwargs, event=event
        )

        assert "A reviewer denied this tool call." in output
        assert "schema" not in output

    def test_timeout_fails_the_task_by_default(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))

        with pytest.raises(HITLTimeoutError):
            _operator(_lookup_and_refund, [shop.toolset()]).resume_after_tool_approval(
                _context(store), **paused.kwargs, event={"error": "expired", "error_type": "timeout"}
            )
        assert shop.calls == [("lookup", 1)]

    def test_a_timed_out_deny_does_not_claim_a_reviewer_refused(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))
        timed_out = {
            "chosen_options": ["Reject"],
            "params_input": {},
            "responded_by_user": None,
            "timedout": True,
        }

        output = _operator(_lookup_and_refund, [shop.toolset()]).resume_after_tool_approval(
            _context(store), **paused.kwargs, event=timed_out
        )

        assert "No reviewer answered within the approval timeout" in output
        assert shop.calls == [("lookup", 1)]

    @patch(REPEAT_SUPPORTED, False)
    def test_before_airflow_3_4_a_second_approval_in_the_same_try_fails_closed(self):
        """Before 3.4, a second request would show the first one's details, so the reviewer would
        approve refund 2 while reading refund 1."""
        shop, store = _Shop(), _FakeTaskStateStore()
        first = _pause(_operator(_two_refunds, [shop.toolset()]), _context(store))

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(ToolApprovalAlreadyRequestedError, match="already asked once"):
                _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
                    _context(store), **first.kwargs, event=APPROVE
                )

        assert shop.calls == [("refund", 1)]
        upsert.assert_not_called()
        assert _TOOL_APPROVAL_TRANSCRIPT_KEY not in store.data

    @patch(REPEAT_SUPPORTED, True)
    def test_a_later_step_pauses_again_with_its_own_call(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        first = _pause(_operator(_two_refunds, [shop.toolset()]), _context(store))

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput) as second:
                _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
                    _context(store), **first.kwargs, event=APPROVE
                )

        assert shop.calls == [("refund", 1)]
        assert first.kwargs["pause_number"] == 1
        assert second.value.kwargs["pause_number"] == 2
        assert second.value.kwargs["tool_call_ids"] == ["c-2"]
        assert upsert.call_args.kwargs["subject"] == "Approve refund for task `t` (request 2)"
        body = upsert.call_args.kwargs["body"]
        assert '"order_id": 2' in body
        assert '"order_id": 1' not in body
        # The new pause's transcript, which its resume needs, survives the resume that paused again.
        assert _TOOL_APPROVAL_TRANSCRIPT_KEY in store.data

    @patch(REPEAT_SUPPORTED, True)
    def test_an_agent_that_needs_ten_approvals_pauses_ten_times_and_finishes(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        model = _refunds_one_per_step(10)
        paused = _pause(_operator(model, [shop.toolset()]), _context(store))
        pauses = 1
        output = None
        while output is None:
            ctx = _context(store)
            with patch(UPSERT, autospec=True):
                try:
                    output = _operator(model, [shop.toolset()]).resume_after_tool_approval(
                        ctx, **paused.kwargs, event=APPROVE
                    )
                except TaskAwaitingInput as again:
                    paused, pauses = again, pauses + 1

        assert pauses == 10
        # Every resume gets its own run_id; the last one is what the run_id XCom holds.
        ctx["task_instance"].xcom_push.assert_any_call(key="run_id", value="ti-1-resumed-10")
        assert shop.calls == [("refund", n) for n in range(1, 11)]
        assert output == " | ".join(f"refunded order {n}" for n in range(1, 11))
        assert _TOOL_APPROVAL_TRANSCRIPT_KEY not in store.data

    @patch(REPEAT_SUPPORTED, True)
    def test_a_rejected_later_step_carries_on_without_that_call(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        first = _pause(_operator(_two_refunds, [shop.toolset()]), _context(store))
        with patch(UPSERT, autospec=True):
            with pytest.raises(TaskAwaitingInput) as second:
                _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
                    _context(store), **first.kwargs, event=APPROVE
                )

        output = _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
            _context(store), **second.value.kwargs, event=_reject("order 2 is outside policy")
        )

        assert shop.calls == [("refund", 1)]
        assert "refunded order 1" in output
        assert "order 2 is outside policy" in output

    @pytest.mark.parametrize(
        "stale_value",
        [
            pytest.param(1, id="number"),
            pytest.param("1", id="string"),
            pytest.param({"value": 1, "schema": {"type": "integer", "const": 1}}, id="param-spec"),
            pytest.param("not-a-number", id="unreadable"),
        ],
    )
    @patch(REPEAT_SUPPORTED, True)
    def test_an_answer_from_a_form_left_open_on_an_earlier_pause_asks_again(self, stale_value):
        """Reviewer B still has request 1's form open; their Approve must not run refund 2."""
        shop, store = _Shop(), _FakeTaskStateStore()
        first = _pause(_operator(_two_refunds, [shop.toolset()]), _context(store))
        with patch(UPSERT, autospec=True):
            with pytest.raises(TaskAwaitingInput) as second:
                _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
                    _context(store), **first.kwargs, event=APPROVE
                )
        stale = {**APPROVE, "params_input": {"request": stale_value}}

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput) as asked_again:
                _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
                    _context(store), **second.value.kwargs, event=stale
                )

        assert shop.calls == [("refund", 1)]
        assert asked_again.value.kwargs["tool_call_ids"] == ["c-2"]
        assert asked_again.value.kwargs["pause_number"] == 3
        assert asked_again.value.kwargs["transcript_sha256"] == second.value.kwargs["transcript_sha256"]
        assert '"order_id": 2' in upsert.call_args.kwargs["body"]
        assert _TOOL_APPROVAL_TRANSCRIPT_KEY in store.data

        output = _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
            _context(store), **asked_again.value.kwargs, event={**APPROVE, "params_input": {"request": 3}}
        )
        assert shop.calls == [("refund", 1), ("refund", 2)]
        assert output == "refunded order 1 | refunded order 2"

    @patch(REPEAT_SUPPORTED, True)
    def test_an_answer_for_the_waiting_request_is_applied(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        first = _pause(_operator(_two_refunds, [shop.toolset()]), _context(store))
        with patch(UPSERT, autospec=True):
            with pytest.raises(TaskAwaitingInput) as second:
                _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
                    _context(store), **first.kwargs, event={**APPROVE, "params_input": {"request": 1}}
                )

        output = _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
            _context(store), **second.value.kwargs, event={**APPROVE, "params_input": {"request": 2}}
        )

        assert output == "refunded order 1 | refunded order 2"

    @patch(REPEAT_SUPPORTED, True)
    def test_a_continuation_without_pause_number_resumes_as_the_first_pause(self):
        """A task paused by an earlier provider version has no ``pause_number`` in its kwargs; it
        resumes as pause 1, so a later pause is numbered 2 and its resume gets a fresh run_id."""
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_two_refunds, [shop.toolset()]), _context(store))
        kwargs = {k: v for k, v in paused.kwargs.items() if k != "pause_number"}

        with patch(UPSERT, autospec=True):
            with pytest.raises(TaskAwaitingInput) as second:
                _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
                    _context(store), **kwargs, event=APPROVE
                )
        output = _operator(_two_refunds, [shop.toolset()]).resume_after_tool_approval(
            _context(store), **second.value.kwargs, event=APPROVE
        )

        assert second.value.kwargs["pause_number"] == 2
        assert output == "refunded order 1 | refunded order 2"

    @pytest.mark.parametrize(
        ("kwargs_override", "event", "error"),
        [
            pytest.param({}, {"error": "expired", "error_type": "timeout"}, HITLTimeoutError, id="timeout"),
            pytest.param({"toolset_ids": ["sql-other"]}, APPROVE, ToolApprovalError, id="toolsets-changed"),
        ],
    )
    def test_the_transcript_is_deleted_when_the_resume_fails(self, kwargs_override, event, error):
        """The transcript holds tool results, so it must not outlive a failed resume."""
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))

        with pytest.raises(error):
            _operator(_lookup_and_refund, [shop.toolset()]).resume_after_tool_approval(
                _context(store), **{**paused.kwargs, **kwargs_override}, event=event
            )

        assert _TOOL_APPROVAL_TRANSCRIPT_KEY not in store.data

    def test_usage_limits_span_the_pause(self):
        """Each side of the pause makes one model request; a limit of one must stop the second."""
        shop, store = _Shop(), _FakeTaskStateStore()
        limits = {"request_limit": 1}
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()], usage_limits=limits), _context(store))

        with pytest.raises(UsageLimitExceeded):
            _operator(_lookup_and_refund, [shop.toolset()], usage_limits=limits).resume_after_tool_approval(
                _context(store), **paused.kwargs, event=APPROVE
            )

    def test_changed_toolsets_refuse_to_run_the_approved_call(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))
        kwargs = {**paused.kwargs, "toolset_ids": ["sql-tenant_acme"]}

        with pytest.raises(ToolApprovalError, match="toolsets changed"):
            _operator(_lookup_and_refund, [shop.toolset()]).resume_after_tool_approval(
                _context(store), **kwargs, event=APPROVE
            )
        assert shop.calls == [("lookup", 1)]

    def test_a_rendered_connection_that_changed_refuses_to_run_the_approved_call(self):
        shop, store = _Shop(), _FakeTaskStateStore()

        def operator():
            toolsets = [shop.toolset(), SQLToolset(db_conn_id="tenant_{{ params.customer }}")]
            return _operator(_lookup_and_refund, toolsets)

        before = operator()
        before.render_template_fields({"params": {"customer": "acme"}})
        paused = _pause(before, _context(store))
        after = operator()
        after.render_template_fields({"params": {"customer": "globex"}})

        assert paused.kwargs["toolset_ids"] == ["sql-tenant_acme"]
        with pytest.raises(ToolApprovalError, match="toolsets changed"):
            after.resume_after_tool_approval(_context(store), **paused.kwargs, event=APPROVE)
        assert shop.calls == [("lookup", 1)]

    def test_modified_transcript_is_refused(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))
        store.data[_TOOL_APPROVAL_TRANSCRIPT_KEY] += " "

        with pytest.raises(ToolApprovalError, match="missing or was modified"):
            _operator(_lookup_and_refund, [shop.toolset()]).resume_after_tool_approval(
                _context(store), **paused.kwargs, event=APPROVE
            )

    def test_message_history_output_holds_the_whole_run(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()], message_history=[]), _context(store))
        ctx = _context(store)

        _operator(_lookup_and_refund, [shop.toolset()], message_history=[]).resume_after_tool_approval(
            ctx, **paused.kwargs, event=APPROVE
        )

        pushed = {c.kwargs["key"]: c.kwargs["value"] for c in ctx["task_instance"].xcom_push.call_args_list}
        assert "order 1 costs $10" in pushed["message_history"]
        assert "refunded order 1" in pushed["message_history"]

    @staticmethod
    def _seed_earlier_attempts_spend(store: _FakeTaskStateStore, requests: int) -> None:
        store.data[USAGE_BUDGET_KEY] = {
            "version": 1,
            "max_tries": 0,
            "usage": dump_run_usage(RunUsage(requests=requests)),
        }

    def test_usage_output_covers_this_attempt_on_both_sides_of_the_pause(self):
        """Earlier attempts' spend stays out of ``usage``; this attempt's pre-pause request stays in."""
        shop, store = _Shop(), _FakeTaskStateStore()
        self._seed_earlier_attempts_spend(store, requests=5)
        limits = {"request_limit": 10}
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()], usage_limits=limits), _context(store))
        ctx = _context(store)

        _operator(_lookup_and_refund, [shop.toolset()], usage_limits=limits).resume_after_tool_approval(
            ctx, **paused.kwargs, event=APPROVE
        )

        pushed = {c.kwargs["key"]: c.kwargs["value"] for c in ctx["task_instance"].xcom_push.call_args_list}
        assert pushed["usage"]["requests"] == 2

    def test_a_continuation_without_attempt_usage_reports_only_the_resumed_side(self):
        """A task paused by a provider version that did not save ``attempt_usage`` still resumes."""
        shop, store = _Shop(), _FakeTaskStateStore()
        self._seed_earlier_attempts_spend(store, requests=5)
        limits = {"request_limit": 10}
        paused = _pause(_operator(_lookup_and_refund, [shop.toolset()], usage_limits=limits), _context(store))
        kwargs = {k: v for k, v in paused.kwargs.items() if k != "attempt_usage"}
        ctx = _context(store)

        _operator(_lookup_and_refund, [shop.toolset()], usage_limits=limits).resume_after_tool_approval(
            ctx, **kwargs, event=APPROVE
        )

        pushed = {c.kwargs["key"]: c.kwargs["value"] for c in ctx["task_instance"].xcom_push.call_args_list}
        assert pushed["usage"]["requests"] == 1


class _NoopBackend(SandboxBackend):
    """A backend that is never reached: these tests stop before any run."""

    name = "noop"

    def create(self, *, spec=None):
        raise NotImplementedError

    def run_command(self, sandbox, command, *, timeout, max_output_bytes):
        raise NotImplementedError

    def destroy(self, sandbox):
        pass


class TestWhenApprovalApplies:
    def test_output_type_gains_deferred_requests_without_changing_the_attribute(self):
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="llm")

        assert op._agent_output_type() == [str, DeferredToolRequests]
        assert op.output_type is str

    def test_a_list_output_type_is_extended_not_nested(self):
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="llm", output_type=[str, int])

        assert op._agent_output_type() == [str, int, DeferredToolRequests]

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"enable_hitl_review": True}, id="hitl_review"),
            pytest.param({"toolsets": [SandboxToolset(_NoopBackend())]}, id="sandbox"),
            pytest.param(
                {"agent_params": {"toolsets": [SandboxToolset(_NoopBackend())]}}, id="sandbox-in-agent-params"
            ),
        ],
    )
    def test_features_that_assume_one_uninterrupted_run_keep_the_output_type(self, kwargs):
        op = AgentOperator(task_id="t", prompt="p", llm_conn_id="llm", **kwargs)

        assert op._agent_output_type() is str

    def test_on_tool_approval_timeout_rejects_unknown_values(self):
        with pytest.raises(ValueError, match="on_tool_approval_timeout"):
            AgentOperator(task_id="t", prompt="p", llm_conn_id="llm", on_tool_approval_timeout="approve")

    @pytest.mark.parametrize("timeout", [timedelta(0), timedelta(seconds=-1)])
    def test_tool_approval_timeout_must_be_positive(self, timeout):
        with pytest.raises(ValueError, match="must be positive"):
            AgentOperator(task_id="t", prompt="p", llm_conn_id="llm", tool_approval_timeout=timeout)

    def test_deny_needs_a_timeout_to_fire(self):
        with pytest.raises(ValueError, match="needs a tool_approval_timeout"):
            AgentOperator(task_id="t", prompt="p", llm_conn_id="llm", on_tool_approval_timeout="deny")

    def test_a_single_assigned_user_is_accepted(self):
        op = AgentOperator(
            task_id="t", prompt="p", llm_conn_id="llm", tool_approval_assigned_users={"id": "u1", "name": "a"}
        )

        assert op.tool_approval_assigned_users == [{"id": "u1", "name": "a"}]

    def test_malformed_assigned_users_fail_at_parse_time(self):
        with pytest.raises(TypeError, match="tool_approval_assigned_users entries must be"):
            AgentOperator(task_id="t", prompt="p", llm_conn_id="llm", tool_approval_assigned_users=["alice"])

    def test_a_sandbox_passed_through_agent_params_is_refused_with_durable(self):
        with pytest.raises(ValueError, match="SandboxToolset"):
            AgentOperator(
                task_id="t",
                prompt="p",
                llm_conn_id="llm",
                durable=True,
                agent_params={"toolsets": [SandboxToolset(_NoopBackend())]},
            )
