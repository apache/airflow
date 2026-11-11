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

import json
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai import (
    Agent,
    ApprovalRequired,
    CallDeferred,
    DeferredToolRequests,
    DeferredToolResults,
    RunContext,
    Tool,
)
from pydantic_ai.capabilities import CapabilityOrdering, HandleDeferredToolCalls
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.toolsets.function import FunctionToolset
from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.decorators.agent import _AgentDecoratedOperator
from airflow.providers.common.ai.durable.journal import DurableRun
from airflow.providers.common.ai.exceptions import (
    ToolApprovalAlreadyRequestedError,
    ToolApprovalError,
    UnsupportedToolDeferralError,
)
from airflow.providers.common.ai.operators.agent import (
    _TOOL_APPROVAL_ANSWERS_KEY,
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
from unit.common.ai.tool_parts import is_tool_result

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
        # Stored as JSON, as the real store does, so a test sees what a resume would read back.
        self.data[key] = json.loads(json.dumps(value))

    def delete(self, key):
        self.data.pop(key, None)


class _Shop:
    """Two tools: ``lookup`` runs freely, ``refund`` needs a human."""

    def __init__(self):
        self.calls: list[tuple[str, int]] = []

    def toolset(self, *, live_lookup: bool = False):
        """``live_lookup`` opts ``lookup`` out of the durable journal, so it runs on every attempt."""

        def lookup(order_id: int) -> str:
            self.calls.append(("lookup", order_id))
            return f"order {order_id} costs $10"

        def refund(order_id: int) -> str:
            self.calls.append(("refund", order_id))
            return f"refunded order {order_id}"

        tools = [Tool(lookup, metadata={"airflow": False}) if live_lookup else lookup, refund]
        return FunctionToolset(tools=tools).approval_required(
            lambda ctx, tool_def, args: tool_def.name == "refund"
        )


def _returns(messages) -> list[str]:
    return [str(p.content) for m in messages for p in m.parts if is_tool_result(p)]


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
        """Adding DeferredToolRequests by hand must not open a pause that HITL review, code mode
        or a sandbox cannot survive."""
        op = _operator(
            _lookup_and_refund,
            [_Shop().toolset()],
            output_type=[str, DeferredToolRequests],
            enable_hitl_review=True,
        )

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(UnsupportedToolDeferralError, match="not available with enable_hitl_review"):
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


class _Counted:
    """A FunctionModel function that counts the requests reaching the model, which replays skip."""

    def __init__(self, model_fn):
        self.model_fn = model_fn
        self.calls = 0

    def __call__(self, messages, info: AgentInfo) -> ModelResponse:
        self.calls += 1
        return self.model_fn(messages, info)


def _fails_once_after_refund(model_fn):
    """``model_fn``, except that the request after the refund ran fails the first time."""
    failed = []

    def model(messages, info: AgentInfo) -> ModelResponse:
        if not failed and any("refunded" in result for result in _returns(messages)):
            failed.append(True)
            raise RuntimeError("model unavailable")
        return model_fn(messages, info)

    return model


def _durable(model_fn, toolsets, **kwargs) -> AgentOperator:
    return _operator(model_fn, toolsets, durable=True, **kwargs)


def _durable_pause(op: AgentOperator, ctx: Any) -> TaskAwaitingInput:
    paused = _pause(op, ctx)
    assert paused.method_name == "resume_after_durable_tool_approval"
    return paused


class TestDurable:
    """With ``durable=True`` the approval is a step of the journal, and the resume replays up to it."""

    def test_pauses_without_saving_a_transcript_or_reporting_a_failed_run(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        ctx = _context(store)

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput) as exc:
                _durable(_lookup_and_refund, [shop.toolset()]).execute(ctx)

        ctx["task_instance"].xcom_push.assert_not_called()

        assert exc.value.method_name == "resume_after_durable_tool_approval"
        assert [name for name, _ in exc.value.kwargs["calls"]] == ["refund"]
        assert exc.value.kwargs["pause_number"] == 1
        assert RunUsage(**exc.value.kwargs["usage"]).requests == 1
        assert shop.calls == [("lookup", 1)]
        assert '"order_id": 1' in upsert.call_args.kwargs["body"]
        assert _TOOL_APPROVAL_TRANSCRIPT_KEY not in store.data

    def test_approve_replays_the_steps_before_the_pause_and_runs_the_call_once(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        model = _Counted(_lookup_and_refund)
        paused = _durable_pause(_durable(model, [shop.toolset()]), _context(store))

        output = _durable(model, [shop.toolset()]).resume_after_durable_tool_approval(
            _context(store), **paused.kwargs, event=APPROVE
        )

        assert output == "order 1 costs $10 | refunded order 1"
        assert shop.calls == [("lookup", 1), ("refund", 1)]
        # One request before the pause, and one after it: the first is replayed on resume.
        assert model.calls == 2

    def test_reject_tells_the_agent_why_and_skips_the_call(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _durable_pause(_durable(_lookup_and_refund, [shop.toolset()]), _context(store))

        output = _durable(_lookup_and_refund, [shop.toolset()]).resume_after_durable_tool_approval(
            _context(store), **paused.kwargs, event=_reject("over the refund limit")
        )

        assert shop.calls == [("lookup", 1)]
        assert "over the refund limit" in output

    @pytest.mark.parametrize("repeat_supported", [False, True], ids=["airflow-3.3", "airflow-3.4"])
    def test_a_retry_after_a_failed_resume_neither_asks_again_nor_runs_the_call_twice(self, repeat_supported):
        """On Airflow 3.3 asking again would also fail the task, as it asks once per task instance."""
        shop, store = _Shop(), _FakeTaskStateStore()
        model = _fails_once_after_refund(_lookup_and_refund)
        with patch(REPEAT_SUPPORTED, repeat_supported):
            paused = _durable_pause(_durable(model, [shop.toolset()]), _context(store))
            with pytest.raises(RuntimeError, match="model unavailable"):
                _durable(model, [shop.toolset()]).resume_after_durable_tool_approval(
                    _context(store), **paused.kwargs, event=APPROVE
                )

            with patch(UPSERT, autospec=True) as upsert:
                output = _durable(model, [shop.toolset()]).execute(_context(store, try_number=2))

        upsert.assert_not_called()
        assert shop.calls == [("lookup", 1), ("refund", 1)]
        assert output == "order 1 costs $10 | refunded order 1"

    def test_a_tool_that_raises_approval_required_itself_is_not_asked_about_again_on_retry(self):
        refunds = []

        def refund(ctx: RunContext[None], order_id: int) -> str:
            if not ctx.tool_call_approved:
                raise ApprovalRequired
            refunds.append(order_id)
            return f"refunded order {order_id}"

        def model(messages, info: AgentInfo) -> ModelResponse:
            if _returns(messages):
                return ModelResponse(parts=[TextPart(" | ".join(_returns(messages)))])
            return ModelResponse(parts=[ToolCallPart("refund", {"order_id": 1}, tool_call_id="c-refund")])

        flaky = _fails_once_after_refund(model)
        store = _FakeTaskStateStore()
        paused = _durable_pause(_durable(flaky, [FunctionToolset(tools=[refund])]), _context(store))
        with pytest.raises(RuntimeError, match="model unavailable"):
            _durable(flaky, [FunctionToolset(tools=[refund])]).resume_after_durable_tool_approval(
                _context(store), **paused.kwargs, event=APPROVE
            )

        with patch(UPSERT, autospec=True) as upsert:
            output = _durable(flaky, [FunctionToolset(tools=[refund])]).execute(_context(store, try_number=2))

        upsert.assert_not_called()
        assert refunds == [1]
        assert output == "refunded order 1"

    @patch(REPEAT_SUPPORTED, True)
    def test_a_later_step_pauses_again_and_the_last_resume_replays_both_approvals(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        model = _Counted(_two_refunds)
        first = _durable_pause(_durable(model, [shop.toolset()]), _context(store))
        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput) as second:
                _durable(model, [shop.toolset()]).resume_after_durable_tool_approval(
                    _context(store), **first.kwargs, event=APPROVE
                )

        output = _durable(model, [shop.toolset()]).resume_after_durable_tool_approval(
            _context(store), **second.value.kwargs, event=APPROVE
        )

        assert second.value.kwargs["pause_number"] == 2
        assert [name for name, _ in second.value.kwargs["calls"]] == ["refund"]
        assert upsert.call_args.kwargs["subject"] == "Approve refund for task `t` (request 2)"
        assert shop.calls == [("refund", 1), ("refund", 2)]
        assert output == "refunded order 1 | refunded order 2"
        assert model.calls == 3

    @patch(REPEAT_SUPPORTED, False)
    def test_before_airflow_3_4_a_second_approval_in_the_same_try_fails_closed(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        first = _durable_pause(_durable(_two_refunds, [shop.toolset()]), _context(store))

        with pytest.raises(ToolApprovalAlreadyRequestedError, match="already asked once"):
            _durable(_two_refunds, [shop.toolset()]).resume_after_durable_tool_approval(
                _context(store), **first.kwargs, event=APPROVE
            )
        assert shop.calls == [("refund", 1)]

    @patch(REPEAT_SUPPORTED, True)
    def test_an_answer_from_a_form_left_open_on_an_earlier_pause_asks_again(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        first = _durable_pause(_durable(_two_refunds, [shop.toolset()]), _context(store))
        with patch(UPSERT, autospec=True):
            with pytest.raises(TaskAwaitingInput) as second:
                _durable(_two_refunds, [shop.toolset()]).resume_after_durable_tool_approval(
                    _context(store), **first.kwargs, event=APPROVE
                )

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput) as asked_again:
                _durable(_two_refunds, [shop.toolset()]).resume_after_durable_tool_approval(
                    _context(store), **second.value.kwargs, event={**APPROVE, "params_input": {"request": 1}}
                )

        assert shop.calls == [("refund", 1)]
        assert asked_again.value.kwargs["pause_number"] == 3
        assert [name for name, _ in asked_again.value.kwargs["calls"]] == ["refund"]
        assert '"order_id": 2' in upsert.call_args.kwargs["body"]

    def test_changed_toolsets_refuse_to_run_the_approved_call(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _durable_pause(_durable(_lookup_and_refund, [shop.toolset()]), _context(store))

        with pytest.raises(ToolApprovalError, match="toolsets changed"):
            _durable(_lookup_and_refund, [shop.toolset()]).resume_after_durable_tool_approval(
                _context(store), **{**paused.kwargs, "toolset_ids": ["sql-tenant_acme"]}, event=APPROVE
            )
        assert shop.calls == [("lookup", 1)]

    def test_usage_output_covers_both_sides_of_the_pause(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _durable_pause(_durable(_lookup_and_refund, [shop.toolset()]), _context(store))
        ctx = _context(store)

        _durable(_lookup_and_refund, [shop.toolset()]).resume_after_durable_tool_approval(
            ctx, **paused.kwargs, event=APPROVE
        )

        pushed = {c.kwargs["key"]: c.kwargs["value"] for c in ctx["task_instance"].xcom_push.call_args_list}
        assert pushed["usage"]["requests"] == 2

    def test_usage_limits_span_the_pause(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        limits = {"request_limit": 1}
        paused = _durable_pause(
            _durable(_lookup_and_refund, [shop.toolset()], usage_limits=limits), _context(store)
        )

        with pytest.raises(UsageLimitExceeded):
            _durable(
                _lookup_and_refund, [shop.toolset()], usage_limits=limits
            ).resume_after_durable_tool_approval(_context(store), **paused.kwargs, event=APPROVE)

    def test_the_journal_run_is_paused_not_failed(self):
        """A failure can hand the next try back to an earlier try's boundary; a pause must not."""
        with (
            patch.object(DurableRun, "pause", autospec=True, side_effect=DurableRun.pause) as pause,
            patch.object(DurableRun, "fail", autospec=True, side_effect=DurableRun.fail) as fail,
        ):
            _durable_pause(_durable(_lookup_and_refund, [_Shop().toolset()]), _context(_FakeTaskStateStore()))

        pause.assert_called_once()
        fail.assert_not_called()

    def test_external_execution_calls_are_refused(self):
        def slow_job() -> str:
            raise CallDeferred

        def model(messages, info):
            return ModelResponse(parts=[ToolCallPart("slow_job", {})])

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(UnsupportedToolDeferralError, match="need external execution"):
                _durable(model, [FunctionToolset(tools=[slow_job])]).execute(_context(_FakeTaskStateStore()))
        upsert.assert_not_called()

    def test_a_retry_replays_the_answer_after_a_tool_opted_out_of_the_journal(self):
        """The live call is claimed just before the answer, so the answer replays by its calls alone."""
        shop, store = _Shop(), _FakeTaskStateStore()

        model = _fails_once_after_refund(_lookup_and_refund)
        paused = _durable_pause(_durable(model, [shop.toolset(live_lookup=True)]), _context(store))
        with pytest.raises(RuntimeError, match="model unavailable"):
            _durable(model, [shop.toolset(live_lookup=True)]).resume_after_durable_tool_approval(
                _context(store), **paused.kwargs, event=APPROVE
            )

        with patch(UPSERT, autospec=True) as upsert:
            retry = _durable(model, [shop.toolset(live_lookup=True)])
            output = retry.execute(_context(store, try_number=2))

        upsert.assert_not_called()
        # The recorded decision replays rather than being made again from the stored answer.
        assert retry._durable_journal is not None
        assert retry._durable_journal.stats.replayed["other"] == 1
        assert shop.calls.count(("refund", 1)) == 1
        assert output == "order 1 costs $10 | refunded order 1"

    @patch(REPEAT_SUPPORTED, True)
    def test_an_answer_is_not_applied_to_another_call_with_the_same_id(self):
        """The Dag changed during the pause, so the resumed run asks the model again and gets another call."""
        shop, store = _Shop(), _FakeTaskStateStore()
        paused = _durable_pause(_durable(_lookup_and_refund, [shop.toolset()]), _context(store))

        def refunds_order_2(messages, info: AgentInfo) -> ModelResponse:
            # Some providers number calls per response, so the id can repeat for another call.
            return ModelResponse(parts=[ToolCallPart("refund", {"order_id": 2}, tool_call_id="c-refund")])

        changed = _durable(refunds_order_2, [shop.toolset()])
        changed.prompt = "Refund order 2"
        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput) as asked:
                changed.resume_after_durable_tool_approval(_context(store), **paused.kwargs, event=APPROVE)

        assert ("refund", 2) not in shop.calls
        assert asked.value.method_name == "resume_after_durable_tool_approval"
        assert [name for name, _ in asked.value.kwargs["calls"]] == ["refund"]
        assert '"order_id": 2' in upsert.call_args.kwargs["body"]

    def test_an_answer_applies_to_the_same_call_under_a_new_id(self):
        """
        A tool opted out of the journal returns something new on resume, so the model is asked again.

        It asks for the same refund under a new call id, as a real model does, and the answer still applies.
        """
        refunds, store = [], _FakeTaskStateStore()
        asked = []

        def stock_level() -> int:
            return len(asked)

        def refund(order_id: int) -> str:
            refunds.append(order_id)
            return f"refunded order {order_id}"

        def model(messages, info: AgentInfo) -> ModelResponse:
            done = _returns(messages)
            asked.append(True)
            if not done:
                return ModelResponse(
                    parts=[ToolCallPart("stock_level", {}, tool_call_id=f"c-stock-{len(asked)}")]
                )
            if len(done) == 1:
                return ModelResponse(
                    parts=[ToolCallPart("refund", {"order_id": 1}, tool_call_id=f"c-refund-{len(asked)}")]
                )
            return ModelResponse(parts=[TextPart(" | ".join(done))])

        def toolsets():
            return [
                FunctionToolset(
                    tools=[
                        Tool(stock_level, metadata={"airflow": False}),
                        Tool(refund, requires_approval=True),
                    ]
                )
            ]

        paused = _durable_pause(_durable(model, toolsets()), _context(store))
        with patch(UPSERT, autospec=True) as upsert:
            output = _durable(model, toolsets()).resume_after_durable_tool_approval(
                _context(store), **paused.kwargs, event=APPROVE
            )

        upsert.assert_not_called()
        assert refunds == [1]
        assert output.endswith("refunded order 1")

    def test_a_handler_of_the_dag_authors_settles_only_the_calls_the_reviewer_did_not(self):
        """The handler approves order 2 and leaves order 1 to a person, who rejects it."""
        refunds, store = [], _FakeTaskStateStore()

        def refund(order_id: int) -> str:
            refunds.append(order_id)
            return f"refunded order {order_id}"

        def model(messages, info: AgentInfo) -> ModelResponse:
            if done := _returns(messages):
                return ModelResponse(parts=[TextPart(" | ".join(done))])
            return ModelResponse(
                parts=[
                    ToolCallPart("refund", {"order_id": 1}, tool_call_id="c-1"),
                    ToolCallPart("refund", {"order_id": 2}, tool_call_id="c-2"),
                ]
            )

        verdicts = []

        seen: list[str] = []

        def judge(ctx, requests: DeferredToolRequests):
            # Approves order 2 the first time, then every call it saw then, given to it or not.
            verdicts.append(True)
            if len(verdicts) > 1:
                return DeferredToolResults(approvals=dict.fromkeys(seen, True))
            seen.extend(c.tool_call_id for c in requests.approvals)
            return requests.build_results(
                approvals={
                    c.tool_call_id: True for c in requests.approvals if c.args_as_dict()["order_id"] == 2
                }
            )

        def operator():
            return _durable(
                model,
                [FunctionToolset(tools=[Tool(refund, requires_approval=True)])],
                capabilities=[HandleDeferredToolCalls(handler=judge)],
            )

        paused = _durable_pause(operator(), _context(store))
        assert [name for name, _ in paused.kwargs["calls"]] == ["refund"]
        operator().resume_after_durable_tool_approval(
            _context(store), **paused.kwargs, event=_reject("order 1 needs a ticket")
        )

        assert refunds == [2]

    @patch(REPEAT_SUPPORTED, True)
    def test_a_judge_that_changes_its_mind_on_resume_still_lets_the_run_finish(self):
        """At each ask the judge settles one of two calls, a different one each time it is asked."""
        refunds, store = [], _FakeTaskStateStore()
        asked = []

        def refund(order_id: int) -> str:
            refunds.append(order_id)
            return f"refunded order {order_id}"

        def model(messages, info: AgentInfo) -> ModelResponse:
            if done := _returns(messages):
                return ModelResponse(parts=[TextPart(" | ".join(sorted(done)))])
            return ModelResponse(
                parts=[
                    ToolCallPart("refund", {"order_id": 1}, tool_call_id="c-1"),
                    ToolCallPart("refund", {"order_id": 2}, tool_call_id="c-2"),
                ]
            )

        def judge(ctx, requests: DeferredToolRequests):
            asked.append(True)
            chosen = 1 + len(asked) % 2
            return requests.build_results(
                approvals={
                    c.tool_call_id: True for c in requests.approvals if c.args_as_dict()["order_id"] == chosen
                }
            )

        def operator():
            return _durable(
                model,
                [FunctionToolset(tools=[Tool(refund, requires_approval=True)])],
                capabilities=[HandleDeferredToolCalls(handler=judge)],
            )

        paused, output, pauses = _durable_pause(operator(), _context(store)), None, 1
        while output is None and pauses < 5:
            with patch(UPSERT, autospec=True):
                try:
                    output = operator().resume_after_durable_tool_approval(
                        _context(store), **paused.kwargs, event=APPROVE
                    )
                except TaskAwaitingInput as again:
                    paused, pauses = again, pauses + 1

        assert output == "refunded order 1 | refunded order 2"
        assert sorted(refunds) == [1, 2]

    @patch(REPEAT_SUPPORTED, True)
    def test_the_same_call_asked_for_again_later_is_asked_about_again(self):
        """An answer covers the call the reviewer saw once, not every later call with the same arguments."""
        refunds, store = [], _FakeTaskStateStore()

        def refund(order_id: int) -> str:
            refunds.append(order_id)
            return f"refunded order {order_id}"

        def model(messages, info: AgentInfo) -> ModelResponse:
            done = _returns(messages)
            if len(done) < 2:
                return ModelResponse(
                    parts=[ToolCallPart("refund", {"order_id": 1}, tool_call_id=f"c-{len(done)}")]
                )
            return ModelResponse(parts=[TextPart("done")])

        def operator():
            return _durable(model, [FunctionToolset(tools=[Tool(refund, requires_approval=True)])])

        first = _durable_pause(operator(), _context(store))
        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput):
                operator().resume_after_durable_tool_approval(_context(store), **first.kwargs, event=APPROVE)

        upsert.assert_called_once()
        assert refunds == [1]

    def test_an_answer_survives_a_handler_that_fails_after_it_was_applied(self):
        """The handler leaves order 1 to a person, then fails once on resume deciding order 2."""
        refunds, store = [], _FakeTaskStateStore()
        verdicts = []

        def refund(order_id: int) -> str:
            refunds.append(order_id)
            return f"refunded order {order_id}"

        def model(messages, info: AgentInfo) -> ModelResponse:
            if done := _returns(messages):
                return ModelResponse(parts=[TextPart(" | ".join(sorted(done)))])
            return ModelResponse(
                parts=[
                    ToolCallPart("refund", {"order_id": 1}, tool_call_id="c-1"),
                    ToolCallPart("refund", {"order_id": 2}, tool_call_id="c-2"),
                ]
            )

        def flaky(ctx, requests: DeferredToolRequests):
            verdicts.append(True)
            if len(verdicts) == 2:
                raise RuntimeError("judge unavailable")
            return requests.build_results(
                approvals={
                    c.tool_call_id: True for c in requests.approvals if c.args_as_dict()["order_id"] == 2
                }
            )

        def operator():
            return _durable(
                model,
                [FunctionToolset(tools=[Tool(refund, requires_approval=True)])],
                capabilities=[HandleDeferredToolCalls(handler=flaky)],
            )

        paused = _durable_pause(operator(), _context(store))
        with pytest.raises(RuntimeError, match="judge unavailable"):
            operator().resume_after_durable_tool_approval(_context(store), **paused.kwargs, event=APPROVE)
        with patch(UPSERT, autospec=True) as upsert:
            output = operator().execute(_context(store, try_number=2))

        upsert.assert_not_called()
        assert output == "refunded order 1 | refunded order 2"
        assert sorted(refunds) == [1, 2]
        assert _TOOL_APPROVAL_ANSWERS_KEY not in store.data

    def test_a_handler_ordered_outermost_cannot_overrule_the_reviewer(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        asked = []

        class _OutermostJudge(HandleDeferredToolCalls):
            def get_ordering(self):
                return CapabilityOrdering(position="outermost")

        def judge(ctx, requests: DeferredToolRequests):
            # Leaves the call to a person, and approves it whenever asked again.
            asked.append(True)
            return requests.build_results(approve_all=True) if len(asked) > 1 else None

        def operator():
            return _durable(
                _lookup_and_refund, [shop.toolset()], capabilities=[_OutermostJudge(handler=judge)]
            )

        paused = _durable_pause(operator(), _context(store))
        operator().resume_after_durable_tool_approval(_context(store), **paused.kwargs, event=_reject())

        assert ("refund", 1) not in shop.calls

    def test_a_task_agent_resumes_with_the_prompt_its_function_returns(self):
        shop, store = _Shop(), _FakeTaskStateStore()

        def operator():
            op = _AgentDecoratedOperator(
                task_id="t",
                python_callable=lambda: "Refund order 1",
                llm_conn_id="llm",
                toolsets=[shop.toolset()],
                durable=True,
            )
            op.llm_hook = MagicMock(spec=["create_agent"])
            op.llm_hook.create_agent.side_effect = lambda **kw: Agent(FunctionModel(_lookup_and_refund), **kw)
            return op

        paused = _durable_pause(operator(), _context(store))
        output = operator().resume_after_durable_tool_approval(
            _context(store), **paused.kwargs, event=APPROVE
        )

        assert output == "order 1 costs $10 | refunded order 1"
        assert shop.calls == [("lookup", 1), ("refund", 1)]

    def test_a_handler_built_per_run_is_asked_as_built(self):
        """The capability the Dag declares leaves the call to a person; the one it builds per run approves it."""
        shop = _Shop()

        class _PerRunJudge(HandleDeferredToolCalls):
            async def for_run(self, ctx):
                return _PerRunJudge(handler=lambda ctx, requests: requests.build_results(approve_all=True))

        with patch(UPSERT, autospec=True) as upsert:
            output = _durable(
                _lookup_and_refund,
                [shop.toolset()],
                capabilities=[_PerRunJudge(handler=lambda ctx, requests: None)],
            ).execute(_context(_FakeTaskStateStore()))

        upsert.assert_not_called()
        assert output == "order 1 costs $10 | refunded order 1"


def _refunds_order_1_twice(fail_once_after_first: bool = False):
    """Refund order 1, then refund order 1 again in a later step; optionally fail once between them."""
    failed: list[bool] = []

    def model(messages, info: AgentInfo) -> ModelResponse:
        done = _returns(messages)
        if len(done) == 1 and fail_once_after_first and not failed:
            failed.append(True)
            raise RuntimeError("model unavailable")
        if len(done) < 2:
            return ModelResponse(
                parts=[ToolCallPart("refund", {"order_id": 1}, tool_call_id=f"c-{len(done)}")]
            )
        return ModelResponse(parts=[TextPart(" | ".join(done))])

    return model


class TestDurableAnswersStayWithTheirRequest:
    """An answer settles the approval step that asked for it, and no later step with the same call."""

    @pytest.mark.parametrize(
        ("answers", "refunds"),
        [
            pytest.param([APPROVE, _reject()], [1], id="approve-then-reject"),
            pytest.param([_reject(), APPROVE], [1], id="reject-then-approve"),
        ],
    )
    @patch(REPEAT_SUPPORTED, True)
    def test_two_requests_for_the_same_call_each_get_their_own_answer(self, answers, refunds):
        shop, store = _Shop(), _FakeTaskStateStore()
        model = _refunds_order_1_twice()
        paused = _durable_pause(_durable(model, [shop.toolset()]), _context(store))
        with patch(UPSERT, autospec=True):
            with pytest.raises(TaskAwaitingInput) as second:
                _durable(model, [shop.toolset()]).resume_after_durable_tool_approval(
                    _context(store), **paused.kwargs, event=answers[0]
                )
            _durable(model, [shop.toolset()]).resume_after_durable_tool_approval(
                _context(store), **second.value.kwargs, event=answers[1]
            )

        assert [order for name, order in shop.calls if name == "refund"] == refunds

    @pytest.mark.parametrize("repeat_supported", [False, True], ids=["airflow-3.3", "airflow-3.4"])
    def test_a_retry_asks_about_a_later_call_like_one_already_approved(self, repeat_supported):
        shop, store = _Shop(), _FakeTaskStateStore()
        model = _refunds_order_1_twice(fail_once_after_first=True)
        with patch(REPEAT_SUPPORTED, repeat_supported):
            paused = _durable_pause(_durable(model, [shop.toolset()]), _context(store))
            with pytest.raises(RuntimeError, match="model unavailable"):
                _durable(model, [shop.toolset()]).resume_after_durable_tool_approval(
                    _context(store), **paused.kwargs, event=APPROVE
                )
            with patch(UPSERT, autospec=True):
                with pytest.raises((TaskAwaitingInput, ToolApprovalAlreadyRequestedError)):
                    _durable(model, [shop.toolset()]).execute(_context(store, try_number=2))

        assert [order for name, order in shop.calls if name == "refund"] == [1]

    def test_a_retry_does_not_apply_an_answer_given_for_another_connection(self):
        shop, store = _Shop(), _FakeTaskStateStore()
        model = _fails_once_after_refund(_lookup_and_refund)

        def operator(customer: str) -> AgentOperator:
            op = _durable(model, [shop.toolset(), SQLToolset(db_conn_id="tenant_{{ params.customer }}")])
            op.render_template_fields({"params": {"customer": customer}})
            return op

        paused = _durable_pause(operator("acme"), _context(store))
        with pytest.raises(RuntimeError, match="model unavailable"):
            operator("acme").resume_after_durable_tool_approval(
                _context(store), **paused.kwargs, event=APPROVE
            )
        with patch(REPEAT_SUPPORTED, True), patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput):
                operator("globex").execute(_context(store, try_number=2))

        upsert.assert_called_once()
        assert shop.calls.count(("refund", 1)) == 1

    def test_a_handler_the_model_has_not_loaded_is_not_asked(self):
        shop = _Shop()
        judge = HandleDeferredToolCalls(
            handler=lambda ctx, requests: requests.build_results(approve_all=True),
            id="judge",
            defer_loading=True,
        )

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput):
                _durable(_lookup_and_refund, [shop.toolset()], capabilities=[judge]).execute(
                    _context(_FakeTaskStateStore())
                )

        upsert.assert_called_once()
        assert ("refund", 1) not in shop.calls

    def test_a_tool_that_needs_approval_called_from_inside_another_tool_fails_the_task(self):
        """A tool runs the agent again, and that run asks for approval: a replay of the tool would skip it."""
        refunds, store = [], _FakeTaskStateStore()

        async def delegate(ctx: RunContext[None]) -> str:
            assert ctx.agent is not None
            result = await ctx.agent.run("CHILD")
            return f"delegate got {result.output!r}"

        def refund(order_id: int) -> str:
            refunds.append(order_id)
            return f"refunded order {order_id}"

        def model(messages, info: AgentInfo) -> ModelResponse:
            done = _returns(messages)
            if "CHILD" in str(messages[0].parts[-1].content) and not done:
                return ModelResponse(parts=[ToolCallPart("refund", {"order_id": 1}, tool_call_id="c-child")])
            if not done:
                return ModelResponse(parts=[ToolCallPart("delegate", {}, tool_call_id="c-delegate")])
            return ModelResponse(parts=[TextPart(" | ".join(done))])

        toolsets = [
            FunctionToolset(tools=[Tool(delegate, takes_ctx=True), Tool(refund, requires_approval=True)])
        ]
        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(UnsupportedToolDeferralError, match="from inside the"):
                _durable(model, toolsets).execute(_context(store))

        upsert.assert_not_called()
        assert refunds == []

    @patch(REPEAT_SUPPORTED, True)
    def test_one_answer_covers_one_call_when_the_resumed_step_has_it_twice(self):
        """A live tool changes on resume, and the model asked again wants refund(1) twice in that step."""
        refunds, store, asked, refund_asks = [], _FakeTaskStateStore(), [], []

        def stock_level() -> int:
            return len(asked)

        def refund(order_id: int) -> str:
            refunds.append(order_id)
            return f"refunded order {order_id}"

        def model(messages, info: AgentInfo) -> ModelResponse:
            done = _returns(messages)
            asked.append(True)
            if not done:
                return ModelResponse(parts=[ToolCallPart("stock_level", {}, tool_call_id=f"s-{len(asked)}")])
            if len(done) == 1:
                refund_asks.append(True)
                return ModelResponse(
                    parts=[
                        ToolCallPart("refund", {"order_id": 1}, tool_call_id=f"r-{len(asked)}-{i}")
                        for i in range(len(refund_asks))
                    ]
                )
            return ModelResponse(parts=[TextPart(" | ".join(done))])

        def toolsets():
            return [
                FunctionToolset(
                    tools=[
                        Tool(stock_level, metadata={"airflow": False}),
                        Tool(refund, requires_approval=True),
                    ]
                )
            ]

        paused = _durable_pause(_durable(model, toolsets()), _context(store))
        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(TaskAwaitingInput) as again:
                _durable(model, toolsets()).resume_after_durable_tool_approval(
                    _context(store), **paused.kwargs, event=APPROVE
                )

        assert len(refund_asks) == 2
        assert refunds == []
        upsert.assert_called_once()
        assert len(again.value.kwargs["calls"]) == 1

    @patch(REPEAT_SUPPORTED, True)
    def test_a_step_a_handler_helped_settle_keeps_its_number_for_the_next_one(self):
        """Step 1: a judge approves order 2 and the reviewer order 1. Step 2: the reviewer approves order 3."""
        refunds, store = [], _FakeTaskStateStore()

        def refund(order_id: int) -> str:
            refunds.append(order_id)
            return f"refunded order {order_id}"

        def model(messages, info: AgentInfo) -> ModelResponse:
            done = _returns(messages)
            if not done:
                return ModelResponse(
                    parts=[
                        ToolCallPart("refund", {"order_id": 1}, tool_call_id="c-1"),
                        ToolCallPart("refund", {"order_id": 2}, tool_call_id="c-2"),
                    ]
                )
            if len(done) == 2:
                return ModelResponse(parts=[ToolCallPart("refund", {"order_id": 3}, tool_call_id="c-3")])
            return ModelResponse(parts=[TextPart(" | ".join(sorted(done)))])

        def judge(ctx, requests: DeferredToolRequests):
            return requests.build_results(
                approvals={
                    c.tool_call_id: True for c in requests.approvals if c.args_as_dict()["order_id"] == 2
                }
            )

        def operator():
            return _durable(
                model,
                [FunctionToolset(tools=[Tool(refund, requires_approval=True)])],
                capabilities=[HandleDeferredToolCalls(handler=judge)],
            )

        first = _durable_pause(operator(), _context(store))
        with patch(UPSERT, autospec=True):
            with pytest.raises(TaskAwaitingInput) as second:
                operator().resume_after_durable_tool_approval(_context(store), **first.kwargs, event=APPROVE)
            output = operator().resume_after_durable_tool_approval(
                _context(store), **second.value.kwargs, event=APPROVE
            )

        assert second.value.method_name == "resume_after_durable_tool_approval"
        assert (first.kwargs["step"], second.value.kwargs["step"]) == (1, 2)
        assert sorted(refunds) == [1, 2, 3]
        assert output == "refunded order 1 | refunded order 2 | refunded order 3"
