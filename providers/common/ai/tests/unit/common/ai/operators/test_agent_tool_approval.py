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
from airflow.providers.standard.exceptions import HITLTimeoutError

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="Per-tool approval needs Airflow 3.3+.")

if AIRFLOW_V_3_3_PLUS:
    from airflow.sdk.exceptions import TaskAwaitingInput

UPSERT = "airflow.providers.common.ai.operators.agent.upsert_hitl_detail"


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


def _two_refunds(messages, info: AgentInfo) -> ModelResponse:
    """Refund order 1, then order 2, one call per step: two separate approvals."""
    done = _returns(messages)
    if len(done) < 2:
        n = len(done) + 1
        return ModelResponse(parts=[ToolCallPart("refund", {"order_id": n}, tool_call_id=f"c-{n}")])
    return ModelResponse(parts=[TextPart(" | ".join(done))])


def _operator(model_fn, toolsets, **kwargs) -> AgentOperator:
    op = AgentOperator(task_id="t", prompt="Refund order 1", llm_conn_id="llm", toolsets=toolsets, **kwargs)
    hook = MagicMock(spec=["create_agent"])
    hook.create_agent.side_effect = lambda **kw: Agent(FunctionModel(model_fn), **kw)
    op.llm_hook = hook
    return op


def _context(store: _FakeTaskStateStore, *, try_number: int = 1) -> Any:  # a Context stand-in
    ti = MagicMock(spec=["id", "dag_id", "task_id", "run_id", "map_index", "try_number", "xcom_push"])
    ti.configure_mock(id="ti-1", dag_id="d", task_id="t", run_id="r", map_index=-1, try_number=try_number)
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
        assert upsert.call_args.kwargs["subject"] == "Approve tool call for task `t`"
        assert upsert.call_args.kwargs["multiple"] is False
        # Optional in the review form: without "null" the UI requires a reason even to approve.
        assert upsert.call_args.kwargs["params"]["reason"]["schema"] == {"type": ["string", "null"]}
        assert upsert.call_args.kwargs["assigned_users"] == [{"id": "u1", "name": "alice"}]
        assert exc.value.timeout == timedelta(hours=1)
        assert store.data[_TOOL_APPROVAL_REQUESTED_KEY] is True

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

    def test_a_later_try_fails_closed_once_an_approval_was_requested(self):
        """Core keeps one approval request per task instance across retries and clears, with the
        first request's subject and body, so a retry's request would show the earlier call."""
        shop, store = _Shop(), _FakeTaskStateStore()
        _pause(_operator(_lookup_and_refund, [shop.toolset()]), _context(store))

        with patch(UPSERT, autospec=True) as upsert:
            with pytest.raises(ToolApprovalAlreadyRequestedError, match="already asked once"):
                _operator(_lookup_and_refund, [shop.toolset()]).execute(_context(store, try_number=2))

        upsert.assert_not_called()

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

    def test_a_second_approval_in_the_same_try_fails_closed(self):
        """Airflow keeps one approval request per task instance and a second one would show the
        first one's details, so the reviewer would approve refund 2 while reading refund 1."""
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
            pytest.param({"durable": True}, id="durable"),
            pytest.param({"code_mode": True}, id="code_mode"),
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
