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
"""HITL review on Airflow 3.1+: on 3.3+ AgentOperator parks between rounds and resumes in a new process; before, it polls."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel
from pydantic_ai import Agent, CallDeferred, Tool
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, UserPromptPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.toolsets import FunctionToolset

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX, build_step_key
from airflow.providers.common.ai.exceptions import HITLMaxIterationsError, UnsupportedToolDeferralError
from airflow.providers.common.ai.mixins.hitl_review import HITLReviewMixin
from airflow.providers.common.ai.operators.agent import _HITL_REVIEW_ANSWERS_KEY, AgentOperator
from airflow.providers.common.ai.utils.hitl_review import (
    XCOM_AGENT_OUTPUT_PREFIX,
    XCOM_HUMAN_ACTION,
    XCOM_HUMAN_FEEDBACK_PREFIX,
)
from airflow.providers.common.ai.utils.usage_budget import USAGE_BUDGET_KEY, TaskStateStoreUsageBudget
from airflow.providers.standard.exceptions import HITLRejectException, HITLTimeoutError

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_3_PLUS
from unit.common.ai.durable.capabilities import IdlessLedger
from unit.common.ai.durable.memory_storage import MemoryStorage
from unit.common.ai.tool_parts import is_tool_result

pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="HITL review needs Airflow 3.1+.")
parks = pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="Parking a review needs Airflow 3.3+.")

REQUEST_CHANGES = {"chosen_options": ["Request changes"], "params_input": {"feedback": "Shorter please"}}
APPROVE = {"chosen_options": ["Approve"], "params_input": {}}
REJECT = {"chosen_options": ["Reject"], "params_input": {}}
UPSERT = "airflow.providers.common.ai.mixins.hitl_review.upsert_hitl_detail"


class Summary(BaseModel):
    text: str
    words: int


class Verdict(BaseModel):
    ok: bool


def to_report(text: str, words: int) -> dict[str, Any]:
    """An output function: the task returns what it returns, not the model's arguments."""
    return {"report": text.upper(), "words": words}


def _prompts(messages) -> list[str]:
    return [str(p.content) for m in messages for p in m.parts if isinstance(p, UserPromptPart)]


def _summarizer(messages, info: AgentInfo) -> ModelResponse:
    """Answer with a long summary, and a short one once the reviewer has asked for it."""
    short = "Shorter please" in _prompts(messages)
    args = {"text": "Short", "words": 1} if short else {"text": "A long summary", "words": 3}
    tool = next(t for t in info.output_tools if "Verdict" not in t.name)
    return ModelResponse(parts=[ToolCallPart(tool.name, args)])


def _answers_42(messages, info: AgentInfo) -> ModelResponse:
    return ModelResponse(parts=[TextPart("42")])


def _operator(model_fn=_summarizer, **kwargs) -> AgentOperator:
    kwargs.setdefault("output_type", Summary)
    op = AgentOperator(
        task_id="t",
        prompt="Summarize the report",
        llm_conn_id="llm",
        enable_hitl_review=True,
        hitl_timeout=timedelta(hours=1),
        **kwargs,
    )
    hook = MagicMock(spec=["create_agent"])
    hook.create_agent.side_effect = lambda **kw: Agent(FunctionModel(model_fn), **kw)
    op.llm_hook = hook
    return op


@pytest.fixture
def ctx(review_context) -> Any:
    return review_context


@parks
def test_review_resumes_in_a_new_operator_and_returns_the_approved_model(park, ctx):
    first = park(lambda: _operator().execute(ctx))

    # Each resume is a new process, so a new operator instance with nothing carried in memory.
    second = park(lambda: _operator().resume_hitl_review(ctx, **first.kwargs, event=REQUEST_CHANGES))
    result = _operator().resume_hitl_review(ctx, **second.kwargs, event=APPROVE)

    assert ctx["task_instance"].xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}1"] == '{"text":"A long summary","words":3}'
    assert ctx["task_instance"].xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}2"] == '{"text":"Short","words":1}'
    assert result == Summary(text="Short", words=1)
    assert ctx["task_state_store"].get("common_ai_hitl_review_state") is None


@parks
@pytest.mark.parametrize(
    ("output_type", "model_fn", "expected"),
    [
        pytest.param(
            [Summary, Verdict], _summarizer, {"text": "A long summary", "words": 3}, id="list-of-types"
        ),
        pytest.param(to_report, _summarizer, {"report": "A LONG SUMMARY", "words": 3}, id="output-function"),
        pytest.param(str | int, _answers_42, "42", id="text-that-reads-back-as-another-type"),
    ],
)
def test_approved_result_is_what_the_task_returns_without_a_park(park, ctx, output_type, model_fn, expected):
    paused = park(lambda: _operator(model_fn, output_type=output_type).execute(ctx))

    result = _operator(model_fn, output_type=output_type).resume_hitl_review(
        ctx, **paused.kwargs, event=APPROVE
    )

    assert result == expected
    assert type(result) is type(expected)


@parks
def test_regeneration_continues_the_conversation_from_before_the_park(park, ctx):
    first = park(lambda: _operator().execute(ctx))
    seen: list[list[str]] = []

    def summarizer(messages, info: AgentInfo) -> ModelResponse:
        seen.append(_prompts(messages))
        return _summarizer(messages, info)

    park(lambda: _operator(summarizer).resume_hitl_review(ctx, **first.kwargs, event=REQUEST_CHANGES))

    assert seen == [["Summarize the report", "Shorter please"]]


@parks
def test_usage_limits_cover_every_round_across_parks(park, ctx):
    def spent_requests() -> int | None:
        if ctx["task_state_store"].get(USAGE_BUDGET_KEY) is None:
            return None
        return TaskStateStoreUsageBudget(ctx["task_state_store"], max_tries=0).load().requests

    first = park(lambda: _operator(usage_limits={"request_limit": 5}).execute(ctx))
    after_first = spent_requests()
    second = park(
        lambda: _operator(usage_limits={"request_limit": 5}).resume_hitl_review(
            ctx, **first.kwargs, event=REQUEST_CHANGES
        )
    )
    after_second = spent_requests()
    _operator(usage_limits={"request_limit": 5}).resume_hitl_review(ctx, **second.kwargs, event=APPROVE)

    assert (after_first, after_second, spent_requests()) == (1, 2, None)


def _changes(feedback: str) -> dict[str, Any]:
    return {"chosen_options": ["Request changes"], "params_input": {"feedback": feedback}}


class _Writer:
    """
    A model that fetches the report once, then writes a summary following all the feedback so far.

    Each summary names the request that wrote it, as a real model words its answer differently
    every time it is asked, so a replayed summary can be told from a new one.
    """

    def __init__(self, *, fail_on: str | None = None):
        self.requests = 0
        self.fetches = 0
        self.fail_on = fail_on
        # The run id each request was made under.
        self.run_ids: list[str | None] = []

    def toolset(self, *, live: bool = False) -> FunctionToolset:
        """``live`` opts the tool out of the journal, so it runs, and returns something new, on every attempt."""

        def fetch_report() -> str:
            self.fetches += 1
            return f"Q3 revenue is up 4% (fetch {self.fetches})"

        return FunctionToolset(
            tools=[Tool(fetch_report, metadata={"airflow": False}) if live else fetch_report]
        )

    def __call__(self, messages, info: AgentInfo) -> ModelResponse:
        self.requests += 1
        self.run_ids.append(messages[-1].run_id)
        feedback = _prompts(messages)[1:]
        if self.fail_on in feedback:
            self.fail_on = None
            raise RuntimeError("model unavailable")
        if not any(is_tool_result(part) for message in messages for part in message.parts):
            return ModelResponse(parts=[ToolCallPart("fetch_report", {}, tool_call_id="c-fetch")])
        return ModelResponse(parts=[TextPart(f"summary {self.requests} after {feedback}")])


@parks
class TestDurable:
    """Under durable execution every round's agent run is journaled, so a retry replays the review."""

    @pytest.fixture(
        autouse=True, params=[{"durable": True}, {"retries": 2}], ids=["durable-true", "default-with-retries"]
    )
    def durable_kwargs(self, request) -> None:
        self._durable_kwargs = request.param

    @pytest.fixture
    def ctx(self, review_context) -> Any:
        store = review_context["task_state_store"]
        set_ = store.set.side_effect
        # Stored as JSON, as the real store does, so a test sees what a later process reads back.
        store.set.side_effect = lambda key, value, retention=None: set_(key, json.loads(json.dumps(value)))
        return review_context

    def _operator(self, writer: _Writer, *, live: bool = False, **kwargs) -> AgentOperator:
        return _operator(
            writer, output_type=str, toolsets=[writer.toolset(live=live)], **self._durable_kwargs, **kwargs
        )

    @staticmethod
    def _retry(ctx) -> None:
        """Start the task's next try: Airflow drops its XComs, and the task state store keeps the journal."""
        ctx["task_instance"].xcoms.clear()
        ctx["task_instance"].try_number += 1

    def test_request_changes_then_approve_runs_every_round_under_one_journal_and_deletes_it(self, park, ctx):
        writer = _Writer()
        first = park(lambda: self._operator(writer).execute(ctx))
        second = park(
            lambda: self._operator(writer).resume_hitl_review(ctx, **first.kwargs, event=_changes("Shorter"))
        )
        # The regeneration is run 1 of the task's journal, beside the first output's run 0.
        assert build_step_key(1, 0) in ctx["task_state_store"].data

        result = self._operator(writer).resume_hitl_review(ctx, **second.kwargs, event=APPROVE)

        assert result == "summary 3 after ['Shorter']"
        assert (writer.requests, writer.fetches) == (3, 1)
        assert [key for key in ctx["task_state_store"].data if key.startswith(DURABLE_KEY_PREFIX)] == []
        assert _HITL_REVIEW_ANSWERS_KEY not in ctx["task_state_store"].data

    def test_a_failed_regeneration_retries_without_asking_about_the_earlier_rounds(self, park, ctx):
        writer = _Writer(fail_on="Add the margin")
        first = park(lambda: self._operator(writer).execute(ctx))
        second = park(
            lambda: self._operator(writer).resume_hitl_review(ctx, **first.kwargs, event=_changes("Shorter"))
        )
        with pytest.raises(RuntimeError, match="model unavailable"):
            self._operator(writer).resume_hitl_review(ctx, **second.kwargs, event=_changes("Add the margin"))
        self._retry(ctx)

        with patch(UPSERT, autospec=True) as upsert:
            third = park(lambda: self._operator(writer).execute(ctx))

        xcoms = ctx["task_instance"].xcoms
        assert third.kwargs["iteration"] == 3
        upsert.assert_called_once()
        # Fetch and first summary, the regeneration, the failed one, and the failed one again on retry.
        assert (writer.requests, writer.fetches) == (5, 1)
        assert [xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}{n}"] for n in (1, 2, 3)] == [
            "summary 2 after []",
            "summary 3 after ['Shorter']",
            "summary 5 after ['Shorter', 'Add the margin']",
        ]
        assert [xcoms[f"{XCOM_HUMAN_FEEDBACK_PREFIX}{n}"] for n in (1, 2)] == ["Shorter", "Add the margin"]

    def test_the_new_first_output_after_a_reject_replays_on_later_retries(self, park, ctx):
        """The reject told the journal to ask the model again; the run that did so takes that back."""
        writer = _Writer(fail_on="Shorter")
        first = park(lambda: self._operator(writer).execute(ctx))
        with pytest.raises(HITLRejectException):
            self._operator(writer).resume_hitl_review(ctx, **first.kwargs, event=REJECT)
        self._retry(ctx)
        new_first = park(lambda: self._operator(writer).execute(ctx))
        with pytest.raises(RuntimeError, match="model unavailable"):
            self._operator(writer).resume_hitl_review(ctx, **new_first.kwargs, event=_changes("Shorter"))
        self._retry(ctx)

        again = park(lambda: self._operator(writer).execute(ctx))

        assert again.kwargs["iteration"] == 2
        assert ctx["task_instance"].xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}1"] == "summary 3 after []"
        assert (writer.requests, writer.fetches) == (5, 1)

    def test_a_retry_after_the_review_timed_out_asks_about_the_same_round_again(self, park, ctx):
        writer = _Writer()
        first = park(lambda: self._operator(writer).execute(ctx))
        second = park(
            lambda: self._operator(writer).resume_hitl_review(ctx, **first.kwargs, event=_changes("Shorter"))
        )
        with pytest.raises(HITLTimeoutError):
            self._operator(writer).resume_hitl_review(
                ctx, **second.kwargs, event={"error": "timed out", "error_type": "timeout"}
            )
        self._retry(ctx)

        again = park(lambda: self._operator(writer).execute(ctx))

        assert again.kwargs["iteration"] == 2
        assert ctx["task_instance"].xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}2"] == "summary 3 after ['Shorter']"
        assert (writer.requests, writer.fetches) == (3, 1)

    @pytest.mark.parametrize(
        ("events", "max_hitl_iterations", "error"),
        [
            pytest.param([REJECT], 5, HITLRejectException, id="reject"),
            pytest.param([_changes("Shorter"), REJECT], 5, HITLRejectException, id="reject-after-changes"),
            pytest.param([_changes("Shorter")], 1, HITLMaxIterationsError, id="max-iterations"),
        ],
    )
    def test_a_review_that_ends_without_approval_starts_over_with_a_new_answer_on_retry(
        self, park, ctx, events, max_hitl_iterations, error
    ):
        writer = _Writer()

        def operator() -> AgentOperator:
            return self._operator(writer, max_hitl_iterations=max_hitl_iterations)

        paused = park(lambda: operator().execute(ctx))
        for event in events[:-1]:
            paused = park(
                lambda kwargs=paused.kwargs, event=event: operator().resume_hitl_review(
                    ctx, **kwargs, event=event
                )
            )
        with pytest.raises(error):
            operator().resume_hitl_review(ctx, **paused.kwargs, event=events[-1])
        requests_before_retry = writer.requests
        self._retry(ctx)

        again = park(lambda: operator().execute(ctx))

        assert again.kwargs["iteration"] == 1
        # Asked again for the first summary only: the fetch before it replays.
        assert writer.fetches == 1
        assert writer.requests == requests_before_retry + 1
        assert (
            ctx["task_instance"].xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}1"]
            == f"summary {requests_before_retry + 1} after []"
        )
        assert _HITL_REVIEW_ANSWERS_KEY not in ctx["task_state_store"].data

    def test_feedback_is_not_applied_to_an_output_the_reviewer_did_not_read(self, park, ctx, caplog):
        """A tool opted out of the journal returns something new on retry, so the first summary changes."""
        writer = _Writer(fail_on="Shorter")
        first = park(lambda: self._operator(writer, live=True).execute(ctx))
        with pytest.raises(RuntimeError, match="model unavailable"):
            self._operator(writer, live=True).resume_hitl_review(
                ctx, **first.kwargs, event=_changes("Shorter")
            )
        self._retry(ctx)

        again = park(lambda: self._operator(writer, live=True).execute(ctx))

        assert again.kwargs["iteration"] == 1
        assert ctx["task_instance"].xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}1"] == "summary 4 after []"
        assert f"{XCOM_HUMAN_FEEDBACK_PREFIX}1" not in ctx["task_instance"].xcoms
        assert "is not the one the reviewer asked to change" in caplog.text

    def test_a_regeneration_keeps_its_run_id_on_retry(self, park, ctx):
        writer = _Writer(fail_on="Shorter")
        first = park(lambda: self._operator(writer).execute(ctx))
        with pytest.raises(RuntimeError, match="model unavailable"):
            self._operator(writer).resume_hitl_review(ctx, **first.kwargs, event=_changes("Shorter"))
        self._retry(ctx)
        park(lambda: self._operator(writer).execute(ctx))

        # Fetch and first summary, then the regeneration that failed and the same one on retry.
        first_run_id = writer.run_ids[0]
        assert writer.run_ids[2:] == [f"{first_run_id}-review-1", f"{first_run_id}-review-1"]

    def test_an_answer_given_again_replaces_the_answers_and_runs_that_followed_the_old_one(
        self, park, ctx, caplog
    ):
        """The first summary changes on retry (a live tool), so the reviewer answers round 1 again."""
        writer = _Writer(fail_on="Add the margin")
        first = park(lambda: self._operator(writer, live=True).execute(ctx))
        second = park(
            lambda: self._operator(writer, live=True).resume_hitl_review(
                ctx, **first.kwargs, event=_changes("Shorter")
            )
        )
        with pytest.raises(RuntimeError, match="model unavailable"):
            self._operator(writer, live=True).resume_hitl_review(
                ctx, **second.kwargs, event=_changes("Add the margin")
            )
        self._retry(ctx)
        asked_again = park(lambda: self._operator(writer, live=True).execute(ctx))
        caplog.clear()

        park(
            lambda: self._operator(writer, live=True).resume_hitl_review(
                ctx, **asked_again.kwargs, event=_changes("Shorter please")
            )
        )

        store = ctx["task_state_store"].data
        assert [(a["iteration"], a["feedback"]) for a in store[_HITL_REVIEW_ANSWERS_KEY]] == [
            (1, "Shorter please")
        ]
        assert [key for key in store if "run_2_" in key] == []
        # The regeneration started run 1 over, rather than finding the old branch's run 1 there.
        assert "took a different path" not in caplog.text
        assert (
            ctx["task_instance"].xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}2"] == "summary 6 after ['Shorter please']"
        )

    @pytest.mark.parametrize(
        ("requires_approval", "match"),
        [
            pytest.param(True, "need approval", id="approval"),
            pytest.param(False, "need external execution", id="external"),
        ],
    )
    def test_a_regeneration_that_defers_a_tool_call_fails_the_task(self, park, ctx, requires_approval, match):
        """A review cannot pause for a deferred call, as the first run's check already says."""

        def send_summary() -> str:
            if not requires_approval:
                raise CallDeferred
            return "sent"

        def model(messages, info: AgentInfo) -> ModelResponse:
            if _prompts(messages)[1:]:
                return ModelResponse(parts=[ToolCallPart("send_summary", {}, tool_call_id="c-send")])
            return ModelResponse(parts=[TextPart("summary")])

        def operator() -> AgentOperator:
            toolset = FunctionToolset(tools=[Tool(send_summary, requires_approval=requires_approval)])
            return _operator(model, output_type=str, toolsets=[toolset], **self._durable_kwargs)

        first = park(lambda: operator().execute(ctx))

        with pytest.raises(UnsupportedToolDeferralError, match=match):
            operator().resume_hitl_review(ctx, **first.kwargs, event=_changes("Send it"))


@parks
def test_a_resume_the_default_cannot_run_durably_stores_nothing_durable(park, ctx):
    """pydantic-ai refuses to build the durable agent; the resume must decide that before it writes anything."""
    writer = _Writer()

    def operator() -> AgentOperator:
        return _operator(
            writer, output_type=str, retries=2, toolsets=[writer.toolset()], capabilities=[IdlessLedger()]
        )

    first = park(lambda: operator().execute(ctx))
    resumed = operator()
    with patch(
        "airflow.providers.common.ai.operators.agent.stamp_identity_on_agent_spans", autospec=True
    ) as stamp:
        park(lambda: resumed.resume_hitl_review(ctx, **first.kwargs, event=_changes("Shorter")))

    assert resumed._durable is False
    assert stamp.call_args.args[1]["airflow.common_ai.durable"] is False
    # The refused durable build, then the non-durable one the regeneration runs.
    assert resumed.llm_hook.create_agent.call_count == 2
    store = ctx["task_state_store"].data
    assert _HITL_REVIEW_ANSWERS_KEY not in store
    assert [key for key in store if key.startswith(DURABLE_KEY_PREFIX)] == []


@parks
@pytest.mark.parametrize(
    ("event", "max_hitl_iterations", "error"),
    [
        pytest.param(REJECT, 5, HITLRejectException, id="reject"),
        pytest.param(REQUEST_CHANGES, 1, HITLMaxIterationsError, id="max-iterations"),
    ],
)
def test_a_review_ending_unapproved_leaves_nothing_durable_for_a_run_that_recorded_nothing(
    park, ctx, event, max_hitl_iterations, error
):
    writer = _Writer()

    def operator() -> AgentOperator:
        return _operator(
            writer,
            output_type=str,
            retries=2,
            toolsets=[writer.toolset()],
            capabilities=[IdlessLedger()],
            max_hitl_iterations=max_hitl_iterations,
        )

    first = park(lambda: operator().execute(ctx))

    with pytest.raises(error):
        operator().resume_hitl_review(ctx, **first.kwargs, event=event)

    assert [key for key in ctx["task_state_store"].data if key.startswith(DURABLE_KEY_PREFIX)] == []


@parks
def test_an_approval_returns_the_output_without_building_the_agent(park, ctx):
    writer = _Writer()
    first = park(
        lambda: _operator(writer, output_type=str, retries=2, toolsets=[writer.toolset()]).execute(ctx)
    )
    resumed = _operator(writer, output_type=str, retries=2, toolsets=[writer.toolset()])
    resumed.llm_hook.create_agent.side_effect = RuntimeError("model unavailable")

    assert resumed.resume_hitl_review(ctx, **first.kwargs, event=APPROVE) == "summary 2 after []"
    assert resumed.llm_hook.create_agent.call_count == 0


@patch.object(HITLReviewMixin, "_parks_between_rounds", autospec=True, return_value=False)
class TestDurablePolling:
    """
    Before Airflow 3.3 the review polls the human action XCom inside one try, with no task state store.

    The journal is kept in memory here, in place of its file under ``durable_cache_path``.
    """

    @pytest.fixture
    def storage(self):
        storage = MemoryStorage()
        with patch.object(AgentOperator, "_build_durable_storage", autospec=True, return_value=storage):
            yield storage

    @pytest.fixture
    def ctx(self, xcom_backed_ti) -> Any:
        return {"task_instance": xcom_backed_ti}

    @staticmethod
    def _polling_operator(writer: _Writer) -> AgentOperator:
        return _operator(
            writer, output_type=str, durable=True, toolsets=[writer.toolset()], hitl_poll_interval=0
        )

    @staticmethod
    def _answer(ctx, *actions: dict[str, Any]) -> None:
        """Make the reviewer give ``actions`` in order, each on the round it is for."""
        ti = ctx["task_instance"]

        def pull(key, task_ids=None, map_indexes=None):
            if key != XCOM_HUMAN_ACTION:
                return ti.xcoms.get(key)
            shown = sum(1 for k in ti.xcoms if k.startswith(XCOM_AGENT_OUTPUT_PREFIX))
            return actions[shown - 1]

        ti.xcom_pull.side_effect = pull

    def test_request_changes_then_approve_deletes_the_journal(self, _parks, storage, ctx):
        writer = _Writer()
        self._answer(
            ctx,
            {"action": "changes_requested", "feedback": "Shorter", "iteration": 1},
            {"action": "approve", "iteration": 2},
        )

        result = self._polling_operator(writer).execute(ctx)

        assert result == "summary 3 after ['Shorter']"
        assert storage.entries == {}

    def test_a_reject_makes_the_retry_ask_the_model_for_a_new_first_output(self, _parks, storage, ctx):
        writer = _Writer()
        self._answer(ctx, {"action": "reject", "iteration": 1})
        with pytest.raises(HITLRejectException):
            self._polling_operator(writer).execute(ctx)
        TestDurable._retry(ctx)
        self._answer(ctx, {"action": "approve", "iteration": 1})

        result = self._polling_operator(writer).execute(ctx)

        assert result == "summary 3 after []"
        assert writer.fetches == 1

    def test_an_answer_on_a_retry_runs_the_regeneration_again(self, _parks, storage, ctx):
        """No feedback is recorded here, so the reviewer may answer differently on the retry."""
        writer = _Writer(fail_on="Add the margin")
        changes = (
            {"action": "changes_requested", "feedback": "Shorter", "iteration": 1},
            {"action": "changes_requested", "feedback": "Add the margin", "iteration": 2},
        )
        self._answer(ctx, *changes)
        with pytest.raises(RuntimeError, match="model unavailable"):
            self._polling_operator(writer).execute(ctx)
        TestDurable._retry(ctx)
        self._answer(ctx, *changes, {"action": "approve", "iteration": 3})

        result = self._polling_operator(writer).execute(ctx)

        # The first regeneration ran on both tries; only the first output replayed.
        assert result == "summary 6 after ['Shorter', 'Add the margin']"
        assert writer.fetches == 1
