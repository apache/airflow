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
"""HITL review on Airflow 3.3+: AgentOperator parks between review rounds and resumes in a new process."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, UserPromptPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.utils.hitl_review import XCOM_AGENT_OUTPUT_PREFIX
from airflow.providers.common.ai.utils.usage_budget import USAGE_BUDGET_KEY, TaskStateStoreUsageBudget

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="Parking a review needs Airflow 3.3+.")

REQUEST_CHANGES = {"chosen_options": ["Request changes"], "params_input": {"feedback": "Shorter please"}}
APPROVE = {"chosen_options": ["Approve"], "params_input": {}}


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


def test_review_resumes_in_a_new_operator_and_returns_the_approved_model(park, ctx):
    first = park(lambda: _operator().execute(ctx))

    # Each resume is a new process, so a new operator instance with nothing carried in memory.
    second = park(lambda: _operator().resume_hitl_review(ctx, **first.kwargs, event=REQUEST_CHANGES))
    result = _operator().resume_hitl_review(ctx, **second.kwargs, event=APPROVE)

    assert ctx["task_instance"].xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}1"] == '{"text":"A long summary","words":3}'
    assert ctx["task_instance"].xcoms[f"{XCOM_AGENT_OUTPUT_PREFIX}2"] == '{"text":"Short","words":1}'
    assert result == Summary(text="Short", words=1)
    assert ctx["task_state_store"].get("common_ai_hitl_review_state") is None


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


def test_regeneration_continues_the_conversation_from_before_the_park(park, ctx):
    first = park(lambda: _operator().execute(ctx))
    seen: list[list[str]] = []

    def summarizer(messages, info: AgentInfo) -> ModelResponse:
        seen.append(_prompts(messages))
        return _summarizer(messages, info)

    park(lambda: _operator(summarizer).resume_hitl_review(ctx, **first.kwargs, event=REQUEST_CHANGES))

    assert seen == [["Summarize the report", "Shorter please"]]


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
