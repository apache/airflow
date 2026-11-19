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
"""``example_agent_tool_judge``: a decision model judging the calls that need approval in AgentOperator."""

from __future__ import annotations

import functools
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# The example's Verdict uses UseEnumMemberDocstrings, new in pydantic-ai-slim 2.46.0.
pytest.importorskip("pydantic_ai", minversion="2.46.0")

from pydantic_ai import Agent
from pydantic_ai.messages import (
    ModelResponse,
    RetryPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel

from airflow.providers.common.ai.example_dags import example_agent_tool_judge as example
from airflow.providers.common.ai.operators.agent import AgentOperator

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS
from unit.common.ai.operators.test_agent_tool_approval import (
    APPROVE,
    UPSERT,
    _context,
    _FakeTaskStateStore,
    _returns,
)

pytestmark = pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="Per-tool approval needs Airflow 3.3+.")

if AIRFLOW_V_3_3_PLUS:
    from airflow.sdk.exceptions import TaskAwaitingInput

JUDGE_HOOK = "airflow.providers.common.ai.example_dags.example_agent_tool_judge.PydanticAIHook"
REQUEST = "The customer says order 7 never arrived. Resolve it."


def _refunds_of(*amounts: float):
    """An agent that proposes one refund of order 7 per step, in ``amounts`` order, then reports the results."""

    def model(messages, info: AgentInfo) -> ModelResponse:
        answered = [p for m in messages for p in m.parts if isinstance(p, (ToolReturnPart, RetryPromptPart))]
        if len(answered) < len(amounts):
            step = len(answered)
            args = {"order_id": 7, "amount": amounts[step], "reason": "lost parcel"}
            return ModelResponse(parts=[ToolCallPart("refund_order", args, tool_call_id=f"c-{step}")])
        return ModelResponse(parts=[TextPart(" | ".join(_returns(messages)))])

    return model


def _judge(answers: list[tuple[str, float | None]], questions: list[str]):
    """A decision model giving ``answers`` in order, as (verdict, confidence), reported the way Jev does."""
    pending = list(answers)

    def model(messages, info: AgentInfo) -> ModelResponse:
        questions.extend(str(p.content) for m in messages for p in m.parts if isinstance(p, UserPromptPart))
        verdict, confidence = pending.pop(0)
        return ModelResponse(
            parts=[ToolCallPart(info.output_tools[0].name, {"response": verdict})],
            provider_details=None if confidence is None else {"confidence": {"response": confidence}},
        )

    return model


def _unreachable(messages, info: AgentInfo) -> ModelResponse:
    raise ConnectionError("decision model unreachable")


def _operator(agent_model) -> AgentOperator:
    """The example Dag's task, with the agent's text model replaced."""
    op = example.example_agent_tool_judge().get_task("handle_ticket")
    assert isinstance(op, AgentOperator)
    hook = MagicMock(spec=["create_agent"])
    hook.create_agent.side_effect = lambda **kw: Agent(FunctionModel(agent_model), **kw)
    op.llm_hook = hook
    return op


@pytest.fixture
def judge():
    """Stand in for the ``decision_default`` connection; set ``judge.model`` to a FunctionModel callable."""
    with patch(JUDGE_HOOK, autospec=True) as hook_cls:
        state = MagicMock(spec=["model"])
        hook_cls.return_value.create_agent.side_effect = lambda **kw: Agent(FunctionModel(state.model), **kw)
        yield state


@pytest.fixture
def refunds():
    """The arguments of every refund whose body ran."""
    calls: list[dict[str, Any]] = []
    schema = example.shop.tools["refund_order"].function_schema

    @functools.wraps(example.refund_order)
    def recording_refund(**kwargs):
        calls.append(kwargs)
        return example.refund_order(**kwargs)

    with patch.object(schema, "function", recording_refund):
        yield calls


def _execute(op: AgentOperator, store: _FakeTaskStateStore | None = None):
    with patch(UPSERT, autospec=True) as upsert:
        output = op.execute(_context(store or _FakeTaskStateStore()))
    upsert.assert_not_called()
    return output


def _pause(op: AgentOperator, store: _FakeTaskStateStore) -> TaskAwaitingInput:
    with patch(UPSERT, autospec=True) as upsert, pytest.raises(TaskAwaitingInput) as paused:
        op.execute(_context(store))
    assert "**refund_order**" in upsert.call_args.kwargs["body"]
    return paused.value


def test_a_confident_allow_runs_the_refund_without_asking_a_person(judge, refunds):
    questions: list[str] = []
    judge.model = _judge([("allow", 0.95)], questions)

    output = _execute(_operator(_refunds_of(42.0)))

    assert refunds == [{"order_id": 7, "amount": 42.0, "reason": "lost parcel"}]
    assert output == "refunded $42.00 on order 7"
    assert REQUEST in questions[0]
    assert '"amount": 42.0' in questions[0]
    assert "Irreversible" in questions[0]


def test_a_confident_deny_never_runs_the_refund(judge, refunds):
    judge.model = _judge([("deny", 0.95)], [])

    output = _execute(_operator(_refunds_of(42.0)))

    assert refunds == []
    assert output == "An automated check refused this call."


@pytest.mark.parametrize(
    "answer",
    [
        pytest.param(("allow", 0.5), id="unsure-allow"),
        pytest.param(("deny", 0.5), id="unsure-deny"),
        pytest.param(("review", 0.99), id="review"),
        pytest.param(("allow", None), id="no-confidence-reported"),
        pytest.param(None, id="judge-unreachable"),
    ],
)
def test_an_unsure_missing_or_failed_answer_asks_a_person(judge, refunds, answer):
    judge.model = _unreachable if answer is None else _judge([answer], [])

    paused = _pause(_operator(_refunds_of(42.0)), _FakeTaskStateStore())

    assert refunds == []
    assert paused.kwargs["tool_call_ids"] == ["c-0"]


def test_the_judge_is_not_asked_again_about_a_call_a_person_approved(judge, refunds):
    questions: list[str] = []
    judge.model = _judge([("review", 0.99)], questions)
    store = _FakeTaskStateStore()
    paused = _pause(_operator(_refunds_of(42.0)), store)

    output = _operator(_refunds_of(42.0)).resume_after_tool_approval(
        _context(store), **paused.kwargs, event=APPROVE
    )

    assert len(questions) == 1
    assert refunds == [{"order_id": 7, "amount": 42.0, "reason": "lost parcel"}]
    assert output == "refunded $42.00 on order 7"


def test_a_resumed_run_still_shows_the_judge_the_customer_request(judge, refunds):
    questions: list[str] = []
    judge.model = _judge([("review", 0.99), ("allow", 0.95)], questions)
    store = _FakeTaskStateStore()
    paused = _pause(_operator(_refunds_of(42.0, 5.0)), store)

    _operator(_refunds_of(42.0, 5.0)).resume_after_tool_approval(
        _context(store), **paused.kwargs, event=APPROVE
    )

    assert [r["amount"] for r in refunds] == [42.0, 5.0]
    assert REQUEST in questions[1]
    assert '"amount": 5.0' in questions[1]


def test_a_refund_above_the_order_total_reaches_neither_the_judge_nor_a_person(judge, refunds):
    questions: list[str] = []
    judge.model = _judge([("allow", 0.95)], questions)

    output = _execute(_operator(_refunds_of(4200.0, 42.0)))

    assert refunds == [{"order_id": 7, "amount": 42.0, "reason": "lost parcel"}]
    assert len(questions) == 1
    assert '"amount": 42.0' in questions[0]
    assert output == "refunded $42.00 on order 7"


def test_a_tool_refused_earlier_in_the_run_goes_to_a_person(judge, refunds):
    questions: list[str] = []
    judge.model = _judge([("deny", 0.95), ("allow", 0.99)], questions)

    paused = _pause(_operator(_refunds_of(42.0, 30.0)), _FakeTaskStateStore())

    assert refunds == []
    assert len(questions) == 1
    assert paused.kwargs["tool_call_ids"] == ["c-1"]
