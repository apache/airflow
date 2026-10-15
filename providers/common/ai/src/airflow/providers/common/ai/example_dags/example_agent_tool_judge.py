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
"""
Example Dag: a decision model decides the agent's refunds, and a person decides the ones it is unsure of.

Prerequisites: Airflow 3.3 or later, ``pydantic-ai-slim`` 2.46.0 or later, a
``pydanticai_default`` connection for the agent's text model, and a ``decision_default``
connection whose model is a decision model, such as TypeSafe Jev: ``password=<API key>``,
``extra='{"model": "typesafe:jev-1.13.0"}'``, and the provider's ``typesafe`` extra.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from enum import Enum

from pydantic_ai import (
    DeferredToolRequests,
    DeferredToolResults,
    ModelRetry,
    RunContext,
    Tool,
    ToolDenied,
    UseEnumMemberDocstrings,
)
from pydantic_ai.capabilities import HandleDeferredToolCalls
from pydantic_ai.messages import ToolReturnPart, UserPromptPart
from pydantic_ai.toolsets.function import FunctionToolset

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.utils.decision import ModelConfidence
from airflow.providers.common.compat.sdk import dag

log = logging.getLogger(__name__)


# [START howto_agent_tool_judge]
ORDERS = {7: 42.00}


def lookup_order(order_id: int) -> str:
    """Look up an order."""
    return f"order {order_id}: ${ORDERS[order_id]:.2f}, carrier marked it lost"


def refund_order(order_id: int, amount: float, reason: str) -> str:
    """Refund an order. Irreversible: the money leaves the account."""
    return f"refunded ${amount:.2f} on order {order_id}"


def within_order_total(ctx: RunContext, order_id: int, amount: float, reason: str) -> None:
    """A rule, not a judgment: a refund it refuses never reaches the model or a person."""
    if order_id not in ORDERS:
        raise ModelRetry(f"There is no order {order_id}.")
    if amount > ORDERS[order_id]:
        raise ModelRetry(f"Order {order_id} cost ${ORDERS[order_id]:.2f}; refund at most that.")


shop = FunctionToolset(
    tools=[
        lookup_order,
        Tool(refund_order, requires_approval=True, args_validator=within_order_total),
    ]
)
TOOL_DESCRIPTIONS = {tool.__name__: tool.__doc__ for tool in (lookup_order, refund_order)}


class Verdict(UseEnumMemberDocstrings, str, Enum):
    """Whether a proposed tool call may run without a person checking it first."""

    allow = "allow"
    """The call does what the request asks, on the right record, at a size the request supports."""

    review = "review"
    """The call may be right, but a person should check it: an unusual amount, an irreversible step the request does not clearly ask for, or context that is missing."""

    deny = "deny"
    """The call does not match the request: the wrong record, an amount far beyond what was asked, or an action nobody asked for."""


# Below this, the answer is not acted on and the call goes to a person.
MIN_CONFIDENCE = 0.7


def customer_request(ctx: RunContext) -> str:
    """The request the agent is working on. A run resumed after a pause has no prompt, only history."""
    if isinstance(ctx.prompt, str):
        return ctx.prompt
    prompts = [
        part.content
        for message in ctx.messages
        for part in message.parts
        if isinstance(part, UserPromptPart) and isinstance(part.content, str)
    ]
    return prompts[-1] if prompts else ""


def refused_earlier(ctx: RunContext, tool_name: str) -> bool:
    """Whether a person or the model refused a call to this tool earlier in the run."""
    return any(
        isinstance(part, ToolReturnPart) and part.tool_name == tool_name and part.outcome == "denied"
        for message in ctx.messages
        for part in message.parts
    )


async def judge_tool_calls(ctx: RunContext, requests: DeferredToolRequests) -> DeferredToolResults:
    """Approve or deny the calls the decision model is sure about; leave the rest for a person."""
    judge = PydanticAIHook(llm_conn_id="decision_default").create_agent(
        output_type=Verdict,
        instructions=(
            "A support agent proposed the tool call below while handling the customer request. "
            "Decide whether it may run without a person checking it first."
        ),
    )
    results = DeferredToolResults()
    for call in requests.approvals:
        if refused_earlier(ctx, call.tool_name):
            # The model does not see why the earlier call was refused, so it must not overrule that.
            log.info(
                "%s (%s) was refused earlier in this run; asking a person", call.tool_name, call.tool_call_id
            )
            continue
        question = (
            f"Customer request:\n{customer_request(ctx)}\n\n"
            f"Proposed tool call: {call.tool_name}\n"
            f"What the tool does: {TOOL_DESCRIPTIONS.get(call.tool_name, 'unknown')}\n"
            f"Arguments: {json.dumps(call.args_as_dict())}"
        )
        try:
            answer = await judge.run(question)
        except Exception:
            # An unreachable judge is not a verdict: leave the call for a person.
            log.warning("Judge failed on %s; asking a person", call.tool_name, exc_info=True)
            continue
        confidence = ModelConfidence.from_result(answer).confidence.get("response")
        acted_on = confidence is not None and confidence >= MIN_CONFIDENCE
        # No arguments here: they can carry secrets, which the review page masks and a log line would not.
        log.info(
            "Judge on %s (%s): %s at confidence %s, bar %s; %s",
            call.tool_name,
            call.tool_call_id,
            answer.output.value,
            confidence,
            MIN_CONFIDENCE,
            "acting on it" if acted_on and answer.output is not Verdict.review else "asking a person",
        )
        if not acted_on:
            continue
        if answer.output is Verdict.allow:
            results.approvals[call.tool_call_id] = True
        elif answer.output is Verdict.deny:
            results.approvals[call.tool_call_id] = ToolDenied("An automated check refused this call.")
    return results


@dag(tags=["example"])
def example_agent_tool_judge():
    AgentOperator(
        task_id="handle_ticket",
        llm_conn_id="pydanticai_default",
        prompt="The customer says order 7 never arrived. Resolve it.",
        toolsets=[shop],
        capabilities=[HandleDeferredToolCalls(handler=judge_tool_calls)],
        tool_approval_timeout=timedelta(hours=4),
    )


# [END howto_agent_tool_judge]

example_agent_tool_judge()
