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
"""Keeps durable replays out of the usage a run is counted and limited against."""

from __future__ import annotations

import dataclasses
from copy import deepcopy
from typing import TYPE_CHECKING

from pydantic_ai.usage import RequestUsage, UsageLimits

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelResponse
    from pydantic_ai.usage import RunUsage

_REQUEST_USAGE_FIELDS = dataclasses.fields(RequestUsage)


def subtract_request_usage(run_usage: RunUsage, usage: RequestUsage) -> None:
    """Subtract one response's token usage and cost from a run's usage, in place."""
    for field in _REQUEST_USAGE_FIELDS:
        if field.name == "details":
            for name, value in usage.details.items():
                run_usage.details[name] = run_usage.details.get(name, 0) - value
        elif field.name == "cost":
            if usage.cost is not None:
                run_usage.cost = (run_usage.cost or 0) - usage.cost
        else:
            setattr(run_usage, field.name, getattr(run_usage, field.name) - getattr(usage, field.name))


def fill_replayed_cost(response: ModelResponse) -> None:
    """
    Price a replayed response in place, the way pydantic-ai's graph is about to.

    ``CachingModel`` stores a live response before the graph prices it, so a cached
    response comes back with ``usage.cost=None`` even for a priced model. The graph fills
    the cost right after ``CachingModel.request`` returns and only when it is still unset,
    so pricing it here first means the cost this module subtracts is exactly the cost the
    graph then adds. Uses the public ``ModelResponse.cost()``, which runs the same price
    lookup as the graph's own best-effort pricing; a response it cannot price stays
    unpriced, as it would in the graph.
    """
    if response.usage.cost is not None or not response.model_name:
        return
    try:
        response.usage.cost = response.cost().total_price
    except (LookupError, ValueError):
        # genai-prices' expected "can't price this" failures; the graph degrades them the same way.
        return
    except Exception:
        # The graph re-runs the same lookup and emits CostCalculationFailedWarning itself, so
        # warning here would report the same failure twice.
        return


def _copy_request_usage(usage: RequestUsage) -> RequestUsage:
    return dataclasses.replace(usage, details=dict(usage.details))


class ReplayUsageLedger:
    """
    Nets durable replays out of the ``RunUsage`` a run is counted and limited against.

    pydantic-ai counts a replayed step exactly like a live one: after
    ``CachingModel.request`` returns, the graph does ``requests += 1`` and adds the
    response's usage, and after ``CachingToolset.call_tool`` returns, the tool manager
    does ``tool_calls += 1``. The ledger cancels each replay when it happens, so the run's
    counts only move for live work. Nothing here is persisted: the result is the same
    whether the cache entry was written by the previous attempt, an attempt that failed
    halfway through its own replay, or an attempt before a clear (which resets the budget
    but keeps the durable cache).

    Two of pydantic-ai's limit checks run before the durable layer sees the step it
    guards: ``check_before_request`` before each model request, and the up-front
    ``check_before_tool_call`` projection over a whole step's function-tool calls. For
    those, the ledger takes a credit ahead of time -- when a replayed model response is
    followed in the cache by the next model step or by cached tool results -- and resolves
    each credit when the step runs: a replay keeps it, a live call gives it back and
    re-runs the check pydantic-ai made against the credited count. Credits that are never
    resolved (the run took a different path, or stopped) are given back by
    :meth:`settle`, so they never reach the persisted total; until then, a credit for a
    stale cache entry the run never reaches has only loosened that one up-front check.

    :param run_usage: The ``RunUsage`` passed to the run as ``usage=``.
    :param usage_limits: The limits passed to the run; ``None`` means pydantic-ai's
        defaults, as in ``Agent.run``.
    """

    def __init__(self, *, run_usage: RunUsage, usage_limits: UsageLimits | None) -> None:
        self.run_usage = run_usage
        self._limits = usage_limits or UsageLimits()
        self._tool_credits: set[int] = set()
        self._request_credit = False
        # pydantic-ai's up-front projection for the current batch of credited tool
        # steps, plus one for each credited call in that batch that has since turned
        # out to be live; see record_live_tool_call.
        self._tool_batch_projection = 0
        # Usage subtracted for the previous segment of a continuation chain, when that
        # segment was replayed; see record_model_replay.
        self._chain_segment_usage: RequestUsage | None = None
        self._chain_response_id: str | None = None

    def credit_request(self) -> None:
        """Credit the next model request, which the cache says will be a replay."""
        if not self._request_credit:
            self._request_credit = True
            self.run_usage.requests -= 1

    def credit_tool_steps(self, steps: list[int], *, batch_calls: int) -> None:
        """
        Credit the tool calls at these step indices, which have cached results.

        :param batch_calls: The number of function-tool calls this replayed response
            issued, i.e. the count pydantic-ai's up-front ``check_before_tool_call``
            projects forward before any of them run.
        """
        for step in steps:
            if step not in self._tool_credits:
                self._tool_credits.add(step)
                self.run_usage.tool_calls -= 1
        self._tool_batch_projection = self.run_usage.tool_calls + batch_calls

    def settle(self) -> bool:
        """
        Give back every unresolved credit.

        :return: Whether a request credit was outstanding.
        """
        self.run_usage.tool_calls += len(self._tool_credits)
        self._tool_credits.clear()
        self._tool_batch_projection = 0
        had_request_credit = self._request_credit
        if had_request_credit:
            self.run_usage.requests += 1
            self._request_credit = False
        return had_request_credit

    def record_model_replay(self, response: ModelResponse, *, continuation: bool) -> None:
        """
        Cancel what the graph is about to add for a replayed model response.

        A continuation segment (Anthropic ``pause_turn``, OpenAI background mode) is not
        a request of its own: the graph counts one request per chain and commits the
        merged response's usage once. Merging sums the segments' usage, except when a
        segment re-polls the same provider response id, where the merged usage is that
        segment's cumulative snapshot -- so the previous segment's subtraction is given
        back before this one is taken.

        Between this subtraction and the graph adding the response's usage back, the
        graph awaits ``after_model_request``; a non-``ModelRetry`` exception raised there,
        or an ``AirflowTaskTimeout`` landing in that window, leaves this response's tokens
        and cost under-counted.
        """
        fill_replayed_cost(response)
        if (
            continuation
            and self._chain_segment_usage is not None
            and self._chain_response_id
            and self._chain_response_id == response.provider_response_id
        ):
            self.run_usage.incr(self._chain_segment_usage)
        subtract_request_usage(self.run_usage, response.usage)
        if not continuation:
            self.run_usage.requests -= 1
        self._chain_segment_usage = _copy_request_usage(response.usage)
        self._chain_response_id = response.provider_response_id

    def record_live_model_request(self, *, had_request_credit: bool) -> None:
        """
        Re-run the pre-request check for a credited request that turned out to be live.

        pydantic-ai checked this request against the credited count, which assumed it
        would replay for free.
        """
        self._chain_segment_usage = None
        self._chain_response_id = None
        if had_request_credit:
            self._limits.check_before_request(self.run_usage)

    def record_tool_replay(self, step: int) -> None:
        """Cancel the ``tool_calls += 1`` the tool manager does after a replayed call returns."""
        if step in self._tool_credits:
            self._tool_credits.discard(step)
        else:
            self.run_usage.tool_calls -= 1

    def record_live_tool_call(self, step: int) -> None:
        """
        Give back a credit whose cached result did not replay, and re-check the limit.

        pydantic-ai's up-front check projected the whole batch this credited call
        belongs to as if every credited call in it would replay for free. With this one
        turning out to be live, that assumption is wrong for one more call: re-run the
        check against pydantic-ai's original projection plus one for each credited call
        in the batch that has turned out to be live so far (including this one). A
        credit that is still unresolved keeps counting as a replay -- if it does replay,
        the original projection was already right for it; if it turns out live later,
        its own call to this method re-checks again. So the last credited call in a
        batch to turn live is checked against the sum of every live call in the batch,
        regardless of the order concurrent calls finish in, and the batch as a whole
        never exceeds the limit.

        ``batch_calls`` (passed to :meth:`credit_tool_steps`) is derived from the
        response's tool-call parts matched against ``model_request_parameters``'s
        ``function_tools``, which also lists tools that need approval or are external
        -- so in a non-resume batch, where those calls are deferred rather than
        executed, ``batch_calls`` can count more than pydantic-ai's own projection
        does. That over-count flows into ``_tool_batch_projection`` and this method's
        recheck, so it can only make the recheck stricter -- a possible false block,
        never a missed one. pydantic-ai's projection, in turn, also covers
        ``'unknown'``-kind calls that ``batch_calls`` does not; that under-count never
        reaches this method or :meth:`record_tool_replay`, because nothing calls
        ``record_tool_call`` for calls of ``'unknown'`` kind, so it can't push the
        final count over the limit.
        """
        if step not in self._tool_credits:
            return
        self._tool_credits.discard(step)
        self.run_usage.tool_calls += 1
        self._tool_batch_projection += 1
        if self._limits.tool_calls_limit is not None:
            projected = deepcopy(self.run_usage)
            projected.tool_calls = self._tool_batch_projection
            self._limits.check_before_tool_call(projected)
