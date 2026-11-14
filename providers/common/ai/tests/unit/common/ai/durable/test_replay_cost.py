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
Durable replay cost accounting against ``cost_limit``, through the real stack.

pydantic-ai's graph adds *every* model response's usage to the run's ``RunUsage``;
it cannot tell a response the journal replayed from a live one. ``AirflowDurability``
cancels each replay through a ``ReplayUsageLedger`` over the run's usage, which is the
cross-attempt total when ``AgentOperator`` passes one as ``usage=``. These tests drive a real file-backed ``DurableStorage``, the journal and a
pydantic-ai ``Agent`` -- no mocked cost arithmetic -- across simulated attempts.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.usage import RequestUsage, RunUsage, UsageLimits

from airflow.providers.common.ai.durable import AirflowDurability
from airflow.providers.common.ai.durable.journal import DurableJournal, journal_scope
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.providers.common.compat.sdk import ObjectStoragePath

PRICED_COST = Decimal("0.10")


def _build_priced_response(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    return ModelResponse(
        parts=[TextPart(content="the answer")],
        usage=RequestUsage(input_tokens=100, output_tokens=50, cost=PRICED_COST),
    )


def _build_unpriced_response(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    return ModelResponse(
        parts=[TextPart(content="the answer")],
        usage=RequestUsage(input_tokens=100_000, output_tokens=10_000),
    )


class _Priced(WrapperModel):
    """Stamps a model genai-prices knows onto each response, leaving ``cost`` unset."""

    async def request(self, *args: Any, **kwargs: Any) -> ModelResponse:
        response = await self.wrapped.request(*args, **kwargs)
        return dataclasses.replace(
            response, model_name="gpt-4o", provider_name="openai", provider_url="https://api.openai.com/v1"
        )


@pytest.fixture(autouse=True)
def journal_dir(tmp_path):
    """Every ``DurableStorage`` built in a test reads and writes the same file-backed journal."""
    with patch("airflow.providers.common.ai.durable.storage._get_base_path", autospec=True) as base:
        base.return_value = ObjectStoragePath(f"file://{tmp_path.as_posix()}")
        yield tmp_path


def _storage() -> DurableStorage:
    """A fresh ``DurableStorage`` for the same task instance -- what a new attempt (process) builds."""
    return DurableStorage(dag_id="dag", task_id="task", run_id="run_1", map_index=-1)


async def _run_one_attempt(
    *, model: Any = None, cost_limit: Decimal | None = None, run_usage: RunUsage | None = None
) -> tuple[Any, DurableJournal]:
    """
    Simulate one Airflow task attempt with a fresh agent and journal over the shared file.

    ``run_usage``, when given, is shared across attempts the way ``AgentOperator`` shares its
    cross-attempt usage; otherwise each attempt starts a fresh ``RunUsage``. Either way the
    capability keeps replays out of it.
    """
    limits = UsageLimits(cost_limit=cost_limit)
    journal = DurableJournal(_storage())
    agent = Agent(
        model or FunctionModel(_build_priced_response), name="a", capabilities=[AirflowDurability()]
    )
    with journal_scope(journal):
        return await agent.run("What is the answer?", usage=run_usage, usage_limits=limits), journal


class TestReplayedCostIsNotCountedAgain:
    @pytest.mark.asyncio
    async def test_an_attempt_that_only_replays_reports_no_usage(self):
        """Each attempt starts a fresh ``RunUsage``; the one that only replays counts nothing."""
        result1, journal1 = await _run_one_attempt()
        assert journal1.stats.recorded["model"] == 1
        assert result1.usage.cost == PRICED_COST

        result2, journal2 = await _run_one_attempt()

        assert journal2.stats.replayed["model"] == 1
        assert journal2.stats.recorded["model"] == 0
        assert (result2.usage.requests, result2.usage.input_tokens, result2.usage.cost or 0) == (0, 0, 0)

    @pytest.mark.asyncio
    async def test_a_retry_with_no_new_spend_stays_under_its_cost_limit(self):
        await _run_one_attempt(cost_limit=PRICED_COST * 2)

        # Below the cost the first attempt already paid, which the retry only replays.
        result, journal = await _run_one_attempt(cost_limit=PRICED_COST / 2)

        assert result.output == "the answer"
        assert journal.stats.replayed["model"] == 1

    @pytest.mark.asyncio
    async def test_replayed_cost_is_not_recounted_in_shared_run_usage(self):
        """
        The ``AgentOperator`` shape: one ``RunUsage`` carried across attempts.

        The model leaves ``cost`` unset, as real providers do, so pydantic-ai prices each
        response after the journal recorded it -- the recorded copy comes back with
        ``cost=None``, and the ledger prices it the same way the graph does.
        """
        seed = RunUsage()
        model = _Priced(FunctionModel(_build_unpriced_response))
        await _run_one_attempt(run_usage=seed, model=model)
        live_cost = seed.cost
        assert live_cost is not None
        assert live_cost > 0

        _, journal = await _run_one_attempt(run_usage=seed, model=model)

        assert journal.stats.replayed["model"] == 1
        assert seed.cost == live_cost
        assert seed.requests == 1
        assert seed.input_tokens == 100_000


class TestReplayOfContinuationChain:
    """A continuation chain (Anthropic ``pause_turn``, OpenAI background mode) is one
    request to pydantic-ai but one journal step per segment."""

    @pytest.mark.parametrize(
        ("same_job", "segment_input_tokens"),
        [
            pytest.param(False, [10, 10, 10], id="accumulate-summed-segments"),
            pytest.param(True, [10, 20, 30], id="poll-same-job-cumulative-snapshots"),
        ],
    )
    @pytest.mark.asyncio
    async def test_replayed_chain_counts_nothing(self, same_job, segment_input_tokens):
        """Two suspended segments and a final one. Replaying the chain must add neither a
        request per segment nor the segments' tokens: segments of a new response are
        summed, while re-polls of the same provider response id replace each other."""
        live_segments: list[int] = []

        def build_segment(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            segment = len(live_segments)
            live_segments.append(segment)
            return ModelResponse(
                parts=[TextPart(content=f"part {segment}")],
                usage=RequestUsage(input_tokens=segment_input_tokens[segment], output_tokens=1),
                state="suspended" if segment < 2 else "complete",
                provider_response_id="job" if same_job else f"response-{segment}",
            )

        seed = RunUsage()
        await _run_one_attempt(run_usage=seed, model=FunctionModel(build_segment))
        assert live_segments == [0, 1, 2]
        live_total = (seed.requests, seed.input_tokens, seed.output_tokens)
        assert live_total[0] == 1

        live_segments.clear()
        _, journal = await _run_one_attempt(run_usage=seed, model=FunctionModel(build_segment))

        assert live_segments == []
        assert journal.stats.replayed["model"] == 3
        assert (seed.requests, seed.input_tokens, seed.output_tokens) == live_total


class TestCostRoundTrip:
    @pytest.mark.asyncio
    async def test_decimal_cost_survives_the_journal(self):
        """The journal file is JSON; a Decimal ``usage.cost`` must come back a Decimal, not a float or None."""
        cost = Decimal("0.0123456789")

        def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
            return ModelResponse(parts=[TextPart("hi")], usage=RequestUsage(input_tokens=1, cost=cost))

        await _run_one_attempt(model=FunctionModel(respond))
        result, _ = await _run_one_attempt(model=FunctionModel(respond))

        replayed = result.all_messages()[-1]
        assert replayed.usage.cost == cost
        assert isinstance(replayed.usage.cost, Decimal)
