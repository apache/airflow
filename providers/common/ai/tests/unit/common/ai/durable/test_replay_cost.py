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
Empirical check of durable replay cost accounting against ``cost_limit``.

pydantic-ai's graph appends *every* model response's usage to the run's
``RunUsage`` in ``_agent_graph.py::_append_response`` -- it cannot distinguish a
response that came from a live model call from one ``CachingModel`` replayed
from the durable cache; compensating for a replay is entirely on the caller.
``CachingModel`` only does that compensation when it is given a ``replay_usage=``
ledger (see ``durable/replay_usage.py``) -- ``AgentOperator`` always passes one
with ``durable=True`` (around a real cross-attempt total on Airflow >= 3.3, or a
fresh per-attempt ``RunUsage()`` otherwise), so its users never see the
double-count described below. ``TestDurableReplayCostDuplication`` exercises the
lower-level ``CachingModel`` API directly, with **no** ledger supplied, to pin down
what happens without that compensation: each Airflow task attempt starts a
fresh ``RunUsage`` (a new ``agent.run`` call), so a step that was already paid
for in a prior, crashed attempt gets its cost added again to that fresh total.
``TestDurableReplayCostWithSharedRunUsage`` is the contrasting case -- the same
real ``CachingModel`` + ``DurableStorage`` + pydantic-ai ``Agent`` stack (no
mocked cost arithmetic), but with a ``run_usage`` shared across both simulated
attempts the way ``AgentOperator`` shares one, confirming the replayed cost is
*not* recounted there.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from pydantic_ai import Agent
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.usage import RequestUsage, RunUsage, UsageLimits

from airflow.providers.common.ai.durable.caching_model import CachingModel
from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger
from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.sdk import ObjectStoragePath

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


@pytest.fixture
def durable_storage(tmp_path):
    """A real, file-backed DurableStorage -- exercises the actual JSON round-trip."""
    with patch("airflow.providers.common.ai.durable.storage._get_base_path") as mock_base:
        mock_base.return_value = ObjectStoragePath(f"file://{tmp_path.as_posix()}")
        yield DurableStorage(dag_id="dag", task_id="task", run_id="run_1", map_index=-1)


async def _run_one_attempt(
    storage: DurableStorage,
    *,
    cost_limit: Decimal | None = None,
    counter: DurableStepCounter | None = None,
    run_usage: RunUsage | None = None,
    model: Any = None,
):
    """Simulate one Airflow task attempt with a fresh Agent and shared cache.

    DurableStepCounter is fresh unless one is supplied. ``run_usage``, when given, is
    both what the ``ReplayUsageLedger`` compensates and the ``agent.run(usage=...)``
    seed -- mirrors how ``AgentOperator`` shares one ``RunUsage`` object between the two
    across attempts. ``None`` (the default) skips compensation entirely, matching a
    bare ``CachingModel`` used outside ``AgentOperator``."""
    counter = counter or DurableStepCounter()
    limits = UsageLimits(cost_limit=cost_limit)
    ledger = ReplayUsageLedger(run_usage, limits) if run_usage is not None else None
    caching = CachingModel(
        model or FunctionModel(_build_priced_response), storage=storage, counter=counter, replay_usage=ledger
    )
    agent = Agent(model=caching)
    try:
        result = await agent.run("What is the answer?", usage=run_usage, usage_limits=limits)
    finally:
        if ledger is not None:
            ledger.settle()
    return result, counter


def _reopen_storage() -> DurableStorage:
    """Build a fresh ``DurableStorage`` for the same dag/task/run -- simulates a new Airflow
    task attempt (new process) reloading the durable cache from disk via the public
    constructor, rather than reaching into the private ``_cache`` attribute."""
    return DurableStorage(dag_id="dag", task_id="task", run_id="run_1", map_index=-1)


class TestDurableReplayCostDuplication:
    """``CachingModel`` used directly, with no replay ledger -- not how ``AgentOperator``
    uses it (see the module docstring); ``TestDurableReplayCostWithSharedRunUsage`` below
    is the contrasting, ``AgentOperator``-shaped case."""

    @pytest.mark.asyncio
    async def test_replayed_step_cost_is_recounted_on_retry_without_shared_run_usage(self, durable_storage):
        """A second attempt that only replays cached steps still reports the replayed cost
        as its own usage: pydantic-ai's graph can't tell a replay from a live call, and
        with no ledger supplied, ``CachingModel`` does not compensate (contrast with the
        shared-``run_usage`` test below)."""
        result1, counter1 = await _run_one_attempt(durable_storage)
        assert counter1.cached_model == 1
        assert counter1.replayed_model == 0
        assert result1.usage.cost == PRICED_COST

        # New attempt: fresh process, so the cache is reloaded from disk via a new
        # DurableStorage -- this is what actually happens on an Airflow task retry.
        result2, counter2 = await _run_one_attempt(_reopen_storage())

        # Zero new model calls this attempt ...
        assert counter2.cached_model == 0
        assert counter2.replayed_model == 1
        # ... yet the replayed step's cost is counted again, identically to attempt 1.
        assert result2.usage.cost == PRICED_COST

    @pytest.mark.asyncio
    async def test_retry_with_zero_new_spend_still_raises_usage_limit_exceeded(self, durable_storage):
        """Without a shared ``run_usage``, a retry that makes no new model calls can
        still raise UsageLimitExceeded, purely from replayed cost -- because
        check_cost() sees the run's cumulative usage, not "money spent in this
        attempt"."""
        # Attempt 1 stays comfortably under budget so it completes normally.
        await _run_one_attempt(durable_storage, cost_limit=PRICED_COST * 2)

        # Attempt 2 sets a limit below the already-paid-for replayed cost: zero new
        # spend, yet the replayed step alone pushes the cumulative usage over it.
        cost_limit = PRICED_COST / 2
        counter2 = DurableStepCounter()
        with pytest.raises(UsageLimitExceeded):
            await _run_one_attempt(_reopen_storage(), cost_limit=cost_limit, counter=counter2)

        # Zero new model calls this attempt -- the raise came purely from replayed cost.
        assert counter2.cached_model == 0
        assert counter2.replayed_model == 1


class TestDurableReplayCostWithSharedRunUsage:
    """The ``AgentOperator``-shaped case: a single ``RunUsage`` object is both what the
    ``ReplayUsageLedger`` compensates and the ``agent.run(usage=...)`` seed across
    simulated attempts, as ``AgentOperator`` does. Contrast with
    ``TestDurableReplayCostDuplication`` above, which uses neither."""

    @pytest.mark.asyncio
    async def test_replayed_step_cost_is_not_recounted_with_shared_run_usage(self, durable_storage):
        """The same scenario as the no-``run_usage`` test above, but with a ``RunUsage``
        shared across both simulated attempts: the replayed step's cost must not be
        added a second time to the cumulative total.

        The model leaves ``cost`` unset, as real providers do, so pydantic-ai prices
        each response itself after ``CachingModel`` has already written it to disk --
        the cached copy therefore comes back with ``cost=None``."""
        seed = RunUsage()
        model = FunctionModel(_build_unpriced_response)
        result1, counter1 = await _run_one_attempt(durable_storage, run_usage=seed, model=_Priced(model))
        assert counter1.cached_model == 1
        assert counter1.replayed_model == 0
        live_cost = seed.cost
        assert live_cost is not None
        assert live_cost > 0

        # New attempt: fresh process (new DurableStorage), but the same cross-attempt
        # RunUsage carried forward -- what TaskStateStoreUsageBudget.load()/save() do
        # via the task state store on Airflow >= 3.3.
        result2, counter2 = await _run_one_attempt(_reopen_storage(), run_usage=seed, model=_Priced(model))

        # Zero new model calls this attempt ...
        assert counter2.cached_model == 0
        assert counter2.replayed_model == 1
        # ... and the cumulative cost is still just the one real charge, not double.
        assert seed.cost == live_cost
        assert seed.requests == 1
        assert seed.input_tokens == 100_000


class TestDurableReplayOfContinuationChain:
    """A continuation chain (Anthropic ``pause_turn``, OpenAI background mode) is one
    request to pydantic-ai but one ``CachingModel`` step per segment."""

    @pytest.mark.parametrize(
        ("same_job", "segment_input_tokens"),
        [
            pytest.param(False, [10, 10, 10], id="accumulate-summed-segments"),
            pytest.param(True, [10, 20, 30], id="poll-same-job-cumulative-snapshots"),
        ],
    )
    @pytest.mark.asyncio
    async def test_replayed_chain_counts_nothing(self, durable_storage, same_job, segment_input_tokens):
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
        await _run_one_attempt(durable_storage, run_usage=seed, model=FunctionModel(build_segment))
        assert live_segments == [0, 1, 2]
        live_total = (seed.requests, seed.input_tokens, seed.output_tokens)
        assert live_total[0] == 1

        live_segments.clear()
        _, counter = await _run_one_attempt(
            _reopen_storage(), run_usage=seed, model=FunctionModel(build_segment)
        )

        assert live_segments == []
        assert counter.replayed_model == 3
        assert (seed.requests, seed.input_tokens, seed.output_tokens) == live_total


class TestDurableStorageCostRoundTrip:
    def test_decimal_cost_survives_json_round_trip(self, durable_storage):
        """DurableStorage serializes the whole cache blob as JSON; confirm a Decimal
        ``usage.cost`` is not silently lost or coerced to float/None by that round-trip."""
        response = ModelResponse(
            parts=[TextPart(content="hi")],
            usage=RequestUsage(input_tokens=1, output_tokens=1, cost=Decimal("0.0123456789")),
        )
        durable_storage.save_model_response("model_step_0", response, fingerprint="fp")

        loaded, _fingerprint = _reopen_storage().load_model_response("model_step_0")

        assert loaded is not None
        assert loaded.usage.cost == Decimal("0.0123456789")
        assert isinstance(loaded.usage.cost, Decimal)
