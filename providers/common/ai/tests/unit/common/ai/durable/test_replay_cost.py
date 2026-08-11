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
Empirical check of whether durable replay double-counts cost against ``cost_limit``.

pydantic-ai's graph appends *every* model response's usage to the run's
``RunUsage`` in ``_agent_graph.py::_append_response`` -- it cannot distinguish a
response that came from a live model call from one ``CachingModel`` replayed
from the durable cache. Each Airflow task attempt starts a fresh ``RunUsage``
(a new ``agent.run`` call), so a step that was already paid for in a prior,
crashed attempt gets its cost added again to the retry's own usage total --
even though the retry made zero new model calls for that step. These tests
exercise the real ``CachingModel`` + ``DurableStorage`` + pydantic-ai ``Agent``
stack (no mocked cost arithmetic) to confirm this, rather than relying on
reading ``_agent_graph.py`` / ``_cost.py`` and assuming.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest
from pydantic_ai import Agent
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.usage import RequestUsage, UsageLimits

from airflow.providers.common.ai.durable.caching_model import CachingModel
from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
from airflow.providers.common.ai.durable.storage import DurableStorage
from airflow.sdk import ObjectStoragePath

PRICED_COST = Decimal("0.10")


def _priced_model_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    return ModelResponse(
        parts=[TextPart(content="the answer")],
        usage=RequestUsage(input_tokens=100, output_tokens=50, cost=PRICED_COST),
    )


@pytest.fixture
def durable_storage(tmp_path):
    """A real, file-backed DurableStorage -- exercises the actual JSON round-trip."""
    with patch("airflow.providers.common.ai.durable.storage._get_base_path") as mock_base:
        mock_base.return_value = ObjectStoragePath(f"file://{tmp_path.as_posix()}")
        yield DurableStorage(dag_id="dag", task_id="task", run_id="run_1", map_index=-1)


async def _run_one_attempt(storage: DurableStorage, *, cost_limit: Decimal | None = None):
    """Simulate one Airflow task attempt: fresh Agent + fresh DurableStepCounter, shared cache."""
    counter = DurableStepCounter()
    caching = CachingModel(FunctionModel(_priced_model_fn), storage=storage, counter=counter)
    agent = Agent(model=caching)
    result = await agent.run("What is the answer?", usage_limits=UsageLimits(cost_limit=cost_limit))
    return result, counter


def _reopen_storage() -> DurableStorage:
    """Build a fresh ``DurableStorage`` for the same dag/task/run -- simulates a new Airflow
    task attempt (new process) reloading the durable cache from disk via the public
    constructor, rather than reaching into the private ``_cache`` attribute."""
    return DurableStorage(dag_id="dag", task_id="task", run_id="run_1", map_index=-1)


class TestDurableReplayCostDuplication:
    @pytest.mark.asyncio
    async def test_replayed_step_cost_is_recounted_on_retry(self, durable_storage):
        """A second attempt that only replays cached steps still reports the replayed cost
        as its own usage -- pydantic-ai cannot tell a replay from a live call."""
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
        """A retry that makes no new model calls can still raise UsageLimitExceeded,
        purely from replayed cost -- because check_cost() sees the run's cumulative
        usage, not "money spent in this attempt"."""
        # Attempt 1 stays comfortably under budget so it completes normally.
        await _run_one_attempt(durable_storage, cost_limit=PRICED_COST * 2)

        # Attempt 2 sets a limit below the already-paid-for replayed cost: zero new
        # spend, yet the replayed step alone pushes the cumulative usage over it.
        cost_limit = PRICED_COST / 2
        with pytest.raises(UsageLimitExceeded):
            await _run_one_attempt(_reopen_storage(), cost_limit=cost_limit)


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
